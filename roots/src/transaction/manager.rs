use std::{
    ops::Deref,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use tokio::sync::Mutex;

use super::{LogEntry, State, TransactionLog};
use crate::{
    async_file::{AsyncFile, AsyncFileManager},
    Error, Vault,
};

#[derive(Clone)]
pub struct TransactionManager {
    state: State,
    transaction_sender: flume::Sender<(TransactionHandle<'static>, flume::Sender<()>)>,
}

impl TransactionManager {
    pub async fn spawn<F: AsyncFile>(
        directory: &Path,
        file_manager: F::Manager,
        vault: Option<Arc<dyn Vault>>,
    ) -> Result<Self, Error> {
        let (transaction_sender, receiver) = flume::bounded(32);
        let log_path = Self::log_path(directory);

        let (state_sender, state_receiver) = flume::bounded(1);
        std::thread::Builder::new()
            .name(String::from("bonsaidb-txlog"))
            .spawn(move || {
                transaction_writer_thread::<F>(
                    state_sender,
                    log_path,
                    receiver,
                    file_manager,
                    vault,
                );
            })
            .map_err(Error::message)?;

        let state = state_receiver
            .recv_async()
            .await
            .expect("failed to initialize")?;
        Ok(Self {
            state,
            transaction_sender,
        })
    }

    pub async fn push(&self, transaction: TransactionHandle<'static>) {
        let (completion_sender, completion_receiver) = flume::bounded(1);
        self.transaction_sender
            .send((transaction, completion_sender))
            .unwrap();
        completion_receiver.recv_async().await.unwrap();
    }

    fn log_path(directory: &Path) -> PathBuf {
        directory.join("transactions")
    }

    pub fn state(&self) -> &State {
        &**self
    }
}

impl Deref for TransactionManager {
    type Target = State;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

// TODO: when an error happens, we should try to recover.
#[allow(clippy::needless_pass_by_value)]
fn transaction_writer_thread<F: AsyncFile>(
    state_sender: flume::Sender<Result<State, Error>>,
    log_path: PathBuf,
    transactions: flume::Receiver<(TransactionHandle<'static>, flume::Sender<()>)>,
    file_manager: F::Manager,
    vault: Option<Arc<dyn Vault>>,
) {
    const BATCH: usize = 16;

    F::Manager::run(async {
        let state = State::default();
        let result = {
            match TransactionLog::<F>::initialize_state(&state, &log_path, vault.as_deref()).await {
                Err(Error::DataIntegrity(err)) => Err(Error::DataIntegrity(err)),
                _ => Ok(()),
            }
        };
        if let Err(err) = result {
            drop(state_sender.send(Err(err)));
            return;
        }

        drop(state_sender.send(Ok(state.clone())));

        let mut log = TransactionLog::<F>::open(&log_path, state, vault, &file_manager)
            .await
            .unwrap();

        while let Ok(transaction) = transactions.recv_async().await {
            let mut transaction_batch = Vec::with_capacity(BATCH);
            transaction_batch.push(transaction.0);
            let mut completion_senders = Vec::with_capacity(BATCH);
            completion_senders.push(transaction.1);
            for _ in 0..BATCH - 1 {
                match transactions.try_recv() {
                    Ok((transaction, sender)) => {
                        transaction_batch.push(transaction);
                        completion_senders.push(sender);
                    }
                    // At this point either type of error we want to finish writing the transactions we have.
                    Err(_) => break,
                }
            }
            log.push(transaction_batch).await.unwrap();
            for completion_sender in completion_senders {
                let _ = completion_sender.send(());
            }
        }
    });
}

pub struct TransactionHandle<'a> {
    pub transaction: LogEntry<'a>,
    pub locked_trees: TreeLocks,
}

pub type TreeLocks = Vec<TreeLockHandle>;

#[derive(Debug)]
pub struct TreeLock {
    data: Arc<TreeLockData>,
}

impl TreeLock {
    pub fn new() -> Self {
        Self {
            data: Arc::new(TreeLockData {
                locked: AtomicBool::new(false),
                rt: tokio::runtime::Handle::current(),
                blocked: Mutex::default(),
            }),
        }
    }
    pub async fn lock(&self) -> TreeLockHandle {
        // Loop until we acquire a lock
        loop {
            // Try to acquire the lock without any possibility of blocking
            if self
                .data
                .locked
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }

            let unblocked_receiver = {
                let mut blocked = self.data.blocked.lock().await;
                // Now that we've acquired this lock, it's possible the lock has
                // been released. If there are no others waiting, we can re-lock
                // it. If there werealready others waiting, we want to allow
                // them to have a chance to wake up first, so we assume that the
                // lock is locked without checking.
                if blocked.is_empty()
                    && self
                        .data
                        .locked
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                {
                    break;
                }

                // Add a new sender to the blocked list, and return it so that
                // we can wait for it to be signalled.
                let (unblocked_sender, unblocked_receiver) = flume::bounded(1);
                blocked.push(unblocked_sender);
                unblocked_receiver
            };
            // Wait for our unblocked signal to be triggered before trying to acquire the lock again.
            let _ = unblocked_receiver.recv_async().await;
        }

        TreeLockHandle(Self {
            data: self.data.clone(),
        })
    }
}

#[derive(Debug)]
struct TreeLockData {
    locked: AtomicBool,
    rt: tokio::runtime::Handle,
    blocked: Mutex<Vec<flume::Sender<()>>>,
}

#[derive(Debug)]
pub struct TreeLockHandle(TreeLock);

impl Drop for TreeLockHandle {
    fn drop(&mut self) {
        self.0.data.locked.store(false, Ordering::SeqCst);

        let data = self.0.data.clone();
        self.0.data.rt.spawn(async move {
            let mut blocked = data.blocked.lock().await;
            for blocked in blocked.drain(..) {
                let _ = blocked.send(());
            }
        });
    }
}
