use std::{
    borrow::Cow,
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use parking_lot::{Mutex, MutexGuard};

use super::{LogEntry, TransactionChanges, TransactionHandle, TreeLock, TreeLocks};

const UNINITIALIZED_ID: u64 = 0;

#[derive(Clone, Debug)]
pub struct State {
    state: Arc<ActiveState>,
}

#[derive(Debug)]
struct ActiveState {
    current_transaction_id: AtomicU64,
    tree_locks: Mutex<HashMap<Cow<'static, [u8]>, TreeLock>>,
    log_position: Mutex<u64>,
}

impl Default for State {
    fn default() -> Self {
        Self::new(UNINITIALIZED_ID, 0)
    }
}

impl State {
    fn new(current_transaction_id: u64, log_position: u64) -> Self {
        Self {
            state: Arc::new(ActiveState {
                tree_locks: Mutex::default(),
                current_transaction_id: AtomicU64::new(current_transaction_id),
                log_position: Mutex::new(log_position),
            }),
        }
    }

    pub fn initialize(&self, current_transaction_id: u64, log_position: u64) {
        let mut state_position = self.state.log_position.lock();
        self.state
            .current_transaction_id
            .compare_exchange(
                UNINITIALIZED_ID,
                current_transaction_id,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .expect("state already initialized");
        *state_position = log_position;
    }

    pub fn current_transaction_id(&self) -> u64 {
        self.state.current_transaction_id.load(Ordering::SeqCst)
    }

    fn fetch_tree_locks<'a>(&'a self, trees: &'a [&[u8]], locks: &mut TreeLocks) {
        let mut tree_locks = self.state.tree_locks.lock();
        for tree in trees {
            if let Some(lock) = tree_locks.get(&Cow::Borrowed(*tree)) {
                locks.push(lock.lock());
            } else {
                let lock = TreeLock::new();
                let locked = lock.lock();
                tree_locks.insert(Cow::Owned(tree.to_vec()), lock);
                locks.push(locked);
            }
        }
    }

    #[allow(clippy::needless_lifetimes)] // lies! I can't seem to get rid of the lifetimes.
    pub fn new_transaction(&self, trees: &[&[u8]]) -> TransactionHandle<'static> {
        let mut locked_trees = Vec::with_capacity(trees.len());
        self.fetch_tree_locks(trees, &mut locked_trees);

        TransactionHandle {
            locked_trees,
            transaction: LogEntry {
                id: self
                    .state
                    .current_transaction_id
                    .fetch_add(1, Ordering::SeqCst),
                changes: TransactionChanges::default(),
            },
        }
    }
}

impl State {
    pub fn lock_for_write(&self) -> MutexGuard<'_, u64> {
        self.state.log_position.lock()
    }
}
