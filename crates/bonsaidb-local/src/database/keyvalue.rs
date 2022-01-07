use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    convert::Infallible,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use async_trait::async_trait;
use bonsaidb_core::{
    keyvalue::{
        Command, KeyCheck, KeyOperation, KeyStatus, KeyValue, Numeric, Output, SetCommand,
        Timestamp, Value,
    },
    transaction::{ChangedKey, Changes},
};
use nebari::{
    io::fs::StdFile,
    tree::{CompareSwap, KeyEvaluation, Operation, Root, Unversioned},
    AbortError, Buffer, Roots,
};
use serde::{Deserialize, Serialize};

use crate::{config::KeyValuePersistence, jobs::Job, Database, Error};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Entry {
    pub value: Value,
    pub expiration: Option<Timestamp>,
}

impl Entry {
    pub(crate) async fn restore(
        self,
        namespace: Option<String>,
        key: String,
        database: &Database,
    ) -> Result<(), bonsaidb_core::Error> {
        database
            .execute_key_operation(KeyOperation {
                namespace,
                key,
                command: Command::Set(SetCommand {
                    value: self.value,
                    expiration: self.expiration,
                    keep_existing_expiration: false,
                    check: None,
                    return_previous_value: false,
                }),
            })
            .await?;
        Ok(())
    }
}

#[async_trait]
impl KeyValue for Database {
    async fn execute_key_operation(
        &self,
        op: KeyOperation,
    ) -> Result<Output, bonsaidb_core::Error> {
        self.data.context.perform_kv_operation(op).await
    }
}

impl Database {
    pub(crate) async fn all_key_value_entries(
        &self,
    ) -> Result<HashMap<(Option<String>, String), Entry>, Error> {
        let database = self.clone();
        tokio::task::spawn_blocking(move || {
            // Find all trees that start with <database>.kv.
            let mut all_entries = HashMap::new();
            database
                .roots()
                .tree(Unversioned::tree(KEY_TREE))?
                .scan::<Error, _, _, _, _>(
                    ..,
                    true,
                    |_, _, _| true,
                    |_, _| KeyEvaluation::ReadData,
                    |key, _, entry: Buffer<'static>| {
                        let entry = bincode::deserialize::<Entry>(&entry)
                            .map_err(|err| AbortError::Other(Error::from(err)))?;
                        let full_key = std::str::from_utf8(&key)
                            .map_err(|err| AbortError::Other(Error::from(err)))?;
                        if let Some(split_key) = split_key(full_key) {
                            all_entries.insert(split_key, entry);
                        }

                        Ok(())
                    },
                )?;
            Ok(all_entries)
        })
        .await?
    }
}

pub(crate) const KEY_TREE: &str = "kv";

fn full_key(namespace: Option<&str>, key: &str) -> String {
    let full_length = namespace.map_or_else(|| 0, str::len) + key.len() + 1;
    let mut full_key = String::with_capacity(full_length);
    if let Some(ns) = namespace {
        full_key.push_str(ns);
    }
    full_key.push('\0');
    full_key.push_str(key);
    full_key
}

fn split_key(full_key: &str) -> Option<(Option<String>, String)> {
    if let Some((namespace, key)) = full_key.split_once('\0') {
        let namespace = if namespace.is_empty() {
            None
        } else {
            Some(namespace.to_string())
        };
        Some((namespace, key.to_string()))
    } else {
        None
    }
}

fn increment(existing: &Numeric, amount: &Numeric, saturating: bool) -> Numeric {
    match amount {
        Numeric::Integer(amount) => {
            let existing_value = existing.as_i64_lossy(saturating);
            let new_value = if saturating {
                existing_value.saturating_add(*amount)
            } else {
                existing_value.wrapping_add(*amount)
            };
            Numeric::Integer(new_value)
        }
        Numeric::UnsignedInteger(amount) => {
            let existing_value = existing.as_u64_lossy(saturating);
            let new_value = if saturating {
                existing_value.saturating_add(*amount)
            } else {
                existing_value.wrapping_add(*amount)
            };
            Numeric::UnsignedInteger(new_value)
        }
        Numeric::Float(amount) => {
            let existing_value = existing.as_f64_lossy();
            let new_value = existing_value + *amount;
            Numeric::Float(new_value)
        }
    }
}

fn decrement(existing: &Numeric, amount: &Numeric, saturating: bool) -> Numeric {
    match amount {
        Numeric::Integer(amount) => {
            let existing_value = existing.as_i64_lossy(saturating);
            let new_value = if saturating {
                existing_value.saturating_sub(*amount)
            } else {
                existing_value.wrapping_sub(*amount)
            };
            Numeric::Integer(new_value)
        }
        Numeric::UnsignedInteger(amount) => {
            let existing_value = existing.as_u64_lossy(saturating);
            let new_value = if saturating {
                existing_value.saturating_sub(*amount)
            } else {
                existing_value.wrapping_sub(*amount)
            };
            Numeric::UnsignedInteger(new_value)
        }
        Numeric::Float(amount) => {
            let existing_value = existing.as_f64_lossy();
            let new_value = existing_value - *amount;
            Numeric::Float(new_value)
        }
    }
}

#[derive(Debug)]
pub struct ExpirationUpdate {
    pub tree_key: String,
    pub expiration: Option<Timestamp>,
}

impl ExpirationUpdate {
    pub fn new(tree_key: String, expiration: Option<Timestamp>) -> Self {
        Self {
            tree_key,
            expiration,
        }
    }
}

#[derive(Debug)]
pub(super) struct KeyValueManager {
    operation_receiver: flume::Receiver<(
        ManagerOp,
        flume::Sender<Result<Output, bonsaidb_core::Error>>,
    )>,
    persistence: KeyValuePersistence,
    last_commit: Timestamp,
    roots: Roots<StdFile>,
    expiring_keys: BTreeMap<String, Timestamp>,
    expiration_order: VecDeque<String>,
    dirty_keys: BTreeMap<String, Option<Entry>>,
}

#[derive(Debug)]
pub(super) enum ManagerOp {
    Op(KeyOperation),
    SetExpiration(ExpirationUpdate),
}

impl KeyValueManager {
    pub fn new(
        operation_receiver: flume::Receiver<(
            ManagerOp,
            flume::Sender<Result<Output, bonsaidb_core::Error>>,
        )>,
        roots: Roots<StdFile>,
        persistence: KeyValuePersistence,
    ) -> Self {
        Self {
            operation_receiver,
            roots,
            persistence,
            last_commit: Timestamp::now(),
            expiring_keys: BTreeMap::new(),
            expiration_order: VecDeque::new(),
            dirty_keys: BTreeMap::new(),
        }
    }

    pub async fn run(&mut self) -> Result<(), Error> {
        while let Some((op, result_sender)) = self.wait_for_next_operation().await? {
            let result = match op {
                ManagerOp::Op(op) => match op.command {
                    Command::Set(command) => {
                        self.execute_set_operation(op.namespace.as_deref(), &op.key, command)
                    }
                    Command::Get { delete } => {
                        self.execute_get_operation(op.namespace.as_deref(), &op.key, delete)
                    }
                    Command::Delete => {
                        self.execute_delete_operation(op.namespace.as_deref(), &op.key)
                    }
                    Command::Increment { amount, saturating } => self.execute_increment_operation(
                        op.namespace.as_deref(),
                        &op.key,
                        &amount,
                        saturating,
                    ),
                    Command::Decrement { amount, saturating } => self.execute_decrement_operation(
                        op.namespace.as_deref(),
                        &op.key,
                        &amount,
                        saturating,
                    ),
                },
                ManagerOp::SetExpiration(expiration) => {
                    self.update_expiration(&expiration);
                    Ok(Output::Status(KeyStatus::Updated))
                }
            };

            let now = Timestamp::now();
            self.remove_expired_keys(now);
            if self.needs_commit(now) {
                self.commit_dirty_keys().await?;
            }

            drop(result_sender.send(result));
        }

        // We are shutting down. If we have any dirty keys, we need to commit
        // them regardless of the current rules.
        self.commit_dirty_keys().await?;

        Ok(())
    }

    fn execute_set_operation(
        &mut self,
        namespace: Option<&str>,
        key: &str,
        set: SetCommand,
    ) -> Result<Output, bonsaidb_core::Error> {
        let mut entry = Entry {
            value: set.value,
            expiration: set.expiration,
        };
        let mut inserted = false;
        let mut updated = false;
        let full_key = full_key(namespace, key);
        let previous_value = self
            .fetch_and_update(&full_key, |existing_value| {
                let should_update = match set.check {
                    Some(KeyCheck::OnlyIfPresent) => existing_value.is_some(),
                    Some(KeyCheck::OnlyIfVacant) => existing_value.is_none(),
                    None => true,
                };
                if should_update {
                    updated = true;
                    inserted = existing_value.is_none();
                    if set.keep_existing_expiration && !inserted {
                        if let Some(existing_value) = &existing_value {
                            entry.expiration = existing_value.expiration;
                        }
                    }
                    Some(entry.clone())
                } else {
                    existing_value
                }
            })
            .map_err(Error::from)?;

        if updated {
            self.update_key_expiration(full_key, entry.expiration);
            if set.return_previous_value {
                Ok(Output::Value(previous_value.map(|entry| entry.value)))
            } else if inserted {
                Ok(Output::Status(KeyStatus::Inserted))
            } else {
                Ok(Output::Status(KeyStatus::Updated))
            }
        } else {
            Ok(Output::Status(KeyStatus::NotChanged))
        }
    }

    fn update_key_expiration(&mut self, key: String, expiration: Option<Timestamp>) {
        self.update_expiration(&ExpirationUpdate::new(key, expiration));
    }

    fn execute_get_operation(
        &mut self,
        namespace: Option<&str>,
        key: &str,
        delete: bool,
    ) -> Result<Output, bonsaidb_core::Error> {
        let full_key = full_key(namespace, key);
        let entry = if delete {
            self.remove(full_key).map_err(Error::from)?
        } else {
            self.get(&full_key).map_err(Error::from)?
        };

        Ok(Output::Value(entry.map(|e| e.value)))
    }

    fn execute_delete_operation(
        &mut self,
        namespace: Option<&str>,
        key: &str,
    ) -> Result<Output, bonsaidb_core::Error> {
        let full_key = full_key(namespace, key);
        let value = self.remove(full_key).map_err(Error::from)?;
        if value.is_some() {
            Ok(Output::Status(KeyStatus::Deleted))
        } else {
            Ok(Output::Status(KeyStatus::NotChanged))
        }
    }

    fn execute_increment_operation(
        &mut self,
        namespace: Option<&str>,
        key: &str,
        amount: &Numeric,
        saturating: bool,
    ) -> Result<Output, bonsaidb_core::Error> {
        self.execute_numeric_operation(namespace, key, amount, saturating, increment)
    }

    fn execute_decrement_operation(
        &mut self,
        namespace: Option<&str>,
        key: &str,
        amount: &Numeric,
        saturating: bool,
    ) -> Result<Output, bonsaidb_core::Error> {
        self.execute_numeric_operation(namespace, key, amount, saturating, decrement)
    }

    fn execute_numeric_operation<F: Fn(&Numeric, &Numeric, bool) -> Numeric>(
        &mut self,
        namespace: Option<&str>,
        key: &str,
        amount: &Numeric,
        saturating: bool,
        op: F,
    ) -> Result<Output, bonsaidb_core::Error> {
        let full_key = full_key(namespace, key);
        let current = self.get(&full_key).map_err(Error::from)?;
        let mut entry = current.unwrap_or(Entry {
            value: Value::Numeric(Numeric::UnsignedInteger(0)),
            expiration: None,
        });

        match entry.value {
            Value::Numeric(existing) => {
                let value = Value::Numeric(op(&existing, amount, saturating));
                entry.value = value.clone();

                self.set(full_key, entry);
                Ok(Output::Value(Some(value)))
            }
            Value::Bytes(_) => Err(bonsaidb_core::Error::Database(String::from(
                "type of stored `Value` is not `Numeric`",
            ))),
        }
    }

    fn fetch_and_update<F>(&mut self, full_key: &str, f: F) -> Result<Option<Entry>, nebari::Error>
    where
        F: FnOnce(Option<Entry>) -> Option<Entry>,
    {
        let current = self.get(full_key)?;
        let next = f(current.clone());
        if let Some(entry) = self.dirty_keys.get_mut(full_key) {
            *entry = next;
        } else {
            self.dirty_keys.insert(full_key.to_string(), next);
        }
        Ok(current)
    }

    fn remove(&mut self, key: String) -> Result<Option<Entry>, nebari::Error> {
        self.update_key_expiration(key.clone(), None);

        if let Some(dirty_entry) = self.dirty_keys.get_mut(&key) {
            Ok(dirty_entry.take())
        } else {
            // There might be a value on-disk we need to remove.
            let previous_value = self
                .roots
                .tree(Unversioned::tree(KEY_TREE))?
                .get(key.as_bytes())?;
            let previous_value =
                previous_value.and_then(|current| bincode::deserialize::<Entry>(&current).ok());
            self.dirty_keys.insert(key, None);
            Ok(previous_value)
        }
    }

    fn get(&self, key: &str) -> Result<Option<Entry>, nebari::Error> {
        if let Some(entry) = self.dirty_keys.get(key) {
            Ok(entry.clone())
        } else {
            self.roots
                .tree(Unversioned::tree(KEY_TREE))?
                .get(key.as_bytes())
                .map(|current| {
                    current.and_then(|current| bincode::deserialize::<Entry>(&current).ok())
                })
        }
    }

    fn set(&mut self, key: String, value: Entry) {
        self.dirty_keys.insert(key, Some(value));
    }

    fn update_expiration(&mut self, update: &ExpirationUpdate) {
        if let Some(expiration) = update.expiration {
            let key = if self.expiring_keys.contains_key(&update.tree_key) {
                // Update the existing entry.
                let existing_entry_index = self
                    .expiration_order
                    .iter()
                    .enumerate()
                    .find_map(|(index, key)| {
                        if &update.tree_key == key {
                            Some(index)
                        } else {
                            None
                        }
                    })
                    .unwrap();
                self.expiration_order.remove(existing_entry_index).unwrap()
            } else {
                update.tree_key.clone()
            };

            // Insert the key into the expiration_order queue
            let mut insert_at = None;
            for (index, expiring_key) in self.expiration_order.iter().enumerate() {
                if self.expiring_keys.get(expiring_key).unwrap() > &expiration {
                    insert_at = Some(index);
                    break;
                }
            }
            if let Some(insert_at) = insert_at {
                self.expiration_order.insert(insert_at, key.clone());
            } else {
                self.expiration_order.push_back(key.clone());
            }
            self.expiring_keys.insert(key, expiration);
        } else if self.expiring_keys.remove(&update.tree_key).is_some() {
            let index = self
                .expiration_order
                .iter()
                .enumerate()
                .find_map(|(index, key)| {
                    if &update.tree_key == key {
                        Some(index)
                    } else {
                        None
                    }
                })
                .unwrap();
            self.expiration_order.remove(index);
        }
    }

    async fn wait_for_next_operation(
        &mut self,
    ) -> Result<
        Option<(
            ManagerOp,
            flume::Sender<Result<Output, bonsaidb_core::Error>>,
        )>,
        Error,
    > {
        loop {
            // Check to see if we have any remaining time before a key expires
            let mut now = Timestamp::now();
            let duration_until_key_expires = self
                .expiration_order
                .get(0)
                .and_then(|key| {
                    let expiration_timeout = self.expiring_keys.get(key).unwrap();
                    *expiration_timeout - now
                })
                .unwrap_or(Duration::MAX);
            let duration_until_commit = self.persistence.duration_until_next_commit(
                self.dirty_keys.len(),
                (now - self.last_commit).unwrap_or_default(),
            );
            let timeout_duration = duration_until_key_expires.min(duration_until_commit);

            let received_update = if timeout_duration == Duration::MAX {
                match self.operation_receiver.recv_async().await {
                    Ok(update) => Some(update),
                    Err(_) => break,
                }
            } else if timeout_duration > Duration::ZERO {
                // Allow flume to receive updates for the remaining time.
                match tokio::time::timeout(timeout_duration, self.operation_receiver.recv_async())
                    .await
                {
                    Ok(Ok(update)) => Some(update),
                    Ok(Err(flume::RecvError::Disconnected)) => break,
                    Err(_elapsed) => {
                        // We've delayed for some duration and need a new timestamp.
                        now = Timestamp::now();
                        None
                    }
                }
            } else {
                // No timeout and we have pending operations. Return any pending operations if we have them.
                match self.operation_receiver.try_recv() {
                    Ok(update) => Some(update),
                    Err(flume::TryRecvError::Empty) => None,
                    Err(flume::TryRecvError::Disconnected) => break,
                }
            };

            // If we've received an update, we bubble it up to process
            if let Some(update) = received_update {
                return Ok(Some(update));
            }

            // Reaching this block means that we didn't receive an update
            // externally. This means we either need to expire some keys or
            // commit some keys to disk.
            // println!(
            //     "No updates. Commit: {:?} ({}), Expire: {:?}",
            //     duration_until_commit,
            //     self.dirty_keys.len(),
            //     duration_until_key_expires
            // );
            self.remove_expired_keys(now);
            if self.needs_commit(now) {
                self.commit_dirty_keys().await?;
            }
        }

        Ok(None)
    }

    fn remove_expired_keys(&mut self, now: Timestamp) {
        while !self.expiration_order.is_empty()
            && self.expiring_keys.get(&self.expiration_order[0]).unwrap() <= &now
        {
            let key = self.expiration_order.pop_front().unwrap();
            self.expiring_keys.remove(&key);
            self.dirty_keys.insert(key, None);
        }
    }

    fn needs_commit(&mut self, now: Timestamp) -> bool {
        let since_last_commit = (now - self.last_commit).unwrap_or_default();
        self.persistence
            .should_commit(self.dirty_keys.len(), since_last_commit)
    }

    async fn commit_dirty_keys(&mut self) -> Result<(), bonsaidb_core::Error> {
        if self.dirty_keys.is_empty() {
            Ok(())
        } else {
            let roots = self.roots.clone();
            let keys = std::mem::take(&mut self.dirty_keys);
            let result = tokio::task::spawn_blocking(move || Self::persist_keys(&roots, keys))
                .await
                .unwrap();
            self.last_commit = Timestamp::now();
            result
        }
    }

    fn persist_keys(
        roots: &Roots<StdFile>,
        mut keys: BTreeMap<String, Option<Entry>>,
    ) -> Result<(), bonsaidb_core::Error> {
        let mut transaction = roots
            .transaction(&[Unversioned::tree(KEY_TREE)])
            .map_err(Error::from)?;
        let all_keys = keys
            .keys()
            .map(|key| Buffer::from(key.as_bytes().to_vec()))
            .collect();
        let mut changed_keys = Vec::new();
        transaction
            .tree::<Unversioned>(0)
            .unwrap()
            .modify(
                all_keys,
                Operation::CompareSwap(CompareSwap::new(&mut |key, existing_value| {
                    let full_key = std::str::from_utf8(key).unwrap();
                    let (namespace, key) = split_key(full_key).unwrap();

                    if let Some(new_value) = keys.remove(full_key).unwrap() {
                        changed_keys.push(ChangedKey {
                            namespace,
                            key,
                            deleted: false,
                        });
                        let bytes = bincode::serialize(&new_value).unwrap();
                        nebari::tree::KeyOperation::Set(Buffer::from(bytes))
                    } else if existing_value.is_some() {
                        changed_keys.push(ChangedKey {
                            namespace,
                            key,
                            deleted: existing_value.is_some(),
                        });
                        nebari::tree::KeyOperation::Remove
                    } else {
                        nebari::tree::KeyOperation::Skip
                    }
                })),
            )
            .map_err(Error::from)?;

        if !changed_keys.is_empty() {
            transaction
                .entry_mut()
                .set_data(pot::to_vec(&Changes::Keys(changed_keys))?)
                .map_err(Error::from)?;
            transaction.commit().map_err(Error::from)?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ExpirationLoader {
    pub database: Database,
}

#[async_trait]
impl Job for ExpirationLoader {
    type Output = ();
    type Error = Error;

    #[cfg_attr(feature = "tracing", tracing::instrument)]
    async fn execute(&mut self) -> Result<Self::Output, Self::Error> {
        let database = self.database.clone();
        let (sender, receiver) = flume::unbounded();

        tokio::task::spawn_blocking(move || {
            // Find all trees that start with <database>.kv.
            let keep_scanning = AtomicBool::new(true);
            database
                .roots()
                .tree(Unversioned::tree(KEY_TREE))?
                .scan::<Infallible, _, _, _, _>(
                    ..,
                    true,
                    |_, _, _| true,
                    |_, _| {
                        if keep_scanning.load(Ordering::SeqCst) {
                            KeyEvaluation::ReadData
                        } else {
                            KeyEvaluation::Stop
                        }
                    },
                    |key, _, entry: Buffer<'static>| {
                        if let Ok(entry) = bincode::deserialize::<Entry>(&entry) {
                            if entry.expiration.is_some()
                                && sender.send((key, entry.expiration)).is_err()
                            {
                                keep_scanning.store(false, Ordering::SeqCst);
                            }
                        }

                        Ok(())
                    },
                )?;

            Result::<(), Error>::Ok(())
        });

        while let Ok((key, expiration)) = receiver.recv_async().await {
            self.database
                .update_key_expiration_async(String::from_utf8(key.to_vec())?, expiration)
                .await;
        }

        self.database
            .storage()
            .tasks()
            .mark_key_value_expiration_loaded(self.database.data.name.clone())
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bonsaidb_core::test_util::{TestDirectory, TimingTest};
    use futures::Future;
    use nebari::io::fs::StdFile;

    use super::*;
    use crate::{config::PersistenceThreshold, database::Context};

    async fn run_test_with_persistence<
        F: Fn(Context, nebari::Roots<StdFile>) -> R + Send,
        R: Future<Output = anyhow::Result<()>> + Send,
    >(
        name: &str,
        persistence: KeyValuePersistence,
        test_contents: &F,
    ) -> anyhow::Result<()> {
        let dir = TestDirectory::new(name);
        let sled = nebari::Config::new(&dir).open()?;

        let context = Context::new(sled.clone(), persistence);

        test_contents(context, sled).await?;

        Ok(())
    }

    async fn run_test<
        F: Fn(Context, nebari::Roots<StdFile>) -> R + Send,
        R: Future<Output = anyhow::Result<()>> + Send,
    >(
        name: &str,
        test_contents: F,
    ) -> anyhow::Result<()> {
        run_test_with_persistence(name, KeyValuePersistence::default(), &test_contents).await
    }

    #[tokio::test]
    async fn basic_expiration() -> anyhow::Result<()> {
        run_test("kv-basic-expiration", |sender, sled| async move {
            loop {
                sled.delete_tree(KEY_TREE)?;
                let tree = sled.tree(Unversioned::tree(KEY_TREE))?;
                tree.set(b"atree\0akey", b"somevalue")?;
                let timing = TimingTest::new(Duration::from_millis(100));
                sender
                    .update_key_expiration_async(
                        full_key(Some("atree"), "akey"),
                        Some(Timestamp::now() + Duration::from_millis(100)),
                    )
                    .await;
                if !timing.wait_until(Duration::from_secs(1)).await {
                    println!("basic_expiration restarting due to timing discrepency");
                    continue;
                }
                assert!(tree.get(b"akey")?.is_none());
                break;
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn updating_expiration() -> anyhow::Result<()> {
        run_test("kv-updating-expiration", |sender, sled| async move {
            loop {
                sled.delete_tree(KEY_TREE)?;
                let tree = sled.tree(Unversioned::tree(KEY_TREE))?;
                tree.set(b"atree\0akey", b"somevalue")?;
                let timing = TimingTest::new(Duration::from_millis(100));
                sender
                    .update_key_expiration_async(
                        full_key(Some("atree"), "akey"),
                        Some(Timestamp::now() + Duration::from_millis(100)),
                    )
                    .await;
                sender
                    .update_key_expiration_async(
                        full_key(Some("atree"), "akey"),
                        Some(Timestamp::now() + Duration::from_secs(1)),
                    )
                    .await;
                if timing.elapsed() > Duration::from_millis(100)
                    || !timing.wait_until(Duration::from_millis(500)).await
                {
                    continue;
                }
                assert!(tree.get(b"atree\0akey")?.is_some());

                timing.wait_until(Duration::from_secs_f32(1.5)).await;
                assert_eq!(tree.get(b"atree\0akey")?, None);
                break;
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn multiple_keys_expiration() -> anyhow::Result<()> {
        run_test("kv-multiple-keys-expiration", |sender, sled| async move {
            loop {
                sled.delete_tree(KEY_TREE)?;
                let tree = sled.tree(Unversioned::tree(KEY_TREE))?;
                tree.set(b"atree\0akey", b"somevalue")?;
                tree.set(b"atree\0bkey", b"somevalue")?;

                let timing = TimingTest::new(Duration::from_millis(100));
                sender
                    .update_key_expiration_async(
                        full_key(Some("atree"), "akey"),
                        Some(Timestamp::now() + Duration::from_millis(100)),
                    )
                    .await;
                sender
                    .update_key_expiration_async(
                        full_key(Some("atree"), "bkey"),
                        Some(Timestamp::now() + Duration::from_secs(1)),
                    )
                    .await;

                if !timing.wait_until(Duration::from_millis(200)).await {
                    continue;
                }

                assert!(tree.get(b"atree\0akey")?.is_none());
                assert!(tree.get(b"atree\0bkey")?.is_some());
                timing.wait_until(Duration::from_millis(1100)).await;
                assert!(tree.get(b"atree\0bkey")?.is_none());

                break;
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn clearing_expiration() -> anyhow::Result<()> {
        run_test("kv-clearing-expiration", |sender, sled| async move {
            loop {
                sled.delete_tree(KEY_TREE)?;
                let tree = sled.tree(Unversioned::tree(KEY_TREE))?;
                tree.set(b"atree\0akey", b"somevalue")?;
                let timing = TimingTest::new(Duration::from_millis(100));
                sender
                    .update_key_expiration_async(
                        full_key(Some("atree"), "akey"),
                        Some(Timestamp::now() + Duration::from_millis(100)),
                    )
                    .await;
                sender
                    .update_key_expiration_async(full_key(Some("atree"), "akey"), None)
                    .await;
                if timing.elapsed() > Duration::from_millis(100) {
                    // Restart, took too long.
                    continue;
                }
                timing.wait_until(Duration::from_millis(150)).await;
                assert!(tree.get(b"atree\0akey")?.is_some());
                break;
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn out_of_order_expiration() -> anyhow::Result<()> {
        run_test("kv-out-of-order-expiration", |sender, sled| async move {
            let tree = sled.tree(Unversioned::tree(KEY_TREE))?;
            tree.set(b"atree\0akey", b"somevalue")?;
            tree.set(b"atree\0bkey", b"somevalue")?;
            tree.set(b"atree\0ckey", b"somevalue")?;
            sender
                .update_key_expiration_async(
                    full_key(Some("atree"), "akey"),
                    Some(Timestamp::now() + Duration::from_secs(3)),
                )
                .await;
            sender
                .update_key_expiration_async(
                    full_key(Some("atree"), "ckey"),
                    Some(Timestamp::now() + Duration::from_secs(1)),
                )
                .await;
            sender
                .update_key_expiration_async(
                    full_key(Some("atree"), "bkey"),
                    Some(Timestamp::now() + Duration::from_secs(2)),
                )
                .await;
            tokio::time::sleep(Duration::from_millis(1200)).await;
            assert!(tree.get(b"atree\0akey")?.is_some());
            assert!(tree.get(b"atree\0bkey")?.is_some());
            assert!(tree.get(b"atree\0ckey")?.is_none());
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(tree.get(b"atree\0akey")?.is_some());
            assert!(tree.get(b"atree\0bkey")?.is_none());
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(tree.get(b"atree\0akey")?.is_none());

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn basic_persistence() -> anyhow::Result<()> {
        run_test_with_persistence(
            "kv-basic-persistence]",
            KeyValuePersistence::lazy([
                PersistenceThreshold::after_changes(2),
                PersistenceThreshold::after_changes(1).and_duration(Duration::from_secs(2)),
            ]),
            &|sender, sled| async move {
                loop {
                    let timing = TimingTest::new(Duration::from_millis(100));
                    let tree = sled.tree(Unversioned::tree(KEY_TREE))?;
                    // Set three keys in quick succession. The first two should
                    // persist immediately, and the third should show up after 2
                    // seconds.
                    sender
                        .perform_kv_operation(KeyOperation {
                            namespace: None,
                            key: String::from("key1"),
                            command: Command::Set(SetCommand {
                                value: Value::Bytes(Vec::new()),
                                expiration: None,
                                keep_existing_expiration: false,
                                check: None,
                                return_previous_value: false,
                            }),
                        })
                        .await
                        .unwrap();
                    sender
                        .perform_kv_operation(KeyOperation {
                            namespace: None,
                            key: String::from("key2"),
                            command: Command::Set(SetCommand {
                                value: Value::Bytes(Vec::new()),
                                expiration: None,
                                keep_existing_expiration: false,
                                check: None,
                                return_previous_value: false,
                            }),
                        })
                        .await
                        .unwrap();
                    sender
                        .perform_kv_operation(KeyOperation {
                            namespace: None,
                            key: String::from("key3"),
                            command: Command::Set(SetCommand {
                                value: Value::Bytes(Vec::new()),
                                expiration: None,
                                keep_existing_expiration: false,
                                check: None,
                                return_previous_value: false,
                            }),
                        })
                        .await
                        .unwrap();
                    if timing.elapsed() > Duration::from_secs(1) {
                        println!("basic_persistence restarting due to timing discrepency");
                        continue;
                    }
                    assert!(tree.get(b"\0key1").unwrap().is_some());
                    assert!(tree.get(b"\0key2").unwrap().is_some());
                    assert!(tree.get(b"\0key3").unwrap().is_none());
                    if !timing.wait_until(Duration::from_secs(3)).await {
                        println!("basic_persistence restarting due to timing discrepency");
                        continue;
                    }
                    assert!(tree.get(b"\0key3").unwrap().is_some());
                    break;
                }

                Ok(())
            },
        )
        .await
    }
}
