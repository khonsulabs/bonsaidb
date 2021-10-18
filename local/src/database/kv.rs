use std::{
    collections::{HashMap, VecDeque},
    convert::Infallible,
    hash::Hash,
    sync::atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use bonsaidb_core::{
    kv::{Command, KeyCheck, KeyOperation, KeyStatus, Kv, Numeric, Output, Timestamp, Value},
    schema::Schema,
};
use bonsaidb_jobs::Job;
use nebari::{
    io::fs::StdFile,
    tree::{KeyEvaluation, Root, Unversioned},
    Buffer, CompareAndSwapError, Tree,
};
use serde::{Deserialize, Serialize};

use crate::{database::Context, Database, Error};

#[derive(Serialize, Deserialize)]
pub struct Entry {
    pub value: Value,
    pub expiration: Option<Timestamp>,
}

#[async_trait]
impl<DB> Kv for Database<DB>
where
    DB: Schema,
{
    async fn execute_key_operation(
        &self,
        op: KeyOperation,
    ) -> Result<Output, bonsaidb_core::Error> {
        let task_self = self.clone();
        tokio::task::spawn_blocking(move || match op.command {
            Command::Set {
                value,
                expiration,
                keep_existing_expiration,
                check,
                return_previous_value,
            } => execute_set_operation(
                key_tree(op.namespace),
                op.key,
                value,
                expiration,
                keep_existing_expiration,
                check,
                return_previous_value,
                &task_self,
            ),
            Command::Get { delete } => {
                execute_get_operation(key_tree(op.namespace), &op.key, delete, &task_self)
            }
            Command::Delete => execute_delete_operation(key_tree(op.namespace), op.key, &task_self),
            Command::Increment { amount, saturating } => execute_increment_operation(
                key_tree(op.namespace),
                &op.key,
                &task_self,
                &amount,
                saturating,
            ),
            Command::Decrement { amount, saturating } => execute_decrement_operation(
                key_tree(op.namespace),
                &op.key,
                &task_self,
                &amount,
                saturating,
            ),
        })
        .await
        .unwrap()
    }
}

fn key_tree(namespace: Option<impl AsRef<str>>) -> String {
    namespace.map_or_else(|| String::from("kv."), |ns| format!("kv.{}", ns.as_ref()))
}

#[allow(clippy::too_many_arguments)]
fn execute_set_operation<DB: Schema>(
    tree_name: String,
    key: String,
    value: Value,
    expiration: Option<Timestamp>,
    keep_existing_expiration: bool,
    check: Option<KeyCheck>,
    return_previous_value: bool,
    db: &Database<DB>,
) -> Result<Output, bonsaidb_core::Error> {
    let kv_tree = db
        .data
        .context
        .roots
        .tree(Unversioned::tree(tree_name.clone()))
        .map_err(Error::from)?;

    let mut entry = Entry { value, expiration };
    let mut inserted = false;
    let mut updated = false;
    let previous_value = fetch_and_update_no_copy(&kv_tree, key.as_bytes(), |existing_value| {
        let should_update = match check {
            Some(KeyCheck::OnlyIfPresent) => existing_value.is_some(),
            Some(KeyCheck::OnlyIfVacant) => existing_value.is_none(),
            None => true,
        };
        if should_update {
            updated = true;
            inserted = existing_value.is_none();
            if keep_existing_expiration && !inserted {
                if let Ok(previous_entry) = bincode::deserialize::<Entry>(&existing_value.unwrap())
                {
                    entry.expiration = previous_entry.expiration;
                }
            }
            let entry_vec = bincode::serialize(&entry).unwrap();
            Some(Buffer::from(entry_vec))
        } else {
            existing_value
        }
    })
    .map_err(Error::from)?;

    if updated {
        db.update_key_expiration(ExpirationUpdate {
            tree_key: TreeKey {
                tree: tree_name,
                key,
            },
            expiration: entry.expiration,
        });
        if return_previous_value {
            if let Some(Ok(entry)) = previous_value.map(|v| bincode::deserialize::<Entry>(&v)) {
                Ok(Output::Value(Some(entry.value)))
            } else {
                Ok(Output::Value(None))
            }
        } else if inserted {
            Ok(Output::Status(KeyStatus::Inserted))
        } else {
            Ok(Output::Status(KeyStatus::Updated))
        }
    } else {
        Ok(Output::Status(KeyStatus::NotChanged))
    }
}

fn execute_get_operation<DB: Schema>(
    tree_name: String,
    key: &str,
    delete: bool,
    db: &Database<DB>,
) -> Result<Output, bonsaidb_core::Error> {
    let tree = db
        .data
        .context
        .roots
        .tree(Unversioned::tree(tree_name.to_string()))
        .map_err(Error::from)?;
    let entry = if delete {
        let entry = tree.remove(key.as_bytes()).map_err(Error::from)?;
        if entry.is_some() {
            db.update_key_expiration(ExpirationUpdate {
                tree_key: TreeKey::new(tree_name, key.to_string()),
                expiration: None,
            });
        }
        entry
    } else {
        tree.get(key.as_bytes()).map_err(Error::from)?
    };

    let entry = entry
        .map(|e| bincode::deserialize::<Entry>(&e))
        .transpose()
        .map_err(Error::from)
        .unwrap()
        .map(|e| e.value);
    Ok(Output::Value(entry))
}

fn execute_delete_operation<DB: Schema>(
    tree_name: String,
    key: String,
    db: &Database<DB>,
) -> Result<Output, bonsaidb_core::Error> {
    let tree = db
        .data
        .context
        .roots
        .tree(Unversioned::tree(tree_name.to_string()))
        .map_err(Error::from)?;
    let value = tree.remove(key.as_bytes()).map_err(Error::from)?;
    if value.is_some() {
        db.update_key_expiration(ExpirationUpdate {
            tree_key: TreeKey::new(tree_name, key),
            expiration: None,
        });

        Ok(Output::Status(KeyStatus::Deleted))
    } else {
        Ok(Output::Status(KeyStatus::NotChanged))
    }
}

fn execute_increment_operation<DB: Schema>(
    tree_name: String,
    key: &str,
    db: &Database<DB>,
    amount: &Numeric,
    saturating: bool,
) -> Result<Output, bonsaidb_core::Error> {
    execute_numeric_operation(tree_name, key, db, amount, saturating, increment)
}

fn execute_decrement_operation<DB: Schema>(
    tree_name: String,
    key: &str,
    db: &Database<DB>,
    amount: &Numeric,
    saturating: bool,
) -> Result<Output, bonsaidb_core::Error> {
    execute_numeric_operation(tree_name, key, db, amount, saturating, decrement)
}

fn execute_numeric_operation<DB: Schema, F: Fn(&Numeric, &Numeric, bool) -> Numeric>(
    tree_name: String,
    key: &str,
    db: &Database<DB>,
    amount: &Numeric,
    saturating: bool,
    op: F,
) -> Result<Output, bonsaidb_core::Error> {
    let tree = db
        .data
        .context
        .roots
        .tree(Unversioned::tree(tree_name))
        .map_err(Error::from)?;

    let mut current = tree.get(key.as_bytes()).map_err(Error::from)?;
    loop {
        let mut entry = current
            .as_ref()
            .map(|current| bincode::deserialize::<Entry>(current))
            .transpose()
            .map_err(Error::from)?
            .unwrap_or(Entry {
                value: Value::Numeric(Numeric::UnsignedInteger(0)),
                expiration: None,
            });

        match entry.value {
            Value::Numeric(existing) => {
                let value = Value::Numeric(op(&existing, amount, saturating));
                entry.value = value.clone();

                let result_bytes = Buffer::from(bincode::serialize(&entry).unwrap());
                match tree.compare_and_swap(key.as_bytes(), current.as_ref(), Some(result_bytes)) {
                    Ok(_) => return Ok(Output::Value(Some(value))),
                    Err(CompareAndSwapError::Conflict(cur)) => {
                        current = cur;
                    }
                    Err(CompareAndSwapError::Error(other)) => {
                        // TODO should roots errors be able to be put in core?
                        return Err(bonsaidb_core::Error::Database(other.to_string()));
                    }
                }
            }
            Value::Bytes(_) => {
                return Err(bonsaidb_core::Error::Database(String::from(
                    "type of stored `Value` is not `Numeric`",
                )))
            }
        }
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

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub struct TreeKey {
    pub tree: String,
    pub key: String,
}

impl TreeKey {
    pub fn new(tree: String, key: impl Into<String>) -> Self {
        Self {
            tree,
            key: key.into(),
        }
    }
}

fn fetch_and_update_no_copy<K, F, R>(
    tree: &Tree<R, StdFile>,
    key: K,
    mut f: F,
) -> Result<Option<Buffer<'static>>, nebari::Error>
where
    R: nebari::tree::Root,
    K: AsRef<[u8]>,
    F: FnMut(Option<Buffer<'static>>) -> Option<Buffer<'static>>,
{
    let key_ref = key.as_ref();
    let mut current = tree.get(key_ref)?;

    loop {
        let next = f(current.clone());
        match tree.compare_and_swap(key_ref, current.as_ref(), next) {
            Ok(()) => return Ok(current),
            Err(CompareAndSwapError::Conflict(cur)) => {
                current = cur;
            }
            Err(CompareAndSwapError::Error(other)) => return Err(other),
        }
    }
}

#[derive(Debug)]
pub struct ExpirationUpdate {
    pub tree_key: TreeKey,
    pub expiration: Option<Timestamp>,
}

#[allow(clippy::needless_pass_by_value)]
pub(crate) fn expiration_thread(
    context: Context,
    updates: flume::Receiver<ExpirationUpdate>,
) -> Result<(), nebari::Error> {
    // expiring_keys will be maintained such that the soonest expiration is at the front and furthest in the future is at the back
    let mut tracked_keys = HashMap::<TreeKey, Timestamp>::new();
    let mut expiration_order = VecDeque::<TreeKey>::new();
    loop {
        let update = if expiration_order.is_empty() {
            if let Ok(update) = updates.try_recv() {
                update
            } else {
                // No updates currently are queued, shut down the thread if none
                // are still queued after locking the sender. This ensures that
                // if an update came in between the last try and the lock being
                // acquired that we still process the update. If there aren't
                // any updates, we clear the sender which will cause the next
                // update to respawn the thread.
                let mut sender = context.kv_expirer.write().unwrap();
                if let Ok(update) = updates.try_recv() {
                    update
                } else {
                    *sender = None;
                    break;
                }
            }
        } else {
            // Check to see if we have any remaining time before a key expires
            let timeout = tracked_keys.get(&expiration_order[0]).unwrap();
            let now = Timestamp::now();
            let remaining_time = *timeout - now;
            let received_update = if let Some(remaining_time) = remaining_time {
                // Allow flume to receive updates for the remaining time.
                match updates.recv_timeout(remaining_time) {
                    Ok(update) => Ok(update),
                    Err(flume::RecvTimeoutError::Timeout) => Err(()),
                    Err(flume::RecvTimeoutError::Disconnected) => break,
                }
            } else {
                Err(())
            };

            // If we've received an update, we bubble it up to process
            if let Ok(update) = received_update {
                update
            } else {
                // Reaching this block means that we didn't receive an update to
                // process, and we have at least one key that is ready to be
                // removed.
                while !expiration_order.is_empty()
                    && tracked_keys.get(&expiration_order[0]).unwrap() <= &now
                {
                    let key_to_remove = expiration_order.pop_front().unwrap();
                    tracked_keys.remove(&key_to_remove);
                    let tree = context
                        .roots
                        .tree(Unversioned::tree(key_to_remove.tree.clone()))?;
                    tree.remove(key_to_remove.key.as_bytes())?;
                }
                continue;
            }
        };

        if let Some(expiration) = update.expiration {
            let key = if tracked_keys.contains_key(&update.tree_key) {
                // Update the existing entry.
                let existing_entry_index = expiration_order
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
                expiration_order.remove(existing_entry_index).unwrap()
            } else {
                update.tree_key.clone()
            };

            // Insert the key into the expiration_order queue
            let mut insert_at = None;
            for (index, expiring_key) in expiration_order.iter().enumerate() {
                if tracked_keys.get(expiring_key).unwrap() > &expiration {
                    insert_at = Some(index);
                    break;
                }
            }
            if let Some(insert_at) = insert_at {
                expiration_order.insert(insert_at, key.clone());
            } else {
                expiration_order.push_back(key.clone());
            }
            tracked_keys.insert(key, expiration);
        } else if tracked_keys.remove(&update.tree_key).is_some() {
            let index = expiration_order
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
            expiration_order.remove(index);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bonsaidb_core::test_util::{TestDirectory, TimingTest};
    use futures::Future;
    use nebari::io::fs::StdFile;

    use super::*;

    async fn run_test<
        F: FnOnce(Context, nebari::Roots<StdFile>) -> R + Send,
        R: Future<Output = anyhow::Result<()>> + Send,
    >(
        name: &str,
        test_contents: F,
    ) -> anyhow::Result<()> {
        let dir = TestDirectory::new(name);
        let sled = nebari::Config::new(&dir).open()?;

        let context = Context::new(sled.clone());

        test_contents(context, sled).await?;

        Ok(())
    }

    #[tokio::test]
    async fn basic_expiration() -> anyhow::Result<()> {
        run_test("kv-basic-expiration", |sender, sled| async move {
            loop {
                sled.delete_tree("kv.atree")?;
                let tree = sled.tree(Unversioned::tree("kv.atree"))?;
                tree.set(b"akey", b"somevalue")?;
                let timing = TimingTest::new(Duration::from_millis(100));
                sender.update_key_expiration(ExpirationUpdate {
                    tree_key: TreeKey::new(key_tree(Some("atree")), String::from("akey")),
                    expiration: Some(Timestamp::now() + Duration::from_millis(100)),
                });
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
                sled.delete_tree("kv.atree")?;
                let tree = sled.tree(Unversioned::tree("kv.atree"))?;
                tree.set(b"akey", b"somevalue")?;
                let timing = TimingTest::new(Duration::from_millis(100));
                sender.update_key_expiration(ExpirationUpdate {
                    tree_key: TreeKey::new(key_tree(Some("atree")), String::from("akey")),
                    expiration: Some(Timestamp::now() + Duration::from_millis(100)),
                });
                sender.update_key_expiration(ExpirationUpdate {
                    tree_key: TreeKey::new(key_tree(Some("atree")), String::from("akey")),
                    expiration: Some(Timestamp::now() + Duration::from_secs(1)),
                });
                if timing.elapsed() > Duration::from_millis(100)
                    || !timing.wait_until(Duration::from_millis(500)).await
                {
                    continue;
                }
                assert!(tree.get(b"akey")?.is_some());

                timing.wait_until(Duration::from_secs_f32(1.5)).await;
                assert_eq!(tree.get(b"akey")?, None);
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
                sled.delete_tree("kv.atree")?;
                let tree = sled.tree(Unversioned::tree("kv.atree"))?;
                tree.set(b"akey", b"somevalue")?;
                tree.set(b"bkey", b"somevalue")?;

                let timing = TimingTest::new(Duration::from_millis(100));
                sender.update_key_expiration(ExpirationUpdate {
                    tree_key: TreeKey::new(key_tree(Some("atree")), String::from("akey")),
                    expiration: Some(Timestamp::now() + Duration::from_millis(100)),
                });
                sender.update_key_expiration(ExpirationUpdate {
                    tree_key: TreeKey::new(key_tree(Some("atree")), String::from("bkey")),
                    expiration: Some(Timestamp::now() + Duration::from_secs(1)),
                });

                if !timing.wait_until(Duration::from_millis(200)).await {
                    continue;
                }

                assert!(tree.get(b"akey")?.is_none());
                assert!(tree.get(b"bkey")?.is_some());
                timing.wait_until(Duration::from_millis(1100)).await;
                assert!(tree.get(b"bkey")?.is_none());

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
                sled.delete_tree("kv.atree")?;
                let tree = sled.tree(Unversioned::tree("kv.atree"))?;
                tree.set(b"akey", b"somevalue")?;
                let timing = TimingTest::new(Duration::from_millis(100));
                sender.update_key_expiration(ExpirationUpdate {
                    tree_key: TreeKey::new(key_tree(Some("atree")), String::from("akey")),
                    expiration: Some(Timestamp::now() + Duration::from_millis(100)),
                });
                sender.update_key_expiration(ExpirationUpdate {
                    tree_key: TreeKey::new(key_tree(Some("atree")), String::from("akey")),
                    expiration: None,
                });
                if timing.elapsed() > Duration::from_millis(100) {
                    // Restart, took too long.
                    continue;
                }
                timing.wait_until(Duration::from_millis(150)).await;
                assert!(tree.get(b"akey")?.is_some());
                break;
            }

            Ok(())
        })
        .await
    }

    #[tokio::test]
    async fn out_of_order_expiration() -> anyhow::Result<()> {
        run_test("kv-out-of-order-expiration", |sender, sled| async move {
            let tree = sled.tree(Unversioned::tree("kv.atree"))?;
            tree.set(b"akey", b"somevalue")?;
            tree.set(b"bkey", b"somevalue")?;
            tree.set(b"ckey", b"somevalue")?;
            sender.update_key_expiration(ExpirationUpdate {
                tree_key: TreeKey::new(key_tree(Some("atree")), String::from("akey")),
                expiration: Some(Timestamp::now() + Duration::from_secs(3)),
            });
            sender.update_key_expiration(ExpirationUpdate {
                tree_key: TreeKey::new(key_tree(Some("atree")), String::from("ckey")),
                expiration: Some(Timestamp::now() + Duration::from_secs(1)),
            });
            sender.update_key_expiration(ExpirationUpdate {
                tree_key: TreeKey::new(key_tree(Some("atree")), String::from("bkey")),
                expiration: Some(Timestamp::now() + Duration::from_secs(2)),
            });
            tokio::time::sleep(Duration::from_millis(1200)).await;
            assert!(tree.get(b"akey")?.is_some());
            assert!(tree.get(b"bkey")?.is_some());
            assert!(tree.get(b"ckey")?.is_none());
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(tree.get(b"akey")?.is_some());
            assert!(tree.get(b"bkey")?.is_none());
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(tree.get(b"akey")?.is_none());

            Ok(())
        })
        .await
    }
}

#[derive(Debug)]
pub struct ExpirationLoader<DB> {
    pub database: Database<DB>,
}

#[async_trait]
impl<DB: Schema> Job for ExpirationLoader<DB> {
    type Output = ();

    async fn execute(&mut self) -> anyhow::Result<Self::Output> {
        let database = self.database.clone();
        let (sender, receiver) = flume::unbounded();

        tokio::task::spawn_blocking(move || {
            // Find all trees that start with <database>.kv.
            let keep_scanning = AtomicBool::new(true);
            for kv_tree in database
                .roots()
                .tree_names()?
                .into_iter()
                .filter(|name| name.starts_with("kv."))
            {
                database
                    .roots()
                    .tree(Unversioned::tree(kv_tree.clone()))?
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
                                    && sender
                                        .send((kv_tree.clone(), key, entry.expiration))
                                        .is_err()
                                {
                                    keep_scanning.store(false, Ordering::SeqCst);
                                }
                            }

                            Ok(())
                        },
                    )?;
            }

            Result::<(), anyhow::Error>::Ok(())
        });

        while let Ok((tree, key, expiration)) = receiver.recv_async().await {
            self.database.update_key_expiration(ExpirationUpdate {
                tree_key: TreeKey {
                    tree,
                    key: String::from_utf8(key.to_vec())?,
                },
                expiration,
            });
        }

        Ok(())
    }
}
