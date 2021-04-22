use std::collections::{HashMap, VecDeque};

use async_trait::async_trait;
use pliantdb_core::{
    kv::{Command, KeyCheck, KeyOperation, KeyStatus, Kv, Output, Timestamp},
    schema::Schema,
};
use serde::{Deserialize, Serialize};
use sled::IVec;

use crate::{error::ResultExt as _, Storage};

#[derive(Serialize, Deserialize)]
struct KvEntry {
    value: Vec<u8>,
    expiration: Option<Timestamp>,
}

#[async_trait]
impl<DB> Kv for Storage<DB>
where
    DB: Schema,
{
    async fn execute_key_operation(
        &self,
        op: KeyOperation,
    ) -> Result<Output, pliantdb_core::Error> {
        tokio::task::block_in_place(|| match op.command {
            Command::Set {
                value,
                expiration,
                keep_existing_expiration,
                check,
                return_previous_value,
            } => execute_set_operation(
                op.namespace,
                op.key,
                value,
                expiration,
                keep_existing_expiration,
                check,
                return_previous_value,
                self,
            ),
            Command::Get { delete } => {
                execute_get_operation(op.namespace, &op.key, delete, &self.data.sled)
            }
            Command::Delete => execute_delete_operation(op.namespace, op.key, &self.data.sled),
        })
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_set_operation<DB: Schema>(
    namespace: Option<String>,
    key: String,
    value: Vec<u8>,
    expiration: Option<Timestamp>,
    keep_existing_expiration: bool,
    check: Option<KeyCheck>,
    return_previous_value: bool,
    storage: &Storage<DB>,
) -> Result<Output, pliantdb_core::Error> {
    let tree_name = format!("kv.{}", namespace.unwrap_or_default());
    let kv_tree = storage
        .data
        .sled
        .open_tree(tree_name.as_bytes())
        .map_err_to_core()?;

    let mut entry = KvEntry { value, expiration };
    let mut inserted = false;
    let mut updated = false;
    let previous_value = kv_tree
        .fetch_and_update(key.as_bytes(), |existing_value| {
            let should_update = match check {
                Some(KeyCheck::OnlyIfPresent) => existing_value.is_some(),
                Some(KeyCheck::OnlyIfVacant) => existing_value.is_none(),
                None => true,
            };
            if should_update {
                updated = true;
                inserted = existing_value.is_none();
                if keep_existing_expiration && !inserted {
                    if let Ok(previous_entry) =
                        bincode::deserialize::<KvEntry>(existing_value.unwrap())
                    {
                        entry.expiration = previous_entry.expiration;
                    }
                }
                let entry_vec = bincode::serialize(&entry).unwrap();
                Some(IVec::from(entry_vec))
            } else {
                None
            }
        })
        .map_err_to_core()?;

    if updated {
        storage.update_key_expiration(ExpirationUpdate {
            tree_key: TreeKey {
                tree: tree_name,
                key,
            },
            expiration: entry.expiration,
        });
        if return_previous_value {
            if let Some(Ok(entry)) = previous_value.map(|v| bincode::deserialize::<KvEntry>(&v)) {
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

fn execute_get_operation(
    namespace: Option<String>,
    key: &str,
    delete: bool,
    sled: &sled::Db,
) -> Result<Output, pliantdb_core::Error> {
    let full_namespace = format!("kv.{}", namespace.unwrap_or_default());

    let tree = sled
        .open_tree(full_namespace.as_bytes())
        .map_err_to_core()?;
    let entry = if delete {
        tree.remove(key.as_bytes()).map_err_to_core()?
    } else {
        tree.get(key.as_bytes()).map_err_to_core()?
    };

    let entry = entry
        .map(|e| bincode::deserialize::<KvEntry>(&e))
        .transpose()
        .map_err_to_core()?
        .map(|e| e.value);
    Ok(Output::Value(entry))
}

fn execute_delete_operation(
    namespace: Option<String>,
    key: String,
    sled: &sled::Db,
) -> Result<Output, pliantdb_core::Error> {
    let full_namespace = format!("kv.{}", namespace.unwrap_or_default());

    let tree = sled
        .open_tree(full_namespace.as_bytes())
        .map_err_to_core()?;
    let value = tree.remove(key).map_err_to_core()?;
    if value.is_some() {
        Ok(Output::Status(KeyStatus::Deleted))
    } else {
        Ok(Output::Status(KeyStatus::NotChanged))
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub struct TreeKey {
    tree: String,
    key: String,
}

#[derive(Debug)]
pub struct ExpirationUpdate {
    tree_key: TreeKey,
    expiration: Option<Timestamp>,
}

#[allow(clippy::needless_pass_by_value)]
pub fn expiration_thread(
    updates: flume::Receiver<ExpirationUpdate>,
    sled: sled::Db,
) -> Result<(), sled::Error> {
    // expiring_keys will be maintained such that the soonest expiration is at the front and furthest in the future is at the back
    let mut tracked_keys = HashMap::<TreeKey, Timestamp>::new();
    let mut expiration_order = VecDeque::<TreeKey>::new();
    loop {
        let update = if expiration_order.is_empty() {
            // No keys are currently tracked for expiration. Block until we receive a key to track.
            match updates.recv() {
                Ok(update) => update,
                Err(_) => break,
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
                    let tree = sled.open_tree(key_to_remove.tree.as_bytes())?;
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

    use futures::Future;
    use pliantdb_core::test_util::TestDirectory;

    use super::*;

    async fn run_test<
        F: FnOnce(flume::Sender<ExpirationUpdate>, sled::Db) -> R + Send,
        R: Future<Output = anyhow::Result<()>> + Send,
    >(
        name: &str,
        test_contents: F,
    ) -> anyhow::Result<()> {
        let dir = TestDirectory::new(name);
        let sled = sled::open(&dir)?;

        let (sender, receiver) = flume::unbounded();
        let task_sled = sled.clone();
        let expiration_task =
            tokio::task::spawn_blocking(move || expiration_thread(receiver, task_sled));
        let checking_task = test_contents(sender, sled);

        // The expiration task is expected to run as long as the test_contents
        // future is running (or if the test drops the sender it can exit
        // early).
        let (r1, r2) = tokio::join!(expiration_task, checking_task);
        r1??;
        r2?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn basic_expiration() -> anyhow::Result<()> {
        run_test("kv-basic-expiration", |sender, sled| async move {
            let tree = sled.open_tree(b"atree")?;
            tree.insert(b"akey", b"somevalue")?;
            sender.send(ExpirationUpdate {
                tree_key: TreeKey {
                    tree: String::from("atree"),
                    key: String::from("akey"),
                },
                expiration: Some(Timestamp::now() + Duration::from_millis(100)),
            })?;
            tokio::time::sleep(Duration::from_millis(110)).await;
            assert!(tree.get(b"akey")?.is_none());

            Ok(())
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn updating_expiration() -> anyhow::Result<()> {
        run_test("kv-updating-expiration", |sender, sled| async move {
            let tree = sled.open_tree(b"atree")?;
            tree.insert(b"akey", b"somevalue")?;
            sender.send(ExpirationUpdate {
                tree_key: TreeKey {
                    tree: String::from("atree"),
                    key: String::from("akey"),
                },
                expiration: Some(Timestamp::now() + Duration::from_millis(100)),
            })?;
            sender.send(ExpirationUpdate {
                tree_key: TreeKey {
                    tree: String::from("atree"),
                    key: String::from("akey"),
                },
                expiration: Some(Timestamp::now() + Duration::from_secs(1)),
            })?;
            tokio::time::sleep(Duration::from_millis(105)).await;
            assert!(tree.get(b"akey")?.is_some());
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(tree.get(b"akey")?.is_none());

            Ok(())
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn multiple_keys_expiration() -> anyhow::Result<()> {
        run_test("kv-multiple-keys-expiration", |sender, sled| async move {
            let tree = sled.open_tree(b"atree")?;
            tree.insert(b"akey", b"somevalue")?;
            tree.insert(b"bkey", b"somevalue")?;
            sender.send(ExpirationUpdate {
                tree_key: TreeKey {
                    tree: String::from("atree"),
                    key: String::from("akey"),
                },
                expiration: Some(Timestamp::now() + Duration::from_millis(10)),
            })?;
            sender.send(ExpirationUpdate {
                tree_key: TreeKey {
                    tree: String::from("atree"),
                    key: String::from("bkey"),
                },
                expiration: Some(Timestamp::now() + Duration::from_secs(1)),
            })?;
            tokio::time::sleep(Duration::from_millis(15)).await;
            assert!(tree.get(b"akey")?.is_none());
            assert!(tree.get(b"bkey")?.is_some());
            tokio::time::sleep(Duration::from_secs(1)).await;
            assert!(tree.get(b"bkey")?.is_none());

            Ok(())
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn clearing_expiration() -> anyhow::Result<()> {
        run_test("kv-clearing-expiration", |sender, sled| async move {
            let tree = sled.open_tree(b"atree")?;
            tree.insert(b"akey", b"somevalue")?;
            sender.send(ExpirationUpdate {
                tree_key: TreeKey {
                    tree: String::from("atree"),
                    key: String::from("akey"),
                },
                expiration: Some(Timestamp::now() + Duration::from_millis(10)),
            })?;
            sender.send(ExpirationUpdate {
                tree_key: TreeKey {
                    tree: String::from("atree"),
                    key: String::from("akey"),
                },
                expiration: None,
            })?;
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(tree.get(b"akey")?.is_some());

            Ok(())
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn out_of_order_expiration() -> anyhow::Result<()> {
        run_test("kv-out-of-order-expiration", |sender, sled| async move {
            let tree = sled.open_tree(b"atree")?;
            tree.insert(b"akey", b"somevalue")?;
            tree.insert(b"bkey", b"somevalue")?;
            tree.insert(b"ckey", b"somevalue")?;
            sender.send(ExpirationUpdate {
                tree_key: TreeKey {
                    tree: String::from("atree"),
                    key: String::from("akey"),
                },
                expiration: Some(Timestamp::now() + Duration::from_secs(3)),
            })?;
            sender.send(ExpirationUpdate {
                tree_key: TreeKey {
                    tree: String::from("atree"),
                    key: String::from("ckey"),
                },
                expiration: Some(Timestamp::now() + Duration::from_secs(1)),
            })?;
            sender.send(ExpirationUpdate {
                tree_key: TreeKey {
                    tree: String::from("atree"),
                    key: String::from("bkey"),
                },
                expiration: Some(Timestamp::now() + Duration::from_secs(2)),
            })?;
            tokio::time::sleep(Duration::from_millis(1100)).await;
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
