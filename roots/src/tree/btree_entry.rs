use std::{fmt::Debug, marker::PhantomData};

use byteorder::{ReadBytesExt, WriteBytesExt};
use futures::{future::LocalBoxFuture, FutureExt};

use super::{
    btree_root::{ChangeResult, EntryChanges},
    interior::Interior,
    key_entry::KeyEntry,
    modify::Modification,
    read_chunk,
    serialization::BinarySerialization,
    PagedWriter,
};
use crate::{AsyncFile, Buffer, ChunkCache, Error, Vault};

/// A B-Tree entry that stores a list of key-`I` pairs.
#[derive(Clone, Debug)]
pub enum BTreeEntry<I, R> {
    /// An inline value. Overall, the B-Tree entry is a key-value pair.
    Leaf(Vec<KeyEntry<I>>),
    /// An interior node that contains pointers to other nodes.
    Interior(Vec<Interior<R>>),
}

impl<I, R> Default for BTreeEntry<I, R> {
    fn default() -> Self {
        Self::Leaf(Vec::new())
    }
}

pub trait Reducer<I> {
    fn reduce(indexes: &[&I]) -> Self;

    fn rereduce(reductions: &[&Self]) -> Self;
}

impl<I> Reducer<I> for () {
    fn reduce(_indexes: &[&I]) -> Self {}

    fn rereduce(_reductions: &[&Self]) -> Self {}
}

pub struct ModificationContext<T, F, I, Indexer, Loader>
where
    Indexer: for<'tf> Fn(
        &'tf Buffer<'static>,
        &'tf T,
        Option<&'tf I>,
        &'tf mut EntryChanges,
        &'tf mut PagedWriter<'_, F>,
    ) -> LocalBoxFuture<'tf, Result<Option<I>, Error>>,
    Loader: for<'tf> Fn(
        &'tf I,
        &'tf mut PagedWriter<'_, F>,
    ) -> LocalBoxFuture<'tf, Result<Option<T>, Error>>,
{
    pub current_order: usize,
    pub indexer: Indexer,
    pub loader: Loader,
    pub _phantom: PhantomData<(T, F, I)>,
}

#[allow(clippy::future_not_send)]
impl<I, R> BTreeEntry<I, R>
where
    I: Clone + BinarySerialization + Debug + 'static,
    R: Clone + Reducer<I> + BinarySerialization + Debug + 'static,
{
    #[allow(clippy::too_many_lines)] // TODO refactor
    pub fn modify<'f, F, T, Indexer, Loader>(
        &'f self,
        modification: &'f mut Modification<'static, T>,
        context: &'f ModificationContext<T, F, I, Indexer, Loader>,
        max_key: Option<&'f Buffer<'static>>,
        changes: &'f mut EntryChanges,
        writer: &'f mut PagedWriter<'_, F>,
    ) -> LocalBoxFuture<'f, Result<ChangeResult<I, R>, Error>>
    where
        F: AsyncFile,
        T: 'static,
        Indexer: for<'tf> Fn(
                &'tf Buffer<'static>,
                &'tf T,
                Option<&'tf I>,
                &'tf mut EntryChanges,
                &'tf mut PagedWriter<'_, F>,
            ) -> LocalBoxFuture<'tf, Result<Option<I>, Error>>
            + 'f,
        Loader: for<'tf> Fn(
                &'tf I,
                &'tf mut PagedWriter<'_, F>,
            ) -> LocalBoxFuture<'tf, Result<Option<T>, Error>>
            + 'f,
    {
        async move {
            match self {
                BTreeEntry::Leaf(children) => {
                    let mut children = children.iter().map(KeyEntry::clone).collect::<Vec<_>>();
                    let mut last_index = 0;
                    while !modification.keys.is_empty() && children.len() < context.current_order {
                        let key = modification.keys.pop().unwrap();
                        match children[last_index..].binary_search_by(|child| child.key.cmp(&key)) {
                            Ok(matching_index) => {
                                last_index += matching_index;
                                let index = match &mut modification.operation {
                                    super::modify::Operation::Set(value) => {
                                        (context.indexer)(
                                            &key,
                                            value,
                                            Some(&children[last_index].index),
                                            changes,
                                            writer,
                                        )
                                        .await?
                                    }
                                    super::modify::Operation::Remove => None,
                                    super::modify::Operation::CompareSwap(callback) => {
                                        if let Some(value) = callback(&key, None) {
                                            (context.indexer)(
                                                &key,
                                                &value,
                                                Some(&children[last_index].index),
                                                changes,
                                                writer,
                                            )
                                            .await?
                                        } else {
                                            None
                                        }
                                    }
                                };

                                match index {
                                    Some(index) => {
                                        children[last_index] = KeyEntry { key, index };
                                    }
                                    None => {
                                        children.remove(last_index);
                                    }
                                }
                            }
                            Err(insert_at) => {
                                last_index += insert_at;
                                if last_index == children.len()
                                    && max_key.map(|max_key| &key > max_key).unwrap_or_default()
                                {
                                    break;
                                }
                                let index = match &mut modification.operation {
                                    super::modify::Operation::Set(new_value) => {
                                        (context.indexer)(&key, new_value, None, changes, writer)
                                            .await?
                                    }
                                    super::modify::Operation::Remove => {
                                        // The key doesn't exist, so a remove is a no-op.
                                        None
                                    }
                                    super::modify::Operation::CompareSwap(callback) => {
                                        if let Some(value) = callback(&key, None) {
                                            (context.indexer)(&key, &value, None, changes, writer)
                                                .await?
                                        } else {
                                            None
                                        }
                                    }
                                };
                                // New node.
                                if let Some(index) = index {
                                    children.insert(last_index, KeyEntry { key, index });
                                }
                            }
                        }
                        debug_assert!(children.windows(2).all(|w| w[0].key < w[1].key));
                    }

                    if children.len() >= context.current_order {
                        // We need to split this leaf into two leafs, moving a new interior node using the middle element.
                        let midpoint = children.len() / 2;
                        let (lower_half, upper_half) = children.split_at(midpoint);

                        Ok(ChangeResult::Split(
                            Self::Leaf(lower_half.to_vec()),
                            Self::Leaf(upper_half.to_vec()),
                        ))
                    } else {
                        Ok(ChangeResult::Replace(Self::Leaf(children)))
                    }
                }
                BTreeEntry::Interior(children) => {
                    let mut children = children.clone();
                    let mut last_index = 0;
                    while let Some(key) = modification.keys.first().cloned() {
                        if children.len() >= context.current_order {
                            break;
                        }
                        let containing_node_index = children[last_index..]
                            .binary_search_by(|child| child.key.cmp(&key))
                            .map_or_else(
                                |not_found| {
                                    if not_found + last_index == children.len() {
                                        // If we can't find a key less than what would fit
                                        // within our children, this key will become the new key
                                        // of the last child.
                                        not_found - 1
                                    } else {
                                        not_found
                                    }
                                },
                                |found| found,
                            );

                        last_index += containing_node_index;
                        let child = &children[last_index];
                        let entry =
                            Self::deserialize_from(&mut writer.read_chunk(child.position).await?)?;
                        match entry
                            .modify(
                                modification,
                                context,
                                Some(entry.max_key()),
                                changes,
                                writer,
                            )
                            .await?
                        {
                            ChangeResult::Unchanged => unreachable!(),
                            ChangeResult::Replace(new_node) => {
                                let child_position =
                                    writer.write_chunk(&new_node.serialize()?).await?;
                                children[last_index] = Interior {
                                    key: new_node.max_key().clone(),
                                    position: child_position,
                                    stats: new_node.stats(),
                                };
                            }
                            ChangeResult::Split(lower, upper) => {
                                // Write the two new children
                                let lower_position =
                                    writer.write_chunk(&lower.serialize()?).await?;
                                let upper_position =
                                    writer.write_chunk(&upper.serialize()?).await?;
                                // Replace the original child with the lower entry.
                                children[last_index] = Interior {
                                    key: lower.max_key().clone(),
                                    position: lower_position,
                                    stats: lower.stats(),
                                };
                                // Insert the upper entry at the next position.
                                children.insert(
                                    last_index + 1,
                                    Interior {
                                        key: upper.max_key().clone(),
                                        position: upper_position,
                                        stats: upper.stats(),
                                    },
                                );
                            }
                        };
                        debug_assert!(children.windows(2).all(|w| w[0].key < w[1].key));
                    }

                    if children.len() >= context.current_order {
                        let midpoint = children.len() / 2;
                        let (_, upper_half) = children.split_at(midpoint);

                        // TODO this re-clones the upper-half children, but splitting a vec
                        // without causing multiple copies of data seems
                        // impossible without unsafe.
                        let upper_half = upper_half.to_vec();
                        assert_eq!(midpoint + upper_half.len(), children.len());
                        children.truncate(midpoint);

                        Ok(ChangeResult::Split(
                            Self::Interior(children),
                            Self::Interior(upper_half),
                        ))
                    } else {
                        Ok(ChangeResult::Replace(Self::Interior(children)))
                    }
                }
            }
        }
        .boxed_local()
    }

    pub fn stats(&self) -> R {
        match self {
            BTreeEntry::Leaf(children) => {
                R::reduce(&children.iter().map(|c| &c.index).collect::<Vec<_>>())
            }
            BTreeEntry::Interior(children) => {
                R::rereduce(&children.iter().map(|c| &c.stats).collect::<Vec<_>>())
            }
        }
    }

    pub fn max_key(&self) -> &Buffer<'static> {
        match self {
            BTreeEntry::Leaf(children) => &children.last().unwrap().key,
            BTreeEntry::Interior(children) => &children.last().unwrap().key,
        }
    }

    pub fn get<'f, F: AsyncFile>(
        &'f self,
        key: &'f [u8],
        file: &'f mut F,
        vault: Option<&'f dyn Vault>,
        cache: Option<&'f ChunkCache>,
    ) -> LocalBoxFuture<'f, Result<Option<I>, Error>> {
        async move {
            match self {
                BTreeEntry::Leaf(children) => {
                    match children.binary_search_by(|child| (&*child.key).cmp(key)) {
                        Ok(matching) => {
                            let entry = &children[matching];
                            Ok(Some(entry.index.clone()))
                        }
                        Err(_) => Ok(None),
                    }
                }
                BTreeEntry::Interior(children) => {
                    let containing_node_index = children
                        .binary_search_by(|child| (&*child.key).cmp(key))
                        .unwrap_or_else(|not_found| not_found);

                    // This isn't guaranteed to succeed because we add one. If
                    // the key being searched for isn't contained, it will be
                    // greater than any of the node's keys.
                    if let Some(child) = children.get(containing_node_index) {
                        let entry = Self::deserialize_from(
                            &mut read_chunk(child.position, file, vault, cache).await?,
                        )?;
                        entry.get(key, file, vault, cache).await
                    } else {
                        Ok(None)
                    }
                }
            }
        }
        .boxed_local()
    }

    #[allow(clippy::too_many_lines)]
    pub async fn split_root<F: AsyncFile>(
        &mut self,
        lower: Self,
        upper: Self,
        writer: &mut PagedWriter<'_, F>,
    ) -> Result<(), Error> {
        // Write the two interiors as chunks
        let lower_half_position = writer.write_chunk(&lower.serialize()?).await?;
        let upper_half_position = writer.write_chunk(&upper.serialize()?).await?;

        // Regardless of what our current type is, the root will always be split
        // into interior nodes.
        *self = Self::Interior(vec![
            Interior {
                key: lower.max_key().clone(),
                position: lower_half_position,
                stats: lower.stats(),
            },
            Interior {
                key: upper.max_key().clone(),
                position: upper_half_position,
                stats: upper.stats(),
            },
        ]);
        Ok(())
    }
}

impl<I: BinarySerialization, R: BinarySerialization> BinarySerialization for BTreeEntry<I, R> {
    fn serialize_to<W: WriteBytesExt>(&self, writer: &mut W) -> Result<usize, Error> {
        let mut bytes_written = 0;
        // The next byte determines the node type.
        match &self {
            Self::Leaf(leafs) => {
                debug_assert!(leafs.windows(2).all(|w| w[0].key < w[1].key));
                writer.write_u8(1)?;
                bytes_written += 1;
                for leaf in leafs {
                    bytes_written += leaf.serialize_to(writer)?;
                }
            }
            Self::Interior(interiors) => {
                debug_assert!(interiors.windows(2).all(|w| w[0].key < w[1].key));
                writer.write_u8(0)?;
                bytes_written += 1;
                for interior in interiors {
                    bytes_written += interior.serialize_to(writer)?;
                }
            }
        }

        Ok(bytes_written)
    }

    fn deserialize_from(reader: &mut Buffer<'static>) -> Result<Self, Error> {
        let node_header = reader.read_u8()?;
        match node_header {
            0 => {
                // Interior
                let mut nodes = Vec::new();
                while !reader.is_empty() {
                    nodes.push(Interior::deserialize_from(reader)?);
                }
                Ok(Self::Interior(nodes))
            }
            1 => {
                // Leaf
                let mut nodes = Vec::new();
                while !reader.is_empty() {
                    nodes.push(KeyEntry::deserialize_from(reader)?);
                }
                Ok(Self::Leaf(nodes))
            }
            _ => Err(Error::data_integrity("invalid node header")),
        }
    }
}
