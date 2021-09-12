use std::fmt::Debug;

use byteorder::{ReadBytesExt, WriteBytesExt};
use futures::{future::LocalBoxFuture, FutureExt};

use super::{
    btree_root::ChangeResult, interior::Interior, key_entry::KeyEntry, read_chunk,
    serialization::BinarySerialization, PagedWriter,
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

#[allow(clippy::future_not_send)]
impl<I, R> BTreeEntry<I, R>
where
    I: Clone + BinarySerialization + Debug,
    R: Clone + Reducer<I> + BinarySerialization + Debug,
{
    #[allow(clippy::too_many_lines)] // TODO refactor
    pub fn insert<'f, F: AsyncFile>(
        &'f self,
        key: Buffer,
        index: I,
        current_order: usize,
        writer: &'f mut PagedWriter<'_, F>,
    ) -> LocalBoxFuture<'f, Result<ChangeResult<I, R>, Error>> {
        async move {
            match self {
                BTreeEntry::Leaf(children) => {
                    let new_leaf = KeyEntry { key, index };
                    let mut children = children.iter().map(KeyEntry::clone).collect::<Vec<_>>();
                    match children.binary_search_by(|child| child.key.cmp(&new_leaf.key)) {
                        Ok(matching_index) => {
                            children[matching_index] = new_leaf;
                        }
                        Err(insert_at) => {
                            children.insert(insert_at, new_leaf);
                        }
                    }

                    if children.len() >= current_order {
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
                    let (containing_node_index, is_new_max) = children
                        .binary_search_by(|child| child.key.cmp(&key))
                        .map_or_else(
                            |not_found| {
                                if not_found == children.len() {
                                    // If we can't find a key less than what would fit
                                    // within our children, this key will become the new key
                                    // of the last child.
                                    (not_found - 1, true)
                                } else {
                                    (not_found, false)
                                }
                            },
                            |found| (found, false),
                        );

                    let child = &children[containing_node_index];
                    let entry =
                        Self::deserialize_from(&mut writer.read_chunk(child.position).await?)?;
                    let mut new_children = children.clone();
                    match entry
                        .insert(key.clone(), index, current_order, writer)
                        .await?
                    {
                        ChangeResult::Unchanged => unreachable!(),
                        ChangeResult::Replace(new_node) => {
                            let child_position = writer.write_chunk(&new_node.serialize()?).await?;
                            new_children[containing_node_index] = Interior {
                                key: if is_new_max {
                                    key.clone()
                                } else {
                                    child.key.clone()
                                },
                                position: child_position,
                                stats: new_node.stats(),
                            };
                        }
                        ChangeResult::Split(lower, upper) => {
                            // Write the two new children
                            let lower_position = writer.write_chunk(&lower.serialize()?).await?;
                            let upper_position = writer.write_chunk(&upper.serialize()?).await?;
                            // Replace the original child with the lower entry.
                            new_children[containing_node_index] = Interior {
                                key: lower.max_key().clone(),
                                position: lower_position,
                                stats: lower.stats(),
                            };
                            // Insert the upper entry at the next position.
                            new_children.insert(
                                containing_node_index + 1,
                                Interior {
                                    key: upper.max_key().clone(),
                                    position: upper_position,
                                    stats: upper.stats(),
                                },
                            );
                        }
                    };

                    if new_children.len() >= current_order {
                        let midpoint = new_children.len() / 2;
                        let (_, upper_half) = new_children.split_at(midpoint);

                        // TODO this re-clones the upper-half children, but splitting a vec
                        // without causing multiple copies of data seems
                        // impossible without unsafe.
                        let upper_half = upper_half.to_vec();
                        assert_eq!(midpoint + upper_half.len(), new_children.len());
                        new_children.truncate(midpoint);

                        Ok(ChangeResult::Split(
                            Self::Interior(new_children),
                            Self::Interior(upper_half),
                        ))
                    } else {
                        Ok(ChangeResult::Replace(Self::Interior(new_children)))
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

    pub fn max_key(&self) -> &Buffer {
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

    fn deserialize_from(reader: &mut Buffer) -> Result<Self, Error> {
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
