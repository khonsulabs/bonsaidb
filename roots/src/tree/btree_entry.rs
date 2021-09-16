use std::{fmt::Debug, marker::PhantomData};

use byteorder::{ReadBytesExt, WriteBytesExt};

use super::{
    btree_root::{ChangeResult, EntryChanges},
    interior::Interior,
    key_entry::KeyEntry,
    modify::{Modification, Operation},
    read_chunk,
    serialization::BinarySerialization,
    PagedWriter,
};
use crate::{tree::interior::Pointer, Buffer, ChunkCache, Error, ManagedFile, Vault};

/// A B-Tree entry that stores a list of key-`I` pairs.
#[derive(Clone, Debug)]
pub enum BTreeEntry<I, R> {
    Uninitialized,
    /// An inline value. Overall, the B-Tree entry is a key-value pair.
    Leaf(Vec<KeyEntry<I>>),
    /// An interior node that contains pointers to other nodes.
    Interior(Vec<Interior<I, R>>),
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
    Indexer: Fn(
        &Buffer<'static>,
        &T,
        Option<&I>,
        &mut EntryChanges,
        &mut PagedWriter<'_, F>,
    ) -> Result<KeyOperation<I>, Error>,
    Loader: Fn(&I, &mut PagedWriter<'_, F>) -> Result<Option<T>, Error>,
{
    pub current_order: usize,
    pub indexer: Indexer,
    pub loader: Loader,
    pub _phantom: PhantomData<(T, F, I)>,
}

impl<I, R> BTreeEntry<I, R>
where
    I: Clone + BinarySerialization + Debug + 'static,
    R: Clone + Reducer<I> + BinarySerialization + Debug + 'static,
{
    pub fn modify<F, T, Indexer, Loader>(
        &mut self,
        modification: &mut Modification<'static, T>,
        context: &ModificationContext<T, F, I, Indexer, Loader>,
        max_key: Option<&Buffer<'static>>,
        changes: &mut EntryChanges,
        writer: &mut PagedWriter<'_, F>,
    ) -> Result<ChangeResult<I, R>, Error>
    where
        F: ManagedFile,
        T: 'static,
        Indexer: Fn(
            &Buffer<'static>,
            &T,
            Option<&I>,
            &mut EntryChanges,
            &mut PagedWriter<'_, F>,
        ) -> Result<KeyOperation<I>, Error>,
        Loader: Fn(&I, &mut PagedWriter<'_, F>) -> Result<Option<T>, Error>,
    {
        match self {
            BTreeEntry::Leaf(children) => {
                Self::modify_leaf(children, modification, context, max_key, changes, writer)?;

                if children.len() >= context.current_order {
                    // We need to split this leaf into two leafs, moving a new interior node using the middle element.
                    let midpoint = children.len() / 2;
                    let (_, upper_half) = children.split_at(midpoint);
                    let upper_half = Self::Leaf(upper_half.to_vec());
                    children.truncate(midpoint);

                    Ok(ChangeResult::Split(upper_half))
                } else {
                    Ok(ChangeResult::Changed)
                }
            }
            BTreeEntry::Interior(children) => {
                Self::modify_interior(children, modification, context, max_key, changes, writer)?;

                if children.len() >= context.current_order {
                    let midpoint = children.len() / 2;
                    let (_, upper_half) = children.split_at(midpoint);

                    // TODO this re-clones the upper-half children, but splitting a vec
                    // without causing multiple copies of data seems
                    // impossible without unsafe.
                    let upper_half = upper_half.to_vec();
                    debug_assert_eq!(midpoint + upper_half.len(), children.len());
                    children.truncate(midpoint);

                    Ok(ChangeResult::Split(Self::Interior(upper_half)))
                } else {
                    Ok(ChangeResult::Changed)
                }
            }
            BTreeEntry::Uninitialized => unreachable!(),
        }
    }

    fn modify_leaf<F, T, Indexer, Loader>(
        children: &mut Vec<KeyEntry<I>>,
        modification: &mut Modification<'static, T>,
        context: &ModificationContext<T, F, I, Indexer, Loader>,
        max_key: Option<&Buffer<'static>>,
        changes: &mut EntryChanges,
        writer: &mut PagedWriter<'_, F>,
    ) -> Result<(), Error>
    where
        F: ManagedFile,
        T: 'static,
        Indexer: Fn(
            &Buffer<'static>,
            &T,
            Option<&I>,
            &mut EntryChanges,
            &mut PagedWriter<'_, F>,
        ) -> Result<KeyOperation<I>, Error>,
        Loader: Fn(&I, &mut PagedWriter<'_, F>) -> Result<Option<T>, Error>,
    {
        let mut last_index = 0;
        while !modification.keys.is_empty() && children.len() < context.current_order {
            let key = modification.keys.last().unwrap();
            let search_result = children[last_index..].binary_search_by(|child| child.key.cmp(key));
            match search_result {
                Ok(matching_index) => {
                    let key = modification.keys.pop().unwrap();
                    last_index += matching_index;
                    let index = match &mut modification.operation {
                        Operation::Set(value) => (context.indexer)(
                            &key,
                            value,
                            Some(&children[last_index].index),
                            changes,
                            writer,
                        )?,
                        Operation::SetEach(values) => (context.indexer)(
                            &key,
                            &values.pop().ok_or_else(|| {
                                Error::message("need the same number of keys as values")
                            })?,
                            Some(&children[last_index].index),
                            changes,
                            writer,
                        )?,

                        Operation::Remove => KeyOperation::Remove,
                        Operation::CompareSwap(callback) => {
                            let current_index = &children[last_index].index;
                            let existing_value = (context.loader)(current_index, writer)?;
                            match callback(&key, existing_value) {
                                KeyOperation::Skip => KeyOperation::Skip,
                                KeyOperation::Set(new_value) => (context.indexer)(
                                    &key,
                                    &new_value,
                                    Some(current_index),
                                    changes,
                                    writer,
                                )?,
                                KeyOperation::Remove => KeyOperation::Remove,
                            }
                        }
                    };

                    match index {
                        KeyOperation::Skip => {}
                        KeyOperation::Set(index) => {
                            children[last_index] = KeyEntry { key, index };
                        }
                        KeyOperation::Remove => {
                            children.remove(last_index);
                        }
                    }
                }
                Err(insert_at) => {
                    last_index += insert_at;
                    if max_key.map(|max_key| key > max_key).unwrap_or_default() {
                        break;
                    }
                    let key = modification.keys.pop().unwrap();
                    let index = match &mut modification.operation {
                        Operation::Set(new_value) => {
                            (context.indexer)(&key, new_value, None, changes, writer)?
                        }
                        Operation::SetEach(new_values) => (context.indexer)(
                            &key,
                            &new_values.pop().ok_or_else(|| {
                                Error::message("need the same number of keys as values")
                            })?,
                            None,
                            changes,
                            writer,
                        )?,
                        Operation::Remove => {
                            // The key doesn't exist, so a remove is a no-op.
                            KeyOperation::Remove
                        }
                        Operation::CompareSwap(callback) => match callback(&key, None) {
                            KeyOperation::Skip => KeyOperation::Skip,
                            KeyOperation::Set(new_value) => {
                                (context.indexer)(&key, &new_value, None, changes, writer)?
                            }
                            KeyOperation::Remove => KeyOperation::Remove,
                        },
                    };
                    // New node.
                    match index {
                        KeyOperation::Set(index) => {
                            if children.capacity() < children.len() + 1 {
                                children.reserve(context.current_order - children.len());
                            }
                            children.insert(last_index, KeyEntry { key, index });
                        }
                        KeyOperation::Skip | KeyOperation::Remove => {}
                    }
                }
            }
            debug_assert!(
                children.windows(2).all(|w| w[0].key < w[1].key),
                "children aren't sorted: {:?}",
                children
            );
        }
        Ok(())
    }

    pub fn modify_interior<F, T, Indexer, Loader>(
        children: &mut Vec<Interior<I, R>>,
        modification: &mut Modification<'static, T>,
        context: &ModificationContext<T, F, I, Indexer, Loader>,
        max_key: Option<&Buffer<'static>>,
        changes: &mut EntryChanges,
        writer: &mut PagedWriter<'_, F>,
    ) -> Result<(), Error>
    where
        F: ManagedFile,
        T: 'static,
        Indexer: Fn(
            &Buffer<'static>,
            &T,
            Option<&I>,
            &mut EntryChanges,
            &mut PagedWriter<'_, F>,
        ) -> Result<KeyOperation<I>, Error>,
        Loader: Fn(&I, &mut PagedWriter<'_, F>) -> Result<Option<T>, Error>,
    {
        let mut last_index = 0;
        while let Some(key) = modification.keys.last().cloned() {
            if children.len() >= context.current_order
                || max_key.map(|max_key| &key > max_key).unwrap_or_default()
            {
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
            let child = &mut children[last_index];
            child.position.load(writer, context.current_order)?;
            let child_entry = child.position.get_mut().unwrap();
            match child_entry
                // TODO evaluate whether Some(key) is right here -- shouldn't it be the max of key/child.key?
                .modify(modification, context, Some(&key), changes, writer)?
            {
                ChangeResult::Unchanged => unreachable!(),
                ChangeResult::Changed => {
                    // let child_bytes = new_node.serialize(writer).await?;
                    // let child_position = writer.write_chunk(&child_bytes).await?;
                    // let node_ref = BTreeEntryRef::new(new_node, &context.slab);

                    // children[last_index] = Interior::from(new_node);
                    children[last_index].key = child_entry.max_key().clone();
                }
                ChangeResult::Split(upper) => {
                    // Write the two new children
                    // let lower_bytes = lower.serialize(writer).await?;
                    // let lower_position = writer.write_chunk(&lower_bytes).await?;
                    // let upper_bytes = upper.serialize(writer).await?;
                    // let upper_position = writer.write_chunk(&upper_bytes).await?;
                    // Replace the original child with the lower entry.
                    // children[last_index] = Interior::from(lower);
                    // Insert the upper entry at the next position.
                    children[last_index].key = child_entry.max_key().clone();
                    if children.capacity() < children.len() + 1 {
                        children.reserve(context.current_order - children.len());
                    }
                    children.insert(last_index + 1, Interior::from(upper));
                }
            };
            debug_assert!(children.windows(2).all(|w| w[0].key < w[1].key));
        }
        Ok(())
    }

    pub fn split_root(&mut self, upper: Self) {
        let mut lower = Self::Uninitialized;
        std::mem::swap(self, &mut lower);
        *self = Self::Interior(vec![Interior::from(lower), Interior::from(upper)]);
    }

    pub fn stats(&self) -> R {
        match self {
            BTreeEntry::Leaf(children) => {
                R::reduce(&children.iter().map(|c| &c.index).collect::<Vec<_>>())
            }
            BTreeEntry::Interior(children) => {
                R::rereduce(&children.iter().map(|c| &c.stats).collect::<Vec<_>>())
            }
            BTreeEntry::Uninitialized => unreachable!(),
        }
    }

    pub fn max_key(&self) -> &Buffer<'static> {
        match self {
            BTreeEntry::Leaf(children) => &children.last().unwrap().key,
            BTreeEntry::Interior(children) => &children.last().unwrap().key,
            BTreeEntry::Uninitialized => unreachable!(),
        }
    }

    #[cfg_attr(
        feature = "tracing",
        tracing::instrument(skip(self, file, vault, cache))
    )]
    pub fn get<F: ManagedFile>(
        &self,
        key: &[u8],
        file: &mut F,
        vault: Option<&dyn Vault>,
        cache: Option<&ChunkCache>,
    ) -> Result<Option<I>, Error> {
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
                    match &child.position {
                        Pointer::OnDisk(position) => {
                            let entry = Self::deserialize_from(
                                &mut read_chunk(*position, file, vault, cache)?,
                                children.len(),
                            )?;
                            entry.get(key, file, vault, cache)
                        }
                        Pointer::Loaded(entry) => entry.get(key, file, vault, cache),
                    }
                } else {
                    Ok(None)
                }
            }
            BTreeEntry::Uninitialized => unreachable!(),
        }
    }
}

impl<
        I: Clone + BinarySerialization + Debug + 'static,
        R: Reducer<I> + Clone + BinarySerialization + Debug + 'static,
    > BinarySerialization for BTreeEntry<I, R>
{
    fn serialize_to<W: WriteBytesExt, F: ManagedFile>(
        &mut self,
        writer: &mut W,
        paged_writer: &mut PagedWriter<'_, F>,
    ) -> Result<usize, Error> {
        let mut bytes_written = 0;
        // The next byte determines the node type.
        match self {
            Self::Leaf(leafs) => {
                debug_assert!(leafs.windows(2).all(|w| w[0].key < w[1].key));
                writer.write_u8(1)?;
                bytes_written += 1;
                for leaf in leafs {
                    bytes_written += leaf.serialize_to(writer, paged_writer)?;
                }
            }
            Self::Interior(interiors) => {
                debug_assert!(interiors.windows(2).all(|w| w[0].key < w[1].key));
                writer.write_u8(0)?;
                bytes_written += 1;
                for interior in interiors {
                    bytes_written += interior.serialize_to(writer, paged_writer)?;
                }
            }
            Self::Uninitialized => unreachable!(),
        }

        Ok(bytes_written)
    }

    fn deserialize_from(reader: &mut Buffer<'_>, current_order: usize) -> Result<Self, Error> {
        let node_header = reader.read_u8()?;
        match node_header {
            0 => {
                // Interior
                let mut nodes = Vec::new();
                nodes.reserve(current_order);
                while !reader.is_empty() {
                    nodes.push(Interior::deserialize_from(reader, current_order)?);
                }
                Ok(Self::Interior(nodes))
            }
            1 => {
                // Leaf
                let mut nodes = Vec::new();
                nodes.reserve(current_order);
                while !reader.is_empty() {
                    nodes.push(KeyEntry::deserialize_from(reader, current_order)?);
                }
                Ok(Self::Leaf(nodes))
            }
            _ => Err(Error::data_integrity("invalid node header")),
        }
    }
}

/// An operation to perform on a key.
#[derive(Debug)]
pub enum KeyOperation<T> {
    /// Do not alter the key.
    Skip,
    /// Set the key to the new value.
    Set(T),
    /// Remove the key.
    Remove,
}
