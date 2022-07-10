use std::{collections::BTreeMap, marker::PhantomData, mem::size_of};

#[cfg(feature = "async")]
use bonsaidb_core::connection::AsyncConnection;
use bonsaidb_core::{
    connection::Connection,
    document::{BorrowedDocument, Emit},
    key::{time::TimestampAsNanoseconds, KeyEncoding},
    schema::{Collection, CollectionName, View, ViewMapResult, ViewSchema},
    transaction::{Operation, Transaction},
};
use derive_where::derive_where;
use serde::{Deserialize, Serialize};

use crate::{direct::BlockInfo, schema::file::File, FileConfig};

#[derive_where(Debug, Default)]
pub struct Block<Config>(PhantomData<Config>)
where
    Config: FileConfig;

impl<Config> Block<Config>
where
    Config: FileConfig,
{
    pub fn append<Database: Connection>(
        data: &[u8],
        file_id: u32,
        database: &Database,
    ) -> Result<(), bonsaidb_core::Error> {
        if !data.is_empty() {
            let mut tx = Transaction::new();
            let now = TimestampAsNanoseconds::now();
            // Verify the file exists as part of appending. If the file was
            // deleted out from underneath the appender, this will ensure no
            // blocks are orphaned.
            tx.push(Operation::check_document_exists::<File<Config>>(&file_id)?);

            let block_collection = Self::collection_name();
            for chunk in data.chunks(Config::BLOCK_SIZE) {
                let mut block =
                    Vec::with_capacity(chunk.len() + size_of::<u32>() + size_of::<i64>());
                block.extend(chunk);
                block.extend(file_id.to_be_bytes());
                block.extend(now.representation().to_be_bytes());
                tx.push(Operation::insert(block_collection.clone(), None, block));
            }

            tx.apply(database)?;
        }
        Ok(())
    }

    #[cfg(feature = "async")]
    pub async fn append_async<Database: AsyncConnection>(
        data: &[u8],
        file_id: u32,
        database: &Database,
    ) -> Result<(), bonsaidb_core::Error> {
        if !data.is_empty() {
            let mut tx = Transaction::new();
            let now = TimestampAsNanoseconds::now();
            // Verify the file exists as part of appending. If the file was
            // deleted out from underneath the appender, this will ensure no
            // blocks are orphaned.
            tx.push(Operation::check_document_exists::<File<Config>>(&file_id)?);

            let block_collection = Self::collection_name();
            for chunk in data.chunks(Config::BLOCK_SIZE) {
                let mut block =
                    Vec::with_capacity(chunk.len() + size_of::<u32>() + size_of::<i64>());
                block.extend(chunk);
                block.extend(file_id.to_be_bytes());
                block.extend(now.representation().to_be_bytes());
                tx.push(Operation::insert(block_collection.clone(), None, block));
            }

            tx.apply_async(database).await?;
        }
        Ok(())
    }

    pub fn load<
        'a,
        DocumentIds: IntoIterator<Item = &'a PrimaryKey, IntoIter = I> + Send + Sync,
        I: Iterator<Item = &'a PrimaryKey> + Send + Sync,
        PrimaryKey: for<'k> KeyEncoding<'k, u64> + 'a,
        Database: Connection,
    >(
        block_ids: DocumentIds,
        database: &Database,
    ) -> Result<BTreeMap<u64, Vec<u8>>, bonsaidb_core::Error> {
        database
            .collection::<Self>()
            .get_multiple(block_ids)?
            .into_iter()
            .map(|block| {
                let mut contents = block.contents.into_vec();
                contents.truncate(contents.len() - size_of::<u32>() - size_of::<i64>());
                block.header.id.deserialize().map(|id| (id, contents))
            })
            .collect()
    }

    #[cfg(feature = "async")]
    pub async fn load_async<
        'a,
        DocumentIds: IntoIterator<Item = &'a PrimaryKey, IntoIter = I> + Send + Sync,
        I: Iterator<Item = &'a PrimaryKey> + Send + Sync,
        PrimaryKey: for<'k> KeyEncoding<'k, u64> + 'a,
        Database: AsyncConnection,
    >(
        block_ids: DocumentIds,
        database: &Database,
    ) -> Result<BTreeMap<u64, Vec<u8>>, bonsaidb_core::Error> {
        database
            .collection::<Self>()
            .get_multiple(block_ids)
            .await?
            .into_iter()
            .map(|block| {
                let mut contents = block.contents.into_vec();
                contents.truncate(contents.len() - size_of::<u32>() - size_of::<i64>());
                block.header.id.deserialize().map(|id| (id, contents))
            })
            .collect()
    }

    pub(crate) fn for_file<Database: Connection>(
        file_id: u32,
        database: &Database,
    ) -> Result<Vec<BlockInfo>, bonsaidb_core::Error> {
        let mut blocks = database
            .view::<ByFile<Config>>()
            .with_key(&file_id)
            .query()?
            .into_iter()
            .map(|mapping| BlockInfo {
                header: mapping.source,
                length: usize::try_from(mapping.value.length).unwrap(),
                timestamp: mapping.value.timestamp.unwrap(),
                offset: 0,
            })
            .collect::<Vec<_>>();
        blocks.sort_by(|a, b| a.header.id.cmp(&b.header.id));
        let mut offset = 0;
        for block in &mut blocks {
            block.offset = offset;
            offset += u64::try_from(block.length).unwrap();
        }
        Ok(blocks)
    }

    pub(crate) fn summary_for_file<Database: Connection>(
        file_id: u32,
        database: &Database,
    ) -> Result<BlockAppendInfo, bonsaidb_core::Error> {
        database
            .view::<ByFile<Config>>()
            .with_key(&file_id)
            .reduce()
    }

    #[cfg(feature = "async")]
    pub(crate) async fn summary_for_file_async<Database: AsyncConnection>(
        file_id: u32,
        database: &Database,
    ) -> Result<BlockAppendInfo, bonsaidb_core::Error> {
        database
            .view::<ByFile<Config>>()
            .with_key(&file_id)
            .reduce()
            .await
    }

    pub(crate) fn summary_for_ids<'a, Database: Connection, Iter: IntoIterator<Item = &'a u32>>(
        file_ids: Iter,
        database: &'a Database,
    ) -> Result<BlockAppendInfo, bonsaidb_core::Error> {
        database
            .view::<ByFile<Config>>()
            .with_keys(file_ids)
            .reduce()
    }

    #[cfg(feature = "async")]
    pub(crate) async fn summary_for_ids_async<
        'a,
        Database: AsyncConnection,
        Iter: IntoIterator<Item = &'a u32>,
    >(
        file_ids: Iter,
        database: &'a Database,
    ) -> Result<BlockAppendInfo, bonsaidb_core::Error> {
        database
            .view::<ByFile<Config>>()
            .with_keys(file_ids)
            .reduce()
            .await
    }

    #[cfg(feature = "async")]
    pub(crate) async fn for_file_async<Database: AsyncConnection>(
        file_id: u32,
        database: &Database,
    ) -> Result<Vec<BlockInfo>, bonsaidb_core::Error> {
        let mut blocks = database
            .view::<ByFile<Config>>()
            .with_key(&file_id)
            .query()
            .await?
            .into_iter()
            .map(|mapping| BlockInfo {
                header: mapping.source,
                length: usize::try_from(mapping.value.length).unwrap(),
                timestamp: mapping.value.timestamp.unwrap(),
                offset: 0,
            })
            .collect::<Vec<_>>();
        blocks.sort_by(|a, b| a.header.id.cmp(&b.header.id));
        let mut offset = 0;
        for block in &mut blocks {
            block.offset = offset;
            offset += u64::try_from(block.length).unwrap();
        }
        Ok(blocks)
    }

    pub fn delete_for_file<Database: Connection>(
        file_id: u32,
        database: &Database,
    ) -> Result<(), bonsaidb_core::Error> {
        database
            .view::<ByFile<Config>>()
            .with_key(&file_id)
            .delete_docs()?;
        Ok(())
    }

    #[cfg(feature = "async")]
    pub async fn delete_for_file_async<Database: AsyncConnection>(
        file_id: u32,
        database: &Database,
    ) -> Result<(), bonsaidb_core::Error> {
        database
            .view::<ByFile<Config>>()
            .with_key(&file_id)
            .delete_docs()
            .await?;
        Ok(())
    }
}

impl<Config> Collection for Block<Config>
where
    Config: FileConfig,
{
    type PrimaryKey = u64;

    fn collection_name() -> CollectionName {
        Config::blocks_name()
    }

    fn define_views(
        schema: &mut bonsaidb_core::schema::Schematic,
    ) -> Result<(), bonsaidb_core::Error> {
        schema.define_view(ByFile::<Config>::default())?;

        Ok(())
    }
}

#[derive_where(Clone, Debug, Default)]
#[derive(View)]
#[view(name = "by-file", collection = Block<Config>, key = u32, value = BlockAppendInfo)]
#[view(core = bonsaidb_core)]
struct ByFile<Config>(PhantomData<Config>)
where
    Config: FileConfig;

impl<Config> ViewSchema for ByFile<Config>
where
    Config: FileConfig,
{
    type View = Self;

    fn version(&self) -> u64 {
        2
    }

    fn map(&self, doc: &BorrowedDocument<'_>) -> ViewMapResult<Self::View> {
        let timestamp_offset = doc.contents.len() - size_of::<i64>();
        let file_id_offset = timestamp_offset - size_of::<u32>();

        let mut file_id = [0; size_of::<u32>()];
        file_id.copy_from_slice(&doc.contents[file_id_offset..timestamp_offset]);
        let file_id = u32::from_be_bytes(file_id);

        let mut timestamp = [0; size_of::<i64>()];
        timestamp.copy_from_slice(&doc.contents[timestamp_offset..]);
        let timestamp = TimestampAsNanoseconds::from_representation(i64::from_be_bytes(timestamp));

        let length = u64::try_from(file_id_offset).unwrap();

        doc.header.emit_key_and_value(
            file_id,
            BlockAppendInfo {
                length,
                timestamp: Some(timestamp),
            },
        )
    }

    fn reduce(
        &self,
        mappings: &[<Self::View as View>::Value],
        _rereduce: bool,
    ) -> Result<<Self::View as View>::Value, bonsaidb_core::Error> {
        Ok(BlockAppendInfo {
            length: mappings.iter().map(|info| info.length).sum(),
            timestamp: mappings.iter().filter_map(|info| info.timestamp).max(),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockAppendInfo {
    pub length: u64,
    pub timestamp: Option<TimestampAsNanoseconds>,
}
