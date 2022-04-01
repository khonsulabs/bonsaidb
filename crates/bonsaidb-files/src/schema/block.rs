use std::{collections::BTreeMap, marker::PhantomData, mem::size_of};

use bonsaidb_core::{
    connection::Connection,
    document::{BorrowedDocument, Emit},
    schema::{Collection, CollectionName, View, ViewMapResult, ViewSchema},
};
use derive_where::derive_where;

use crate::{BlockInfo, FileConfig};

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
            for chunk in data.chunks(Config::BLOCK_SIZE) {
                let mut block = Vec::with_capacity(chunk.len() + size_of::<u32>());
                block.extend(chunk);
                block.extend(file_id.to_be_bytes());
                database.collection::<Self>().push_bytes(block)?;
            }
        }
        Ok(())
    }

    pub fn load<
        Ids: IntoIterator<Item = u64, IntoIter = IdsIter> + Send + Sync,
        IdsIter: Iterator<Item = u64> + Send + Sync,
        Database: Connection,
    >(
        block_ids: Ids,
        database: &Database,
    ) -> Result<BTreeMap<u64, Vec<u8>>, bonsaidb_core::Error> {
        database
            .collection::<Self>()
            .get_multiple(block_ids)?
            .into_iter()
            .map(|block| {
                let mut contents = block.contents.into_vec();
                contents.truncate(contents.len() - 4);
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
            .with_key(file_id)
            .query()?
            .into_iter()
            .map(|mapping| BlockInfo {
                id: mapping.source.id.deserialize().unwrap(),
                length: usize::try_from(mapping.value).unwrap(),
                offset: 0,
            })
            .collect::<Vec<_>>();
        blocks.sort_by(|a, b| a.id.cmp(&b.id));
        let mut offset = 0;
        for block in &mut blocks {
            block.offset = offset;
            offset += u64::try_from(block.length).unwrap();
        }
        Ok(blocks)
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
#[view(name = "by-name", collection = Block<Config>, key = u32, value = u32)]
#[view(core = bonsaidb_core)]
struct ByFile<Config>(PhantomData<Config>)
where
    Config: FileConfig;

impl<Config> ViewSchema for ByFile<Config>
where
    Config: FileConfig,
{
    type View = Self;

    fn map(&self, doc: &BorrowedDocument<'_>) -> ViewMapResult<Self::View> {
        let mut file_id = [0; 4];
        let length_offset = doc.contents.len() - 4;
        file_id.copy_from_slice(&doc.contents[length_offset..]);
        let file_id = u32::from_be_bytes(file_id);
        let length = u32::try_from(doc.contents[..length_offset].len()).unwrap();

        doc.header.emit_key_and_value(file_id, length)
    }
}
