use std::{
    collections::VecDeque,
    io::{ErrorKind, Read, Seek, SeekFrom},
    marker::PhantomData,
};

use bonsaidb_core::{
    connection::Connection,
    document::CollectionDocument,
    schema::{CollectionName, Qualified, Schema, SchemaName, Schematic, SerializedCollection},
};

pub mod metadata;
mod schema;

use derive_where::derive_where;
use metadata::Permissions;

use crate::metadata::Metadata;

pub trait FileConfig: Sized + Send + Sync + 'static {
    const BLOCK_SIZE: usize;
    fn files_name() -> CollectionName;
    fn blocks_name() -> CollectionName;

    fn register_collections(schema: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
        schema.define_collection::<schema::file::File<Self>>()?;
        schema.define_collection::<schema::block::Block<Self>>()?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct BonsaiFiles;

impl FileConfig for BonsaiFiles {
    const BLOCK_SIZE: usize = 65_536;

    fn files_name() -> CollectionName {
        CollectionName::new("bonsaidb", "files")
    }
    fn blocks_name() -> CollectionName {
        CollectionName::new("bonsaidb", "blocks")
    }
}

impl Schema for BonsaiFiles {
    fn schema_name() -> SchemaName {
        SchemaName::new("bonsaidb", "files")
    }

    fn define_collections(schema: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
        Self::register_collections(schema)
    }
}

#[derive_where(Debug)]
pub struct File<Config: FileConfig = BonsaiFiles> {
    doc: CollectionDocument<schema::file::File<Config>>,
}

impl<Config> File<Config>
where
    Config: FileConfig,
{
    fn new_file<Database: Connection>(
        path: String,
        permissions: Option<Permissions>,
        contents: &[u8],
        database: &Database,
    ) -> Result<Self, bonsaidb_core::Error> {
        Ok(Self {
            doc: schema::file::File::create_file(path, permissions, contents, database)?,
        })
    }

    pub fn get<Database: Connection>(
        id: u32,
        database: &Database,
    ) -> Result<Option<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::get(id, database).map(|doc| doc.map(|doc| Self { doc }))
    }

    pub fn id(&self) -> u32 {
        self.doc.header.id
    }

    pub fn parent_id(&self) -> Option<u32> {
        self.doc.contents.parent
    }

    pub fn name(&self) -> &str {
        &self.doc.contents.name
    }

    pub fn metadata(&self) -> &Metadata {
        &self.doc.contents.metadata
    }

    pub fn parent<Database: Connection>(
        &self,
        database: &Database,
    ) -> Result<Option<Self>, bonsaidb_core::Error> {
        if let Some(id) = self.doc.contents.parent {
            Self::get(id, database)
        } else {
            Ok(None)
        }
    }

    pub fn contents<'a, Database: Connection>(
        &self,
        database: &'a Database,
    ) -> Result<Contents<'a, Database, Config>, bonsaidb_core::Error> {
        let mut blocks = schema::block::Block::<Config>::for_file(self.id(), database)?;
        blocks.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(Contents {
            database,
            blocks,
            loaded: VecDeque::default(),
            current_block: 0,
            offset: 0,
            batch_size: 10,
            _config: PhantomData,
        })
    }
}

#[derive(Debug, Clone)]
pub struct CreateFile<'a> {
    path: String,
    permissions: Option<Permissions>,
    contents: &'a [u8],
}

impl<'a> CreateFile<'a> {
    pub fn at_path<Path: Into<String>>(path: Path) -> Self {
        Self {
            path: path.into(),
            permissions: None,
            contents: b"",
        }
    }

    pub fn permissions(mut self, permissions: Permissions) -> Self {
        self.permissions = Some(permissions);
        self
    }

    pub fn contents(mut self, contents: &'a [u8]) -> Self {
        self.contents = contents;
        self
    }

    pub fn execute<Config: FileConfig, Database: Connection>(
        self,
        database: &Database,
    ) -> Result<File<Config>, bonsaidb_core::Error> {
        File::new_file(self.path, self.permissions, self.contents, database)
    }
}

pub struct Contents<'a, Database: Connection, Config: FileConfig> {
    database: &'a Database,
    blocks: Vec<BlockInfo>,
    loaded: VecDeque<LoadedBlock>,
    current_block: usize,
    offset: usize,
    batch_size: usize,
    _config: PhantomData<Config>,
}

struct LoadedBlock {
    index: usize,
    contents: Vec<u8>,
}

impl<'a, Database: Connection, Config: FileConfig> Contents<'a, Database, Config> {
    pub fn len(&self) -> u64 {
        self.blocks
            .last()
            .map(|b| b.offset + u64::try_from(b.length).unwrap())
            .unwrap_or_default()
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty() || (self.blocks.len() == 1 && self.blocks[0].length == 0)
    }

    pub fn into_vec(mut self) -> std::io::Result<Vec<u8>> {
        let mut contents = Vec::with_capacity(usize::try_from(self.len()).unwrap());
        self.read_to_end(&mut contents)?;
        Ok(contents)
    }

    pub fn batching_by_blocks(mut self, block_count: usize) -> Self {
        self.batch_size = block_count;
        self
    }

    fn load_blocks(&mut self) -> std::io::Result<()> {
        self.loaded.clear();
        let last_block = (self.current_block + self.batch_size).min(self.blocks.len());
        for (index, (id, contents)) in schema::block::Block::<Config>::load(
            self.blocks[self.current_block..last_block]
                .iter()
                .map(|info| info.id),
            self.database,
        )
        .map_err(|err| std::io::Error::new(ErrorKind::Other, err))?
        .into_iter()
        .enumerate()
        {
            println!("Received {index} {id}");
            self.loaded.push_back(LoadedBlock {
                index: self.current_block + index,
                contents,
            });
        }

        Ok(())
    }
}

impl<'a, Database: Connection, Config: FileConfig> Read for Contents<'a, Database, Config> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if self.loaded.is_empty() || self.loaded.front().unwrap().index != self.current_block {
                let is_last_block = self.current_block + 1 == self.blocks.len();

                if self.current_block < self.blocks.len()
                    || (is_last_block && self.offset < self.blocks.last().unwrap().length)
                {
                    self.load_blocks()?;
                } else {
                    // End of file
                    return Ok(0);
                }
            }
            while let Some(block) = self.loaded.front() {
                let read_length = buf.len().min(block.contents.len() - self.offset);
                if read_length > 0 {
                    let next_offset = self.offset + read_length;
                    buf[..read_length].copy_from_slice(&block.contents[self.offset..next_offset]);
                    self.offset = next_offset;
                    return Ok(read_length);
                } else {
                    self.loaded.pop_front();
                    self.offset = 0;
                    self.current_block += 1;
                }
            }
        }
    }
}

impl<'a, Database: Connection, Config: FileConfig> Seek for Contents<'a, Database, Config> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let seek_to = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(from_end) => {
                if from_end < 0 {
                    self.len() - u64::try_from(from_end.saturating_abs()).unwrap()
                } else {
                    // Seek to the end
                    self.len()
                }
            }
            SeekFrom::Current(from_current) => {
                if self.blocks.is_empty() {
                    return Ok(0);
                } else {
                    u64::try_from(
                        i64::try_from(
                            self.blocks[self.current_block].offset
                                + u64::try_from(self.offset).unwrap(),
                        )
                        .unwrap()
                            + from_current,
                    )
                    .unwrap()
                }
            }
        };
        if let Some((index, block)) = self
            .blocks
            .iter()
            .enumerate()
            .find(|b| b.1.offset + u64::try_from(b.1.length).unwrap() > seek_to)
        {
            self.current_block = index;
            self.offset = usize::try_from(seek_to - block.offset).unwrap();
            Ok(seek_to)
        } else if let Some(last_block) = self.blocks.last() {
            // Set to the end of the file
            self.current_block = self.blocks.len() - 1;
            self.offset = last_block.length;
            Ok(last_block.offset + u64::try_from(last_block.length).unwrap())
        } else {
            // Empty
            self.current_block = 0;
            self.offset = 0;
            Ok(0)
        }
    }
}

struct BlockInfo {
    offset: u64,
    length: usize,
    id: u64,
}

#[cfg(test)]
mod tests;
