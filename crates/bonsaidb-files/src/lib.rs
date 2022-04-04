use std::{
    collections::VecDeque,
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
};

use bonsaidb_core::{
    connection::Connection,
    document::{CollectionDocument, Header},
    key::time::TimestampAsNanoseconds,
    schema::{
        CollectionName, InsertError, Qualified, Schema, SchemaName, Schematic, SerializedCollection,
    },
};

mod schema;

use derive_where::derive_where;

pub trait FileConfig: Sized + Send + Sync + Unpin + 'static {
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

#[derive_where(Default, Debug)]
pub struct FilesSchema<Config: FileConfig = BonsaiFiles>(PhantomData<Config>);

impl<Config: FileConfig> Schema for FilesSchema<Config> {
    fn schema_name() -> SchemaName {
        SchemaName::new("bonsaidb", "files")
    }

    fn define_collections(schema: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
        Config::register_collections(schema)
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
        path: Option<String>,
        name: String,
        create_directories: bool,
        contents: &[u8],
        database: &Database,
    ) -> Result<Self, Error> {
        Ok(Self {
            doc: schema::file::File::create_file(
                path,
                name,
                create_directories,
                contents,
                database,
            )?,
        })
    }

    pub fn get<Database: Connection>(
        id: u32,
        database: &Database,
    ) -> Result<Option<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::get(id, database).map(|doc| doc.map(|doc| Self { doc }))
    }

    pub fn load<Database: Connection>(
        path: &str,
        database: &Database,
    ) -> Result<Option<Self>, Error> {
        schema::file::File::<Config>::find(path, database).map(|opt| opt.map(|doc| Self { doc }))
    }

    pub fn list<Database: Connection>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::list_path_contents(path, database)
            .map(|vec| vec.into_iter().map(|doc| Self { doc }).collect())
    }

    pub fn list_recursive<Database: Connection>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::list_recursive_path_contents(path, database)
            .map(|vec| vec.into_iter().map(|doc| Self { doc }).collect())
    }

    pub fn id(&self) -> u32 {
        self.doc.header.id
    }

    pub fn containing_path(&self) -> &str {
        self.doc.contents.path.as_deref().unwrap_or("/")
    }

    pub fn name(&self) -> &str {
        &self.doc.contents.name
    }

    pub fn path(&self) -> String {
        let containing_path = self.containing_path();
        let ends_in_slash = self.containing_path().ends_with('/');
        let mut full_path = String::with_capacity(
            containing_path.len() + if ends_in_slash { 0 } else { 1 } + self.name().len(),
        );
        full_path.push_str(containing_path);
        if !ends_in_slash {
            full_path.push('/');
        }
        full_path.push_str(self.name());

        full_path
    }

    pub fn created_at(&self) -> TimestampAsNanoseconds {
        self.doc.contents.created_at
    }

    pub fn parent<Database: Connection>(&self, database: &Database) -> Result<Option<Self>, Error> {
        let path = self.containing_path();
        if path == "/" {
            Ok(None)
        } else {
            Self::load(path, database)
        }
    }

    pub fn children<Database: Connection>(
        &self,
        database: &Database,
    ) -> Result<Vec<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::list_path_contents(&self.path(), database)
            .map(|docs| docs.into_iter().map(|doc| Self { doc }).collect())
    }

    pub fn contents<'a, Database: Connection>(
        &self,
        database: &'a Database,
    ) -> Result<Contents<'a, Database, Config>, bonsaidb_core::Error> {
        let blocks = schema::block::Block::<Config>::for_file(self.id(), database)?;
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

    pub fn truncate<Database: Connection>(
        &self,
        new_length: u64,
        from: TruncateFrom,
        database: &Database,
    ) -> Result<(), bonsaidb_core::Error> {
        schema::file::File::<Config>::truncate(&self.doc, new_length, from, database)
    }

    pub fn append<Database: Connection>(
        &self,
        data: &[u8],
        database: &Database,
    ) -> Result<(), bonsaidb_core::Error> {
        schema::block::Block::<Config>::append(data, self.doc.header.id, database)
    }

    pub fn append_buffered<'a, Database: Connection>(
        &'a mut self,
        database: &'a Database,
    ) -> BufferedAppend<'a, Config, Database> {
        BufferedAppend {
            file: self,
            database,
            buffer: Vec::new(),
            _config: PhantomData,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateFile<'a> {
    path: Option<String>,
    name: String,
    create_directories: bool,
    contents: &'a [u8],
}

impl<'a> CreateFile<'a> {
    pub fn named<Name: Into<String>>(name: Name) -> Self {
        Self {
            path: None,
            name: name.into(),
            create_directories: false,
            contents: b"",
        }
    }

    pub fn at_path<Path: Into<String>>(mut self, path: Path) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn contents(mut self, contents: &'a [u8]) -> Self {
        self.contents = contents;
        self
    }

    pub fn creating_missing_directories(mut self) -> Self {
        self.create_directories = true;
        self
    }

    pub fn execute<Config: FileConfig, Database: Connection>(
        self,
        database: &Database,
    ) -> Result<File<Config>, Error> {
        File::new_file(
            self.path,
            self.name,
            self.create_directories,
            self.contents,
            database,
        )
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

impl<'a, Database: Connection, Config: FileConfig> Clone for Contents<'a, Database, Config> {
    fn clone(&self) -> Self {
        Self {
            database: self.database,
            blocks: self.blocks.clone(),
            loaded: self.loaded.clone(),
            current_block: self.current_block,
            offset: self.offset,
            batch_size: self.batch_size,
            _config: PhantomData,
        }
    }
}

#[derive(Clone)]
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

    pub fn to_vec(&self) -> std::io::Result<Vec<u8>> {
        self.clone().into_vec()
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
        for (index, (_, contents)) in schema::block::Block::<Config>::load(
            self.blocks[self.current_block..last_block]
                .iter()
                .map(|info| info.header.id),
            self.database,
        )
        .map_err(|err| std::io::Error::new(ErrorKind::Other, err))?
        .into_iter()
        .enumerate()
        {
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

#[derive(Clone)]
struct BlockInfo {
    offset: u64,
    length: usize,
    header: Header,
}

pub enum TruncateFrom {
    Start,
    End,
}

pub struct BufferedAppend<'a, Config: FileConfig, Database: Connection> {
    file: &'a mut File<Config>,
    buffer: Vec<u8>,
    database: &'a Database,
    _config: PhantomData<Config>,
}

impl<'a, Config: FileConfig, Database: Connection> BufferedAppend<'a, Config, Database> {
    pub fn set_buffer_size(&mut self, capacity: usize) -> std::io::Result<()> {
        if self.buffer.capacity() > 0 {
            self.flush()?;
        }
        self.buffer = Vec::with_capacity(capacity);
        Ok(())
    }
}

impl<'a, Config: FileConfig, Database: Connection> Write for BufferedAppend<'a, Config, Database> {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if self.buffer.capacity() == 0 {
            const ONE_MEGABYTE: usize = 1024 * 1024;
            // By default, reserve the largest multiple of BLOCK_SIZE that is
            // less than or equal to 1 megabyte.
            self.buffer
                .reserve_exact(ONE_MEGABYTE / Config::BLOCK_SIZE * Config::BLOCK_SIZE);
        } else if self.buffer.capacity() == self.buffer.len() {
            self.flush()?;
        }

        if data.is_empty() {
            Ok(0)
        } else {
            let bytes_to_write = data.len().min(self.buffer.capacity() - self.buffer.len());
            self.buffer.extend(&data[..bytes_to_write]);
            Ok(bytes_to_write)
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file
            .append(&self.buffer, self.database)
            .map_err(|err| std::io::Error::new(ErrorKind::Other, err))?;
        self.buffer.clear();
        Ok(())
    }
}

impl<'a, Config: FileConfig, Database: Connection> Drop for BufferedAppend<'a, Config, Database> {
    fn drop(&mut self) {
        drop(self.flush())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("database error: {0}")]
    Database(#[from] bonsaidb_core::Error),
    #[error("names must not contain '/'")]
    InvalidName,
    #[error("all paths must start with a leading '/'")]
    InvalidPath,
}

impl<T> From<InsertError<T>> for Error {
    fn from(err: InsertError<T>) -> Self {
        Self::Database(err.error)
    }
}

#[cfg(test)]
mod tests;
