#[cfg(feature = "async")]
use std::collections::BTreeMap;
#[cfg(feature = "async")]
use std::task::Poll;
use std::{
    collections::VecDeque,
    io::{ErrorKind, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
};

#[cfg(feature = "async")]
use bonsaidb_core::{circulate::flume, connection::AsyncConnection};
use bonsaidb_core::{
    connection::Connection,
    document::{CollectionDocument, DocumentId, Header},
    key::time::TimestampAsNanoseconds,
    schema::SerializedCollection,
};
use derive_where::derive_where;
use futures::future::BoxFuture;
#[cfg(feature = "async")]
use futures::ready;
#[cfg(feature = "async")]
use futures::FutureExt;
#[cfg(feature = "async")]
use tokio::io::AsyncWriteExt;

use crate::{schema, BonsaiFiles, Error, FileConfig, TruncateFrom};

#[derive_where(Debug, Clone)]
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
        contents: &[u8],
        database: &Database,
    ) -> Result<Self, Error> {
        Ok(Self {
            doc: schema::file::File::create_file(path, name, contents, database)?,
        })
    }

    #[cfg(feature = "async")]
    async fn new_file_async<Database: AsyncConnection>(
        path: Option<String>,
        name: String,
        contents: &[u8],
        database: &Database,
    ) -> Result<Self, Error> {
        Ok(Self {
            doc: schema::file::File::create_file_async(path, name, contents, database).await?,
        })
    }

    pub fn get<Database: Connection>(
        id: u32,
        database: &Database,
    ) -> Result<Option<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::get(id, database).map(|doc| doc.map(|doc| Self { doc }))
    }

    #[cfg(feature = "async")]
    pub async fn get_async<Database: AsyncConnection>(
        id: u32,
        database: &Database,
    ) -> Result<Option<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::get_async(id, database)
            .await
            .map(|doc| doc.map(|doc| Self { doc }))
    }

    pub fn load<Database: Connection>(
        path: &str,
        database: &Database,
    ) -> Result<Option<Self>, Error> {
        schema::file::File::<Config>::find(path, database).map(|opt| opt.map(|doc| Self { doc }))
    }

    #[cfg(feature = "async")]
    pub async fn load_async<Database: AsyncConnection>(
        path: &str,
        database: &Database,
    ) -> Result<Option<Self>, Error> {
        schema::file::File::<Config>::find_async(path, database)
            .await
            .map(|opt| opt.map(|doc| Self { doc }))
    }

    pub fn list<Database: Connection>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::list_path_contents(path, database)
            .map(|vec| vec.into_iter().map(|doc| Self { doc }).collect())
    }

    #[cfg(feature = "async")]
    pub async fn list_async<Database: AsyncConnection>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::list_path_contents_async(path, database)
            .await
            .map(|vec| vec.into_iter().map(|doc| Self { doc }).collect())
    }

    pub fn list_recursive<Database: Connection>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::list_recursive_path_contents(path, database)
            .map(|vec| vec.into_iter().map(|doc| Self { doc }).collect())
    }

    #[cfg(feature = "async")]
    pub async fn list_recursive_async<Database: AsyncConnection>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::list_recursive_path_contents_async(path, database)
            .await
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
            #[cfg(feature = "async")]
            async_blocks: None,
            _config: PhantomData,
        })
    }

    #[cfg(feature = "async")]
    pub async fn contents_async<'a, Database: AsyncConnection>(
        &self,
        database: &'a Database,
    ) -> Result<Contents<'a, Database, Config>, bonsaidb_core::Error> {
        let blocks = schema::block::Block::<Config>::for_file_async(self.id(), database).await?;
        Ok(Contents {
            database,
            blocks,
            loaded: VecDeque::default(),
            current_block: 0,
            offset: 0,
            batch_size: 10,
            #[cfg(feature = "async")]
            async_blocks: None,
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

    #[cfg(feature = "async")]
    pub async fn truncate_async<Database: AsyncConnection>(
        &self,
        new_length: u64,
        from: TruncateFrom,
        database: &Database,
    ) -> Result<(), bonsaidb_core::Error> {
        schema::file::File::<Config>::truncate_async(&self.doc, new_length, from, database).await
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

    #[cfg(feature = "async")]
    pub async fn append_async<Database: AsyncConnection>(
        &self,
        data: &[u8],
        database: &Database,
    ) -> Result<(), bonsaidb_core::Error> {
        schema::block::Block::<Config>::append_async(data, self.doc.header.id, database).await
    }

    #[cfg(feature = "async")]
    pub fn append_buffered_async<'a, Database: AsyncConnection + Clone>(
        &'a mut self,
        database: &'a Database,
    ) -> AsyncBufferedAppend<'a, Config, Database> {
        AsyncBufferedAppend {
            file: self,
            database,
            buffer: Vec::new(),
            flush_future: None,
            _config: PhantomData,
        }
    }

    fn update_document_for_move(
        &self,
        new_path: &str,
    ) -> CollectionDocument<schema::file::File<Config>> {
        let mut doc = self.doc.clone();
        if new_path.as_bytes().ends_with(b"/") {
            if new_path.len() > 1 {
                doc.contents.path = Some(new_path.to_string());
            } else {
                doc.contents.path = None;
            }
        } else {
            let (path, name) = new_path.rsplit_once('/').unwrap();
            doc.contents.path = (!path.is_empty()).then(|| path.to_string());
            doc.contents.name = name.to_string();
        }

        // Force path to end in a slash
        if let Some(path) = doc.contents.path.as_mut() {
            if path.bytes().last() != Some(b'/') {
                path.push('/');
            }
        }

        doc
    }

    pub fn move_to<Database: Connection>(
        &mut self,
        new_path: &str,
        database: &Database,
    ) -> Result<(), Error> {
        if !new_path.as_bytes().starts_with(b"/") {
            return Err(Error::InvalidPath);
        }

        let mut doc = self.update_document_for_move(new_path);
        doc.update(database)?;
        self.doc = doc;
        Ok(())
    }

    #[cfg(feature = "async")]
    pub async fn move_to_async<Database: AsyncConnection>(
        &mut self,
        new_path: &str,
        database: &Database,
    ) -> Result<(), Error> {
        if !new_path.as_bytes().starts_with(b"/") {
            return Err(Error::InvalidPath);
        }

        let mut doc = self.update_document_for_move(new_path);
        doc.update_async(database).await?;
        self.doc = doc;
        Ok(())
    }

    pub fn rename<Database: Connection>(
        &mut self,
        new_name: String,
        database: &Database,
    ) -> Result<(), Error> {
        if new_name.as_bytes().contains(&b'/') {
            return Err(Error::InvalidName);
        }

        // Prevent mutating self until after the database is updated.
        let mut doc = self.doc.clone();
        doc.contents.name = new_name;
        doc.update(database)?;
        self.doc = doc;
        Ok(())
    }

    #[cfg(feature = "async")]
    pub async fn rename_async<Database: AsyncConnection>(
        &mut self,
        new_name: String,
        database: &Database,
    ) -> Result<(), Error> {
        if new_name.as_bytes().contains(&b'/') {
            return Err(Error::InvalidName);
        }

        // Prevent mutating self until after the database is updated.
        let mut doc = self.doc.clone();
        doc.contents.name = new_name;
        doc.update_async(database).await?;
        self.doc = doc;
        Ok(())
    }

    pub fn delete<Database: Connection>(&self, database: &Database) -> Result<(), Error> {
        self.doc.delete(database)?;
        schema::block::Block::<Config>::delete_for_file(self.doc.header.id, database)?;
        Ok(())
    }

    #[cfg(feature = "async")]
    pub async fn delete_async<Database: AsyncConnection>(
        &self,
        database: &Database,
    ) -> Result<(), Error> {
        self.doc.delete_async(database).await?;
        schema::block::Block::<Config>::delete_for_file_async(self.doc.header.id, database).await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CreateFile<'a> {
    path: Option<String>,
    name: String,
    contents: &'a [u8],
}

impl<'a> CreateFile<'a> {
    pub fn named<Name: Into<String>>(name: Name) -> Self {
        Self {
            path: None,
            name: name.into(),
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

    pub fn execute<Config: FileConfig, Database: Connection>(
        self,
        database: &Database,
    ) -> Result<File<Config>, Error> {
        File::new_file(self.path, self.name, self.contents, database)
    }

    #[cfg(feature = "async")]
    pub async fn execute_async<
        Config: FileConfig,
        Database: bonsaidb_core::connection::AsyncConnection,
    >(
        self,
        database: &Database,
    ) -> Result<File<Config>, Error> {
        File::new_file_async(self.path, self.name, self.contents, database).await
    }
}

pub struct Contents<'a, Database, Config: FileConfig> {
    database: &'a Database,
    blocks: Vec<BlockInfo>,
    loaded: VecDeque<LoadedBlock>,
    current_block: usize,
    offset: usize,
    batch_size: usize,
    #[cfg(feature = "async")]
    async_blocks: Option<AsyncBlockTask>,
    _config: PhantomData<Config>,
}

#[cfg(feature = "async")]
struct AsyncBlockTask {
    block_receiver:
        flume::r#async::RecvFut<'static, Result<BTreeMap<u64, Vec<u8>>, std::io::Error>>,
    requested: bool,
    request_sender: flume::Sender<Vec<DocumentId>>,
}

impl<'a, Database, Config: FileConfig> Clone for Contents<'a, Database, Config> {
    fn clone(&self) -> Self {
        Self {
            database: self.database,
            blocks: self.blocks.clone(),
            loaded: VecDeque::new(),
            current_block: self.current_block,
            offset: self.offset,
            batch_size: self.batch_size,
            #[cfg(feature = "async")]
            async_blocks: None,
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
    pub fn to_vec(&self) -> std::io::Result<Vec<u8>> {
        self.clone().into_vec()
    }

    pub fn into_vec(mut self) -> std::io::Result<Vec<u8>> {
        let mut contents = Vec::with_capacity(usize::try_from(self.len()).unwrap());
        self.read_to_end(&mut contents)?;
        Ok(contents)
    }

    fn load_blocks(&mut self) -> std::io::Result<()> {
        self.loaded.clear();
        for (index, (_, contents)) in
            schema::block::Block::<Config>::load(self.next_blocks(), self.database)
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

#[cfg(feature = "async")]
impl<
        'a,
        Database: bonsaidb_core::connection::AsyncConnection + Clone + 'static,
        Config: FileConfig,
    > Contents<'a, Database, Config>
{
    pub async fn to_vec_async(&self) -> std::io::Result<Vec<u8>> {
        self.clone().into_vec_async().await
    }

    pub async fn into_vec_async(mut self) -> std::io::Result<Vec<u8>> {
        let mut contents = vec![0; usize::try_from(self.len()).unwrap()];
        <Self as tokio::io::AsyncReadExt>::read_exact(&mut self, &mut contents).await?;
        Ok(contents)
    }
}

impl<'a, Database, Config: FileConfig> Contents<'a, Database, Config> {
    fn next_blocks(&self) -> Vec<DocumentId> {
        let last_block = (self.current_block + self.batch_size).min(self.blocks.len());
        self.blocks[self.current_block..last_block]
            .iter()
            .map(|info| info.header.id)
            .collect()
    }

    pub fn batching_by_blocks(mut self, block_count: usize) -> Self {
        self.batch_size = block_count;
        self
    }

    pub fn len(&self) -> u64 {
        self.blocks
            .last()
            .map(|b| b.offset + u64::try_from(b.length).unwrap())
            .unwrap_or_default()
    }

    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty() || (self.blocks.len() == 1 && self.blocks[0].length == 0)
    }

    fn non_blocking_read<F: FnMut(&[u8]) -> usize>(
        &mut self,
        mut read_callback: F,
    ) -> NonBlockingReadResult {
        loop {
            if self.loaded.is_empty() || self.loaded.front().unwrap().index != self.current_block {
                let is_last_block = self.current_block + 1 == self.blocks.len();

                if self.current_block < self.blocks.len()
                    || (is_last_block && self.offset < self.blocks.last().unwrap().length)
                {
                    return NonBlockingReadResult::NeedBlocks;
                } else {
                    return NonBlockingReadResult::Eof;
                }
            }
            while let Some(block) = self.loaded.front() {
                let read_length = read_callback(&block.contents[self.offset..]);
                if read_length > 0 {
                    self.offset += read_length;
                    return NonBlockingReadResult::ReadBytes(read_length);
                } else {
                    self.loaded.pop_front();
                    self.offset = 0;
                    self.current_block += 1;
                }
            }
        }
    }
}

enum NonBlockingReadResult {
    NeedBlocks,
    ReadBytes(usize),
    Eof,
}

impl<'a, Database: Connection, Config: FileConfig> Read for Contents<'a, Database, Config> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            match self.non_blocking_read(|block| {
                let bytes_to_read = buf.len().min(block.len());
                buf[..bytes_to_read].copy_from_slice(&block[..bytes_to_read]);
                bytes_to_read
            }) {
                NonBlockingReadResult::ReadBytes(bytes) => return Ok(bytes),
                NonBlockingReadResult::Eof => return Ok(0),
                NonBlockingReadResult::NeedBlocks => self.load_blocks()?,
            }
        }
    }
}

impl<'a, Database, Config: FileConfig> Seek for Contents<'a, Database, Config> {
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

#[cfg(feature = "async")]
impl<
        'a,
        Database: bonsaidb_core::connection::AsyncConnection + Clone + 'static,
        Config: FileConfig,
    > tokio::io::AsyncRead for Contents<'a, Database, Config>
{
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.async_blocks.is_none() {
            // Spawn the task
            let (block_sender, block_receiver) = flume::unbounded();
            let (request_sender, request_receiver) = flume::unbounded();

            let task_database = self.database.clone();
            tokio::task::spawn(async move {
                while let Ok(doc_ids) = request_receiver.recv_async().await {
                    let blocks =
                        schema::block::Block::<Config>::load_async(doc_ids, &task_database)
                            .await
                            .map_err(|err| std::io::Error::new(ErrorKind::Other, err));
                    if block_sender.send(blocks).is_err() {
                        break;
                    }
                }
            });

            self.async_blocks = Some(AsyncBlockTask {
                block_receiver: block_receiver.into_recv_async(),
                request_sender,
                requested: false,
            });
        }

        loop {
            match self.non_blocking_read(|block| {
                let bytes_to_read = buf.remaining().min(block.len());
                buf.put_slice(&block[..bytes_to_read]);
                bytes_to_read
            }) {
                NonBlockingReadResult::NeedBlocks => {
                    if self.async_blocks.as_mut().unwrap().requested {
                        match ready!(self
                            .async_blocks
                            .as_mut()
                            .unwrap()
                            .block_receiver
                            .poll_unpin(cx))
                        {
                            Ok(Ok(blocks)) => {
                                self.async_blocks.as_mut().unwrap().requested = false;
                                for (index, (_, contents)) in blocks.into_iter().enumerate() {
                                    let loaded_block = LoadedBlock {
                                        index: self.current_block + index,
                                        contents,
                                    };
                                    self.loaded.push_back(loaded_block);
                                }
                            }
                            Ok(Err(db_err)) => {
                                return Poll::Ready(Err(std::io::Error::new(
                                    ErrorKind::Other,
                                    db_err,
                                )))
                            }
                            Err(flume_error) => {
                                return Poll::Ready(Err(std::io::Error::new(
                                    ErrorKind::BrokenPipe,
                                    flume_error,
                                )))
                            }
                        }
                    } else {
                        let blocks = self.next_blocks();
                        if blocks.is_empty() {
                            return Poll::Ready(Ok(()));
                        } else {
                            self.loaded.clear();
                            self.async_blocks.as_mut().unwrap().requested = true;
                            if let Err(err) = self
                                .async_blocks
                                .as_mut()
                                .unwrap()
                                .request_sender
                                .send(blocks)
                            {
                                return Poll::Ready(Err(std::io::Error::new(
                                    ErrorKind::BrokenPipe,
                                    err,
                                )));
                            }
                        }
                    }
                }
                NonBlockingReadResult::ReadBytes(bytes) => {
                    if bytes == 0 || buf.remaining() == 0 {
                        return Poll::Ready(Ok(()));
                    }
                }
                NonBlockingReadResult::Eof => return Poll::Ready(Ok(())),
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct BlockInfo {
    pub offset: u64,
    pub length: usize,
    pub header: Header,
}

pub struct BufferedAppend<'a, Config: FileConfig, Database: Connection> {
    file: &'a mut File<Config>,
    pub(crate) buffer: Vec<u8>,
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

pub struct AsyncBufferedAppend<'a, Config: FileConfig, Database: AsyncConnection + Clone + 'static>
{
    file: &'a mut File<Config>,
    pub(crate) buffer: Vec<u8>,
    database: &'a Database,
    flush_future: Option<BoxFuture<'a, Result<(), std::io::Error>>>,
    _config: PhantomData<Config>,
}

impl<'a, Config: FileConfig, Database: AsyncConnection + Clone + 'static>
    AsyncBufferedAppend<'a, Config, Database>
{
    pub async fn set_buffer_size(&mut self, capacity: usize) -> std::io::Result<()> {
        if self.buffer.capacity() > 0 {
            self.flush().await?;
        }
        self.buffer = Vec::with_capacity(capacity);
        Ok(())
    }
}

impl<'a, Config: FileConfig, Database: AsyncConnection + Clone + 'static> tokio::io::AsyncWrite
    for AsyncBufferedAppend<'a, Config, Database>
{
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        data: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        if self.buffer.capacity() == 0 {
            const ONE_MEGABYTE: usize = 1024 * 1024;
            // By default, reserve the largest multiple of BLOCK_SIZE that is
            // less than or equal to 1 megabyte.
            self.buffer
                .reserve_exact(ONE_MEGABYTE / Config::BLOCK_SIZE * Config::BLOCK_SIZE);
        }

        if self.flush_future.is_some() {
            if let Err(err) = ready!(std::pin::Pin::new(&mut self).poll_flush(cx)) {
                return Poll::Ready(Err(err));
            }
        } else if self.buffer.capacity() == self.buffer.len() {
            match ready!(std::pin::Pin::new(&mut self).poll_flush(cx)) {
                Ok(_) => {}
                Err(err) => {
                    return Poll::Ready(Err(err));
                }
            }
        }

        if data.is_empty() {
            Poll::Ready(Ok(0))
        } else {
            let bytes_to_write = data.len().min(self.buffer.capacity() - self.buffer.len());
            self.buffer.extend(&data[..bytes_to_write]);
            Poll::Ready(Ok(bytes_to_write))
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        if let Some(flush_future) = &mut self.flush_future {
            let result = ready!(flush_future.poll_unpin(cx));
            self.flush_future = None;
            Poll::Ready(result)
        } else if self.buffer.is_empty() {
            Poll::Ready(Ok(()))
        } else {
            let database = self.database.clone();
            let file = self.file.clone();

            let mut buffer = Vec::with_capacity(self.buffer.capacity());
            std::mem::swap(&mut buffer, &mut self.buffer);

            let mut flush_task = async move {
                file.append_async(&buffer, &database)
                    .await
                    .map_err(|err| std::io::Error::new(ErrorKind::Other, err))
            }
            .boxed();
            let poll_result = flush_task.poll_unpin(cx);
            self.flush_future = Some(flush_task);
            poll_result
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        self.poll_flush(cx)
    }
}

impl<'a, Config: FileConfig, Database: AsyncConnection + Clone + 'static> Drop
    for AsyncBufferedAppend<'a, Config, Database>
{
    fn drop(&mut self) {
        if !self.buffer.is_empty() {
            assert!(
                self.flush_future.is_none(),
                "flush() was started but not completed before dropped"
            );
            let mut buffer = Vec::new();
            std::mem::swap(&mut buffer, &mut self.buffer);
            let database = self.database.clone();
            let mut file = self.file.clone();

            tokio::runtime::Handle::current().spawn(async move {
                drop(
                    AsyncBufferedAppend {
                        file: &mut file,
                        buffer,
                        database: &database,
                        flush_future: None,
                        _config: PhantomData,
                    }
                    .flush()
                    .await,
                );
            });
        }
    }
}
