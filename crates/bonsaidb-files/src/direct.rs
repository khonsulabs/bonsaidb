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
#[cfg(feature = "async")]
use futures::{future::BoxFuture, ready, FutureExt};
#[cfg(feature = "async")]
use tokio::io::AsyncWriteExt;

use crate::{schema, BonsaiFiles, Error, FileConfig, Truncate};

/// A handle to a file stored in a database.
#[derive_where(Debug, Clone)]
pub struct File<Database: Clone, Config: FileConfig = BonsaiFiles> {
    doc: CollectionDocument<schema::file::File<Config>>,
    #[derive_where(skip(Debug))]
    database: Database,
}

impl<Database, Config> PartialEq for File<Database, Config>
where
    Database: Clone,
    Config: FileConfig,
{
    fn eq(&self, other: &Self) -> bool {
        self.doc.header == other.doc.header
    }
}

/// A blocking database connection.
#[derive(Clone)]
pub struct Blocking<Database: Connection>(Database);

/// An async database connection.
#[cfg(feature = "async")]
#[derive(Clone)]
pub struct Async<Database: AsyncConnection>(Database);

impl<Database, Config> File<Blocking<Database>, Config>
where
    Database: Connection + Clone,
    Config: FileConfig,
{
    fn new_file(
        path: Option<String>,
        name: String,
        contents: &[u8],
        metadata: Option<Config::Metadata>,
        database: Database,
    ) -> Result<Self, Error> {
        Ok(Self {
            doc: schema::file::File::create_file(path, name, contents, metadata, &database)?,
            database: Blocking(database),
        })
    }

    pub(crate) fn get(id: u32, database: Database) -> Result<Option<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::get(&id, &database).map(|doc| {
            doc.map(|doc| Self {
                doc,
                database: Blocking(database),
            })
        })
    }

    pub(crate) fn load(path: &str, database: Database) -> Result<Option<Self>, Error> {
        schema::file::File::<Config>::find(path, &database).map(|opt| {
            opt.map(|doc| Self {
                doc,
                database: Blocking(database),
            })
        })
    }

    pub(crate) fn list(path: &str, database: &Database) -> Result<Vec<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::list_path_contents(path, database).map(|vec| {
            vec.into_iter()
                .map(|doc| Self {
                    doc,
                    database: Blocking(database.clone()),
                })
                .collect()
        })
    }

    pub(crate) fn list_recursive(
        path: &str,
        database: &Database,
    ) -> Result<Vec<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::list_recursive_path_contents(path, database).map(|vec| {
            vec.into_iter()
                .map(|doc| Self {
                    doc,
                    database: Blocking(database.clone()),
                })
                .collect()
        })
    }

    /// Return all direct descendents of this file. For example, consider this
    /// list of files:
    ///
    /// - /top-level
    /// - /top-level/sub-level
    /// - /top-level/sub-level/file.txt
    ///
    /// If this instance were `/top-level`, this function would return
    /// `sub-level` but not `sub-level/file.txt`.
    pub fn children(&self) -> Result<Vec<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::list_path_contents(&self.path(), &self.database.0).map(
            |docs| {
                docs.into_iter()
                    .map(|doc| Self {
                        doc,
                        database: self.database.clone(),
                    })
                    .collect()
            },
        )
    }

    /// Moves this file to a new location. If `new_path` ends with a `/`, the
    /// file will be moved to that path with its name preserved. Otherwise, the
    /// file will be renamed as part of the move.
    ///
    /// For example, moving `/a/file.txt` to `/b/` will result in the full path
    /// being `/b/file.txt`. Moving `/a/file.txt` to `/b/new-name.txt` will
    /// result in the full path being `/b/new-name.txt`.
    pub fn move_to(&mut self, new_path: &str) -> Result<(), Error> {
        if !new_path.as_bytes().starts_with(b"/") {
            return Err(Error::InvalidPath);
        }

        let mut doc = self.update_document_for_move(new_path);
        doc.update(&self.database.0)?;
        self.doc = doc;
        Ok(())
    }

    /// Renames this file to the new name.
    pub fn rename(&mut self, new_name: String) -> Result<(), Error> {
        if new_name.as_bytes().contains(&b'/') {
            return Err(Error::InvalidName);
        }

        // Prevent mutating self until after the database is updated.
        let mut doc = self.doc.clone();
        doc.contents.name = new_name;
        doc.update(&self.database.0)?;
        self.doc = doc;
        Ok(())
    }

    /// Deletes the file.
    pub fn delete(&self) -> Result<(), Error> {
        self.doc.delete(&self.database.0)?;
        schema::block::Block::<Config>::delete_for_file(self.doc.header.id, &self.database.0)?;
        Ok(())
    }

    /// Returns the contents of the file, which allows random and buffered
    /// access to the file stored in the database.
    ///
    /// The default buffer size is ten times
    /// [`Config::BLOCK_SIZE`](FileConfig::BLOCK_SIZE).
    pub fn contents(
        &self,
    ) -> Result<Contents<'_, Blocking<Database>, Config>, bonsaidb_core::Error> {
        let blocks = schema::block::Block::<Config>::for_file(self.id(), &self.database.0)?;
        Ok(Contents {
            file: self,
            blocks,
            loaded: VecDeque::default(),
            current_block: 0,
            offset: 0,
            buffer_size: Config::BLOCK_SIZE * 10,
            #[cfg(feature = "async")]
            async_blocks: None,
            _config: PhantomData,
        })
    }

    /// Truncates the file, removing data from either the start or end of the
    /// file until the file is within
    /// [`Config::BLOCK_SIZE`](FileConfig::BLOCK_SIZE) of `new_length`.
    /// Truncating currently will not split a block, causing the resulting
    /// length to not always match the length requested.
    ///
    /// If `new_length` is 0 and this call succeeds, the file's length is
    /// guaranteed to be 0.
    pub fn truncate(&self, new_length: u64, from: Truncate) -> Result<(), bonsaidb_core::Error> {
        schema::file::File::<Config>::truncate(&self.doc, new_length, from, &self.database.0)
    }

    /// Appends `data` to the end of the file. The data will be split into
    /// chunks no larger than [`Config::BLOCK_SIZE`](FileConfig::BLOCK_SIZE)
    /// when stored in the database.
    pub fn append(&self, data: &[u8]) -> Result<(), bonsaidb_core::Error> {
        schema::block::Block::<Config>::append(data, self.doc.header.id, &self.database.0)
    }

    /// Returns a writer that will buffer writes to the end of the file.
    pub fn append_buffered(&mut self) -> BufferedAppend<'_, Config, Database> {
        BufferedAppend {
            file: self,
            buffer: Vec::new(),
            _config: PhantomData,
        }
    }

    /// Updates the metadata for this file.
    pub fn update_metadata(
        &mut self,
        metadata: impl Into<Option<Config::Metadata>>,
    ) -> Result<(), bonsaidb_core::Error> {
        let mut doc = self.doc.clone();
        doc.contents.metadata = metadata.into();
        doc.update(&self.database.0)?;
        self.doc = doc;
        Ok(())
    }
}

#[cfg(feature = "async")]
impl<Database, Config> File<Async<Database>, Config>
where
    Database: AsyncConnection + Clone,
    Config: FileConfig,
{
    async fn new_file_async(
        path: Option<String>,
        name: String,
        contents: &[u8],
        metadata: Option<Config::Metadata>,
        database: Database,
    ) -> Result<Self, Error> {
        Ok(Self {
            doc: schema::file::File::create_file_async(path, name, contents, metadata, &database)
                .await?,
            database: Async(database),
        })
    }

    pub(crate) async fn get_async(
        id: u32,
        database: Database,
    ) -> Result<Option<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::get_async(&id, &database)
            .await
            .map(|doc| {
                doc.map(|doc| Self {
                    doc,
                    database: Async(database),
                })
            })
    }

    pub(crate) async fn load_async(path: &str, database: Database) -> Result<Option<Self>, Error> {
        schema::file::File::<Config>::find_async(path, &database)
            .await
            .map(|opt| {
                opt.map(|doc| Self {
                    doc,
                    database: Async(database),
                })
            })
    }

    pub(crate) async fn list_async(
        path: &str,
        database: &Database,
    ) -> Result<Vec<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::list_path_contents_async(path, database)
            .await
            .map(|vec| {
                vec.into_iter()
                    .map(|doc| Self {
                        doc,
                        database: Async(database.clone()),
                    })
                    .collect()
            })
    }

    pub(crate) async fn list_recursive_async(
        path: &str,
        database: &Database,
    ) -> Result<Vec<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::list_recursive_path_contents_async(path, database)
            .await
            .map(|vec| {
                vec.into_iter()
                    .map(|doc| Self {
                        doc,
                        database: Async(database.clone()),
                    })
                    .collect()
            })
    }

    /// Return all direct descendents of this file. For example, consider this
    /// list of files:
    ///
    /// - /top-level
    /// - /top-level/sub-level
    /// - /top-level/sub-level/file.txt
    ///
    /// If this instance were `/top-level`, this function would return
    /// `sub-level` but not `sub-level/file.txt`.
    pub async fn children(&self) -> Result<Vec<Self>, bonsaidb_core::Error> {
        schema::file::File::<Config>::list_path_contents_async(&self.path(), &self.database.0)
            .await
            .map(|docs| {
                docs.into_iter()
                    .map(|doc| Self {
                        doc,
                        database: self.database.clone(),
                    })
                    .collect()
            })
    }

    /// Moves this file to a new location. If `new_path` ends with a `/`, the
    /// file will be moved to that path with its name preserved. Otherwise, the
    /// file will be renamed as part of the move.
    ///
    /// For example, moving `/a/file.txt` to `/b/` will result in the full path
    /// being `/b/file.txt`. Moving `/a/file.txt` to `/b/new-name.txt` will
    /// result in the full path being `/b/new-name.txt`.
    pub async fn move_to(&mut self, new_path: &str) -> Result<(), Error> {
        if !new_path.as_bytes().starts_with(b"/") {
            return Err(Error::InvalidPath);
        }

        let mut doc = self.update_document_for_move(new_path);
        doc.update_async(&self.database.0).await?;
        self.doc = doc;
        Ok(())
    }

    /// Renames this file to the new name.
    pub async fn rename(&mut self, new_name: String) -> Result<(), Error> {
        if new_name.as_bytes().contains(&b'/') {
            return Err(Error::InvalidName);
        }

        // Prevent mutating self until after the database is updated.
        let mut doc = self.doc.clone();
        doc.contents.name = new_name;
        doc.update_async(&self.database.0).await?;
        self.doc = doc;
        Ok(())
    }

    /// Deletes the file.
    pub async fn delete(&self) -> Result<(), Error> {
        self.doc.delete_async(&self.database.0).await?;
        schema::block::Block::<Config>::delete_for_file_async(self.doc.header.id, &self.database.0)
            .await?;
        Ok(())
    }

    /// Returns the contents of the file, which allows random and buffered
    /// access to the file stored in the database.
    ///
    /// The default buffer size is ten times
    /// [`Config::BLOCK_SIZE`](FileConfig::BLOCK_SIZE).
    pub async fn contents(
        &self,
    ) -> Result<Contents<'_, Async<Database>, Config>, bonsaidb_core::Error> {
        let blocks =
            schema::block::Block::<Config>::for_file_async(self.id(), &self.database.0).await?;
        Ok(Contents {
            file: self,
            blocks,
            loaded: VecDeque::default(),
            current_block: 0,
            offset: 0,
            buffer_size: Config::BLOCK_SIZE * 10,
            #[cfg(feature = "async")]
            async_blocks: None,
            _config: PhantomData,
        })
    }

    /// Truncates the file, removing data from either the start or end of the
    /// file until the file is within
    /// [`Config::BLOCK_SIZE`](FileConfig::BLOCK_SIZE) of `new_length`.
    /// Truncating currently will not split a block, causing the resulting
    /// length to not always match the length requested.
    ///
    /// If `new_length` is 0 and this call succeeds, the file's length is
    /// guaranteed to be 0.
    pub async fn truncate(
        &self,
        new_length: u64,
        from: Truncate,
    ) -> Result<(), bonsaidb_core::Error> {
        schema::file::File::<Config>::truncate_async(&self.doc, new_length, from, &self.database.0)
            .await
    }

    /// Appends `data` to the end of the file. The data will be split into
    /// chunks no larger than [`Config::BLOCK_SIZE`](FileConfig::BLOCK_SIZE)
    /// when stored in the database.
    pub async fn append(&self, data: &[u8]) -> Result<(), bonsaidb_core::Error> {
        schema::block::Block::<Config>::append_async(data, self.doc.header.id, &self.database.0)
            .await
    }

    /// Returns a writer that will buffer writes to the end of the file.
    pub fn append_buffered(&mut self) -> AsyncBufferedAppend<'_, Config, Database> {
        AsyncBufferedAppend {
            file: self,
            buffer: Vec::new(),
            flush_future: None,
            _config: PhantomData,
        }
    }

    /// Updates the metadata for this file.
    pub async fn update_metadata(
        &mut self,
        metadata: impl Into<Option<Config::Metadata>>,
    ) -> Result<(), bonsaidb_core::Error> {
        let mut doc = self.doc.clone();
        doc.contents.metadata = metadata.into();
        doc.update_async(&self.database.0).await?;
        self.doc = doc;
        Ok(())
    }
}

impl<Database, Config> File<Database, Config>
where
    Database: Clone,
    Config: FileConfig,
{
    /// Returns the unique id of this file. The file id is only unique within a
    /// single database and [`FileConfig`].
    pub fn id(&self) -> u32 {
        self.doc.header.id
    }

    /// Returns the path containing this file. For example, if the full path to
    /// the file is `/some-path/file.txt`, this function will return
    /// `/some-path/`.
    pub fn containing_path(&self) -> &str {
        self.doc.contents.path.as_deref().unwrap_or("/")
    }

    /// Returns the name of this file.
    pub fn name(&self) -> &str {
        &self.doc.contents.name
    }

    /// Returns the absolute path of this file.
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

    /// Returns the timestamp the file was created at.
    pub fn created_at(&self) -> TimestampAsNanoseconds {
        self.doc.contents.created_at
    }

    /// Returns the metadata for this file, if any was set.
    pub fn metadata(&self) -> Option<&Config::Metadata> {
        self.doc.contents.metadata.as_ref()
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
}

/// A builder to create a [`File`].
#[derive(Debug, Clone)]
#[must_use]
pub struct FileBuilder<'a, Config>
where
    Config: FileConfig,
{
    path: Option<String>,
    name: String,
    contents: &'a [u8],
    metadata: Option<Config::Metadata>,
    _config: PhantomData<Config>,
}

impl<'a, Config: FileConfig> FileBuilder<'a, Config> {
    pub(crate) fn new<NameOrPath: AsRef<str>>(name_or_path: NameOrPath) -> Self {
        let mut name_or_path = name_or_path.as_ref();
        let (path, name) = if name_or_path.starts_with('/') {
            // Trim the trailing / if there is one.
            if name_or_path.ends_with('/') && name_or_path.len() > 1 {
                name_or_path = &name_or_path[..name_or_path.len() - 1];
            }
            let (path, name) = name_or_path.rsplit_once('/').unwrap();
            let path = match path {
                "" | "/" => None,
                other => Some(other.to_string()),
            };
            (path, name.to_string())
        } else {
            (None, name_or_path.to_string())
        };
        Self {
            path,
            name,
            contents: b"",
            metadata: None,
            _config: PhantomData,
        }
    }

    /// Creates this file at `path`. This does not change the file's name
    /// specified when creating the builder.
    pub fn at_path<Path: Into<String>>(mut self, path: Path) -> Self {
        self.path = Some(path.into());
        self
    }

    /// Sets the file's initial contents.
    pub fn contents(mut self, contents: &'a [u8]) -> Self {
        self.contents = contents;
        self
    }

    /// Sets the file's initial metadata.
    pub fn metadata(mut self, metadata: Config::Metadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Creates the file and returns a handle to the created file.
    pub fn create<Database: Connection + Clone>(
        self,
        database: Database,
    ) -> Result<File<Blocking<Database>, Config>, Error> {
        File::new_file(self.path, self.name, self.contents, self.metadata, database)
    }

    /// Creates the file and returns a handle to the created file.
    #[cfg(feature = "async")]
    pub async fn create_async<Database: bonsaidb_core::connection::AsyncConnection + Clone>(
        self,
        database: Database,
    ) -> Result<File<Async<Database>, Config>, Error> {
        File::new_file_async(self.path, self.name, self.contents, self.metadata, database).await
    }
}

/// Buffered access to the contents of a [`File`].
#[must_use]
pub struct Contents<'a, Database: Clone, Config: FileConfig> {
    file: &'a File<Database, Config>,
    blocks: Vec<BlockInfo>,
    loaded: VecDeque<LoadedBlock>,
    current_block: usize,
    offset: usize,
    buffer_size: usize,
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

impl<'a, Database: Clone, Config: FileConfig> Clone for Contents<'a, Database, Config> {
    fn clone(&self) -> Self {
        Self {
            file: self.file,
            blocks: self.blocks.clone(),
            loaded: VecDeque::new(),
            current_block: self.current_block,
            offset: self.offset,
            buffer_size: self.buffer_size,
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

impl<'a, Database: Connection + Clone, Config: FileConfig>
    Contents<'a, Blocking<Database>, Config>
{
    /// Returns the remaining contents as a `Vec<u8>`. If no bytes have been
    /// read, this returns the entire contents.
    pub fn to_vec(&self) -> std::io::Result<Vec<u8>> {
        self.clone().into_vec()
    }

    /// Returns the remaining contents as a string. If no bytes have been read,
    /// this returns the entire contents.
    pub fn to_string(&self) -> std::io::Result<String> {
        String::from_utf8(self.to_vec()?)
            .map_err(|err| std::io::Error::new(ErrorKind::InvalidData, err))
    }

    /// Returns the remaining contents as a `Vec<u8>`. If no bytes have been
    /// read, this returns the entire contents.
    #[allow(clippy::missing_panics_doc)] // Not reachable
    pub fn into_vec(mut self) -> std::io::Result<Vec<u8>> {
        let mut contents = Vec::with_capacity(usize::try_from(self.len()).unwrap());
        self.read_to_end(&mut contents)?;
        Ok(contents)
    }

    /// Returns the remaining contents as a string. If no bytes have been read,
    /// this returns the entire contents.
    pub fn into_string(self) -> std::io::Result<String> {
        String::from_utf8(self.into_vec()?)
            .map_err(|err| std::io::Error::new(ErrorKind::InvalidData, err))
    }

    fn load_blocks(&mut self) -> std::io::Result<()> {
        self.loaded.clear();
        for (index, (_, contents)) in
            schema::block::Block::<Config>::load(&self.next_blocks(), &self.file.database.0)
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
    > Contents<'a, Async<Database>, Config>
{
    /// Returns the remaining contents as a `Vec<u8>`. If no bytes have been
    /// read, this returns the entire contents.
    pub async fn to_vec(&self) -> std::io::Result<Vec<u8>> {
        self.clone().into_vec().await
    }

    /// Returns the remaining contents as a `Vec<u8>`. If no bytes have been
    /// read, this returns the entire contents.
    #[allow(clippy::missing_panics_doc)] // Not reachable
    pub async fn into_vec(mut self) -> std::io::Result<Vec<u8>> {
        let mut contents = vec![0; usize::try_from(self.len()).unwrap()];
        <Self as tokio::io::AsyncReadExt>::read_exact(&mut self, &mut contents).await?;
        Ok(contents)
    }

    /// Returns the remaining contents as a string. If no bytes have been read,
    /// this returns the entire contents.
    pub async fn to_string(&self) -> std::io::Result<String> {
        String::from_utf8(self.to_vec().await?)
            .map_err(|err| std::io::Error::new(ErrorKind::InvalidData, err))
    }

    /// Returns the remaining contents as a string. If no bytes have been read,
    /// this returns the entire contents.
    pub async fn into_string(self) -> std::io::Result<String> {
        String::from_utf8(self.into_vec().await?)
            .map_err(|err| std::io::Error::new(ErrorKind::InvalidData, err))
    }

    fn spawn_block_fetching_task(&mut self) {
        if self.async_blocks.is_none() {
            // Spawn the task
            let (block_sender, block_receiver) = flume::unbounded();
            let (request_sender, request_receiver) = flume::unbounded();

            let task_database = self.file.database.0.clone();
            tokio::task::spawn(async move {
                while let Ok(doc_ids) = request_receiver.recv_async().await {
                    let blocks =
                        schema::block::Block::<Config>::load_async(&doc_ids, &task_database)
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
    }

    fn fetch_blocks(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<bool, std::io::Error>> {
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
                    Poll::Ready(Ok(true))
                }
                Ok(Err(db_err)) => Poll::Ready(Err(std::io::Error::new(ErrorKind::Other, db_err))),
                Err(flume_error) => {
                    Poll::Ready(Err(std::io::Error::new(ErrorKind::BrokenPipe, flume_error)))
                }
            }
        } else {
            let blocks = self.next_blocks();
            if blocks.is_empty() {
                return Poll::Ready(Ok(false));
            }
            self.loaded.clear();
            self.async_blocks.as_mut().unwrap().requested = true;
            if let Err(err) = self
                .async_blocks
                .as_mut()
                .unwrap()
                .request_sender
                .send(blocks)
            {
                return Poll::Ready(Err(std::io::Error::new(ErrorKind::BrokenPipe, err)));
            }

            Poll::Ready(Ok(true))
        }
    }
}

impl<'a, Database: Clone, Config: FileConfig> Contents<'a, Database, Config> {
    fn next_blocks(&self) -> Vec<DocumentId> {
        let mut last_block = self.current_block;
        let mut requesting_size = 0;
        for index in self.current_block..self.blocks.len() {
            let size_if_requested = self.blocks[index].length.saturating_add(requesting_size);
            if size_if_requested > self.buffer_size {
                break;
            }

            requesting_size = size_if_requested;
            last_block = index;
        }

        self.blocks[self.current_block..=last_block]
            .iter()
            .map(|info| info.header.id.clone())
            .collect()
    }

    /// Sets the maximum buffer size in bytes and returns `self`. When buffering
    /// reads from the database, requests will be made to fill at-most
    /// `size_in_bytes` of memory.
    pub fn with_buffer_size(mut self, size_in_bytes: usize) -> Self {
        self.buffer_size = size_in_bytes;
        self
    }

    /// Returns the total length of the file.
    #[allow(clippy::missing_panics_doc)] // Not reachable
    #[must_use]
    pub fn len(&self) -> u64 {
        self.blocks
            .last()
            .map(|b| b.offset + u64::try_from(b.length).unwrap())
            .unwrap_or_default()
    }

    /// Returns true if the file's length is 0.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.blocks.is_empty() || (self.blocks.len() == 1 && self.blocks[0].length == 0)
    }

    /// Returns the timestamp that the last data was written to the file.
    /// Returns None if the file is empty.
    #[must_use]
    pub fn last_appended_at(&self) -> Option<TimestampAsNanoseconds> {
        self.blocks.last().map(|b| b.timestamp)
    }

    fn non_blocking_read_block(&mut self) -> NonBlockingBlockReadResult {
        let block = self.loaded.pop_front();

        if let Some(mut block) = block {
            if block.index == self.current_block {
                self.current_block += 1;
                if self.offset > 0 {
                    block.contents.splice(..self.offset, []);
                    self.offset = 0;
                }
                return NonBlockingBlockReadResult::ReadBlock(block.contents);
            }
        }

        // We need to load blocks. We need to ensure we aren't in an EOF
        // position.
        let is_last_block = self.current_block + 1 == self.blocks.len();
        if self.current_block < self.blocks.len()
            || (is_last_block && self.offset < self.blocks.last().unwrap().length)
        {
            return NonBlockingBlockReadResult::NeedBlocks;
        }

        NonBlockingBlockReadResult::Eof
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
                }

                return NonBlockingReadResult::Eof;
            }
            while let Some(block) = self.loaded.front() {
                let read_length = read_callback(&block.contents[self.offset..]);
                if read_length > 0 {
                    self.offset += read_length;
                    return NonBlockingReadResult::ReadBytes(read_length);
                }

                self.loaded.pop_front();
                self.offset = 0;
                self.current_block += 1;
            }
        }
    }
}

enum NonBlockingBlockReadResult {
    NeedBlocks,
    ReadBlock(Vec<u8>),
    Eof,
}

enum NonBlockingReadResult {
    NeedBlocks,
    ReadBytes(usize),
    Eof,
}

impl<'a, Database: Connection + Clone, Config: FileConfig> Read
    for Contents<'a, Blocking<Database>, Config>
{
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

impl<'a, Database: Clone, Config: FileConfig> Seek for Contents<'a, Database, Config> {
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
                }

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
    > tokio::io::AsyncSeek for Contents<'a, Async<Database>, Config>
{
    fn start_seek(mut self: std::pin::Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        self.seek(position).map(|_| ())
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<u64>> {
        if self.blocks.is_empty() {
            Poll::Ready(Ok(0))
        } else if self.current_block < self.blocks.len() {
            Poll::Ready(Ok(
                self.blocks[self.current_block].offset + u64::try_from(self.offset).unwrap()
            ))
        } else {
            Poll::Ready(Ok(self.len()))
        }
    }
}

#[cfg(feature = "async")]
impl<
        'a,
        Database: bonsaidb_core::connection::AsyncConnection + Clone + 'static,
        Config: FileConfig,
    > tokio::io::AsyncRead for Contents<'a, Async<Database>, Config>
{
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        self.spawn_block_fetching_task();
        loop {
            match self.non_blocking_read(|block| {
                let bytes_to_read = buf.remaining().min(block.len());
                buf.put_slice(&block[..bytes_to_read]);
                bytes_to_read
            }) {
                NonBlockingReadResult::NeedBlocks => match self.fetch_blocks(cx) {
                    Poll::Ready(Ok(true)) => continue,
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(false)) => return Poll::Ready(Ok(())),
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                },
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

impl<'a, Database: Connection + Clone, Config: FileConfig> Iterator
    for Contents<'a, Blocking<Database>, Config>
{
    type Item = std::io::Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.non_blocking_read_block() {
                NonBlockingBlockReadResult::ReadBlock(bytes) => return Some(Ok(bytes)),
                NonBlockingBlockReadResult::Eof => return None,
                NonBlockingBlockReadResult::NeedBlocks => match self.load_blocks() {
                    Ok(()) => {}
                    Err(err) => return Some(Err(err)),
                },
            }
        }
    }
}
#[cfg(feature = "async")]
impl<
        'a,
        Database: bonsaidb_core::connection::AsyncConnection + Clone + 'static,
        Config: FileConfig,
    > futures::Stream for Contents<'a, Async<Database>, Config>
{
    type Item = std::io::Result<Vec<u8>>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.spawn_block_fetching_task();
        loop {
            match self.non_blocking_read_block() {
                NonBlockingBlockReadResult::NeedBlocks => match self.fetch_blocks(cx) {
                    Poll::Ready(Ok(true)) => continue,
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(false)) => return Poll::Ready(None),
                    Poll::Ready(Err(err)) => return Poll::Ready(Some(Err(err))),
                },
                NonBlockingBlockReadResult::ReadBlock(block) => {
                    return Poll::Ready(Some(Ok(block)))
                }
                NonBlockingBlockReadResult::Eof => return Poll::Ready(None),
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct BlockInfo {
    pub offset: u64,
    pub length: usize,
    pub timestamp: TimestampAsNanoseconds,
    pub header: Header,
}

/// A buffered [`std::io::Write`] and [`std::io::Seek`] implementor for a
/// [`File`].
pub struct BufferedAppend<'a, Config: FileConfig, Database: Connection + Clone> {
    file: &'a mut File<Blocking<Database>, Config>,
    pub(crate) buffer: Vec<u8>,
    _config: PhantomData<Config>,
}

impl<'a, Config: FileConfig, Database: Connection + Clone> BufferedAppend<'a, Config, Database> {
    /// Sets the size of the buffer. For optimal use, this should be a multiple
    /// of [`Config::BLOCK_SIZE`](FileConfig::BLOCK_SIZE).
    ///
    /// If any data is already buffered, it will be flushed before the buffer is
    /// resized.
    pub fn set_buffer_size(&mut self, capacity: usize) -> std::io::Result<()> {
        if self.buffer.capacity() > 0 {
            self.flush()?;
        }
        self.buffer = Vec::with_capacity(capacity);
        Ok(())
    }
}

impl<'a, Config: FileConfig, Database: Connection + Clone> Write
    for BufferedAppend<'a, Config, Database>
{
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
            .append(&self.buffer)
            .map_err(|err| std::io::Error::new(ErrorKind::Other, err))?;
        self.buffer.clear();
        Ok(())
    }
}

impl<'a, Config: FileConfig, Database: Connection + Clone> Drop
    for BufferedAppend<'a, Config, Database>
{
    fn drop(&mut self) {
        drop(self.flush());
    }
}

/// A buffered [`tokio::io::AsyncWrite`] and [`std::io::Seek`] implementor for a
/// [`File`].
#[cfg(feature = "async")]
pub struct AsyncBufferedAppend<'a, Config: FileConfig, Database: AsyncConnection + Clone + 'static>
{
    file: &'a mut File<Async<Database>, Config>,
    pub(crate) buffer: Vec<u8>,
    flush_future: Option<BoxFuture<'a, Result<(), std::io::Error>>>,
    _config: PhantomData<Config>,
}

#[cfg(feature = "async")]
impl<'a, Config: FileConfig, Database: AsyncConnection + Clone + 'static>
    AsyncBufferedAppend<'a, Config, Database>
{
    /// Sets the size of the buffer. For optimal use, this should be a multiple
    /// of [`Config::BLOCK_SIZE`](FileConfig::BLOCK_SIZE).
    ///
    /// If any data is already buffered, it will be flushed before the buffer is
    /// resized.
    pub async fn set_buffer_size(&mut self, capacity: usize) -> std::io::Result<()> {
        if self.buffer.capacity() > 0 {
            self.flush().await?;
        }
        self.buffer = Vec::with_capacity(capacity);
        Ok(())
    }
}

#[cfg(feature = "async")]
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
            let file = self.file.clone();

            let mut buffer = Vec::with_capacity(self.buffer.capacity());
            std::mem::swap(&mut buffer, &mut self.buffer);

            let mut flush_task = async move {
                file.append(&buffer)
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

#[cfg(feature = "async")]
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
            let mut file = self.file.clone();

            tokio::runtime::Handle::current().spawn(async move {
                drop(
                    AsyncBufferedAppend {
                        file: &mut file,
                        buffer,
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
