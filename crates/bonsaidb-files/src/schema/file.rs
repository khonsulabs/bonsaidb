use std::{borrow::Cow, marker::PhantomData};

#[cfg(feature = "async")]
use bonsaidb_core::connection::AsyncConnection;
use bonsaidb_core::{
    connection::{Connection, Range},
    document::{CollectionDocument, Emit},
    key::{
        time::TimestampAsNanoseconds, CompositeKeyDecoder, CompositeKeyEncoder, CompositeKeyError,
        IntoPrefixRange, Key, KeyEncoding,
    },
    schema::{
        Collection, CollectionName, CollectionViewSchema, DefaultSerialization,
        SerializedCollection, View, ViewMapResult,
    },
    transaction::{Operation, Transaction},
};
use bonsaidb_utils::next_string_sequence;
use derive_where::derive_where;
use serde::{Deserialize, Serialize};

use crate::{
    direct::BlockInfo, schema::block::Block, BonsaiFiles, Error, FileConfig, TruncateFrom,
};

#[derive_where(Debug, Clone)]
#[derive(Serialize, Deserialize)]
pub struct File<Config = BonsaiFiles> {
    pub path: Option<String>,
    pub name: String,
    pub created_at: TimestampAsNanoseconds,

    #[serde(skip)]
    _name: PhantomData<Config>,
}

impl<Config> File<Config>
where
    Config: FileConfig,
{
    pub fn create_file<Database: Connection>(
        mut path: Option<String>,
        name: String,
        contents: &[u8],
        database: &Database,
    ) -> Result<CollectionDocument<Self>, Error> {
        if name.contains('/') {
            return Err(Error::InvalidName);
        }

        // Force path to end with a /
        if let Some(path) = path.as_mut() {
            if path.bytes().last() != Some(b'/') {
                path.push('/');
            }
        }

        let now = TimestampAsNanoseconds::now();
        let file = File {
            path,
            name,
            created_at: now,
            _name: PhantomData,
        }
        .push_into(database)?;
        Block::<Config>::append(contents, file.header.id, database)?;
        Ok(file)
    }

    #[cfg(feature = "async")]
    pub async fn create_file_async<Database: AsyncConnection>(
        mut path: Option<String>,
        name: String,
        contents: &[u8],
        database: &Database,
    ) -> Result<CollectionDocument<Self>, Error> {
        if name.contains('/') {
            return Err(Error::InvalidName);
        }

        // Force path to end with a /
        if let Some(path) = path.as_mut() {
            if path.bytes().last() != Some(b'/') {
                path.push('/');
            }
        }

        let now = TimestampAsNanoseconds::now();
        let file = File {
            path,
            name,
            created_at: now,
            _name: PhantomData,
        }
        .push_into_async(database)
        .await?;
        Block::<Config>::append_async(contents, file.header.id, database).await?;
        Ok(file)
    }

    pub fn find<Database: Connection>(
        mut path: &str,
        database: &Database,
    ) -> Result<Option<CollectionDocument<Self>>, Error> {
        if path.is_empty() {
            return Err(Error::InvalidPath);
        }

        // If the search is for a directory, the name is of the last component.
        // Remove the trailing slash if it's present
        if path.as_bytes()[path.len() - 1] == b'/' {
            path = &path[..path.len() - 1];
        }

        let key = if let Some(separator_index) = path.rfind('/') {
            let (path, name) = path.split_at(separator_index + 1);
            FileKey {
                path: Cow::Borrowed(if path.is_empty() { "/" } else { path }),
                name: Cow::Borrowed(name),
            }
        } else {
            FileKey {
                path: Cow::Borrowed("/"),
                name: Cow::Borrowed(path),
            }
        };
        Ok(database
            .view::<ByPath<Config>>()
            .with_key(key)
            .query_with_collection_docs()?
            .documents
            .into_iter()
            .map(|(_, doc)| doc)
            .next())
    }

    #[cfg(feature = "async")]
    pub async fn find_async<Database: AsyncConnection>(
        mut path: &str,
        database: &Database,
    ) -> Result<Option<CollectionDocument<Self>>, Error> {
        if path.is_empty() {
            return Err(Error::InvalidPath);
        }

        // If the search is for a directory, the name is of the last component.
        // Remove the trailing slash if it's present
        if path.as_bytes()[path.len() - 1] == b'/' {
            path = &path[..path.len() - 1];
        }

        let key = if let Some(separator_index) = path.rfind('/') {
            let (path, name) = path.split_at(separator_index + 1);
            FileKey {
                path: Cow::Borrowed(if path.is_empty() { "/" } else { path }),
                name: Cow::Borrowed(name),
            }
        } else {
            FileKey {
                path: Cow::Borrowed("/"),
                name: Cow::Borrowed(path),
            }
        };
        Ok(database
            .view::<ByPath<Config>>()
            .with_key(key)
            .query_with_collection_docs()
            .await?
            .documents
            .into_iter()
            .map(|(_, doc)| doc)
            .next())
    }

    pub fn list_path_contents<Database: Connection>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<CollectionDocument<Self>>, bonsaidb_core::Error> {
        Ok(database
            .view::<ByPath<Config>>()
            .with_key_prefix(ExactPathKey { path, start: true })
            .query_with_collection_docs()?
            .documents
            .into_iter()
            .map(|(_, doc)| doc)
            .collect())
    }

    #[cfg(feature = "async")]
    pub async fn list_path_contents_async<Database: AsyncConnection>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<CollectionDocument<Self>>, bonsaidb_core::Error> {
        Ok(database
            .view::<ByPath<Config>>()
            .with_key_prefix(ExactPathKey { path, start: true })
            .query_with_collection_docs()
            .await?
            .documents
            .into_iter()
            .map(|(_, doc)| doc)
            .collect())
    }

    pub fn list_recursive_path_contents<Database: Connection>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<CollectionDocument<Self>>, bonsaidb_core::Error> {
        Ok(database
            .view::<ByPath<Config>>()
            .with_key_prefix(RecursivePathKey { path, start: true })
            .query_with_collection_docs()?
            .documents
            .into_iter()
            .map(|(_, doc)| doc)
            .collect())
    }

    #[cfg(feature = "async")]
    pub async fn list_recursive_path_contents_async<Database: AsyncConnection>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<CollectionDocument<Self>>, bonsaidb_core::Error> {
        Ok(database
            .view::<ByPath<Config>>()
            .with_key_prefix(RecursivePathKey { path, start: true })
            .query_with_collection_docs()
            .await?
            .documents
            .into_iter()
            .map(|(_, doc)| doc)
            .collect())
    }

    pub fn truncate<Database: Connection>(
        file: &CollectionDocument<Self>,
        new_length: u64,
        from: TruncateFrom,
        database: &Database,
    ) -> Result<(), bonsaidb_core::Error> {
        let tx = Self::create_truncate_transaction(
            Block::<Config>::for_file(file.header.id, database)?,
            new_length,
            from,
        )?;

        tx.apply(database)?;
        Ok(())
    }

    fn create_truncate_transaction(
        mut blocks: Vec<BlockInfo>,
        new_length: u64,
        from: TruncateFrom,
    ) -> Result<Transaction, bonsaidb_core::Error> {
        let total_length: u64 = blocks
            .iter()
            .map(|b| u64::try_from(b.length).unwrap())
            .sum();
        let mut tx = Transaction::new();
        if let Some(mut bytes_to_remove) = total_length.checked_sub(new_length) {
            let block_collection = Config::blocks_name();
            while bytes_to_remove > 0 && !blocks.is_empty() {
                let offset = match from {
                    TruncateFrom::Start => 0,
                    TruncateFrom::End => blocks.len() - 1,
                };
                let block_length = u64::try_from(blocks[offset].length).unwrap();
                if block_length <= bytes_to_remove {
                    tx.push(Operation::delete(
                        block_collection.clone(),
                        blocks[offset].header.clone(),
                    ));
                    blocks.remove(offset);
                    bytes_to_remove -= block_length;
                } else {
                    // Partial removal. For now, we're just not going to support
                    // partial removes. This is just purely to keep things simple.
                    break;
                }
            }
        }
        Ok(tx)
    }

    #[cfg(feature = "async")]
    pub async fn truncate_async<Database: AsyncConnection>(
        file: &CollectionDocument<Self>,
        new_length: u64,
        from: TruncateFrom,
        database: &Database,
    ) -> Result<(), bonsaidb_core::Error> {
        let tx = Self::create_truncate_transaction(
            Block::<Config>::for_file_async(file.header.id, database).await?,
            new_length,
            from,
        )?;

        tx.apply_async(database).await?;
        Ok(())
    }
}

impl<Config> Collection for File<Config>
where
    Config: FileConfig,
{
    type PrimaryKey = u32;

    fn collection_name() -> CollectionName {
        Config::files_name()
    }

    fn define_views(
        schema: &mut bonsaidb_core::schema::Schematic,
    ) -> Result<(), bonsaidb_core::Error> {
        schema.define_view(ByPath::<Config>::default())?;

        Ok(())
    }
}

impl<Config> DefaultSerialization for File<Config> where Config: FileConfig {}

#[derive_where(Clone, Debug, Default)]
#[derive(View)]
#[view(name = "by-path", collection = File<Config>, key = OwnedFileKey, value = TimestampAsNanoseconds)]
#[view(core = bonsaidb_core)]
struct ByPath<Config>(PhantomData<Config>)
where
    Config: FileConfig;

impl<Config> CollectionViewSchema for ByPath<Config>
where
    Config: FileConfig,
{
    type View = Self;

    fn unique(&self) -> bool {
        true
    }

    fn map(&self, doc: CollectionDocument<File<Config>>) -> ViewMapResult<Self::View> {
        doc.header.emit_key_and_value(
            OwnedFileKey {
                path: doc.contents.path.unwrap_or_else(|| String::from("/")),
                name: doc.contents.name,
            },
            doc.contents.created_at,
        )
    }
}

#[derive(Debug, Clone)]
struct ExactPathKey<'a> {
    path: &'a str,
    start: bool,
}

impl<'k, 'pk> KeyEncoding<'k, OwnedFileKey> for ExactPathKey<'pk> {
    type Error = CompositeKeyError;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'k self) -> Result<std::borrow::Cow<'k, [u8]>, Self::Error> {
        let mut bytes = Vec::new();
        // The path needs to end with a /. Rather than force an allocation to
        // append it to a string before calling encode_composite_key, we're
        // manually encoding the key taking this adjustment into account.

        bytes.extend(self.path.bytes());

        if !self.path.as_bytes().ends_with(b"/") {
            bytes.push(b'/');
        }
        // Variable encoding adds a null byte at the end of the string, we can
        // use this padding byte to create our exclusive range
        if self.start {
            bytes.push(0)
        } else {
            bytes.push(1);
        }
        Ok(Cow::Owned(bytes))
    }
}

impl<'k> IntoPrefixRange for ExactPathKey<'k> {
    fn into_prefix_range(mut self) -> Range<Self> {
        self.start = true;
        let end = Self {
            path: self.path,
            start: false,
        };
        Range::from(self..end)
    }
}

#[derive(Debug, Clone)]
struct RecursivePathKey<'a> {
    path: &'a str,
    start: bool,
}

impl<'k, 'pk> KeyEncoding<'k, OwnedFileKey> for RecursivePathKey<'pk> {
    type Error = CompositeKeyError;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'k self) -> Result<std::borrow::Cow<'k, [u8]>, Self::Error> {
        let mut encoder = CompositeKeyEncoder::default();
        if self.start {
            encoder.encode(&self.path)?;
        } else {
            let next = next_string_sequence(self.path);
            encoder.encode(&next)?;
        }

        Ok(Cow::Owned(encoder.finish()))
    }
}

impl<'k> IntoPrefixRange for RecursivePathKey<'k> {
    fn into_prefix_range(mut self) -> Range<Self> {
        self.start = true;
        let end = Self {
            path: self.path,
            start: false,
        };
        Range::from(self..end)
    }
}

#[derive(Debug, Clone)]
struct OwnedFileKey {
    path: String,
    name: String,
}

impl<'k> Key<'k> for OwnedFileKey {
    fn from_ord_bytes(bytes: &'k [u8]) -> Result<Self, Self::Error> {
        let mut decoder = CompositeKeyDecoder::new(bytes);

        let path = decoder.decode()?;
        let name = decoder.decode()?;
        decoder.finish()?;
        Ok(Self { path, name })
    }
}

impl<'k> KeyEncoding<'k, Self> for OwnedFileKey {
    type Error = CompositeKeyError;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'k self) -> Result<std::borrow::Cow<'k, [u8]>, Self::Error> {
        let mut encoder = CompositeKeyEncoder::default();
        encoder.encode(&self.path)?;
        encoder.encode(&self.name)?;
        Ok(Cow::Owned(encoder.finish()))
    }
}

#[derive(Debug, Clone)]
struct FileKey<'a> {
    path: Cow<'a, str>,
    name: Cow<'a, str>,
}

impl<'k, 'fk> KeyEncoding<'k, OwnedFileKey> for FileKey<'fk> {
    type Error = CompositeKeyError;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'k self) -> Result<std::borrow::Cow<'k, [u8]>, Self::Error> {
        let mut encoder = CompositeKeyEncoder::default();
        encoder.encode(&self.path)?;
        encoder.encode(&self.name)?;
        Ok(Cow::Owned(encoder.finish()))
    }
}
