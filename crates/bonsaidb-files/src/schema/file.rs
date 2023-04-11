use std::borrow::{Borrow, Cow};
use std::marker::PhantomData;

#[cfg(feature = "async")]
use bonsaidb_core::connection::AsyncConnection;
use bonsaidb_core::connection::{Bound, BoundRef, Connection, RangeRef, ViewMappings};
use bonsaidb_core::document::{CollectionDocument, Emit};
use bonsaidb_core::key::time::TimestampAsNanoseconds;
use bonsaidb_core::key::{
    ByteSource, CompositeKeyDecoder, CompositeKeyEncoder, CompositeKeyError, CompositeKind,
    IntoPrefixRange, Key, KeyEncoding, KeyKind,
};
use bonsaidb_core::schema::{
    Collection, CollectionMapReduce, CollectionName, DefaultSerialization, SerializedCollection,
    View, ViewMapResult,
};
use bonsaidb_core::transaction::{Operation, Transaction};
use bonsaidb_macros::ViewSchema;
use bonsaidb_utils::next_string_sequence;
use derive_where::derive_where;
use serde::{Deserialize, Serialize};

use crate::direct::BlockInfo;
use crate::schema::block::Block;
use crate::{BonsaiFiles, Error, FileConfig, Statistics, Truncate};

#[derive_where(Debug, Clone)]
#[derive(Serialize, Deserialize)]
pub struct File<Config = BonsaiFiles>
where
    Config: FileConfig,
{
    pub path: Option<String>,
    pub name: String,
    pub created_at: TimestampAsNanoseconds,
    pub metadata: Config::Metadata,

    #[serde(skip)]
    #[derive_where(skip)]
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
        metadata: Config::Metadata,
        database: &Database,
    ) -> Result<CollectionDocument<Self>, Error> {
        if name.contains('/') || name.is_empty() {
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
            metadata,
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
        metadata: Config::Metadata,
        database: &Database,
    ) -> Result<CollectionDocument<Self>, Error> {
        if name.contains('/') || name.is_empty() {
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
            metadata,
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
            FileKey::Full {
                path: Cow::Borrowed(if path.is_empty() { "/" } else { path }),
                name: Cow::Borrowed(name),
            }
        } else {
            FileKey::Full {
                path: Cow::Borrowed("/"),
                name: Cow::Borrowed(path),
            }
        };
        Ok(convert_mappings_to_documents(
            database.view::<ByPath<Config>>().with_key(&key).query()?,
        )?
        .into_iter()
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
            FileKey::Full {
                path: Cow::Borrowed(if path.is_empty() { "/" } else { path }),
                name: Cow::Borrowed(name),
            }
        } else {
            FileKey::Full {
                path: Cow::Borrowed("/"),
                name: Cow::Borrowed(path),
            }
        };
        Ok(convert_mappings_to_documents(
            database
                .view::<ByPath<Config>>()
                .with_key(&key)
                .query()
                .await?,
        )?
        .into_iter()
        .next())
    }

    pub fn list_path_contents<Database: Connection>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<CollectionDocument<Self>>, bonsaidb_core::Error> {
        convert_mappings_to_documents(
            database
                .view::<ByPath<Config>>()
                .with_key_prefix(&FileKey::ExactPath {
                    start: Box::new(FileKey::ExactPathPart { path, start: true }),
                    end: Box::new(FileKey::ExactPathPart { path, start: false }),
                })
                .query()?,
        )
    }

    #[cfg(feature = "async")]
    pub async fn list_path_contents_async<Database: AsyncConnection>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<CollectionDocument<Self>>, bonsaidb_core::Error> {
        convert_mappings_to_documents(
            database
                .view::<ByPath<Config>>()
                .with_key_prefix(&FileKey::ExactPath {
                    start: Box::new(FileKey::ExactPathPart { path, start: true }),
                    end: Box::new(FileKey::ExactPathPart { path, start: false }),
                })
                .query()
                .await?,
        )
    }

    pub fn list_recursive_path_contents<Database: Connection>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<CollectionDocument<Self>>, bonsaidb_core::Error> {
        convert_mappings_to_documents(
            database
                .view::<ByPath<Config>>()
                .with_key_prefix(&FileKey::RecursivePath {
                    start: Box::new(FileKey::RecursivePathPart { path, start: true }),
                    end: Box::new(FileKey::RecursivePathPart { path, start: false }),
                })
                .query()?,
        )
    }

    #[cfg(feature = "async")]
    pub async fn list_recursive_path_contents_async<Database: AsyncConnection>(
        path: &str,
        database: &Database,
    ) -> Result<Vec<CollectionDocument<Self>>, bonsaidb_core::Error> {
        convert_mappings_to_documents(
            database
                .view::<ByPath<Config>>()
                .with_key_prefix(&FileKey::RecursivePath {
                    start: Box::new(FileKey::RecursivePathPart { path, start: true }),
                    end: Box::new(FileKey::RecursivePathPart { path, start: false }),
                })
                .query()
                .await?,
        )
    }

    pub fn summarize_recursive_path_contents<Database: Connection>(
        path: &str,
        database: &Database,
    ) -> Result<Statistics, bonsaidb_core::Error> {
        let ids = database
            .view::<ByPath<Config>>()
            .with_key_prefix(&FileKey::RecursivePath {
                start: Box::new(FileKey::RecursivePathPart { path, start: true }),
                end: Box::new(FileKey::RecursivePathPart { path, start: false }),
            })
            .query()?
            .iter()
            .map(|mapping| mapping.source.id.deserialize())
            .collect::<Result<Vec<u32>, _>>()?;
        let append_info = Block::<Config>::summary_for_ids(&ids, database)?;
        Ok(Statistics {
            total_bytes: append_info.length,
            last_appended_at: append_info.timestamp,
            file_count: ids.len(),
        })
    }

    #[cfg(feature = "async")]
    pub async fn summarize_recursive_path_contents_async<Database: AsyncConnection>(
        path: &str,
        database: &Database,
    ) -> Result<Statistics, bonsaidb_core::Error> {
        let ids = database
            .view::<ByPath<Config>>()
            .with_key_prefix(&FileKey::RecursivePath {
                start: Box::new(FileKey::RecursivePathPart { path, start: true }),
                end: Box::new(FileKey::RecursivePathPart { path, start: false }),
            })
            .query()
            .await?
            .iter()
            .map(|mapping| mapping.source.id.deserialize())
            .collect::<Result<Vec<u32>, _>>()?;
        let append_info = Block::<Config>::summary_for_ids_async(&ids, database).await?;
        Ok(Statistics {
            total_bytes: append_info.length,
            last_appended_at: append_info.timestamp,
            file_count: ids.len(),
        })
    }

    pub fn truncate<Database: Connection>(
        file: &CollectionDocument<Self>,
        new_length: u64,
        from: Truncate,
        database: &Database,
    ) -> Result<(), bonsaidb_core::Error> {
        let tx = Self::create_truncate_transaction(
            Block::<Config>::for_file(file.header.id, database)?,
            new_length,
            from,
        );

        tx.apply(database)?;
        Ok(())
    }

    fn create_truncate_transaction(
        mut blocks: Vec<BlockInfo>,
        new_length: u64,
        from: Truncate,
    ) -> Transaction {
        let total_length: u64 = blocks
            .iter()
            .map(|b| u64::try_from(b.length).unwrap())
            .sum();
        let mut tx = Transaction::new();
        if let Some(mut bytes_to_remove) = total_length.checked_sub(new_length) {
            let block_collection = Config::blocks_name();
            while bytes_to_remove > 0 && !blocks.is_empty() {
                let offset = match from {
                    Truncate::RemovingStart => 0,
                    Truncate::RemovingEnd => blocks.len() - 1,
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
        tx
    }

    #[cfg(feature = "async")]
    pub async fn truncate_async<Database: AsyncConnection>(
        file: &CollectionDocument<Self>,
        new_length: u64,
        from: Truncate,
        database: &Database,
    ) -> Result<(), bonsaidb_core::Error> {
        let tx = Self::create_truncate_transaction(
            Block::<Config>::for_file_async(file.header.id, database).await?,
            new_length,
            from,
        );

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
#[derive(View, ViewSchema)]
#[view(name = "by-path", collection = File<Config>, key = OwnedFileKey, value = (TimestampAsNanoseconds, Config::Metadata))]
#[view(core = bonsaidb_core)]
#[view_schema(version = 3, unique = true, core = bonsaidb_core)]
pub struct ByPath<Config>(PhantomData<Config>)
where
    Config: FileConfig;

impl<Config> CollectionMapReduce for ByPath<Config>
where
    Config: FileConfig,
{
    fn map<'doc>(&self, doc: CollectionDocument<File<Config>>) -> ViewMapResult<'doc, Self> {
        doc.header.emit_key_and_value(
            OwnedFileKey(FileKey::Full {
                path: Cow::Owned(doc.contents.path.unwrap_or_else(|| String::from("/"))),
                name: Cow::Owned(doc.contents.name),
            }),
            (doc.contents.created_at, doc.contents.metadata),
        )
    }
}

impl<'a> PartialEq<FileKey<'a>> for OwnedFileKey {
    fn eq(&self, other: &FileKey<'a>) -> bool {
        self.0.eq(other)
    }
}

impl<'a> IntoPrefixRange<'a, OwnedFileKey> for FileKey<'a> {
    fn to_prefix_range(&'a self) -> RangeRef<'a, OwnedFileKey, Self> {
        match self {
            FileKey::ExactPath { start, end } | FileKey::RecursivePath { start, end } => RangeRef {
                start: BoundRef::borrowed(Bound::Included(start)),
                end: BoundRef::borrowed(Bound::Excluded(end)),
            },
            FileKey::Full { .. }
            | FileKey::ExactPathPart { .. }
            | FileKey::RecursivePathPart { .. } => {
                unreachable!()
            }
        }
    }
}

fn convert_mappings_to_documents<Config: FileConfig>(
    mappings: ViewMappings<ByPath<Config>>,
) -> Result<Vec<CollectionDocument<File<Config>>>, bonsaidb_core::Error> {
    let mut docs = Vec::with_capacity(mappings.len());
    for mapping in mappings {
        if let OwnedFileKey(FileKey::Full { path, name }) = mapping.key {
            docs.push(CollectionDocument {
                header: mapping.source.try_into()?,
                contents: File {
                    path: match path.into_owned() {
                        path if path == "/" => None,
                        other => Some(other),
                    },
                    name: name.into_owned(),
                    created_at: mapping.value.0,
                    metadata: mapping.value.1,
                    _name: PhantomData,
                },
            });
        }
    }

    Ok(docs)
}

#[derive(Debug, Clone, PartialEq)]
pub struct OwnedFileKey(FileKey<'static>);

impl<'a> Borrow<FileKey<'a>> for OwnedFileKey {
    fn borrow(&self) -> &FileKey<'a> {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
enum FileKey<'a> {
    Full {
        path: Cow<'a, str>,
        name: Cow<'a, str>,
    },
    ExactPath {
        start: Box<FileKey<'a>>,
        end: Box<FileKey<'a>>,
    },
    ExactPathPart {
        path: &'a str,
        start: bool,
    },
    RecursivePath {
        start: Box<FileKey<'a>>,
        end: Box<FileKey<'a>>,
    },
    RecursivePathPart {
        path: &'a str,
        start: bool,
    },
}

impl<'k> Key<'k> for OwnedFileKey {
    const CAN_OWN_BYTES: bool = false;

    fn from_ord_bytes<'e>(bytes: ByteSource<'k, 'e>) -> Result<Self, Self::Error> {
        let mut decoder = CompositeKeyDecoder::default_for(bytes);

        let path = Cow::Owned(decoder.decode::<String>()?);
        let name = Cow::Owned(decoder.decode::<String>()?);
        decoder.finish()?;

        Ok(Self(FileKey::Full { path, name }))
    }
}

impl KeyEncoding<Self> for OwnedFileKey {
    type Error = CompositeKeyError;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: bonsaidb_core::key::KeyVisitor,
    {
        FileKey::describe(visitor);
    }

    fn as_ord_bytes(&self) -> Result<std::borrow::Cow<'_, [u8]>, Self::Error> {
        self.0.as_ord_bytes()
    }
}

impl<'fk> KeyEncoding<OwnedFileKey> for FileKey<'fk> {
    type Error = CompositeKeyError;

    const LENGTH: Option<usize> = None;

    fn describe<Visitor>(visitor: &mut Visitor)
    where
        Visitor: bonsaidb_core::key::KeyVisitor,
    {
        visitor.visit_composite(
            CompositeKind::Struct(Cow::Borrowed("bonsaidb_files::schema::file::FileKey")),
            2,
        );
        visitor.visit_type(KeyKind::String);
        visitor.visit_type(KeyKind::String);
    }

    fn as_ord_bytes(&self) -> Result<std::borrow::Cow<'_, [u8]>, Self::Error> {
        match self {
            FileKey::Full { path, name } => {
                let mut encoder = CompositeKeyEncoder::default();
                encoder.encode(&path)?;
                encoder.encode(&name)?;
                Ok(Cow::Owned(encoder.finish()))
            }
            FileKey::ExactPathPart { path, start } => {
                let mut bytes = Vec::new();
                // The path needs to end with a /. Rather than force an allocation to
                // append it to a string before calling encode_composite_key, we're
                // manually encoding the key taking this adjustment into account.

                bytes.extend(path.bytes());

                if !path.as_bytes().ends_with(b"/") {
                    bytes.push(b'/');
                }
                // Variable encoding adds a null byte at the end of the string, we can
                // use this padding byte to create our exclusive range
                if *start {
                    bytes.push(0);
                } else {
                    bytes.push(1);
                }
                Ok(Cow::Owned(bytes))
            }
            FileKey::RecursivePathPart { path, start } => {
                let mut encoder = CompositeKeyEncoder::default();
                if *start {
                    encoder.encode(path)?;
                } else {
                    let next = next_string_sequence(path);
                    encoder.encode(&next)?;
                }
                Ok(Cow::Owned(encoder.finish()))
            }
            FileKey::ExactPath { .. } | FileKey::RecursivePath { .. } => unreachable!(),
        }
    }
}
