use std::{borrow::Cow, marker::PhantomData};

use bonsaidb_core::{
    connection::Connection,
    document::{CollectionDocument, Emit},
    key::{
        decode_composite_field, encode_composite_field, time::TimestampAsSeconds,
        CompositeKeyError, Key, KeyEncoding,
    },
    schema::{
        Collection, CollectionName, CollectionViewSchema, DefaultSerialization,
        SerializedCollection, View, ViewMapResult,
    },
};
use derive_where::derive_where;
use serde::{Deserialize, Serialize};

use crate::{
    metadata::{Kind, Metadata, Permissions},
    schema::block::Block,
    BonsaiFiles, FileConfig,
};

#[derive_where(Debug)]
#[derive(Serialize, Deserialize)]
pub struct File<Config = BonsaiFiles> {
    pub parent: Option<u32>,
    pub name: String,
    pub metadata: Metadata,

    #[serde(skip)]
    _name: PhantomData<Config>,
}

impl<Config> File<Config>
where
    Config: FileConfig,
{
    pub fn create_file<Database: Connection>(
        path: String,
        permissions: Option<Permissions>,
        contents: &[u8],
        database: &Database,
    ) -> Result<CollectionDocument<Self>, bonsaidb_core::Error> {
        let mut current_directory_id = Option::<u32>::None;
        let mut components = path.split('/').peekable();
        let name = loop {
            let name = components.next().unwrap();
            if components.peek().is_some() {
                // Find this file, verify it's a directory, and loop
                let mappings = database
                    .view::<ByName<Config>>()
                    .with_key(FileKey {
                        parent_id: current_directory_id,
                        name: Cow::Borrowed(name),
                    })
                    .query()?;
                if let Some(mapping) = mappings.first() {
                    current_directory_id = Some(match mapping.value.kind {
                        Kind::File => {
                            todo!("file reached when looking for directories")
                        }
                        Kind::Directory => mapping.source.id.deserialize()?,
                        Kind::Link { .. } => todo!("resolve link"),
                    });
                }
            } else {
                // This is the location we need to create the file at.
                break name;
            }
        };

        let now = TimestampAsSeconds::now();
        let file = File {
            parent: current_directory_id,
            name: name.to_string(),
            metadata: Metadata {
                kind: Kind::File,
                permissions,
                created_at: now,
                last_updated_at: now,
            },
            _name: PhantomData,
        }
        .push_into(database)?;
        Block::<Config>::append(contents, file.header.id, database)?;
        Ok(file)
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
        schema.define_view(ByName::<Config>::default())?;

        Ok(())
    }
}

impl<Config> DefaultSerialization for File<Config> where Config: FileConfig {}

#[derive_where(Clone, Debug, Default)]
#[derive(View)]
#[view(name = "by-name", collection = File<Config>, key = OwnedFileKey, value = Metadata)]
#[view(core = bonsaidb_core)]
struct ByName<Config>(PhantomData<Config>)
where
    Config: FileConfig;

impl<Config> CollectionViewSchema for ByName<Config>
where
    Config: FileConfig,
{
    type View = Self;

    fn unique(&self) -> bool {
        true
    }

    fn map(&self, doc: CollectionDocument<File<Config>>) -> ViewMapResult<Self::View> {
        doc.header.emit_key_and_value(
            OwnedFileKey(FileKey {
                parent_id: doc.contents.parent,
                name: Cow::Owned(doc.contents.name),
            }),
            doc.contents.metadata,
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct FileKey<'a> {
    parent_id: Option<u32>,
    name: Cow<'a, str>,
}

impl<'a> From<FileKey<'a>> for OwnedFileKey {
    fn from(file: FileKey<'a>) -> Self {
        Self(FileKey {
            parent_id: file.parent_id,
            name: match file.name {
                Cow::Borrowed(str) => Cow::Owned(str.to_string()),
                Cow::Owned(owned) => Cow::Owned(owned),
            },
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(transparent)]
struct OwnedFileKey(FileKey<'static>);

impl<'k> Key<'k> for FileKey<'k> {
    fn from_ord_bytes(bytes: &'k [u8]) -> Result<Self, Self::Error> {
        let (parent_id, bytes) = decode_composite_field(bytes)?;
        let (name, _bytes) = decode_composite_field(bytes)?;
        // TODO verify eof
        Ok(Self { parent_id, name })
    }
}

impl<'k> KeyEncoding<'k, Self> for FileKey<'k> {
    type Error = CompositeKeyError;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'k self) -> Result<std::borrow::Cow<'k, [u8]>, Self::Error> {
        let mut bytes = Vec::new();
        encode_composite_field(&self.parent_id, &mut bytes)?;
        encode_composite_field(&self.name, &mut bytes)?;
        Ok(Cow::Owned(bytes))
    }
}

impl<'k, 'fk> KeyEncoding<'k, OwnedFileKey> for FileKey<'fk> {
    type Error = CompositeKeyError;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'k self) -> Result<std::borrow::Cow<'k, [u8]>, Self::Error> {
        let mut bytes = Vec::new();
        encode_composite_field(&self.parent_id, &mut bytes)?;
        encode_composite_field(&self.name, &mut bytes)?;
        Ok(Cow::Owned(bytes))
    }
}

impl<'k> Key<'k> for OwnedFileKey {
    fn from_ord_bytes(bytes: &'k [u8]) -> Result<Self, Self::Error> {
        FileKey::from_ord_bytes(bytes).map(Self::from)
    }
}

impl<'k> KeyEncoding<'k, Self> for OwnedFileKey {
    type Error = CompositeKeyError;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'k self) -> Result<std::borrow::Cow<'k, [u8]>, Self::Error> {
        KeyEncoding::<Self>::as_ord_bytes(&self.0)
    }
}
