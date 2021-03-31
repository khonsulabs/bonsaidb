//! Local database tool to save and load databases into plain an easy-to-consume
//! filesystem structure.
//!
//! This tool is provided to ensure you always have a way to get your data
//! backed up in a format that's easily consumable. This tool also provides a
//! safety mechanism allowing a path of migrating across underlying storage
//! layer changes that might have no other migration path.
//!
//! To back up an existing local database:
//!
//! ```sh
//! pliantdb local-backup <database_path> save
//! ```
//!
//! To restore a backup:
//!
//! ```sh
//! pliantdb local-backup <database_path> load <backup_location>
//! ```

use std::{
    borrow::Cow,
    ffi::OsString,
    path::{Path, PathBuf},
    str::FromStr,
};

use flume::Receiver;
use pliantdb_core::{
    document::{Document, Header, Revision},
    schema::{collection, Key},
};
use structopt::StructOpt;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::{
    config::Configuration,
    storage::{document_tree_name, Storage},
};

/// The command line interface for `pliantdb local-backup`.
#[derive(StructOpt, Debug)]
pub struct Cli {
    /// The path to the database you wish to operate on.
    pub database_path: PathBuf,

    /// The command to execute on the database.
    #[structopt(subcommand)]
    pub command: Command,
}

/// The command to execute.
#[derive(StructOpt, Debug)]
pub enum Command {
    /// Exports all of the data into a straightforward file structure.
    ///
    /// This command will create a single folder within `output_directory` named
    /// `output_name`. Within that folder, one subfolder will be created for
    /// each collection [`collection::Id`]. Each folder will contain files named
    /// `<Document.header.id>.<Document.header.revision.id>`, and the files will
    /// contain the raw bytes stored inside of the documents. Assuming you're
    /// using the built in Serialization, the data will be in the
    /// [CBOR](https://cbor.io/) format, otherwise it will be the bytes you
    /// stored within the database.
    ///
    /// This format should make it easy to migrate data as well as back it up
    /// using many traditional methods, and should be considered the official
    /// way to do a full export of a database without using the API.
    Save {
        /// The directory to export the data within. The process will create a
        /// subfolder using `output_name`. If omitted, the export is performed
        /// next to the source database.
        output_directory: Option<PathBuf>,

        /// The name of the folder to export the data to. If not specified, the
        /// ".backup" is appended to the source database's name and used.
        output_name: Option<String>,
    },

    /// Loads all of the data from a previously saved backup. Any documents
    /// with the same IDs will be overwritten by the documents in this backup.
    Load {
        /// The path to the previously saved backup.
        backup: PathBuf,
    },
}

impl Command {
    /// Executes the command.
    pub async fn execute(&self, database_path: PathBuf) -> anyhow::Result<()> {
        match self {
            Self::Save {
                output_directory,
                output_name,
            } => {
                self.save(database_path, output_directory, output_name)
                    .await
            }
            Self::Load { backup } => self.load(&database_path, backup).await,
        }
    }

    async fn save(
        &self,
        database_path: PathBuf,
        output_directory: &Option<PathBuf>,
        output_name: &Option<String>,
    ) -> anyhow::Result<()> {
        if !database_path.exists() {
            anyhow::bail!("database_path does not exist");
        }

        let db = Storage::<()>::open_local(&database_path, &Configuration::default()).await?;

        let output_directory = if let Some(output_directory) = output_directory {
            output_directory.clone()
        } else {
            database_path.parent().map(ToOwned::to_owned).unwrap()
        };
        let output_name = if let Some(output_name) = output_name.clone() {
            PathBuf::from_str(&output_name)?
        } else {
            let mut name = database_path.file_name().unwrap().to_owned();
            name.push(&OsString::from(".backup"));
            PathBuf::from(name)
        };
        let backup_directory = output_directory.join(output_name);

        // use a channel to split receiving documents to save them and writing
        // to disk. We're using a bounded channel to limit RAM usage, since
        // reading will likely be much faster than writing.
        let (sender, receiver) = flume::bounded(100);
        let document_writer = tokio::spawn(write_documents(receiver, backup_directory));
        tokio::task::block_in_place::<_, anyhow::Result<()>>(|| {
            for collection_tree in db
                .sled
                .tree_names()
                .into_iter()
                .filter(|tree| tree.starts_with(b"collection::"))
            {
                let tree = db.sled.open_tree(&collection_tree)?;
                for result in tree.iter() {
                    let (_, document) = result?;
                    let document = bincode::deserialize::<Document<'_>>(&document)?;
                    sender.send(document.to_owned())?;
                }
            }

            Ok(())
        })?;

        drop(sender);

        document_writer.await?
    }

    async fn load(&self, database_path: &Path, backup: &Path) -> anyhow::Result<()> {
        let db = Storage::<()>::open_local(database_path, &Configuration::default()).await?;
        let (sender, receiver) = flume::bounded(100);

        let document_restorer = tokio::task::spawn_blocking(|| restore_documents(receiver, db));

        let mut collections = tokio::fs::read_dir(&backup).await?;
        while let Some(collection_folder) = collections.next_entry().await? {
            let collection_folder = collection_folder.path();
            let collection = collection_folder
                .file_name()
                .unwrap()
                .to_str()
                .expect("invalid collection name encountered");
            let collection = collection::Id::from(collection.to_owned());
            println!("Restoring {}", collection);

            let mut entries = tokio::fs::read_dir(&collection_folder).await?;
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                if path.extension() == Some(&OsString::from("cbor")) {
                    let file_name = path
                        .file_name()
                        .unwrap()
                        .to_str()
                        .expect("invalid file name encountered");
                    let parts = file_name.split('.').collect::<Vec<_>>();
                    let id = parts[0].parse::<u64>()?;
                    let revision = parts[1].parse::<u32>()?;
                    let mut file = File::open(&path).await?;
                    let mut contents = Vec::new();
                    file.read_to_end(&mut contents).await?;

                    let doc = Document {
                        header: Cow::Owned(Header {
                            id,
                            revision: Revision::with_id(revision, &contents),
                        }),
                        collection: collection.clone(),
                        contents: Cow::Owned(contents),
                    };
                    sender.send_async(doc).await?;
                }
            }
        }

        drop(sender);

        document_restorer.await?
    }
}

async fn write_documents(
    receiver: Receiver<Document<'static>>,
    backup: PathBuf,
) -> anyhow::Result<()> {
    if !backup.exists() {
        tokio::fs::create_dir(&backup).await?;
    }

    while let Ok(document) = receiver.recv_async().await {
        let collection_directory = backup.join(document.collection.0.as_ref());
        if !collection_directory.exists() {
            tokio::fs::create_dir(&collection_directory).await?;
        }
        let document_path = collection_directory.join(format!(
            "{}.{}.cbor",
            document.header.id, document.header.revision.id
        ));
        let mut file = File::create(&document_path).await?;
        file.write_all(&document.contents).await?;
    }

    Ok(())
}

#[allow(clippy::clippy::needless_pass_by_value)] // it's not needless, it's to avoid a borrow that would need to span a 'static lifetime
fn restore_documents(receiver: Receiver<Document<'static>>, db: Storage<()>) -> anyhow::Result<()> {
    while let Ok(doc) = receiver.recv() {
        let tree = db.sled.open_tree(document_tree_name(&doc.collection))?;
        tree.insert(
            doc.header.id.as_big_endian_bytes()?,
            bincode::serialize(&doc)?,
        )?;
    }

    db.sled.flush()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use pliantdb_core::{
        connection::Connection,
        test_util::{Basic, TestDirectory},
    };

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn backup_restore() -> anyhow::Result<()> {
        let backup_destination = TestDirectory::new("backup-restore.pliantdb.backup");

        // First, create a database that we'll be restoring. `TestDirectory`
        // will automatically erase the database when it drops out of scope,
        // which is why we're creating a nested scope here.
        let test_doc = {
            let database_directory = TestDirectory::new("backup-restore.pliantdb");
            let db = Storage::<Basic>::open_local(&database_directory, &Configuration::default())
                .await?;
            let test_doc = db
                .collection::<Basic>()?
                .push(&Basic::new("somevalue"))
                .await?;
            drop(db);

            Command::Save {
                output_directory: None,
                output_name: Some(
                    backup_destination
                        .0
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_owned(),
                ),
            }
            .execute(database_directory.0.clone())
            .await?;
            test_doc
        };

        // `backup_destination` now contains an export of the database, time to try loading it:
        let database_directory = TestDirectory::new("backup-restore.pliantdb");
        Command::Load {
            backup: backup_destination.0.clone(),
        }
        .execute(database_directory.0.clone())
        .await?;

        let db =
            Storage::<Basic>::open_local(&database_directory, &Configuration::default()).await?;
        let doc = db
            .get::<Basic>(test_doc.id)
            .await?
            .expect("Backed up document.not found");
        let contents = doc.contents::<Basic>()?;
        assert_eq!(contents.value, "somevalue");

        Ok(())
    }
}
