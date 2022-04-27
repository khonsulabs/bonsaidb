use std::io::{Read, Write};

use bonsaidb_core::{
    connection::Connection,
    schema::{CollectionName, Qualified, Schema, SchemaName, Schematic},
};
use bonsaidb_files::{BonsaiFiles, FileConfig};
use bonsaidb_local::{
    config::{Builder, StorageConfiguration},
    Database,
};

#[derive(Debug)]
enum MultipleConfigs {}

impl Schema for MultipleConfigs {
    fn schema_name() -> SchemaName {
        SchemaName::private("multiple-configs")
    }

    fn define_collections(schema: &mut Schematic) -> Result<(), bonsaidb_core::Error> {
        BonsaiFiles::register_collections(schema)?;
        ProcessedFiles::register_collections(schema)?;

        Ok(())
    }
}

#[derive(Debug)]
enum ProcessedFiles {}

impl FileConfig for ProcessedFiles {
    type Metadata = ();
    const BLOCK_SIZE: usize = 16_384;

    fn files_name() -> CollectionName {
        CollectionName::private("processed-files")
    }

    fn blocks_name() -> CollectionName {
        CollectionName::private("processed-file-blocks")
    }
}

#[cfg_attr(test, test)]
fn main() -> anyhow::Result<()> {
    let database =
        Database::open::<MultipleConfigs>(StorageConfiguration::new("file-configs.bonsaidb"))?;

    cleanup_old_job(&database)?;
    // When using two separate configurations where the `CollectionName`s are
    // unique, we can have two unique "filesystems" stored within a single
    // database.
    //
    // This could be used to store files that need to be processed in one
    // location while storing the processed results in another location.
    BonsaiFiles::build("job1")
        .contents(b"some contents")
        .create(&database)?;

    process_file("/job1", &database)?;

    Ok(())
}

fn process_file<Database: Connection + Clone>(
    path: &str,
    database: &Database,
) -> anyhow::Result<()> {
    let input_file = BonsaiFiles::load(path, database)?.expect("Input file not found");
    let mut output_file = ProcessedFiles::build(path).create(database)?;

    if let Err(err) = process_input(input_file.contents()?, output_file.append_buffered()) {
        output_file.delete()?;
        anyhow::bail!(err);
    }

    Ok(())
}

fn process_input<Reader: Read, Writer: Write>(
    mut input: Reader,
    mut output: Writer,
) -> std::io::Result<u64> {
    // This could be something other than a simple copy.
    std::io::copy(&mut input, &mut output)
}

fn cleanup_old_job<Database: Connection + Clone>(database: &Database) -> anyhow::Result<()> {
    if let Some(file) = BonsaiFiles::load("/job1", database)? {
        file.delete()?;
    }

    if let Some(file) = ProcessedFiles::load("/job1", database)? {
        file.delete()?;
    }
    Ok(())
}
