use std::{
    io::{Read, Seek, SeekFrom, Write},
    mem::size_of,
};

use bonsaidb_files::{BonsaiFiles, FileConfig, FilesSchema};
use bonsaidb_local::{
    config::{Builder, StorageConfiguration},
    Database,
};

#[cfg_attr(test, test)]
fn main() -> anyhow::Result<()> {
    // Create a database for our files. If you would like to use these
    // collections in an existing datasbase/schema, `BonsaiFiles` exposes a
    // function `define_collections()` which can be called from your
    // `Schema::define_collections()` implementation.
    //
    // Or, if you're using the Schema derive macro, you can add a parameter
    // `include = [FilesSchema<BonsaiFiles>]` to use BonsaiFiles within your
    // existing schema.
    let database = Database::open::<FilesSchema<BonsaiFiles>>(StorageConfiguration::new(
        "basic-files.bonsaidb",
    ))?;

    // This crate provides a very basic path-based file storage. Documents can
    // be up to 4GB in size, but must be loaded completely to access. Files
    // stored using `bonsaidb-files` are broken into blocks and can be streamed
    // and/or randomly accessed.
    //
    // The `BonsaiFiles` type implements `FileConfig` and defines a block size
    // of 64kb.
    let mut one_megabyte = Vec::with_capacity(1024 * 1024);
    for i in 0..one_megabyte.capacity() / size_of::<u32>() {
        // Each u32 in the file will be the current offset in the file.
        let offset = u32::try_from(i * size_of::<u32>()).unwrap();
        one_megabyte.extend(offset.to_be_bytes());
    }
    let mut file = BonsaiFiles::build("some-file")
        .contents(&one_megabyte)
        .create(database)?;

    // By default, files will be stored at the root level:
    assert_eq!(file.path(), "/some-file");

    // We can access this file's contents using `std::io::Read` and
    // `std::io::Seek`.
    let mut contents = file.contents()?;
    assert_eq!(contents.len(), u64::try_from(one_megabyte.len()).unwrap());
    contents.seek(SeekFrom::Start(1024))?;
    let mut offset = [0; size_of::<u32>()];
    contents.read_exact(&mut offset)?;
    let offset = u32::from_be_bytes(offset);
    assert_eq!(offset, 1024);
    drop(contents);

    // Files can be appended to, but existing contents cannot be modified.
    // `File::append()` can be used to write data that you have in memory.
    // Alternatively, a buffered writer can be used to write larger amounts of
    // data using `std::io::Write`.
    let mut writer = file.append_buffered();
    let mut reader = &one_megabyte[..];
    let bytes_written = std::io::copy(&mut reader, &mut writer)?;
    assert_eq!(bytes_written, u64::try_from(one_megabyte.len()).unwrap());
    writer.flush()?;
    // The writer will attempt to flush on drop if there are any bytes remaining
    // in the buffer. Any errors will be ignored, however, so it is safer to
    // flush where you can handle the error.
    drop(writer);

    // Verify the file has the new contents.
    let contents = file.contents()?;
    assert_eq!(
        contents.len(),
        u64::try_from(one_megabyte.len()).unwrap() * 2
    );

    // Clean up the file.
    file.delete()?;

    Ok(())
}
