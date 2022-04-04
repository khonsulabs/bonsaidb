use std::{
    io::{Read, Seek, Write},
    mem::size_of,
};

use bonsaidb_core::test_util::TestDirectory;
use bonsaidb_local::{
    config::{Builder, StorageConfiguration},
    Database,
};

use crate::{BonsaiFiles, CreateFile, Error, File, FileConfig, FilesSchema, TruncateFrom};

#[test]
fn simple_file_test() {
    let directory = TestDirectory::new("simple-file");
    let database = Database::open::<FilesSchema>(StorageConfiguration::new(&directory)).unwrap();

    let file: File = CreateFile::named("hello.txt")
        .contents(b"hello, world!")
        .execute(&database)
        .unwrap();
    let contents = file.contents(&database).unwrap();
    println!("Created file: {file:?}, length: {}", contents.len());
    let bytes = contents.into_vec().unwrap();
    assert_eq!(bytes, b"hello, world!");

    let file = File::<BonsaiFiles>::load("/hello.txt", &database)
        .unwrap()
        .unwrap();
    assert_eq!(file.name(), "hello.txt");
    assert_eq!(file.containing_path(), "/");
}

#[test]
fn invalid_name_test() {
    let directory = TestDirectory::new("invalid-name");
    let database = Database::open::<FilesSchema>(StorageConfiguration::new(&directory)).unwrap();

    let err = CreateFile::named("/hello.txt")
        .contents(b"hello, world!")
        .execute::<BonsaiFiles, _>(&database)
        .unwrap_err();
    assert!(matches!(err, Error::InvalidName));
}

#[test]
fn simple_path_test() {
    let directory = TestDirectory::new("simple-path");
    let database = Database::open::<FilesSchema>(StorageConfiguration::new(&directory)).unwrap();

    let file: File = CreateFile::named("hello.txt")
        .at_path("/some/containing/path")
        .creating_missing_directories()
        .contents(b"hello, world!")
        .execute(&database)
        .unwrap();
    let contents = file.contents(&database).unwrap();
    println!("Created file: {file:?}, length: {}", contents.len());
    let bytes = contents.into_vec().unwrap();
    assert_eq!(bytes, b"hello, world!");

    let file = File::<BonsaiFiles>::load("/some/containing/path/hello.txt", &database)
        .unwrap()
        .unwrap();
    assert_eq!(file.name(), "hello.txt");
    assert_eq!(file.containing_path(), "/some/containing/path/");

    // Internally `ExactPathKey` will append a `/` to the end of the key, and
    // the code in File::list() uses `File::path()` to construct the path to
    // list. This constructed path has no trailing `/`, so each of the
    // `children()` calls below will test the path in `ExactKeyPath` where the
    // `/` is appended. This query has the trailing slash to test the path where
    // no `/` is added.
    let some_contents = File::<BonsaiFiles>::list("/some/", &database).unwrap();
    assert_eq!(some_contents.len(), 1);
    assert_eq!(some_contents[0].name(), "containing");
    let containing_contents = some_contents[0].children(&database).unwrap();
    assert_eq!(containing_contents.len(), 1);
    assert_eq!(containing_contents[0].name(), "path");
    let path_contents = containing_contents[0].children(&database).unwrap();
    assert_eq!(path_contents.len(), 1);
    assert_eq!(path_contents[0].name(), "hello.txt");

    // Test parent()
    let some = some_contents[0].parent(&database).unwrap().unwrap();
    assert_eq!(some.name(), "some");
    assert!(matches!(some.parent(&database).unwrap(), None));

    let all_contents = File::<BonsaiFiles>::list_recursive("/", &database).unwrap();
    assert_eq!(all_contents.len(), 4);
}

#[test]
fn blocked_file_test() {
    enum SmallBlocks {}
    impl FileConfig for SmallBlocks {
        const BLOCK_SIZE: usize = 8;

        fn files_name() -> bonsaidb_core::schema::CollectionName {
            BonsaiFiles::files_name()
        }

        fn blocks_name() -> bonsaidb_core::schema::CollectionName {
            BonsaiFiles::blocks_name()
        }
    }
    let mut big_file = Vec::with_capacity(SmallBlocks::BLOCK_SIZE * 31 / 2);
    let mut counter = 0_u8;
    while big_file.len() + 4 < big_file.capacity() {
        counter += 1;
        for _ in 0..4 {
            big_file.push(counter);
        }
    }
    let directory = TestDirectory::new("blocked-file");
    let database =
        Database::open::<FilesSchema<SmallBlocks>>(StorageConfiguration::new(&directory)).unwrap();

    let mut file: File<SmallBlocks> = CreateFile::named("hello.txt")
        .contents(&big_file)
        .execute(&database)
        .unwrap();
    let contents = file.contents(&database).unwrap();
    println!("Created file: {file:?}, length: {}", contents.len());
    let bytes = contents.into_vec().unwrap();
    assert_eq!(bytes, big_file);

    // Truncate the beginning of the file
    let new_length = u64::try_from(SmallBlocks::BLOCK_SIZE * 3).unwrap();
    big_file.splice(..big_file.len() - SmallBlocks::BLOCK_SIZE * 3, []);
    file.truncate(new_length, TruncateFrom::Start, &database)
        .unwrap();

    let contents = file.contents(&database).unwrap();
    assert_eq!(contents.len(), new_length);
    assert_eq!(contents.into_vec().unwrap(), big_file);

    // Truncate the end of the file.
    let new_length = u64::try_from(SmallBlocks::BLOCK_SIZE).unwrap();
    big_file.truncate(SmallBlocks::BLOCK_SIZE);
    file.truncate(new_length, TruncateFrom::End, &database)
        .unwrap();

    let contents = file.contents(&database).unwrap();
    assert_eq!(contents.len(), new_length);
    assert_eq!(contents.to_vec().unwrap(), big_file);

    // Clear the file.
    file.truncate(0, TruncateFrom::End, &database).unwrap();

    let mut writer = file.append_buffered(&database);
    let buffer_size = SmallBlocks::BLOCK_SIZE * 3 / 2;
    writer.set_buffer_size(buffer_size).unwrap();
    // Write more than the single buffer size.
    let data_written = &bytes[0..SmallBlocks::BLOCK_SIZE * 2];
    writer.write_all(data_written).unwrap();
    assert_eq!(writer.buffer.len(), SmallBlocks::BLOCK_SIZE / 2);
    drop(writer);

    let contents = file.contents(&database).unwrap();
    assert_eq!(contents.to_vec().unwrap(), data_written);
}

#[test]
fn seek_read_test() {
    let mut afile = Vec::with_capacity(BonsaiFiles::BLOCK_SIZE * 3);
    let word_size = size_of::<usize>();
    while afile.len() + word_size < afile.capacity() {
        afile.extend(afile.len().to_be_bytes());
    }
    let directory = TestDirectory::new("seek-read");
    let database = Database::open::<FilesSchema>(StorageConfiguration::new(&directory)).unwrap();

    let file: File = CreateFile::named("hello.bin")
        .contents(&afile)
        .execute(&database)
        .unwrap();
    let mut contents = file.contents(&database).unwrap().batching_by_blocks(1);
    // Read the last 16 bytes
    contents.seek(std::io::SeekFrom::End(-16)).unwrap();
    let mut buffer = [0; 16];
    contents.read_exact(&mut buffer).unwrap();
    assert_eq!(&afile[afile.len() - 16..], &buffer);

    // Seek slightly ahead of the start, and read 16 bytes.
    contents.seek(std::io::SeekFrom::Start(16)).unwrap();
    contents.read_exact(&mut buffer).unwrap();
    assert_eq!(&afile[16..32], &buffer);

    // Move foward into the next block.
    contents
        .seek(std::io::SeekFrom::Current(
            i64::try_from(BonsaiFiles::BLOCK_SIZE).unwrap(),
        ))
        .unwrap();
    contents.read_exact(&mut buffer).unwrap();
    assert_eq!(
        &afile[BonsaiFiles::BLOCK_SIZE + 32..BonsaiFiles::BLOCK_SIZE + 48],
        &buffer
    );
    // Re-read the last 16 bytes
    contents.seek(std::io::SeekFrom::Current(-16)).unwrap();
    assert_eq!(
        &afile[BonsaiFiles::BLOCK_SIZE + 32..BonsaiFiles::BLOCK_SIZE + 48],
        &buffer
    );
    // Try seeking to the end of the universe. It should return the file's length.
    assert_eq!(
        contents.seek(std::io::SeekFrom::End(i64::MAX)).unwrap(),
        contents.len()
    );
    // Verify read returns 0 bytes.
    assert_eq!(contents.read(&mut buffer).unwrap(), 0);
}
