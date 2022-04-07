use std::{
    io::{Read, Seek, Write},
    mem::size_of,
};

use bonsaidb_core::test_util::TestDirectory;
#[cfg(feature = "async")]
use bonsaidb_local::AsyncDatabase;
use bonsaidb_local::{
    config::{Builder, StorageConfiguration},
    Database,
};
#[cfg(feature = "async")]
use tokio::io::AsyncReadExt;

use crate::{
    direct::{CreateFile, File},
    BonsaiFiles, Error, FileConfig, FilesSchema, TruncateFrom,
};

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

    file.delete(&database).unwrap();
    assert!(File::<BonsaiFiles>::load("/hello.txt", &database)
        .unwrap()
        .is_none());
}

#[cfg(feature = "async")]
#[tokio::test]
async fn async_simple_file_test() {
    let directory = TestDirectory::new("simple-file-async");
    let database = AsyncDatabase::open::<FilesSchema>(StorageConfiguration::new(&directory))
        .await
        .unwrap();

    let file: File = CreateFile::named("hello.txt")
        .contents(b"hello, world!")
        .execute_async(&database)
        .await
        .unwrap();
    let contents = file.contents_async(&database).await.unwrap();
    println!("Created file: {file:?}, length: {}", contents.len());
    let bytes = contents.into_vec_async().await.unwrap();
    assert_eq!(bytes, b"hello, world!");

    let file = File::<BonsaiFiles>::load_async("/hello.txt", &database)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(file.name(), "hello.txt");
    assert_eq!(file.containing_path(), "/");

    file.delete_async(&database).await.unwrap();
    assert!(File::<BonsaiFiles>::load_async("/hello.txt", &database)
        .await
        .unwrap()
        .is_none());
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

#[cfg(feature = "async")]
#[tokio::test]
async fn async_invalid_name_test() {
    let directory = TestDirectory::new("invalid-name-async");
    let database = AsyncDatabase::open::<FilesSchema>(StorageConfiguration::new(&directory))
        .await
        .unwrap();

    let err = CreateFile::named("/hello.txt")
        .contents(b"hello, world!")
        .execute_async::<BonsaiFiles, _>(&database)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::InvalidName));
}

#[test]
fn simple_path_test() {
    let directory = TestDirectory::new("simple-path");
    let database = Database::open::<FilesSchema>(StorageConfiguration::new(&directory)).unwrap();

    let file: File = CreateFile::named("hello.txt")
        .at_path("/some/containing/path")
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

    // One query intentionally ends with a / and one doesn't.
    let some_contents = File::<BonsaiFiles>::list("/some/", &database).unwrap();
    assert_eq!(some_contents.len(), 0);
    let path_contents = File::<BonsaiFiles>::list("/some/containing/path", &database).unwrap();
    assert_eq!(path_contents.len(), 1);
    assert_eq!(path_contents[0].name(), "hello.txt");

    let all_contents = File::<BonsaiFiles>::list_recursive("/", &database).unwrap();
    assert_eq!(all_contents.len(), 1);

    // Test renaming and moving
    let mut file = file;
    file.rename(String::from("new-name.txt"), &database)
        .unwrap();
    assert!(
        File::<BonsaiFiles>::load("/some/containing/path/hello.txt", &database)
            .unwrap()
            .is_none()
    );
    let mut file = File::<BonsaiFiles>::load("/some/containing/path/new-name.txt", &database)
        .unwrap()
        .unwrap();
    file.move_to("/new/path/", &database).unwrap();
    assert!(
        File::<BonsaiFiles>::load("/new/path/new-name.txt", &database)
            .unwrap()
            .is_some()
    );
    file.move_to("/final/path/and_name.txt", &database).unwrap();
    let file = File::<BonsaiFiles>::load("/final/path/and_name.txt", &database)
        .unwrap()
        .unwrap();
    let contents = file.contents(&database).unwrap().into_vec().unwrap();
    assert_eq!(contents, b"hello, world!");
}

#[cfg(feature = "async")]
#[tokio::test]
async fn async_simple_path_test() {
    let directory = TestDirectory::new("simple-path-async");
    let database = AsyncDatabase::open::<FilesSchema>(StorageConfiguration::new(&directory))
        .await
        .unwrap();

    let file: File = CreateFile::named("hello.txt")
        .at_path("/some/containing/path")
        .contents(b"hello, world!")
        .execute_async(&database)
        .await
        .unwrap();
    let contents = file.contents_async(&database).await.unwrap();
    println!("Created file: {file:?}, length: {}", contents.len());
    let bytes = contents.into_vec_async().await.unwrap();
    assert_eq!(bytes, b"hello, world!");

    let file = File::<BonsaiFiles>::load_async("/some/containing/path/hello.txt", &database)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(file.name(), "hello.txt");
    assert_eq!(file.containing_path(), "/some/containing/path/");

    // One query intentionally ends with a / and one doesn't.
    let some_contents = File::<BonsaiFiles>::list_async("/some/", &database)
        .await
        .unwrap();
    assert_eq!(some_contents.len(), 0);
    let path_contents = File::<BonsaiFiles>::list_async("/some/containing/path", &database)
        .await
        .unwrap();
    assert_eq!(path_contents.len(), 1);
    assert_eq!(path_contents[0].name(), "hello.txt");

    let all_contents = File::<BonsaiFiles>::list_recursive_async("/", &database)
        .await
        .unwrap();
    assert_eq!(all_contents.len(), 1);

    // Test renaming and moving
    let mut file = file;
    file.rename_async(String::from("new-name.txt"), &database)
        .await
        .unwrap();
    assert!(
        File::<BonsaiFiles>::load_async("/some/containing/path/hello.txt", &database)
            .await
            .unwrap()
            .is_none()
    );
    let mut file = File::<BonsaiFiles>::load_async("/some/containing/path/new-name.txt", &database)
        .await
        .unwrap()
        .unwrap();
    file.move_to_async("/new/path/", &database).await.unwrap();
    assert!(
        File::<BonsaiFiles>::load_async("/new/path/new-name.txt", &database)
            .await
            .unwrap()
            .is_some()
    );
    file.move_to_async("/final/path/and_name.txt", &database)
        .await
        .unwrap();
    let file = File::<BonsaiFiles>::load_async("/final/path/and_name.txt", &database)
        .await
        .unwrap()
        .unwrap();
    let contents = file
        .contents_async(&database)
        .await
        .unwrap()
        .into_vec_async()
        .await
        .unwrap();
    assert_eq!(contents, b"hello, world!");
}

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

#[test]
fn blocked_file_test() {
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

#[cfg(feature = "async")]
#[tokio::test]
async fn async_blocked_file_test() {
    let mut big_file = Vec::with_capacity(SmallBlocks::BLOCK_SIZE * 31 / 2);
    let mut counter = 0_u8;
    while big_file.len() + 4 < big_file.capacity() {
        counter += 1;
        for _ in 0..4 {
            big_file.push(counter);
        }
    }
    let directory = TestDirectory::new("blocked-file-async");
    let database =
        AsyncDatabase::open::<FilesSchema<SmallBlocks>>(StorageConfiguration::new(&directory))
            .await
            .unwrap();

    let file: File<SmallBlocks> = CreateFile::named("hello.txt")
        .contents(&big_file)
        .execute_async(&database)
        .await
        .unwrap();
    let contents = file.contents_async(&database).await.unwrap();
    println!("Created file: {file:?}, length: {}", contents.len());
    let bytes = contents.into_vec_async().await.unwrap();
    assert_eq!(bytes, big_file);
    println!("a");
    // Truncate the beginning of the file
    let new_length = u64::try_from(SmallBlocks::BLOCK_SIZE * 3).unwrap();
    big_file.splice(..big_file.len() - SmallBlocks::BLOCK_SIZE * 3, []);
    file.truncate_async(new_length, TruncateFrom::Start, &database)
        .await
        .unwrap();

    println!("b");
    let contents = file.contents_async(&database).await.unwrap();
    assert_eq!(contents.len(), new_length);
    assert_eq!(contents.into_vec_async().await.unwrap(), big_file);

    // Truncate the end of the file.
    let new_length = u64::try_from(SmallBlocks::BLOCK_SIZE).unwrap();
    big_file.truncate(SmallBlocks::BLOCK_SIZE);
    file.truncate_async(new_length, TruncateFrom::End, &database)
        .await
        .unwrap();

    let contents = file.contents_async(&database).await.unwrap();
    assert_eq!(contents.len(), new_length);
    assert_eq!(contents.to_vec_async().await.unwrap(), big_file);

    // Clear the file.
    file.truncate_async(0, TruncateFrom::End, &database)
        .await
        .unwrap();

    // let mut writer = file.append_buffered(&database);
    // let buffer_size = SmallBlocks::BLOCK_SIZE * 3 / 2;
    // writer.set_buffer_size(buffer_size).unwrap();
    // // Write more than the single buffer size.
    // let data_written = &bytes[0..SmallBlocks::BLOCK_SIZE * 2];
    // writer.write_all(data_written).unwrap();
    // assert_eq!(writer.buffer.len(), SmallBlocks::BLOCK_SIZE / 2);
    // drop(writer);

    // let contents = file.contents(&database).unwrap();
    // assert_eq!(contents.to_vec().unwrap(), data_written);
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

#[cfg(feature = "async")]
#[tokio::test]
async fn async_seek_read_test() {
    let mut afile = Vec::with_capacity(BonsaiFiles::BLOCK_SIZE * 3);
    let word_size = size_of::<usize>();
    while afile.len() + word_size < afile.capacity() {
        afile.extend(afile.len().to_be_bytes());
    }
    let directory = TestDirectory::new("seek-read-async");
    let database = AsyncDatabase::open::<FilesSchema>(StorageConfiguration::new(&directory))
        .await
        .unwrap();

    let file: File = CreateFile::named("hello.bin")
        .contents(&afile)
        .execute_async(&database)
        .await
        .unwrap();
    let mut contents = file
        .contents_async(&database)
        .await
        .unwrap()
        .batching_by_blocks(1);
    // Read the last 16 bytes
    contents.seek(std::io::SeekFrom::End(-16)).unwrap();
    let mut buffer = [0; 16];
    contents.read_exact(&mut buffer).await.unwrap();
    assert_eq!(&afile[afile.len() - 16..], &buffer);

    // Seek slightly ahead of the start, and read 16 bytes.
    contents.seek(std::io::SeekFrom::Start(16)).unwrap();
    contents.read_exact(&mut buffer).await.unwrap();
    assert_eq!(&afile[16..32], &buffer);

    // Move foward into the next block.
    contents
        .seek(std::io::SeekFrom::Current(
            i64::try_from(BonsaiFiles::BLOCK_SIZE).unwrap(),
        ))
        .unwrap();
    contents.read_exact(&mut buffer).await.unwrap();
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
    assert_eq!(contents.read(&mut buffer).await.unwrap(), 0);
}
