use bonsaidb::{
    core::schema::Qualified,
    files::{BonsaiFiles, FileConfig},
};

#[test]
fn defaults() {
    #[derive(FileConfig)]
    struct Test;

    assert_eq!(
        <Test as FileConfig>::Metadata::default(),
        <BonsaiFiles as FileConfig>::Metadata::default()
    );
    assert_eq!(Test::BLOCK_SIZE, BonsaiFiles::BLOCK_SIZE);
    assert_eq!(Test::files_name(), BonsaiFiles::files_name());
    assert_eq!(Test::blocks_name(), BonsaiFiles::blocks_name());
}

#[test]
fn names_only() {
    #[derive(FileConfig)]
    #[file_config(blocks_name = "blocks_test", files_name = "files_test")]
    struct Test;

    assert_eq!(Test::files_name(), Qualified::private("files_test"));
    assert_eq!(Test::blocks_name(), Qualified::private("blocks_test"));
}

#[test]
fn names_and_authority() {
    #[derive(FileConfig)]
    #[file_config(
        authority = "test",
        blocks_name = "blocks_test",
        files_name = "files_test"
    )]
    struct Test;

    assert_eq!(Test::files_name(), Qualified::new("test", "files_test"));
    assert_eq!(Test::blocks_name(), Qualified::new("test", "blocks_test"));
}

#[test]
fn block_size() {
    #[derive(FileConfig)]
    #[file_config(block_size = 10)]
    struct Test;

    assert_eq!(Test::BLOCK_SIZE, 10);
}

#[test]
fn response() {
    #[derive(FileConfig)]
    #[file_config(metadata = String)]
    struct Test;

    assert_eq!(<Test as FileConfig>::Metadata::default(), String::default());
}
