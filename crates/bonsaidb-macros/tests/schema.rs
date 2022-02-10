use core::fmt::Debug;

use bonsaidb::core::schema::{Collection, CollectionName, Schema, Schematic};

#[test]
fn core() {
    #[derive(Schema, Debug)]
    #[schema(name = "name", core = ::bonsaidb::core)]
    struct Test<T: Sync + Send + Debug + 'static>(T);

    assert_eq!(
        Test::<String>::schema_name(),
        bonsaidb::core::schema::SchemaName::private("name")
    );
}

#[test]
fn name_only() {
    #[derive(Schema, Debug)]
    #[schema(name = "name")]
    struct Test<T: Sync + Send + Debug + 'static>(T);

    assert_eq!(
        Test::<String>::schema_name(),
        bonsaidb::core::schema::SchemaName::private("name")
    );
}
#[test]
fn name_and_authority() {
    #[derive(Schema, Debug)]
    #[schema(name = "name", authority = "authority")]
    struct Test<T: Sync + Send + Debug + 'static>(T);

    assert_eq!(
        Test::<String>::schema_name(),
        bonsaidb::core::schema::SchemaName::new("authority", "name")
    );
}
#[test]
fn collections() {
    #[derive(Schema, Debug)]
    #[schema(name = "name", authority = "authority", collections = [TestCollection])]
    struct TestSchema;

    let schematic = Schematic::from_schema::<TestSchema>().unwrap();
    assert!(schematic
        .collections()
        .contains(&CollectionName::private("name")));

    #[derive(Collection, Debug)]
    #[collection(name = "name")]
    struct TestCollection;
}
