# Integrating BonsaiDb Locally

BonsaiDb supports multiple [databases](../about/concepts/database.md) and multiple [schemas](../about/concepts/schema.md). However, for many applications, you only need a single database.

If you're only wanting a single database, the setup is straightforward: (from [`examples/basic-local/examples/basic-local.rs`]({{REPO_BASE_URL}}/examples/basic-local/examples/basic-local.rs))

```rust,noplayground,no_run
let db = Database::open::<Message>(
    StorageConfiguration::new("basic.bonsaidb")
)?;
```

Under the hood, BonsaiDb is creating a multi-database [`Storage`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.Storage.html) with a local [`Database`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.Database.html) named `default` for you. If you need to switch to a multi-database model, you can open the storage and access the `default` database: (adapted from [`examples/basic-local/examples/basic-local.rs`]({{REPO_BASE_URL}}/examples/basic-local/examples/basic-local-multidb.rs))

```rust,noplayground,no_run
let storage = Storage::open(
        StorageConfiguration::new("basic.bonsaidb")
            .with_schema::<Message>()?
)?;
let db = storage.create_database::<Message>(
    "messages",
    true
)?;
```

You can register multiple schemas so that databases can be purpose-built.

## Common Traits

To help your code transition between different modes of accessing BonsaiDb, you can use these common traits to make your methods accept any style of BonsaiDb access.

* [`Database`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.Database.html) implements [`Connection`](../traits/connection.md), [`KeyValue`](../traits/key-value.md), and [`PubSub`](../traits/pubsub.md).
* [`AsyncDatabase`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.AsyncDatabase.html) implements [`AsyncConnection`](../traits/connection.md), [`AsyncKeyValue`](../traits/key-value.md), and [`AsyncPubSub`](../traits/pubsub.md).
* [`Storage`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.Storage.html)/[`AsyncStorage`]({{DOCS_BASE_URL}}/bonsaidb/local/struct.AsyncStorage.html) implement [`StorageConnection`/`AsyncStorageConnection`](../traits/storage_connection.md), respectively.

For example, [`examples/basic-local/examples/basic-local.rs`]({{REPO_BASE_URL}}/examples/basic-local/examples/basic-local-multidb.rs) uses this helper method to insert a record:

```rust,noplayground,no_run
{{#include ../../../examples/basic-local/examples/basic-local-multidb.rs:reusable-code}}
```
