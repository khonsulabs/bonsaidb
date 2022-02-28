# BonsaiDb Architecture

This document is currently aimed at being a quick introduction to help a
potentially new contributor understand how to navigate this project. Better
overall documentation of architecture will be written as the project matures.

## Crate Organization

### [`bonsaidb-core`](crates/bonsaidb-core/)

BonsaiDb is designed with an "onion" peel approach. The core of the onion is
`bonsaidb-core`, which exposes the common public API
surface. This crate contains these modules:

- `admin`: Defines the types used for the "administration" database that
  BonsaiDb provides. This includes the list of created databases, permissions,
  and users.
- `connection`: Defines the [`StorageConnection`][storage-connection] and
  [`Connection`][connection] traits, as well as the types that support those
  APIs.
- `custom_api`: Defines the types needed to [define a CustomApi
  server][custom-api]. This is currently only functional with Client/Server, but
  this should be generalized so that custom_api can work in `bonsaidb-local`
  without networking.
- `document`: Defines the types used when interacting with
  [documents][document].
- `key`: Defines the `Key` trait and related error types. The Key trait is used
  when defining a [custom primary key][custom-primary-keys] or a [View][view]
  key.
- `keyvalue`: Defines the [`KeyValue`][keyvalue] trait, which exposes the atomic
  Key-Value store.
- `networking`: Defines the types that allow implementing all of the BonsaiDb
  functionality using serializable types.
- `permissions`: Defines all types related to managing permissions.
  - `permissions::bonsai`: Defines all of the permissible actions and resource
    names used within BonsaiDb.
- `pubsub`: Defines the [`PubSub`][pubsub] trait, which enables subscribers to
  receive messages published to subscribed topics.
- `schema`: Defines all of the types for defining schemas for the ACID-compliant
  [Collection][collection] storage.
- `transaction`: Defines the types used to create multi-operation transactions.

### [`bonsaidb-local`](crates/bonsaidb-local/)

The `bonsaidb-local` crate implements a local-only database implementation using
[`Nebari`][nebari] as the underlying database. Nebari provides ACID-compliant,
multi-tree transactional storage, but it is still a low-level library.
BonsaiDb's goal is to provide an efficient, high-level database implementation
atop a lower-level database implementation.

#### Storage

The [`Storage`][storage] type provides a multi-schema, multi-database storage
implementation.

Here's a directory listing of the Storage's path after executing `cargo run
--example basic-local-multidb`:

```sh
basic.bonsaidb
├── _admin
│  ├── _transactions
│  ├── collection.bonsaidb.databases.nebari
│  ├── kv.nebari
│  ├── view-versions.bonsaidb.databases.nebari
│  ├── view.bonsaidb.databases.by-name.document-map.nebari
│  ├── view.bonsaidb.databases.by-name.invalidated.nebari
│  ├── view.bonsaidb.databases.by-name.nebari
├── master-keys
├── messages
│  ├── _transactions
│  ├── collection.private.messages.nebari
│  └── kv.nebari
├── private-messages
│  ├── _transactions
│  ├── collection.private.messages.nebari
│  └── kv.nebari
├── server-id
└── vault-keys
   └── f9de4e7254872399
```

The folders `_admin`, `messages`, and `private-messages` are all databases. The
`_admin` database is managed by BonsaiDb automatically and contains the schema
types defined in `bonsaidb_core::admin`. The "Database" section details the
files within these folders.

The `server-id` file is named based on the original name of the
`StorageConnection` trait. The file should be renamed. This file is a plain-text
number that represents the unique ID of the storage instance. This value can be
used to uniquely identify a Storage instance. It is currently only used to store
unique vault keys for each storage instance.

The `master-keys` file contains an encrypted version of all keys used to encrypt
data within BonsaiDb. The `vault-keys` folder is where the
[LocalVaultKeyStorage][local-vaultkey-storage] by default is configured to store
the keys that are used to encrypt the database. This is *not secure*, but the
examples need to be able to run without configuring external `VaultKeyStorage`.

More information on how encryption is implemented can be found in [bonsaidb_local::vault][vault]

#### Database

The [`Database`][database] type allows grouping data stored within a `Storage` instance by name and schema. A [Schema][schema] is what defines the available [collections][collection] within the database.

Each database contains multiple trees managed by [Nebari][nebari]. The primary trees are named `collection.AUTHORITY.NAME.nebari` and `kv.nebari`. The collection trees contain all of the collection data, and the `kv` tree contains all of the Key-Value store data.

The `_transactions` file is the Nebari [transaction log][nebari-log].

The trees that begin with `view` are used to power the [Map/Reduce View system][view]. The trees are:

- `view.COLLECTION_NAME.VIEW_NAME.nebari`: The key for this tree is the `Key` of
  the View, and the stored values are serialized `ViewEntry` values. Multiple
  values can be emitted for the same key, so a ViewEntry may contain multiple
  values from one or more documents.
- `view.COLLECTION_NAME.VIEW_NAME.invalidated.nebari`: When documents are
  updated, the document ID is added to this tree. This tree is treaded as a Set
  -- empty values are stored for each key. The view mapper determines what
  documents need to be reindexed by looking at this tree.
- `view.COLLECTION_NAME.VIEW_NAME.document-map.nebari`: When a document is
  updated, it is possible for an existing view entry to no longer be valid. The
  document map stores a list of all keys that each document emits, which allows
  the view mapper to remove entries that were previously emitted but are no
  longer returned from the view's `map()` function.

### [`bonsaidb-server`](crates/bonsaidb-server/)

The `bonsaidb-server` crate builds atop `bonsaidb-local` by exposing a networked
server implementation.

Currently the `bonsaidb-server` crate also enforces permissions, but [there are
plans](https://github.com/khonsulabs/bonsaidb/issues/68) to move this
functionality to `bonsaidb-local`.

### [`bonsaidb-client`](crates/bonsaidb-client/)

The `bonsaidb-client` crate exposes a networked client implementation. This
crate is able to be compiled to WebAssembly, but is only able to connect using
WebSockets in WASM.

[storage-connection]:
    https://dev.bonsaidb.io/main/guide/traits/storage_connection.html
[connection]: https://dev.bonsaidb.io/main/guide/traits/connection.html
[custom-api]:
    https://dev.bonsaidb.io/main/guide/about/access-models/custom-api-server.html
[document]: https://dev.bonsaidb.io/main/guide/about/concepts/document.html
[custom-primary-keys]:
    https://dev.bonsaidb.io/main/guide/about/concepts/collection.html#primary-keys
[view]: https://dev.bonsaidb.io/main/guide/about/concepts/view.html
[keyvalue]: https://dev.bonsaidb.io/main/guide/traits/key-value.html
[pubsub]: https://dev.bonsaidb.io/main/guide/traits/pubsub.html
[collection]: https://dev.bonsaidb.io/main/guide/about/concepts/collection.html
[nebari]: https://github.com/khonsulabs/nebari
[storage]: https://dev.bonsaidb.io/main/docs/bonsaidb_local/struct.Storage.html
[nebari-log]: https://docs.rs/nebari/latest/nebari/transaction/struct.TransactionLog.html
[local-vaultkey-storage]: https://dev.bonsaidb.io/main/docs/bonsaidb_local/vault/struct.LocalVaultKeyStorage.html
[vault]: https://dev.bonsaidb.io/main/docs/bonsaidb_local/vault/
[database]: https://dev.bonsaidb.io/main/docs/bonsaidb_local/struct.Database.html
[schema]: https://dev.bonsaidb.io/main/guide/about/concepts/schema.html
