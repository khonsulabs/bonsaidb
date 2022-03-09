# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Breaking changes

- All APIs have had their `limit` parameters changed from `usize` to `u32`.
  Since `usize` is platform-dependent, picking a fixed-width type is more
  appropriate.

### Added

- `Range::default()` now returns an unbounded range, and `Bound::default()`
  returns `Bound::Unbounded`.
- `Range` now has several builder-pattern style methods to help construct
  ranges. In general, users should simply use the built-in range operators
  (`..`, `start..`, `start..end`, `start..=end`), as they are able to represent
  nearly every range pattern. The built-in range operators do not support
  specifying an excluded start bound, while the new method `Range::after` allows
  setting an excluded start bound.

## v0.3.0

### Breaking Changes

- `bonsaidb::local::jobs` is now private. It used to be a separate, public crate
  in the PliantDb days. After thinking about the job scheduler more, this
  initial implementation is better suited for the internal task management than
  the higher-level jobs system. As such, it has been internalized.
- `bonsaidb::core::transaction::Changes::Documents` has been changed to store
  the `CollectionName`s separately from the `ChangedDocument`s. This makes the
  transaction log entries smaller, as collection names aren't copied for each
  document.

  The storage layer is fully backwards compatible and will automatically convert
  existing transactions to the new format.

### Fixed

- Listing executed transactions that were written in `v0.1` was broken in
  `v0.2`. Backwards compatibility is now automatically tested to help ensure
  this sort of issue won't happen in the future again.

### Added

- `SerializedCollection::list_with_prefix`,
  `connection::Collection::list_with_prefix`, and
  `connection::View::with_key_prefix` have been added as an easy way to filter
  results based on whether the key starts with the given prefix.

  This is supported by a new trait, `IntoPrefixRange`. This trait has been
  implemented for all byte-based key implementations as well as for `String`.
- `Operation::push_serialized` has been added, which calls `natural_id` before
  creating an `Operation::Insert` variant.
- `Tasks::parallelization` and `Builder::workers_parallelization` have been
  added as a way to control how many threads can be used by any given
  task/worker. This is automatically configured to be the number of cpu cores
  detected.
- `count()` is a new function on the list builders, available via:

  - `SerializedCollection::all(db).count().await`
  - `SerializedCollection::list(42.., db).count().await`
  - `db.collection::<Collection>().all().count().await`
  - `db.collection::<Collection>().list(42..).count().await`

  The performance of this call is not as good as it will eventually be, as it is
  currently doing more work than strictly necessary.
- [#215][215]: `StorageConnection::delete_user` has been added, enabling
  deletions of users without directly interacting with the `admin` database.

[215]: https://github.com/khonsulabs/bonsaidb/pull/215

### Changed

- The view map/reduce system has been optimized to take advantage of some
  parallelism. The view system is still not hightly optimized, but this change
  makes a significant improvement on performance.

## v0.2.0

### Breaking Changes

- `bonsaidb::core::Error::DocumentConflict` now contains a `Header` instead of
  just the document's ID. This allows an application to re-submit an update with
  the updated header without another request to the database.
- `StorageConfiguratation::vault_key_storage` now uses an `Arc` instead of a
  `Box`. This change allows `StorageConfiguration` and `ServerConfiguration` to
  implement `Clone`.
- `Document::create_new_revision` has been removed. It was meant to be an
  internal function.
- `Document` now requires `AsRef<Header>` and `AsMut<Header>` instead of
  `Deref<Header>`/`DerefMut`. The publicly visible change is that the shortcut
  of accessing `document.header.emit_*` through deref by using `document.emit_*`
  will no longer work. This impacts `CollectionDocument`, `OwnedDocument`, and
  `BorrowedDocument`.

  This removes a little magic, but in some code flows, it was impossible to use
  Deref anyways due to Deref borrowing the entire document, not just the header.
- `Collection::PrimaryKey` is a new associated type that allows customizing the
  type that uniquely identifies documents inside of a `Collection`. Users of the
  derive macro will be unaffected by this change. If you're upgrading existing
  collections and wish to maintain backwards compatibility, use `u64` as the
  type.

  A `natural_id()` function can now be implemented on `SerializedCollection` or
  `DefaultSerialization` which allows extracting a primary key value from a new
  document being pushed
  
  A new example, `primary-keys.rs`, as been added showing basic usage of
  changing the primary key type. This change resulted in a sequnce of breaking
  changes that will be listed independently.
- `Key` has been moved from `bonsaidb::core::schema::view` to
  `bonsaidb::core::key`.
- `Key::as/from_big_endian_bytes` have been renamed to `Key::as/from_ord_bytes`.
- `Key::first_value()` and `Key::next_value()` are new provided functions. By
  default, these functions return `NextValueError::Unsupported`.

  `Key::first_value()` allows a `Key` type to define the first value in its
  sequence. For example, `0_u64` is the result of `u64::first_value()`.

  `Key::next_value()` allows a `Key` type to find the next value in sequence
  from the current value. Implementors should never wrap, and should instead
  return `NextValueError::WouldWrap`.

  Sensible defaults have been implemented for all numeric types.
- `Connection` and its related types have had all previously hard-coded
  primary keys pf `u64` changed to generic parameters that can accept either a
  `DocumentId` or `Collection::PrimaryKey`. The affected methods are:

  - `Connection::insert`
  - `Connection::overwrite`
  - `Connection::get`
  - `Connection::get_multiple`
  - `Connection::list`
  - `connection::Collection::insert`
  - `connection::Collection::overwrite`
  - `connection::Collection::get`
  - `connection::Collection::get_multiple`
  - `connection::Collection::list`
  - `SerializedCollection::insert`
  - `SerializedCollection::insert_into`
  - `SerializedCollection::overwrite`
  - `SerializedCollection::overwrite_into`
  - `SerializedCollection::get`
  - `SerializedCollection::get_multiple`
  - `SerializedCollection::list`
  - `transaction::Operation::insert_serialized`
  - `transaction::Operation::overwrite_serialized`

- `Header::id` has changed from `u64` to `DocumentId`, and
  `CollectionHeader<PrimaryKey>` has been added which contains
  `Collection::PrimaryKey` deserialized.

  These previous usages of `Header` have been changed to `CollectionHeader`:

  - `Connection::insert` result type
  - `Connection::overwrite` result type
  - `connection::Collection::insert` result type
  - `connection::Collection::insert_bytes` result type
  - `connection::Collection::push` result type
  - `connection::Collection::push_bytes` result type
  - `CollectionDocument::header`
  
  The `Header::emit*` functions have been moved to a new trait, `Emit`. This
  trait is implemented by both `Header` and `CollectionHeader`. The functions moved are:

  - `emit()`
  - `emit_key()`
  - `emit_value()`
  - `emit_key_and_value()`

  These functions now return a `Result`, as encoding a primary key value can
  fail if it is larger than `DocumentId::MAX_LENGTH`.
- `HasHeader` is a new trait that allows accessing a `Header` generically from
  many types. This type is used in `Connection::delete` and
  `connection::Collection::delete`.
- Types and functions that used `u64` as a document ID have been replaced with
  `DocumentId`s. The number of locations are too many to list. If you need to
  convert from a u64 to a `DocumentId`, you can use `DocumentId::from_u64()`.
- `Document::contents` and `Document::set_contents` are now ore "painful" to
  access due to the generic parameter added to `Document`.
  `SerializedCollection::document_contents(doc)` and
  `SerializedCollection::set_document_contents(doc, new_contents)` have been
  provided as easier ways to invoke the same functionality. For example:

  ```rust
  let contents = doc.contents::<MyCollection>()?;
  ```

  Becomes:

  ```rust
  let contents = MyCollection::document_contents(&doc)?;
  ```

- Backups made prior to `0.2.0` will not be able to be restored with this
  updated version. The document IDs are encoded differently than in prior
  versions.

### Added

- Optional compression is now available, using the LZ4 algorithm.
  `StorageConfiguration::default_compression` controls the setting. When using
  the `bonsaidb` crate, the feature can be made available using either
  `local-compression` or `server-compression`. When using `bonsaidb-server` or
  `bonsaidb-local` directly, the feature name is `compression`.

  This compression is currently applied on all chunks of data written to
  BonsaiDb that are larger than a hardcoded threshold. This includes the
  Key-Value store. The threshold may be configurable in the future.

  Some of the benchmark suite has been expanded to include comparisons between
  local storage with and without compression.
  
- Added ability to "overwrite" documents without checking the stored revision
information. Because this removes a layer of safety, it has its own permissible
action: `DocumentAction::Overwrite`. The functions that have been added are:

  - `connection::Connection::overwrite`
  - `connection::Collection::overwrite`
  - `schema::SerializedCollection::overwrite`
  - `schema::SerializedCollection::overwrite_into`
  - `document::CollectionDocument::overwrite`
  - `transaction::Transaction::overwrite`
  - `transaction::Operation::overwrite`

### Changed

- Internal dependencies between crates are now pinned based on their needs. This
  means that `bonsaidb-sever` will require a matching verison of
  `bonsaidb-local` when compiling. A simple example of a change that is a
  breaking compilation change but is not breaking from a compatibility
  standpoint is a change to a structure where `#[serde(rename)]` is used to
  remap an old value to a new value.

  The only crate currently not pinning its dependencies is
  `bonsaidb-keystorage-s3`. This crate, and hopefully many crates to come, are
  only tying themselves to the public API of BonsaiDb.

  This may generate slightly more crate updates than absolutely necessary, but
  for a small team it seems like the most manageable approach.

### Fixed

- The view system now tracks an internal version number in addition to the
  version specified in the view definiton. This allows internal structures to be
  upgraded transparently.
- Applying a transaction to a collection with a unique view now ensures a view
  mapping job has finished if the view's integrity check spawns one before
  allowing the transaction to begin.

## v0.1.2

### Fixed

- [#196][196]: Fixed a panic a user reported relating to an undocumented panic
      within `tokio::time::sleep()`.

[196]: https://github.com/khonsulabs/bonsaidb/pull/196

## v0.1.1

### Added

- `SchemaName:private` and `CollectionName::private` are two new constructors
  that allow defining names without specifying an authority. Developers
  creatingreusable collections and/or schemas should not use these methods as
  namespacing is meant to help prevent name collisions.
- `connection::Collection::all()` and `SchemaCollection::all()` have been
  implemented as simple wrappers around `list(..)`.
- [#146][146], [#187][187]: The `Schema`, `Collection`, and `View` traits can
      now be derived rather than manually implemented:

  ```rust
  #[derive(Debug, Schema)]
  #[schema(name = "my-schema", collections = [Shape])]
  struct MySchema;

  #[derive(Debug, Serialize, Deserialize, Collection)]
  #[collection(name = "shapes", views = [ShapesByNumberOfSides])]
  struct Shape {
      pub sides: u32,
  }

  #[derive(Debug, Clone, View)]
  #[view(collection = Shape, key = u32, value = usize, name = "by-number-of-sides")]
  struct ShapesByNumberOfSides;
  ```

- Memory-only instances of Storage can be created now. This is primarily intended for testing purposes.

### Changed

- Inline examples have been added for every `connection::Collection` and
  `connection::View` function.
- All examples have been updated to the new derive macro syntax. Additionally,
  documentation and examples for deriving Schema, Collection, and View have been
  added to the respective traits.

[146]: https://github.com/khonsulabs/bonsaidb/pull/146
[187]: https://github.com/khonsulabs/bonsaidb/pull/187

## v0.1.0

### Added

- `bonsaidb::local::admin` now exposes collections that are used to manage `BonsaiDb`.
- Ability to add users, set a user's password, and log in as a user.
- Each `bonsaidb::local::Storage` now has a unique ID. It will be randomly
  generated upon launch. If for some reason a random value isn't desired, it can
  be overridden in the `Configuration`.
- Centralized secrets vault: Enables limited at-rest encryption. See
  `bonsaidb::core::vault` for more information.
- For serializable types, `Collection` now defines easier methods for dealing
  with documents. `NamedCollection` allows collections that expose a unique name
  view to have easy ways to convert between IDs and names.
- Server `Backend` trait now defines connection lifecycle functions that can be
  overridden to customize behavior when clients connect, disconnect, or
  authenticate.
- `Client` now has a `build()` method to allow for customizing connections.
- `CustomApi` responses can now be sent by the server via
  `ConnectedClient::send()`. The client can now register a callback to receive
  out-of-band responses.
- `Backend` has a new associated type, `CustomApiDispatcher` which replaces
  `Server::set_custom_api_dispatcher`. This change allows custom api dispatchers
  to receive the `server` or `client` during initialization if needed. For
  example, if a custom API needs to know the caller's identity, you can store
  the `client` in your dispatcher and access it in your handlers.
- `Backend` has a new associated type: `ClientData`. This associated type can be
  used to associate data on a per-`ConnectedClient` basis.
- `ServerConfiguration` has a new setting:
  `client_simultaneous_request_limit`. It controls the amount of query
  pipelining a single connection can achieve. Submitting more queries on a
  single connection will block reading additional requests from the network
  connection until responses have been sent.
- `ServerConfiguration` now supports `authenticated_permissions`,
  allowing a set of permissions to be defined that are applied to any user that
  has successfully authenticated.
- `CollectionView` is a new trait that can be implemented instead of `View`. The
  `map()` function takes a `CollectionDocument` parameter that is already
  deserialized for you.
- `bonsaidb_core` now has two new macros to ease some tedium of writing simple
  views: `define_basic_mapped_view` and `define_basic_unique_mapped_view`.
- BonsaiDb now uses [`log`](https://crates.io/crates/log) internally to report
  errors that are being ignored or happened in a background task.
- Multiple crates now offer an "instrument" feature which enables
  instrumentation using the [`tracing`](https://tracing.rs/) ecosystem.
- Moved all `database()` functions to `StorageConnection`. This allows fully
  generic code to be written against a "server".
- Added `listen_for_shutdown()` which listens for SIGINT and SIGQUIT and attemps
  to shut the server down gracefully.
- Automatic TLS certificate acquisition can be performed using ACME on TCP port
  443. This is automatically performed if the feature flag is enabled.
- BonsaiDb server can now listen for generic TCP connections with and without
  TLS. This is primarily designed to allow extending the HTTP port to support
  additional HTTP endpoints than just websockets. However, because the TLS
  certificate aquired on port 443 can be used in other protocols such as SMTP,
  it can be useful to allow BonsaiDb to control the networking connections.
  `listen_for_tcp_on` and `listen_for_secure_tcp_on` accept a parameter that
  implements the `TcpService` trait. See the Axum example for how this
  integration can be used in conjunction with websockets.
- Added convenience methods to `Transaction` and `Operation` to make it easier
  to build multi-operation transactions.
- The `Key` trait is now automatically implemented for tuples of up to 8 entries
  in length.
- `CollectionDocument::modify()` enables updating a document using a flow that
  will automatically fetch the document if a conflict is detected, re-invoking
  the callback to redo the modifications. This is useful for situations where
  you may have cached a document locally and wish to update it in the future,
  but don't want to refresh it before saving. It also will help, in general,
  with saving documents that might have some contention.
- `CustomServer::authenticate_client_as()` allows setting a `ConnectedClient`'s
  authenticated user, skipping BonsaiDb's authentication. This allows developers
  to still utilize the users and permissions within BonsaiDb while
  authenticating via some other mechanism.
- `SerializedCollection::list()` and `SerializedCollection::get_multiple()` have
  been added to make it easier to retrieve `CollectionDocument<T>`s.

### Changed

- Configuration has been refactored to use a builder-style pattern.
  `bonsaidb::local::config::Configuration` has been renamed
  `StorageConfiguration`, and `bonsaidb::server::ServerConfiguration` has been
  renamed `ServerConfiguration`.
- `Database::open_local` and `Storage::open_local` have been renamed to `open`.
- `Database::open`, `Storage::open`, and `Server::open` no longer take a path
  argument. The path is provided from the configuration.
- Listing all schemas and databases will now include the built-in admin database.
- The underlying dependency on `sled` has been changed for an in-house storage
  implementation [`nebari`](https://github.com/khonsulabs/nebari).
- The command-line interface has received an overhaul.
  - A new trait, `CommandLine` can be implemented on a type that implements
    `Backend` to utilize the built-in, extensible command line interface. An
    example of this is located at
    [`./examples/basic-server/examples/cli.rs`](./examples/basic-server/examples/cli.rs).
  - The parameter types to `execute()` functions have changed.
  - This interface will receive further refinement as part of switching to clap
    3.0 once it is fully released.
- `View::map` now returns a `Mappings` instead of an `Option`, allowing for
  emitting of multiple keys.
- View mapping now stores the source document header, not just the ID.
- `ServerConfiguration::default_permissions` has been changed into a
  `DefaultPermissions` enum.
- Changed the default serialization format from `CBOR` to an in-house format,
  [`Pot`](https://github.com/khonsulabs/pot).
- `Key` now has a new associated constant: `LENGTH`. If a value is provided, the
  result of converting the value should always produce the length specified.
  This new information is used to automatically implement the `Key` trait for
  tuples.
- The `Key` implementation for `EnumKey` has been updated to use
  `ordered-varint` to minimize the size of the indexes. Previously, each entry
  in the view was always 8 bytes.
- `Connection` and `SerializedCollection` have had their `insert`/`insert_into`
  functions modified to include an id parameter. New functions named `push` and
  `push_into` have been added that do not accept an id parameter. This is in an
  effort to keep naming consistent with common collection names in Rust.
- `Operation::insert` and `Command::Insert` now take an optional u64 parameter
  which can be used to insert a document with a specific ID rather than having
  one chosen. If an document exists already, a conflict error will be returned.
- `bonsaidb::local::backup` has been replaced with `Storage::backup` and
  `Storage::restore`. This backup format is incompatible with the old format,
  but is built with proper support for restoring at-rest encrypted collections.
  Backups are *not* encrypted, but the old implementation could not be updated
  to support restoring the documents into an encrypted state.
  
  This new functionality exposes `BackupLocation`, an async_trait that enables
  arbitrary backup locations.
- `KeyValue` storage has internally changed its format. Because this was
  pre-alpha, this data loss was premitted. If this is an issue for anyone, the
  data is still there, the format of the key has been changed. By editing any
  database files directly using Nebari, you can change the format from
  "namespace.key" to "namespace\0key", where `\0` is a single null byte.
- `ExecutedTransactions::changes` is now a `Changes` enum, which can be a list
  of `ChangedDocument`s or `ChangedKey`s.
- The Key-Value store is now semi-transactional and more optimized. The behavior
  of persistence can be customized using the [`key_value_persistence`
  option](https://dev.bonsaidb.io/main/guide/administration/configuration.html#key-value-persistence)
  when opening a BonsaiDb instance. This can enable higher performace at the
  risk of data loss in the event of an unexpected hardware or power failure.
- A new trait, `SerializedCollection`, now controls serialization within
  `CollectionDocument`, `CollectionView`, and other helper methods that
  serialized document contents. This allows any serialization format that
  implements `transmog::Format` can be used within BonsaiDb by setting the
  `Format` associated type within your `SerializedCollection` implementation.

  For users who just want the default serialization, a convenience trait
  `DefaultSerialization` has been added. All types that implement this trait
  will automatically implement `SerializedCollection` using BonsaiDb's preferred
  settings.
- A new trait, `SerializedView`, now controls serialization of view values. This
  uses a similar approach to `SerializedCollection`. For users who just want the
  default serialization, a convenience trait `DefaultViewSerialization` has been
  added. All types that implement this trait will automatically implement
  `SerializedView` using BonsaiDb's preferred settings.

  The `view-histogram` example has been updated to define a custom
  `transmog::Format` implementation rather creating a Serde-based wrapper.
- `View` has been split into two traits to allow separating client and server
  logic entirely if desired. The `ViewSchema` trait is where `map()`,
  `reduce()`, `version()`, and `unique()` have moved. If you're using a
  `CollectionView`, the implementation should now be a combination of `View` and
  `CollectionViewSchema`.
- `CollectionName`, `SchemaName`, and `Name` all no longer generate errors if
  using invalid characters. When BonsaiDb needs to use these names in a context
  that must be able to be parsed, the names are encoded automatically into a
  safe format. This change also means that `View::view_name()`,
  `Collection::collection_name()`, and `Schema::schema_name()` have been updated
  to not return error types.
- The `Document` type has been renamed to `OwnedDocument`. A trait named
  `Document` has been introduced that contains most of the functionality of
  `Document`, but is now implemented by `OwnedDocument` in addition to a new
  type: `BorrowedDocument`. Most APIs have been updated to return
  `OwnedDocument`s. The View mapping process receives a `BorrowedDocument`
  within the `map()` function.

  This refactor changes the signatures of `ViewSchema::map()`,
  `ViewSchema::reduce()`, `CollectionViewSchema::map()`, and
  `CollectionViewSchema::reduce()`.

  The benefit of this breaking change is that the view mapping process now can
  happen with fewer copies of data.
- A new function, `query_with_collection_docs()` is provided that is
  functionally identical to `query_with_docs()` except the return type contains
  already deserialized `CollectionDocument<T>`s.

- Moved `CollectionDocument` from `bonsaidb_core::schema` to
  `bonsaidb_core::document`.

- Due to issues with unmaintained crates, X25519 has been swapped for P256 in
  the vault implementation. This is an intra-alpha breaking change. Use the
  backup functionality with the existing version of BonsaiDb to export a
  decrypted version of your data that you can restore into the new version of
  BonsaiDb.

  If you have encryption enabled but aren't actually storing any encrypted data yet, you can remove these files from inside your database:

  - `mydb.bonsaidb/master-keys`
  - `mydb.bonsaidb/vault-keys/` (or remove the keys from your S3 bucket)
- `query_with_docs()` and `query_with_collection_docs()` now return a
  `MappedDocuments` structure, which contains a list of mappings and a
  `BTreeMap` containing the associated documents. Documents are allowed to emit
  more than one mapping, and due to that design, a single document can be
  returned multiple times.

### Fixed

- Adding two collections with the same name now throw an error.

## v0.1.0-dev.4

### Added

- [`View::unique()`](https://dev.bonsaidb.io/main/bonsaidb/core/schema/trait.View.html#method.unique)
  has been added, allowing for a `View` to restrict saving documents when
  multiple documents would end up with the same key emitted for this view. For
  example, if you have a `User` collection and want to ensure each `User` has a
  unique `email_address`, you could create a `View` with the key of
  `email_address` and return true from `unique()`, and `BonsaiDb` will enforce
  that constraint.

- Permissions support has been added across the platform with granular access.
  The
  [`bonsaidb::core::permissions`](https://dev.bonsaidb.io/main/bonsaidb/core/permissions/)
  module contains the data types involved. More documentation and examples are
  to-come -- users and roles haven't been added yet.

- The initial underpinnings of customizing the `BonsaiDb` server have been
  added. First, there's the
  [`Backend`](https://dev.bonsaidb.io/main/bonsaidb/server/trait.Backend.html)
  trait. Right now, its only purpose is to allow defining a
  [`CustomApi`](https://dev.bonsaidb.io/main/bonsaidb/core/custom_api/trait.CustomApi.html).
  This allows applications built with `BonsaiDb` to extend the network protocol
  with `Request` and `Response` types that just need to support Serde. For a
  full example, [check out this in-development `Gooey`
  example](https://github.com/khonsulabs/gooey/tree/6d4c682552bad5aa558c86a8333ee123372a7537/integrated-examples/bonsaidb/counter).

- An initial version of a WebAssembly client is now supported. It only supports
  WebSockets. While there has been news of `QUIC` support in the browser, it's a
  limited implementation that only exposes an HTTP protocol. As such, it is
  incompatible with the `BonsaiDb` protocol. Eventually, we hope to support
  `WebRTC` as an alternative to TCP in WebAssembly. The example linked in the
  previous bullet point can be built and loaded in a browser.
