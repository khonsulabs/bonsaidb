# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

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
- `Backend` has a new method, `dispatcher_for` which replaces
  `Server::set_custom_api_dispatcher`. This change allows custom api dispatchers
  to receive the `server` or `client` during initialization if needed. For
  example, if a custom API needs to know the caller's identity, you can store
  the `client` in your dispatcher and access it in your handlers.
- `Backend` has a new associated type: `ClientData`. This associated type can be
  used to associate data on a per-`ConnectedClient` basis.
- `bonsaidb::server::Configuration` has a new setting:
  `client_simultaneous_request_limit`. It controls the amount of query
  pipelining a single connection can achieve. Submitting more queries on a
  single connection will block reading additional requests from the network
  connection until responses have been sent.
- `bonsaidb::server::Configuration` now supports `authenticated_permissions`,
  allowing a set of permissions to be defined that are applied to any user that
  has successfully authenticated.
- `Collection::serializer` is a new function that allows a collection to define
  what serialization format it should use.
- `CollectionView` is a new trait that can be implemented instead of `View`. The
  `map()` function takes a `CollectionDocument` parameter that is already
  deserialized for you.
- `bonsaidb_core` now has two new macros to ease some tedium of writing simple
  views: `define_basic_mapped_view` and `define_basic_unique_mapped_view`.

### Changed

- Listing all schemas and databases will now include the built-in admin database.
- The underlying dependency on `sled` has been changed for an in-house storage
  implementation [`nebari`](https://github.com/khonsulabs/nebari).
- The command-line interface has received an overhaul.
- `View::map` now returns a `Vec` instead of an `Option`, allowing for emitting
  of multiple keys. This may change again to provide a non-heap allocation
  mechanism for a single emit. Please provide feedback if this shows up in a
  benchmark in a meaningful amount.
- `bonsaidb::server::Configuration::default_permissions` has been changed into a
  `DefaultPermissions` enum.
- Changed the default serialization format from `CBOR` to an in-house format,
  [`Pot`](https://github.com/khonsulabs/pot).

### Removed

- `bonsaidb::local::backup` has been removed. The main limitation arose from how
  it was written to not need to be aware of the `Schema` type, but to properly
  support backing up encrypted databases, it must. Never fear, we still have
  backup strategies in mind -- the tool may be rewritten in the future, but in
  the nearer term, we are prioritizing
  [replication](https://github.com/khonsulabs/bonsaidb/issues/90).
  

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
  with `Request` and `Response` types that just need to support `serde`. For a
  full example, [check out this in-development `Gooey`
  example](https://github.com/khonsulabs/gooey/tree/6d4c682552bad5aa558c86a8333ee123372a7537/integrated-examples/bonsaidb/counter).

- An initial version of a WebAssembly client is now supported. It only supports
  WebSockets. While there has been news of `QUIC` support in the browser, it's a
  limited implementation that only exposes an HTTP protocol. As such, it is
  incompatible with the `BonsaiDb` protocol. Eventually, we hope to support
  `WebRTC` as an alternative to TCP in WebAssembly. The example linked in the
  previous bullet point can be built and loaded in a browser.
