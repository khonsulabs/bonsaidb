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
- Centralized secrets vault: Enables limited at-rest encryption. Access to keys
  can be controlled via permissions. See `bonsaidb::core::vault` for more
  information.
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

### Changed

- Listing all schemas and databases will now include the built-in admin database.

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
