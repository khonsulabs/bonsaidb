## Feature Flags

By default, the `full` feature is enabled. These features are prefixed by
`client-` when being enabled from the omnibus `bonsaidb` crate.

- `full`: Enables `trusted-dns` and `websockets`
- `trusted-dns`: Enables using trust-dns for DNS resolution. If not
  enabled, all DNS resolution is done with the OS's default name resolver.
- `websockets`: Enables `WebSocket` support for `bonsaidb-client`.
- `password-hashing`: Enables the ability to use password authentication
  using Argon2.
- `tracing`: Enables `tracing` annotations on some functions and dependencies.
