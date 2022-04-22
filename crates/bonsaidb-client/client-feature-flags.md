## Feature Flags

By default, the `full` feature is enabled.

- `full`: Enables `trusted-dns` and `websockets`
- `trusted-dns`: Enables using trust-dns for DNS resolution. If not
  enabled, all DNS resolution is done with the OS's default name resolver.
- `websockets`: Enables `WebSocket` support for `bonsaidb-client`.
- `password-hashing`: Enables the ability to use password authentication
  using Argon2.
- `token-authentication`: Enables the ability to authenticate using
  authentication tokens, which are similar to API keys.
- `tracing`: Enables `tracing` annotations on some functions and dependencies.
