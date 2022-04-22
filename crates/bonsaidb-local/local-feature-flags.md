## Feature Flags

By default, the `full` feature is enabled.

- `full`: Enables all the flags below
- `async`: Enables async-compatible types
- `cli`: Enables the `clap` structures for embedding database management
  commands into your own command-line interface.
- `encryption`: Enables at-rest encryption.
- `instrument`: Enables instrumenting with `tracing`.
- `multiuser`: Enables multi-user support.
- `password-hashing`: Enables the ability to use password authentication using
  Argon2.
- `token-authentication`: Enables the ability to authenticate using
  authentication tokens, which are similar to API keys.
