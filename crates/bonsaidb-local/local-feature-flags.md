## Feature Flags

By default, the `full` feature is enabled. These features are prefixed by
`local-` when being enabled from the omnibus `bonsaidb` crate.

- `full`: Enables all the flags below
- `cli`: Enables the `clap` structures for embedding database management
  commands into your own command-line interface.
- `encryption`: Enables at-rest encryption.
- `instrument`: Enables instrumenting with `tracing`.
- `multiuser`: Enables multi-user support.
- `password-hashing`: Enables the ability to use password authentication using
  Argon2.
