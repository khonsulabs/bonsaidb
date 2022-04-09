## Feature Flags

By default, the `full` feature is enabled.

- `full`: Enables all the flags below,
- `acme`: Enables automtic certificate acquisition through ACME/LetsEncrypt.
- `cli`: Enables the `cli` module.
- `encryption`: Enables at-rest encryption.
- `hyper`: Enables convenience functions for upgrading websockets using `hyper`.
- `instrument`: Enables instrumenting with `tracing`.
- `pem`: Enables the ability to install a certificate using the PEM format.
- `websockets`: Enables `WebSocket` support.
- `password-hashing`: Enables the ability to use password authentication
  using Argon2.
