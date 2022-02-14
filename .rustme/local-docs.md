BonsaiDb's offline database implementation.

This crate exposes BonsaiDb's local database implementation. The
[`Storage`]($storage-type$) type provides its most common functionality by
implementing the [`StorageConnection`]($storage-connection-trait$).

## Minimum Supported Rust Version (MSRV)

While this project is alpha, we are actively adopting the current version of
Rust. The current minimum version is `1.58`, and we plan on updating the MSRV to
implement [namespaced
Features](https://github.com/khonsulabs/bonsaidb/issues/178) as soon as the
feature is released.
