# At-Rest Encryption

`BonsaiDb` offers at-rest encryption. An overview of how it works is available [in the `bonsaidb::local::vault` module](https://dev.bonsaidb.io/main/bonsaidb/local/vault/index.html).

## Enabling at-rest encryption by default

When opening your `BonsaiDb` instance, there is a configuration option [`default_encryption_key`](https://dev.bonsaidb.io/main/bonsaidb/local/config/struct.Configuration.html#structfield.default_encryption_key). Once this is set, all new data written that supports being encrypted will be encrypted at-rest.

```rust,noplayground,no_run
let storage = Storage::open_local(
    Path::new("encrypted-at-rest.bonsaidb"),
    Configuration {
        default_encryption_key: Some(KeyId::Master),
        ..Default::default()
    },
)
.await?;
```
