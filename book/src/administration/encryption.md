# At-Rest Encryption

BonsaiDb offers at-rest encryption. An overview of how it works is available [in the `bonsaidb::local::vault` module]({{DOCS_BASE_URL}}/bonsaidb/local/vault/index.html).

## Enabling at-rest encryption by default

When opening your BonsaiDb instance, there is a configuration option [`default_encryption_key`]({{DOCS_BASE_URL}}/bonsaidb/local/config/struct.StorageConfiguration.html#structfield.default_encryption_key). Once this is set, all new data written that supports being encrypted will be encrypted at-rest.

```rust,noplayground,no_run
let storage = Storage::open(
    StorageConfiguration::new(&directory)
        .vault_key_storage(vault_key_storage)
        .default_encryption_key(KeyId::Master)
)?;
```

## Enabling at-rest encryption on a per-collection basis

[`Collection::encryption_key()`]({{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.Collection.html#method.encryption_key) can be overridden on a per-Collection basis. If a collection requests encryption but the feature is disabled, an error will be generated.

To enable a collection to be encrypted when the feature is enabled, only return a key when [ENCRYPTION_ENABLED]({{DOCS_BASE_URL}}/bonsaidb/core/constant.ENCRYPTION_ENABLED.html) is true.
