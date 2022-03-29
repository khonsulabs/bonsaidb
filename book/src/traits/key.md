# Key Trait

The [Key][key] trait enables types to define a serialization and deserialization
format that preserves the order of the original type in serialized form. Whe
comparing two values encoded with [`as_ord_bytes()`][as-ord] using a
byte-by-byte comparison operation should match the result produced by comparing
the two original values using the [`Ord`][ord]. For integer formats, this
generally means encoding the bytes in network byte order (big endian).

For example, let's consider two values:

| Value    | `as_ord_bytes()` |
|----------|------------------|
| `1u16`   | `[ 0, 1]`        |
| `300u16` | `[ 1, 44]`       |

`1_u16.cmp(&300_u16)` and `1_u16.as_ord_bytes()?.cmp(&300_u16.as_ord_bytes()?)`
both produce Ordering::Less.

## Implementing the `Key` trait

The [`Key`][key] trait declares two functions: [`as_ord_bytes()`][as-ord] and
[`from_ord_bytes`][from-ord]. The intention is to convert the type to bytes
using a network byte order for numerical types, and for non-numerical types, the
bytes need to be stored in binary-sortable order.

Here is how BonsaiDb implements Key for `EnumKey`:

```rust,noplayground,no_run
{{#include ../../../crates/bonsaidb-core/src/key.rs:impl_key_for_enumkey}}
```

By implementing `Key` you can take full control of converting your view keys.

[key]: {{DOCS_BASE_URL}}/bonsaidb/core/key/trait.Key.html
[as-ord]: {{DOCS_BASE_URL}}/bonsaidb/core/key/trait.KeyEncoding.html#tymethod.as_ord_bytes
[from-ord]: {{DOCS_BASE_URL}}/bonsaidb/core/key/trait.Key.html#tymethod.from_ord_bytes
[ord]: https://doc.rust-lang.org/std/cmp/trait.Ord.html
[uuid]: https://docs.rs/uuid/latest/uuid/struct.Uuid.html
