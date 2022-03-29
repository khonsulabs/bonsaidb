## Using an Enum as a Key

The easiest way to expose an enum is to derive [`num_traits::FromPrimitive`](https://docs.rs/num-traits/0.2.14/num_traits/cast/trait.FromPrimitive.html) and [`num_traits::ToPrimitive`](https://docs.rs/num-traits/0.2.14/num_traits/cast/trait.ToPrimitive.html) using [num-derive](https://crates.io/crates/num-derive), and add an `impl EnumKey` line:

```rust,noplayground,no_run
{{#include ../../../book-examples/tests/view-example-enum.rs:enum}}
```

The View code remains unchanged, although the associated Key type can now be set to `Option<Category>`. The queries can now use the enum instead of a `String`:

```rust,noplayground,no_run
{{#include ../../../book-examples/tests/view-example-enum.rs:reduce_one_key}}
```

BonsaiDb will convert the enum to a u64 and use that value as the Key. A u64 was chosen to ensure fairly wide compatibility even with some extreme usages of bitmasks. If you wish to customize this behavior, you can implement `Key` directly.
