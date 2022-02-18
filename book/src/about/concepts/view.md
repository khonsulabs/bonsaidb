# View

A [View][view-trait] is a [map/reduce](https://en.wikipedia.org/wiki/MapReduce)-powered method of quickly accessing information inside of a [Collection](./collection.md). A View can only belong to one Collection.

Views define two important associated types: a Key type and a Value type. You can think of these as the equivalent entries in a map/dictionary-like collection that supports more than one entry for each Key. The Key is used to filter the View's results, and the Value is used by your application or the `reduce()` function.

Views are a powerful, yet abstract concept. Let's look at a concrete example: blog posts with categories.

```rust,no_run,noplayground
{{#include ../../../book-examples/tests/view-example-string.rs:struct}}
```

Let's insert this data for these examples:

```rust,no_run,noplayground
{{#include ../../../book-examples/tests/view-example-string.rs:insert_data}}
```

> All examples on this page are available in their full form in the repository at [book/book-examples/tests](https://github.com/khonsulabs/bonsaidb/tree/main/book/book-examples/tests).

While `category` should be an enum, let's first explore using `String` and upgrade to an enum at the end (it requires one additional step). Let's implement a View that will allow users to find blog posts by their category as well as count the number of posts in each category.

```rust,noplayground,no_run
{{#include ../../../book-examples/tests/view-example-string.rs:view}}
```

The two traits being implemented are [View][view-trait] and
[ViewSchema][viewschema-trait]. These traits are designed to allow keeping the
`View` implementation in a shared code library that is used by both client-side
and server-side code, while keeping the `ViewSchema` implementation in the
server executable only.

## Views for [`SerializedCollection`][serialized-collection]

For users who are using [`SerializedCollection`][serialized-collection], [`CollectionViewSchema`][collection-view-schema] can be implemented instead of [`ViewSchema`][viewschema-trait]. The only difference between the two is that the [`map()`][collection-view-schema-map] function takes a [`CollectionDocument`][collection-document] instead of a [`BorrowedDocument`][borrowed-document].

## Value Serialization

For views to function, the Value type must able to be serialized and deserialized from storage. To accomplish this, all views must implement the [`SerializedView`]({{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.SerializedView.html) trait. For [Serde](https://serde.rs/)-compatible data structures, [`DefaultSerializedView`]({{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.DefaultViewSerialization.html) is an empty trait that can be implemented instead to provide the default serialization that BonsaiDb recommends.

## Map

The first line of the `map` function calls [`SerializedCollection::document_contents()`]({{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.SerializedCollection.html#method.document_contents) to deserialize the stored `BlogPost`. The second line returns an emitted Key and Value -- in our case a clone of the post's category and the value `1_u32`. With the map function, we're able to use [`query()`]({{DOCS_BASE_URL}}/bonsaidb/core/connection/struct.View.html#method.query) and [`query_with_docs()`]({{DOCS_BASE_URL}}/bonsaidb/core/connection/struct.View.html#method.query_with_docs):

```rust,noplayground,no_run
{{#include ../../../book-examples/tests/view-example-string.rs:query_with_docs}}
```

The above snippet queries the [Database](./database.md) for all documents in the `BlogPost` Collection that emitted a Key of `Some("Rust")`.

If you're using a [`SerializedCollection`][serialized-collection], you can use [`query_with_collection_docs()`]({{DOCS_BASE_URL}}/bonsaidb/core/connection/struct.View.html#method.query_with_collection_docs) to have the deserialization done automatically for you:

```rust,noplayground,no_run
{{#include ../../../book-examples/tests/view-example-string.rs:query_with_collection_docs}}
```

## Reduce

The second function to learn about is the `reduce()` function. It is responsible for turning an array of Key/Value pairs into a single Value. In some cases, BonsaiDb might need to call `reduce()` with values that have already been reduced one time. If this is the case, `rereduce` is set to true.

In this example, we're using the built-in [`Iterator::sum()`](https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.sum) function to turn our Value of `1_u32` into a single `u32` representing the total number of documents.

```rust,noplayground,no_run
{{#include ../../../book-examples/tests/view-example-string.rs:reduce_one_key}}
```

## Changing an exising view

If you have data stored in a view, but want to update the view to store data
differently, implement [`ViewSchema::version()`][viewschema-version] and return
a unique number. When BonsaiDb checks the view's integrity, it will notice that
there is a version mis-match and automatically re-index the view.

There is no mechanism to access the data until this operation is complete.

### Understanding Re-reduce

Let's examine this data set:

| Document ID | BlogPost Category |
| ----------- | ----------------- |
| 1           | Some("Rust")      |
| 2           | Some("Rust")      |
| 3           | Some("Cooking")   |
| 4           | None              |

When updating views, each view entry is reduced and the value is cached. These
are the view entries:

| View Entry ID   | Reduced Value |
| --------------- | ------------- |
| Some("Rust")    | 2             |
| Some("Cooking") | 1             |
| None            | 1             |

When a reduce query is issued for a single key, the value can be returned without further processing. But, if the reduce query matches multiple keys, the View's `reduce()` function will be called with the already reduced values with `rereduce` set to `true`. For example, retrieving the total count of blog posts:

```rust,noplayground,no_run
{{#include ../../../book-examples/tests/view-example-string.rs:reduce_multiple_keys}}
```

Once BonsaiDb has gathered each of the key's reduced values, it needs to further reduce that list into a single value. To accomplish this, the View's `reduce()` function to be invoked with `rereduce` set to `true`, and with mappings containing:

| Key             | Value |
| --------------- | ----- |
| Some("Rust")    | 2     |
| Some("Cooking") | 1     |
| None            | 1     |

This produces a final value of 4.

## How does BonsaiDb make this efficient?

When saving Documents, BonsaiDb does not immediately update related views. It instead notes what documents have been updated since the last time the View was indexed.

When a View is accessed, the queries include an [`AccessPolicy`]({{DOCS_BASE_URL}}/bonsaidb/core/connection/enum.AccessPolicy.html). If you aren't overriding it, [`UpdateBefore`]({{DOCS_BASE_URL}}/bonsaidb/core/connection/enum.AccessPolicy.html#variant.UpdateBefore) is used. This means that when the query is evaluated, BonsaiDb will first check if the index is out of date due to any updated data. If it is, it will update the View before evaluating the query.

If you're wanting to get results quickly and are willing to accept data that might not be updated, the access policies [`UpdateAfter`]({{DOCS_BASE_URL}}/bonsaidb/core/connection/enum.AccessPolicy.html#variant.UpdateAfter) and [`NoUpdate`]({{DOCS_BASE_URL}}/bonsaidb/core/connection/enum.AccessPolicy.html#variant.NoUpdate) can be used depending on your needs.

If multiple simulataneous queries are being evaluted for the same View and the View is outdated, BonsaiDb ensures that only a single view indexer will execute while both queries wait for it to complete.

## Using arbitrary types as a View Key

In our previous example, we used `String` for the Key type. The reason is important: Keys must be sortable by [our underlying storage engine](http://sled.rs/), which means special care must be taken. Most serialization types do not guarantee binary sort order. Instead, BonsaiDb exposes the [`Key`][key] trait. On that documentation page, you can see that BonsaiDb implements `Key` for many built-in types.

### Using an enum as a View Key

The easiest way to expose an enum is to derive [`num_traits::FromPrimitive`](https://docs.rs/num-traits/0.2.14/num_traits/cast/trait.FromPrimitive.html) and [`num_traits::ToPrimitive`](https://docs.rs/num-traits/0.2.14/num_traits/cast/trait.ToPrimitive.html) using [num-derive](https://crates.io/crates/num-derive), and add an `impl EnumKey` line:

```rust,noplayground,no_run
{{#include ../../../book-examples/tests/view-example-enum.rs:enum}}
```

The View code remains unchanged, although the associated Key type can now be set to `Option<Category>`. The queries can now use the enum instead of a `String`:

```rust,noplayground,no_run
{{#include ../../../book-examples/tests/view-example-enum.rs:reduce_one_key}}
```

BonsaiDb will convert the enum to a u64 and use that value as the Key. A u64 was chosen to ensure fairly wide compatibility even with some extreme usages of bitmasks. If you wish to customize this behavior, you can implement `Key` directly.

### Implementing the `Key` trait

The [`Key`][key] trait declares two functions: [`as_ord_bytes()`]({{DOCS_BASE_URL}}/bonsaidb/core/key/trait.Key.html#tymethod.as_ord_bytes) and [`from_ord_bytes`]({{DOCS_BASE_URL}}/bonsaidb/core/key/trait.Key.html#tymethod.from_ord_bytes). The intention is to convert the type to bytes using a network byte order for numerical types, and for non-numerical types, the bytes need to be stored in binary-sortable order.

Here is how BonsaiDb implements Key for `EnumKey`:

```rust,noplayground,no_run
{{#include ../../../../crates/bonsaidb-core/src/key.rs:impl_key_for_enumkey}}
```

By implementing `Key` you can take full control of converting your view keys.

[key]: {{DOCS_BASE_URL}}/bonsaidb/core/key/trait.Key.html
[view-trait]: {{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.View.html
[viewschema-trait]: {{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.ViewSchema.html
[viewschema-version]: {{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.ViewSchema.html#method.version
[serialized-collection]: {{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.SerializedCollection.html
[borrowed-document]: {{DOCS_BASE_URL}}/bonsaidb/core/document/trait.Document.html
[collection-document]: {{DOCS_BASE_URL}}/bonsaidb/core/document/struct.CollectionDocument.html
[collection-view-schema]: {{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.CollectionViewSchema.html
[collection-view-schema-map]: {{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.CollectionViewSchema.html#tymethod.map
