# View

A [View](https://pliantdb.dev/main/pliantdb/core/schema/trait.View.html) is a [map/reduce](https://en.wikipedia.org/wiki/MapReduce)-powered method of quickly accessing information inside of a [Collection](./collection.md). A View can only belong to one Collection.

Views define two important associated types: a Key type and a Value type. You can think of these as the equivalent entries in a map/dictionary-like collection that supports more than one entry for each Key. The Key is used to filter the View's results, and the Value is used by your application or the `reduce()` function.

Views are a powerful, yet abstract concept. Let's look at a concrete example: blog posts with categories.

```rust,no_run,noplayground
{{#include ../../view-example-string.rs:struct}}
```

While `category` should be an enum, let's first explore using `String` and upgrade to an enum at the end (it requires one additional step). Let's implement a View that will allow users to find blog posts by their category as well as count the number of posts in each category.

```rust,noplayground,no_run
{{#include ../../view-example-string.rs:view}}
```

## Map

The first line of the `map` function calls [`Document::contents()`](https://pliantdb.dev/main/pliantdb/core/document/struct.Document.html#method.contents) to deserialize the stored `BlogPost`. The second line returns an emitted Key and Value -- in our case a clone of the post's category and the value `1_u32`. With the map function, we're able to use [`query()`](https://pliantdb.dev/main/pliantdb/core/connection/struct.View.html#method.query) and [`query_with_docs()`](https://pliantdb.dev/main/pliantdb/core/connection/struct.View.html#method.query_with_docs):

```rust,noplayground,no_run
{{#include ../../view-example-string.rs:query_with_docs}}
```

The above queries the [Database](./database.md) for all documents in the `BlogPost` Collection that emitted a Key of `Some("Rust")`.

## Reduce

The second function to learn about is the `reduce()` function. It is responsible for turning an array of Key/Value pairs into a single Value. In some cases, PliantDB might need to call `reduce()` with values that have already been reduced one time. If this is the case, `rereduce` is set to true.

In this example, we're using the built-in `Iterator::sum()`(https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.sum) function to turn our Value of `1_u32` into a single `u32` representing the total number of documents.

```rust,noplayground,no_run
{{#include ../../view-example-string.rs:reduce_one_key}}
```

### Understanding Re-reduce

Let's examine this data set:

| Document ID | BlogPost Category |
| ----------- | ----------------- |
| 1           | Some("Rust")      |
| 2           | Some("Rust")      |
| 3           | Some("Cooking")   |
| 4           | None              |

When updating views, each view entry is reduced and the value is cached. These are the view entries:

| View Entry ID   | Reduced Value |
| --------------- | ------------- |
| Some("Rust")    | 2             |
| Some("Cooking") | 1             |
| None            | 1             |

When a reduce query is issued for a single key, the value can be returned without further processing. But, if the reduce query matches multiple keys, the View's `reduce()` function will be called with the already reduced values with `rereduce` set to `true`. For example, retrieving the total count of blog posts:

```rust,noplayground,no_run
{{#include ../../view-example-string.rs:reduce_multiple_keys}}
```

Once PliantDB has gathered each of the key's reduced values, it needs to further reduce that list into a single value. To accomplish this, the View's `reduce()` function to be invoked with `rereduce` set to `true`, and with mappings containing:

| Key             | Value |
| --------------- | ----- |
| Some("Rust")    | 2     |
| Some("Cooking") | 1     |
| None            | 1     |

This produces a final value of 4.

## How does PliantDB make this efficient?

When saving Documents, PliantDB does not immediately update related views. It instead notes what documents have been updated since the last time the View was indexed.

When a View is accessed, the queries include an [`AccessPolicy`](https://pliantdb.dev/main/pliantdb/core/connection/enum.AccessPolicy.html). If you aren't overriding it, [`UpdateBefore`](https://pliantdb.dev/main/pliantdb/core/connection/enum.AccessPolicy.html#variant.UpdateBefore) is used. This means that when the query is evaluated, PliantDB will first check if the index is out of date due to any updated data. If it is, it will update the View before evaluating the query.

If you're wanting to get results quickly and are willing to accept data that might not be updated, the access policies [`UpdateAfter`](https://pliantdb.dev/main/pliantdb/core/connection/enum.AccessPolicy.html#variant.UpdateAfter) and [`NoUpdate`](https://pliantdb.dev/main/pliantdb/core/connection/enum.AccessPolicy.html#variant.NoUpdate) can be used depending on your needs.

If multiple simulataneous queries are being evaluted for the same View and the View is outdated, PliantDB ensures that only a single view indexer will execute while both queries wait for it to complete.
