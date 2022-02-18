# Collection

A [Collection]({{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.Collection.html) is
a group of [Documents](./document.md) and associated functionality. Collections
are stored on-disk using ACID-compliant, transactional storage, ensuring your
data is protected in the event of a sudden power failure or other unfortunate
event.

The goal of a Collection is to encapsulate the logic for a set of data in
such a way that Collections could be designed to be shared and reused in
multiple [Schemas](./schema.md) or applications.

Each Collection must have a unique
[`CollectionName`]({{DOCS_BASE_URL}}/bonsaidb/core/schema/struct.CollectionName.html).
To help prevent naming collisions, an `authority` can be specified which
provides a level of namespacing.

A Collection can contain one or more [Views](./view.md).

## Primary Keys

All documents stored in a collection have a unique id. Primary keys in BonsaiDb
are immutable -- once a document has an id, it cannot be changed. If you wish
for a unique key that can be updated, use a unique view, and use a separate
value as a primary key.

The type is controlled by the [`Collection::PrimaryKey` associated
type][primary-key]. If you're using the derive macro, the type can be specified
using the `primary_key` parameter as in [this example][example]:

```rust,noplayground,no_run
{{#include ../../../../examples/basic-local/examples/primary-keys.rs:derive}}
```

If no `primary_key` is specified in the derive, `u64` will be used.

Inserting and accessing the collection can be done using the newly defined
primary key type:

```rust,noplayground,no_run
{{#include ../../../../examples/basic-local/examples/primary-keys.rs:query}}
```

### Natural Ids

It's not uncommon to need to store data in a database that has an "external"
identifier. Some examples could be externally authenticated user profiles,
social networking site posts, or for normalizing a single type's fields across
multiple Collections. These types of values are often called "Natural Keys" or
"Natural Identifiers".

[`SerializedCollection::natural_id()`][sc-natural-id] or
[`DefaultSerialzation::natural_id`][ds-natural-id] can be implemented to return
a value from the contents of a new document. When using the derive marco, the
`natural_id` parameter can be specified with either a closure or a path to a
function with the same signature.

In this [example][example], the `UserProfile` type is used to represent a user that has a
unique ID in an external database:

```rust,noplayground,no_run
{{#include ../../../../examples/basic-local/examples/primary-keys.rs:derive_with_natural_id}}
```

When pushing a `UserProfile` into the collection, the id will automatically be
assigned by calling `natural_id()`:

```rust,noplayground,no_run
{{#include ../../../../examples/basic-local/examples/primary-keys.rs:natural_id_query}}
```

### Custom Primary Keys

All primary keys must implement the [`Key` trait][key] . BonsaiDb provides implementations for many types, but any type that implements the trait can be used.

When using `push`/`push_into`, BonsaiDb needs to assign a unique ID to the incoming document. If `natural_id()` returns None, the storage backend will handle id assignment.

If the document being pushed is the first document in the collection, [`Key::first_value()`][key-first-value] is called and the resulting value is used as the document's id.

If the collection already has documents, the highest-ordered key is queried from
the collection. [`Key::next_value()`][key-next-value] is then called and the resulting value is
used as the document's id. `Key` implementors should not allow `next_value()` to
return a value that is less than the current value. `NextValueError::WouldWrap`
should be returned instead of wrapping.

Both `first_value()` and `next_value()` by default return
`NextValueError::Unimplemented`. If any error occurs while trying to assign a
unique id, the transaction will be aborted and rolled back.

[example]: https://github.com/khonsulabs/bonsaidb/blob/main/examples/basic-local/examples/primary-keys.rs
[primary-key]: {{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.Collection.html#associatedtype.PrimaryKey
[primary-key-derive]: {{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.Collection.html#selecting-a-primary-key-type
[key]: {{DOCS_BASE_URL}}/bonsaidb/core/key/trait.Key.html
[key-first-value]: {{DOCS_BASE_URL}}/bonsaidb/core/key/trait.Key.html#method.first_value
[key-next-value]: {{DOCS_BASE_URL}}/bonsaidb/core/key/trait.Key.html#method.next_value
[sc-natural-id]: {{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.SerializedCollection.html#method.natural_id
[ds-natural-id]: {{DOCS_BASE_URL}}/bonsaidb/core/schema/trait.DefaultSerialization.html#method.natural_id
