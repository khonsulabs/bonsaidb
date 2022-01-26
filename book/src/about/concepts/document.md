# Document

A [Document][document] is a single piece of stored data. Each document is stored within a [`Collection`](./collection.md), and has a unique ID within that Collection. There are two document types: [`OwnedDocument`][owned-document] and [`BorrowedDocument`][borrowed-document]. The [`View::map()` function][view-map] takes a [`BorrowedDocument`][borrowed-document], but nearly every other API utilizes [`OwnedDocument`][owned-document].

When a document is updated, BonsaiDb will check that the revision information passed matches the currently stored information. If not, a [conflict error](https://dev.bonsaidb.io/main/bonsaidb/core/enum.Error.html#variant.DocumentConflict) will be returned. This simple check ensures that if two writers try to update the document simultaneously, one will succeed and the other will receive an error.

## Serializable Collections

BonsaiDb provides the [`SerializedCollection`](https://dev.bonsaidb.io/main/bonsaidb/core/schema/trait.SerializedCollection.html) trait, which allows automatic serialization and deserialization in many sitautions. When using [`Document::contents()`](https://dev.bonsaidb.io/main/bonsaidb/core/document/trait.Document.html#method.contents) function, the document is serialized and deserialized by the format returned from [`SerializedCollection::format()`](https://dev.bonsaidb.io/main/bonsaidb/core/schema/trait.SerializedCollection.html#tymethod.format).

The [`CollectionDocument`](https://dev.bonsaidb.io/main/bonsaidb/core/schema/struct.CollectionDocument.html) type provides convenience methods of interacting with serializable documents.

### Default serialization of Serde-compatible types

BonsaiDb provides a convenience trait for [Serde](https://serde.rs/)-compatible data types: [`DefaultSerialization`](https://dev.bonsaidb.io/main/bonsaidb/core/schema/trait.DefaultSerialization.html). This empty trait can be implemented on any collection to have BonsaiDb provide its preferred serialization format, [Pot](https://github.com/khonsulabs/pot).

## Raw Collections

If you would prefer to manually manage the data stored inside of a Document, you can directly manage the [`contents`](https://dev.bonsaidb.io/main/bonsaidb/core/document/struct.OwnedDocument.html#structfield.contents) field. BonsaiDb will not interact with the `contents` of a Document. Only code that you write will parse or update the stored data.

[document]: https://dev.bonsaidb.io/main/bonsaidb/core/document/trait.Document.html
[owned-document]: https://dev.bonsaidb.io/main/bonsaidb/core/document/struct.OwnedDocument.html
[borrowed-document]: https://dev.bonsaidb.io/main/bonsaidb/core/document/struct.BorrowedDocument.html
[view-map]: ./view.md#map