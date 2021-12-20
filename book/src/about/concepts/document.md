# Document

A [Document](https://dev.bonsaidb.io/main/bonsaidb/core/document/struct.Document.html) is a single piece of stored data. Each document is stored within a [`Collection`](./collection.md), and has a unique ID within that Collection.

When a Document is updated, BonsaiDb will check that the revision information passed matches the currently stored information. If not, a [conflict error](https://dev.bonsaidb.io/main/bonsaidb/core/enum.Error.html#variant.DocumentConflict) will be returned. This simple check ensures that if two writers try to update the document simultaneously, one will succeed and the other will receive an error.

## `serde`-powered Documents

BonsaiDb provides APIs for storing [serde](https://serde.rs/)-compatible data structures using several formats. When using [`Document::contents()`](https://dev.bonsaidb.io/main/bonsaidb/core/document/struct.Document.html#method.contents) function, the document is serialized and deserialized by the format returned from [`Collection::serializer()`](https://dev.bonsaidb.io/main/bonsaidb/core/schema/trait.Collection.html#method.serializer). The default format is [Pot](https://github.com/khonsulabs/pot/).

The [`CollectionDocument`](https://dev.bonsaidb.io/main/bonsaidb/core/schema/struct.CollectionDocument.html) type provides convenience methods of interacting with `serde`-serializable documents.

## Raw Collections

If you would prefer to manually manage the data stored inside of a Document, you can directly manage the [`contents`](https://dev.bonsaidb.io/main/bonsaidb/core/document/struct.Document.html#structfield.contents) field. BonsaiDb will not interact with the `contents` of a Document. Only code that you write will parse or update the stored data.
