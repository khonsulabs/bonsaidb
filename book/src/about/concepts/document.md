# Document

A [Document](https://bonsaidb.dev/main/bonsaidb/core/document/struct.Document.html) is a single piece of stored data. Each document is stored within a [`Collection`](./collection.md), and has a unique ID within that Collection. A Document also contains a revision ID as well as a digest matching the current contents of the document.

When a Document is updated, BonsaiDb will check that the revision information passed matches the currently stored information. If not, a [conflict error](https://bonsaidb.dev/main/bonsaidb/core/enum.Error.html#variant.DocumentConflict) will be returned. This simple check ensures that if two writers try to update the document simultaneously, one will succeed and the other will receive an error.

BonsaiDb provides APIs for storing [serde](https://serde.rs/)-compatible data structures using the [CBOR](https://cbor.io/) format. CBOR provides larger data type compatibility than JSON, and is a more efficent format. It also provides a bit more resilliance for parsing structures that have changed than some other encoding formats, but care still needs to be taken when updating structures that represent already-stored data.

If you would prefer to manually manage the data stored inside of a Document, you can directly manage the [`contents`](https://bonsaidb.dev/main/bonsaidb/core/document/struct.Document.html#structfield.contents) field. BonsaiDb will not interact with the `contents` of a Document. Only code that you write will parse or update the stored data.
