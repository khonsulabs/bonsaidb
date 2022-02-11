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
