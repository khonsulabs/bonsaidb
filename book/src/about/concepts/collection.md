# Collection

A [Collection](https://pliantdb.dev/main/pliantdb/core/schema/trait.Collection.html) is a group of [Documents](./document.md) and associated functionality. The goal of a Collection is to encapsulate the logic for a set of data in such a way that Collections could be designed to be shared and reused in multiple [Schemas](./schema.md) or applications.

Each Collection must have a unique [`CollectionId`](https://pliantdb.dev/main/pliantdb/core/schema/struct.CollectionId.html).

A Collection can contain one or more [Views](./view.md).