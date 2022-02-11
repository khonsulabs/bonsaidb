Views allow querying documents contained within [`Collection`s][Collection] in
an efficient manner.

The [`ViewSchema::map()`] function is responsible for "mapping" data from the
stored document into the `Key` type. In the example below, the field `rank` is
being used as the View's [`Key`](View::Key) type, and the field `score` is being
used as the View's [`Value`](View::Value)

The [`ViewSchema::reduce()`] function is responsible for "reducing" a list of
[`Value`s](View::Value) into a single value. The example below implements
`reduce()` to return an average value.

This enables [querying the view](crate::connection::View) in many powerful ways:

- [`db.view::<MyView>().query().await`](crate::connection::View::query): Return
   all entries in the view (without the associated documents).
- [`db.view::<MyView>().query_with_docs().await`](crate::connection::View::query_with_docs):
   Return all entries in the view, including the associated
   [`OwnedDocument`s](crate::document::OwnedDocument).
- [`db.view::<MyView>().query_with_collection_docs().await`](crate::connection::View::query_with_collection_docs):
   Return all entries in the view, including the associated
   [`CollectionDocument<T>`s](crate::document::CollectionDocument).
- [`db.view::<MyView>().reduce().await`](crate::connection::View::reduce):
   Returns the reduced value of the view query. For the example below, the
   result type of this call is `f32`.
- [`db.view::<MyView>().reduce_grouped().await`](crate::connection::View::reduce_grouped):
   Returns the reduced value of the view query, grouped by key. For the example
   below, the `value` returned will be an `f32` that is the average `score` of
   all documents with a matching `rank`.

All of the queries above can be filtered and customized by the methods available
on [`connection::View`](crate::connection::View).

For a more detailed walkthrough, see our [user guide's section on
Views](https://dev.bonsaidb.io/main/release/about/concepts/view.html).
