use bonsaidb::core::schema::Collection;
use core::fmt::Debug;

#[test]
fn name_only() {
    use bonsaidb::core::schema::View;

    #[derive(Collection, Debug)]
    #[collection(name = "name", authority = "authority")]
    struct TestCollection;

    #[derive(View, Debug)]
    #[view(collection = TestCollection, name = "some strange name äöü")]
    #[view(key = ())]
    struct TestView;
}
