use core::fmt::Debug;

use bonsaidb::core::schema::{Collection, View};

#[derive(Collection, Debug)]
#[collection(name = "name", authority = "authority")]
struct TestCollection;

#[test]
fn name_only() {
    #[derive(View, Debug)]
    #[view(collection = TestCollection, name = "some strange name äöü")]
    #[view(key = ())]
    struct TestView;
}

#[test]
fn serialization() {
    #[derive(View, Debug)]
    #[view(collection = TestCollection)]
    #[view(key = ())]
    #[view(serialization = transmog_bincode::Bincode)]
    struct TestView;
}

#[test]
fn serialization_none() {
    #[derive(View, Debug)]
    #[view(collection = TestCollection)]
    #[view(key = ())]
    #[view(serialization = None)]
    struct TestView;
}
