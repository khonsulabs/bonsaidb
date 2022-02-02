use bonsaidb::core::schema::Collection;

#[derive(Collection)]
#[collection(name = "hi", authority = "hello", "hi")]
struct Test;

fn main() {}
