use bonsaidb::core::schema::Collection;

#[derive(Collection)]
#[collection(name = "hi", authority = "hello", "hi")]
struct Test;

#[derive(Collection)]
#[collection(name = "hi", authority = "hello", field = 200)]
struct Test2;

fn main() {}
