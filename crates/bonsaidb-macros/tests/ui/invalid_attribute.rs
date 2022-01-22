use bonsaidb_macros::Collection;

#[derive(Collection)]
#[collection(name = "hi", authority = "hello", "hi")]
struct Test;

fn main() {}
