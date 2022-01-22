use bonsaidb_macros::Collection;

#[derive(Collection)]
#[collection(authority = "hello")]
struct Test;

fn main() {}
