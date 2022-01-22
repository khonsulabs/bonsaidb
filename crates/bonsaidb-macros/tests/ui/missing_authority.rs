use bonsaidb_macros::Collection;

#[derive(Collection)]
#[collection(name = "hello")]
struct Test;

fn main() {}
