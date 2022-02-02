use bonsaidb::core::schema::Collection;

#[derive(Collection)]
#[collection(authority = "hello")]
struct Test;

fn main() {}
