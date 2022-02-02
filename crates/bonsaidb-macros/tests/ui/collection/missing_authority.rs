use bonsaidb::core::schema::Collection;

#[derive(Collection)]
#[collection(name = "hello")]
struct Test;

fn main() {}
