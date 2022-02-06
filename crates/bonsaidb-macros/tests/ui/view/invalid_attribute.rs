use bonsaidb::core::schema::View;

#[derive(View)]
#[view(name = "hi", authority = "hello", "hi")]
struct Test;

fn main() {}
