use bonsaidb::core::schema::View;

#[derive(View)]
#[view(name = "hi", "hi")]
struct Test1;

#[derive(View)]
#[view(name = "hi", authority = "hello", "hi")]
struct Test2;

fn main() {}
