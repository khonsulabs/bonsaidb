use bonsaidb::core::schema::View;

#[derive(View)]
#[view(key = ())]
struct Test;

fn main() {}
