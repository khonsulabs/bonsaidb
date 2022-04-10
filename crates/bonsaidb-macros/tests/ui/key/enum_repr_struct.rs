use bonsaidb::core::key::Key;


#[derive(Key)]
#[key(enum_repr = usize)]
struct Test;

fn main() {}
