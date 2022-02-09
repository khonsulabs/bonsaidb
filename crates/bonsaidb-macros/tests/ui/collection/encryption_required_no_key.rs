use bonsaidb::core::schema::Collection;

#[derive(Collection)]
#[collection(name = "name", encryption_required)]
struct Test;

fn main() {}
