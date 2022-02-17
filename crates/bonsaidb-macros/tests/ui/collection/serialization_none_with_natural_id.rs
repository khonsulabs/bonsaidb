use bonsaidb::core::schema::Collection;

#[derive(Collection)]
#[collection(name = "name", serialization = None, natural_id = |t: &Self| None)]
struct Test;

fn main() {}
