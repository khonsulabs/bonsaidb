use bonsaidb::core::schema::Schema;


#[derive(Schema)]
#[schema(name = "name", "hi")]
struct Test1;

#[derive(Schema)]
#[schema(name = "name", test = "hi")]
struct Test2;

fn main() {}
