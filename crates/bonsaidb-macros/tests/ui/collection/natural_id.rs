use bonsaidb::core::schema::Collection;

#[derive(Collection)]
#[collection(name = "name", natural_id = Some(self.field))]
struct Test1 {
    #[natural_id]
    field: usize,
}

#[derive(Collection)]
#[collection(name = "name", natural_id = Some(self.field))]
struct Test2(#[natural_id] usize);

#[derive(Collection)]
#[collection(name = "name")]
struct Test3 {
    #[natural_id]
    field: usize,
    #[natural_id]
    field2: usize,
}

#[derive(Collection)]
#[collection(name = "name")]
struct Test4(#[natural_id] usize, #[natural_id] usize);

fn main() {}
