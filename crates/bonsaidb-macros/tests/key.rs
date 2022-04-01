use bonsaidb::core::key::Key;

#[test]
fn tuple_struct() {
    #[derive(Clone, Debug, Key)]
    struct Test(i32, i32, String);
}

#[test]
fn struct_struct() {
    #[derive(Clone, Debug, Key)]
    struct Test {
        a: i32,
        b: String,
    }
}

#[test]
fn r#enum() {
    #[derive(Clone, Debug, Key)]
    enum Test {
        A,
        B(i32, String),
        C { a: String, b: i32 },
    }
}
