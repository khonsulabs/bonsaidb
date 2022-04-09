use bonsaidb::core::key::{Key, KeyEncoding};

#[test]
fn tuple_struct() {
    #[derive(Clone, Debug, Key)]
    struct Test(i32, i32, String);

    assert_eq!(
        &[0, 0, 0, 1, 0, 0, 0, 2, 116, 101, 115, 116, 0, 4],
        Test(1, 2, "test".into()).as_ord_bytes().unwrap().as_ref()
    )
}

#[test]
fn struct_struct() {
    #[derive(Clone, Debug, Key)]
    struct Test {
        a: i32,
        b: String,
    }
    assert_eq!(
        &[255, 255, 255, 214, 109, 101, 97, 110, 105, 110, 103, 0, 7],
        Test {
            a: -42,
            b: "meaning".into()
        }
        .as_ord_bytes()
        .unwrap()
        .as_ref()
    )
}

#[test]
fn r#enum() {
    #[derive(Clone, Debug, Key)]
    enum Test {
        A,
        B(i32, String),
        C { a: String, b: i32 },
    }

    assert_eq!(
        &[0, 0, 0, 0, 0, 0, 0, 0],
        Test::A.as_ord_bytes().unwrap().as_ref()
    );

    assert_eq!(
        &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 97, 0, 1],
        Test::B(2, "a".into()).as_ord_bytes().unwrap().as_ref()
    );

    assert_eq!(
        &[0, 0, 0, 0, 0, 0, 0, 2, 98, 0, 0, 0, 0, 3, 1],
        Test::C {
            a: "b".into(),
            b: 3
        }
        .as_ord_bytes()
        .unwrap()
        .as_ref()
    )
}
