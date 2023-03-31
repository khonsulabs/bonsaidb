use std::borrow::Cow;

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
fn transparent_structs() {
    #[derive(Clone, Debug, Key)]
    struct Test(i32);
    #[derive(Clone, Debug, Key)]
    struct TestNamed {
        named: i32,
    }

    assert_eq!(&[0, 0, 0, 1], Test(1).as_ord_bytes().unwrap().as_ref());
    assert_eq!(
        &[0, 0, 0, 1],
        TestNamed { named: 1 }.as_ord_bytes().unwrap().as_ref()
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
fn unit_struct() {
    #[derive(Clone, Debug, Key)]
    struct Test;

    assert_eq!(b"", Test.as_ord_bytes().unwrap().as_ref())
}

#[test]
fn r#enum() {
    #[derive(Clone, Debug, Key)]
    enum Test {
        A,
        B(i32, String),
        C { a: String, b: i32 },
    }

    assert_eq!(&[128, 0, 1], Test::A.as_ord_bytes().unwrap().as_ref());

    assert_eq!(
        &[129, 0, 0, 0, 0, 2, 97, 0, 1, 1],
        Test::B(2, "a".into()).as_ord_bytes().unwrap().as_ref()
    );

    assert_eq!(
        &[130, 0, 98, 0, 0, 0, 0, 3, 1, 1],
        Test::C {
            a: "b".into(),
            b: 3
        }
        .as_ord_bytes()
        .unwrap()
        .as_ref()
    )
}

#[test]
fn enum_repr() {
    #[repr(u8)]
    #[derive(Clone, Debug, Key)]
    enum Test1 {
        A = 1,
        B = 2,
    }

    #[derive(Clone, Debug, Key)]
    #[key(enum_repr = u8)]
    enum Test2 {
        A = 2,
        B = 1,
    }

    assert_eq!(
        Test1::A.as_ord_bytes().unwrap(),
        Test2::B.as_ord_bytes().unwrap()
    );
    assert_eq!(Test1::A.as_ord_bytes().unwrap().as_ref(), &[1]);

    assert_eq!(
        Test1::B.as_ord_bytes().unwrap(),
        Test2::A.as_ord_bytes().unwrap()
    );
    assert_eq!(Test1::B.as_ord_bytes().unwrap().as_ref(), &[2]);
}

#[test]
fn enum_u64() {
    #[repr(u64)]
    #[derive(Clone, Debug, Key)]
    enum Test {
        A = 0,
        B = u64::MAX - 1,
        C,
    }
    assert_eq!(
        Test::C.as_ord_bytes().unwrap().as_ref(),
        &[255, 255, 255, 255, 255, 255, 255, 255]
    );
}

#[test]
fn lifetime() {
    #[derive(Clone, Debug, Key)]
    struct Test<'a, 'b>(Cow<'a, str>, Cow<'b, str>);

    assert_eq!(
        &[97, 0, 98, 0, 1, 1],
        Test("a".into(), "b".into())
            .as_ord_bytes()
            .unwrap()
            .as_ref()
    )
}
