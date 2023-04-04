#![feature(prelude_import)]
#[prelude_import]
use std::prelude::rust_2021::*;
#[macro_use]
extern crate std;
use std::borrow::Cow;
use bonsaidb::core::key::{Key, KeyEncoding};
extern crate test;
#[cfg(test)]
#[rustc_test_marker = "tuple_struct"]
pub const tuple_struct: test::TestDescAndFn = test::TestDescAndFn {
    desc: test::TestDesc {
        name: test::StaticTestName("tuple_struct"),
        ignore: false,
        ignore_message: ::core::option::Option::None,
        source_file: "crates/bonsaidb-macros/tests/key.rs",
        start_line: 6usize,
        start_col: 4usize,
        end_line: 6usize,
        end_col: 16usize,
        compile_fail: false,
        no_run: false,
        should_panic: test::ShouldPanic::No,
        test_type: test::TestType::IntegrationTest,
    },
    testfn: test::StaticTestFn(|| test::assert_test_result(tuple_struct())),
};
fn tuple_struct() {
    struct Test(i32, i32, String);
    #[automatically_derived]
    impl ::core::clone::Clone for Test {
        #[inline]
        fn clone(&self) -> Test {
            Test(
                ::core::clone::Clone::clone(&self.0),
                ::core::clone::Clone::clone(&self.1),
                ::core::clone::Clone::clone(&self.2),
            )
        }
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for Test {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_tuple_field3_finish(f, "Test", &self.0, &self.1, &&self.2)
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::Key<'__bonsaidb_macros_key> for Test {
        const CAN_OWN_BYTES: bool = false;
        type Owned = Test;
        fn into_owned(self) -> Self::Owned {
            Test(
                self.0.into_owned(),
                self.1.into_owned(),
                self.2.into_owned(),
            )
        }
        fn from_ord_bytes<'__bonsaidb_macros_b>(
            mut __bonsaidb_macros_bytes: ::bonsaidb::core::key::ByteSource<
                '__bonsaidb_macros_key,
                '__bonsaidb_macros_b,
            >,
        ) -> ::core::prelude::v1::Result<Self, Self::Error> {
            let mut __bonsaidb_macros_decoder =
                ::bonsaidb::core::key::CompositeKeyDecoder::default_for(__bonsaidb_macros_bytes);
            let __bonsaidb_macros_self_ = Self(
                __bonsaidb_macros_decoder.decode()?,
                __bonsaidb_macros_decoder.decode()?,
                __bonsaidb_macros_decoder.decode()?,
            );
            __bonsaidb_macros_decoder.finish()?;
            ::core::prelude::v1::Ok(__bonsaidb_macros_self_)
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::KeyEncoding<'__bonsaidb_macros_key, Self>
        for Test
    {
        type Error = ::bonsaidb::core::key::CompositeKeyError;
        const LENGTH: ::core::prelude::v1::Option<usize> = ::core::prelude::v1::None;
        fn describe<Visitor>(visitor: &mut Visitor)
        where
            Visitor: ::bonsaidb::core::key::KeyVisitor,
        {
            visitor.visit_composite(
                ::bonsaidb::core::key::CompositeKind::Struct(std::borrow::Cow::Borrowed(
                    std::any::type_name::<Self>(),
                )),
                3usize,
            );
            <i32>::describe(visitor);
            <i32>::describe(visitor);
            <String>::describe(visitor);
        }
        fn as_ord_bytes(
            &'__bonsaidb_macros_key self,
        ) -> ::core::prelude::v1::Result<
            ::std::borrow::Cow<'__bonsaidb_macros_key, [u8]>,
            Self::Error,
        > {
            let mut __bonsaidb_macros_encoder =
                ::bonsaidb::core::key::CompositeKeyEncoder::default();
            __bonsaidb_macros_encoder.encode(&self.0)?;
            __bonsaidb_macros_encoder.encode(&self.1)?;
            __bonsaidb_macros_encoder.encode(&self.2)?;
            ::core::prelude::v1::Ok(::std::borrow::Cow::Owned(
                __bonsaidb_macros_encoder.finish(),
            ))
        }
    }
    match (
        &&[0, 0, 0, 1, 0, 0, 0, 2, 116, 101, 115, 116, 0, 4],
        &Test(1, 2, "test".into()).as_ord_bytes().unwrap().as_ref(),
    ) {
        (left_val, right_val) => {
            if !(*left_val == *right_val) {
                let kind = ::core::panicking::AssertKind::Eq;
                ::core::panicking::assert_failed(
                    kind,
                    &*left_val,
                    &*right_val,
                    ::core::option::Option::None,
                );
            }
        }
    }
}
extern crate test;
#[cfg(test)]
#[rustc_test_marker = "transparent_structs"]
pub const transparent_structs: test::TestDescAndFn = test::TestDescAndFn {
    desc: test::TestDesc {
        name: test::StaticTestName("transparent_structs"),
        ignore: false,
        ignore_message: ::core::option::Option::None,
        source_file: "crates/bonsaidb-macros/tests/key.rs",
        start_line: 17usize,
        start_col: 4usize,
        end_line: 17usize,
        end_col: 23usize,
        compile_fail: false,
        no_run: false,
        should_panic: test::ShouldPanic::No,
        test_type: test::TestType::IntegrationTest,
    },
    testfn: test::StaticTestFn(|| test::assert_test_result(transparent_structs())),
};
fn transparent_structs() {
    struct Test(i32);
    #[automatically_derived]
    impl ::core::clone::Clone for Test {
        #[inline]
        fn clone(&self) -> Test {
            Test(::core::clone::Clone::clone(&self.0))
        }
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for Test {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_tuple_field1_finish(f, "Test", &&self.0)
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::Key<'__bonsaidb_macros_key> for Test {
        const CAN_OWN_BYTES: bool = <i32>::CAN_OWN_BYTES;
        type Owned = Test;
        fn into_owned(self) -> Self::Owned {
            Test(self.0.into_owned())
        }
        fn from_ord_bytes<'__bonsaidb_macros_b>(
            bytes: ::bonsaidb::core::key::ByteSource<'__bonsaidb_macros_key, '__bonsaidb_macros_b>,
        ) -> ::core::prelude::v1::Result<Self, Self::Error> {
            <i32>::from_ord_bytes(bytes).map(Self)
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::KeyEncoding<'__bonsaidb_macros_key, Self>
        for Test
    {
        type Error = <i32 as ::bonsaidb::core::key::KeyEncoding<'__bonsaidb_macros_key>>::Error;
        const LENGTH: ::core::prelude::v1::Option<usize> = <i32>::LENGTH;
        fn describe<Visitor>(visitor: &mut Visitor)
        where
            Visitor: ::bonsaidb::core::key::KeyVisitor,
        {
            <i32>::describe(visitor)
        }
        fn as_ord_bytes(
            &'__bonsaidb_macros_key self,
        ) -> ::core::prelude::v1::Result<
            ::std::borrow::Cow<'__bonsaidb_macros_key, [u8]>,
            Self::Error,
        > {
            self.0.as_ord_bytes()
        }
    }
    struct TestNamed {
        named: i32,
    }
    #[automatically_derived]
    impl ::core::clone::Clone for TestNamed {
        #[inline]
        fn clone(&self) -> TestNamed {
            TestNamed {
                named: ::core::clone::Clone::clone(&self.named),
            }
        }
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for TestNamed {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_struct_field1_finish(
                f,
                "TestNamed",
                "named",
                &&self.named,
            )
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::Key<'__bonsaidb_macros_key> for TestNamed {
        const CAN_OWN_BYTES: bool = <i32>::CAN_OWN_BYTES;
        type Owned = TestNamed;
        fn into_owned(self) -> Self::Owned {
            TestNamed {
                named: self.named.into_owned(),
            }
        }
        fn from_ord_bytes<'__bonsaidb_macros_b>(
            bytes: ::bonsaidb::core::key::ByteSource<'__bonsaidb_macros_key, '__bonsaidb_macros_b>,
        ) -> ::core::prelude::v1::Result<Self, Self::Error> {
            <i32>::from_ord_bytes(bytes).map(|value| Self { named: value })
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::KeyEncoding<'__bonsaidb_macros_key, Self>
        for TestNamed
    {
        type Error = <i32 as ::bonsaidb::core::key::KeyEncoding<'__bonsaidb_macros_key>>::Error;
        const LENGTH: ::core::prelude::v1::Option<usize> = <i32>::LENGTH;
        fn describe<Visitor>(visitor: &mut Visitor)
        where
            Visitor: ::bonsaidb::core::key::KeyVisitor,
        {
            <i32>::describe(visitor)
        }
        fn as_ord_bytes(
            &'__bonsaidb_macros_key self,
        ) -> ::core::prelude::v1::Result<
            ::std::borrow::Cow<'__bonsaidb_macros_key, [u8]>,
            Self::Error,
        > {
            self.named.as_ord_bytes()
        }
    }
    match (&&[0, 0, 0, 1], &Test(1).as_ord_bytes().unwrap().as_ref()) {
        (left_val, right_val) => {
            if !(*left_val == *right_val) {
                let kind = ::core::panicking::AssertKind::Eq;
                ::core::panicking::assert_failed(
                    kind,
                    &*left_val,
                    &*right_val,
                    ::core::option::Option::None,
                );
            }
        }
    };
    match (
        &&[0, 0, 0, 1],
        &TestNamed { named: 1 }.as_ord_bytes().unwrap().as_ref(),
    ) {
        (left_val, right_val) => {
            if !(*left_val == *right_val) {
                let kind = ::core::panicking::AssertKind::Eq;
                ::core::panicking::assert_failed(
                    kind,
                    &*left_val,
                    &*right_val,
                    ::core::option::Option::None,
                );
            }
        }
    }
}
extern crate test;
#[cfg(test)]
#[rustc_test_marker = "struct_struct"]
pub const struct_struct: test::TestDescAndFn = test::TestDescAndFn {
    desc: test::TestDesc {
        name: test::StaticTestName("struct_struct"),
        ignore: false,
        ignore_message: ::core::option::Option::None,
        source_file: "crates/bonsaidb-macros/tests/key.rs",
        start_line: 33usize,
        start_col: 4usize,
        end_line: 33usize,
        end_col: 17usize,
        compile_fail: false,
        no_run: false,
        should_panic: test::ShouldPanic::No,
        test_type: test::TestType::IntegrationTest,
    },
    testfn: test::StaticTestFn(|| test::assert_test_result(struct_struct())),
};
fn struct_struct() {
    struct Test {
        a: i32,
        b: String,
    }
    #[automatically_derived]
    impl ::core::clone::Clone for Test {
        #[inline]
        fn clone(&self) -> Test {
            Test {
                a: ::core::clone::Clone::clone(&self.a),
                b: ::core::clone::Clone::clone(&self.b),
            }
        }
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for Test {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_struct_field2_finish(
                f, "Test", "a", &self.a, "b", &&self.b,
            )
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::Key<'__bonsaidb_macros_key> for Test {
        const CAN_OWN_BYTES: bool = false;
        type Owned = Test;
        fn into_owned(self) -> Self::Owned {
            Self {
                a: self.a.into_owned(),
                b: self.b.into_owned(),
            }
        }
        fn from_ord_bytes<'__bonsaidb_macros_b>(
            mut __bonsaidb_macros_bytes: ::bonsaidb::core::key::ByteSource<
                '__bonsaidb_macros_key,
                '__bonsaidb_macros_b,
            >,
        ) -> ::core::prelude::v1::Result<Self, Self::Error> {
            let mut __bonsaidb_macros_decoder =
                ::bonsaidb::core::key::CompositeKeyDecoder::default_for(__bonsaidb_macros_bytes);
            let __bonsaidb_macros_self_ = Self {
                a: __bonsaidb_macros_decoder.decode()?,
                b: __bonsaidb_macros_decoder.decode()?,
            };
            __bonsaidb_macros_decoder.finish()?;
            ::core::prelude::v1::Ok(__bonsaidb_macros_self_)
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::KeyEncoding<'__bonsaidb_macros_key, Self>
        for Test
    {
        type Error = ::bonsaidb::core::key::CompositeKeyError;
        const LENGTH: ::core::prelude::v1::Option<usize> = ::core::prelude::v1::None;
        fn describe<Visitor>(visitor: &mut Visitor)
        where
            Visitor: ::bonsaidb::core::key::KeyVisitor,
        {
            visitor.visit_composite(
                ::bonsaidb::core::key::CompositeKind::Struct(std::borrow::Cow::Borrowed(
                    std::any::type_name::<Self>(),
                )),
                2usize,
            );
            <i32>::describe(visitor);
            <String>::describe(visitor);
        }
        fn as_ord_bytes(
            &'__bonsaidb_macros_key self,
        ) -> ::core::prelude::v1::Result<
            ::std::borrow::Cow<'__bonsaidb_macros_key, [u8]>,
            Self::Error,
        > {
            let mut __bonsaidb_macros_encoder =
                ::bonsaidb::core::key::CompositeKeyEncoder::default();
            __bonsaidb_macros_encoder.encode(&self.a)?;
            __bonsaidb_macros_encoder.encode(&self.b)?;
            ::core::prelude::v1::Ok(::std::borrow::Cow::Owned(
                __bonsaidb_macros_encoder.finish(),
            ))
        }
    }
    match (
        &&[255, 255, 255, 214, 109, 101, 97, 110, 105, 110, 103, 0, 7],
        &Test {
            a: -42,
            b: "meaning".into(),
        }
        .as_ord_bytes()
        .unwrap()
        .as_ref(),
    ) {
        (left_val, right_val) => {
            if !(*left_val == *right_val) {
                let kind = ::core::panicking::AssertKind::Eq;
                ::core::panicking::assert_failed(
                    kind,
                    &*left_val,
                    &*right_val,
                    ::core::option::Option::None,
                );
            }
        }
    }
}
extern crate test;
#[cfg(test)]
#[rustc_test_marker = "unit_struct"]
pub const unit_struct: test::TestDescAndFn = test::TestDescAndFn {
    desc: test::TestDesc {
        name: test::StaticTestName("unit_struct"),
        ignore: false,
        ignore_message: ::core::option::Option::None,
        source_file: "crates/bonsaidb-macros/tests/key.rs",
        start_line: 52usize,
        start_col: 4usize,
        end_line: 52usize,
        end_col: 15usize,
        compile_fail: false,
        no_run: false,
        should_panic: test::ShouldPanic::No,
        test_type: test::TestType::IntegrationTest,
    },
    testfn: test::StaticTestFn(|| test::assert_test_result(unit_struct())),
};
fn unit_struct() {
    struct Test;
    #[automatically_derived]
    impl ::core::clone::Clone for Test {
        #[inline]
        fn clone(&self) -> Test {
            Test
        }
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for Test {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::write_str(f, "Test")
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::Key<'__bonsaidb_macros_key> for Test {
        const CAN_OWN_BYTES: bool = false;
        type Owned = Test;
        fn into_owned(self) -> Self::Owned {
            Test
        }
        fn from_ord_bytes<'__bonsaidb_macros_b>(
            bytes: ::bonsaidb::core::key::ByteSource<'__bonsaidb_macros_key, '__bonsaidb_macros_b>,
        ) -> ::core::prelude::v1::Result<Self, Self::Error> {
            ::core::prelude::v1::Ok(Self)
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::KeyEncoding<'__bonsaidb_macros_key, Self>
        for Test
    {
        type Error = std::convert::Infallible;
        const LENGTH: ::core::prelude::v1::Option<usize> = ::core::prelude::v1::Some(0);
        fn describe<Visitor>(visitor: &mut Visitor)
        where
            Visitor: ::bonsaidb::core::key::KeyVisitor,
        {
            visitor.visit_type(::bonsaidb::core::key::KeyKind::Unit);
        }
        fn as_ord_bytes(
            &'__bonsaidb_macros_key self,
        ) -> ::core::prelude::v1::Result<
            ::std::borrow::Cow<'__bonsaidb_macros_key, [u8]>,
            Self::Error,
        > {
            ::core::prelude::v1::Ok(::std::borrow::Cow::Borrowed(&[]))
        }
    }
    match (&b"", &Test.as_ord_bytes().unwrap().as_ref()) {
        (left_val, right_val) => {
            if !(*left_val == *right_val) {
                let kind = ::core::panicking::AssertKind::Eq;
                ::core::panicking::assert_failed(
                    kind,
                    &*left_val,
                    &*right_val,
                    ::core::option::Option::None,
                );
            }
        }
    }
}
extern crate test;
#[cfg(test)]
#[rustc_test_marker = "r#enum"]
pub const r#enum: test::TestDescAndFn = test::TestDescAndFn {
    desc: test::TestDesc {
        name: test::StaticTestName("r#enum"),
        ignore: false,
        ignore_message: ::core::option::Option::None,
        source_file: "crates/bonsaidb-macros/tests/key.rs",
        start_line: 60usize,
        start_col: 4usize,
        end_line: 60usize,
        end_col: 10usize,
        compile_fail: false,
        no_run: false,
        should_panic: test::ShouldPanic::No,
        test_type: test::TestType::IntegrationTest,
    },
    testfn: test::StaticTestFn(|| test::assert_test_result(r#enum())),
};
fn r#enum() {
    enum Test {
        A,
        B(i32, String),
        C { a: String, b: i32 },
    }
    #[automatically_derived]
    impl ::core::clone::Clone for Test {
        #[inline]
        fn clone(&self) -> Test {
            match self {
                Test::A => Test::A,
                Test::B(__self_0, __self_1) => Test::B(
                    ::core::clone::Clone::clone(__self_0),
                    ::core::clone::Clone::clone(__self_1),
                ),
                Test::C {
                    a: __self_0,
                    b: __self_1,
                } => Test::C {
                    a: ::core::clone::Clone::clone(__self_0),
                    b: ::core::clone::Clone::clone(__self_1),
                },
            }
        }
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for Test {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            match self {
                Test::A => ::core::fmt::Formatter::write_str(f, "A"),
                Test::B(__self_0, __self_1) => {
                    ::core::fmt::Formatter::debug_tuple_field2_finish(f, "B", __self_0, &__self_1)
                }
                Test::C {
                    a: __self_0,
                    b: __self_1,
                } => ::core::fmt::Formatter::debug_struct_field2_finish(
                    f, "C", "a", __self_0, "b", &__self_1,
                ),
            }
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::Key<'__bonsaidb_macros_key> for Test {
        const CAN_OWN_BYTES: bool = false;
        type Owned = Test;
        fn into_owned(self) -> Self::Owned {
            match self {
                Self::A => Test::A,
                Self::B(__bonsaidb_macros_field_0, __bonsaidb_macros_field_1) => Test::B(
                    __bonsaidb_macros_field_0.into_owned(),
                    __bonsaidb_macros_field_1.into_owned(),
                ),
                Self::C { a, b } => Test::C {
                    a: a.into_owned(),
                    b: b.into_owned(),
                },
            }
        }
        fn from_ord_bytes<'__bonsaidb_macros_b>(
            mut __bonsaidb_macros_bytes: ::bonsaidb::core::key::ByteSource<
                '__bonsaidb_macros_key,
                '__bonsaidb_macros_b,
            >,
        ) -> ::core::prelude::v1::Result<Self, Self::Error> {
            let mut __bonsaidb_macros_decoder =
                ::bonsaidb::core::key::CompositeKeyDecoder::default_for(__bonsaidb_macros_bytes);
            const __bonsaidb_macros_discriminant0: isize = 0;
            const __bonsaidb_macros_discriminant1: isize = __bonsaidb_macros_discriminant0 + 1;
            const __bonsaidb_macros_discriminant2: isize = __bonsaidb_macros_discriminant1 + 1;
            let __bonsaidb_macros_self_ = match __bonsaidb_macros_decoder.decode::<isize>()? {
                __bonsaidb_macros_discriminant0 => Self::A,
                __bonsaidb_macros_discriminant1 => Self::B(
                    __bonsaidb_macros_decoder.decode()?,
                    __bonsaidb_macros_decoder.decode()?,
                ),
                __bonsaidb_macros_discriminant2 => Self::C {
                    a: __bonsaidb_macros_decoder.decode()?,
                    b: __bonsaidb_macros_decoder.decode()?,
                },
                _ => {
                    return ::core::prelude::v1::Err(
                        ::bonsaidb::core::key::CompositeKeyError::from(::std::io::Error::from(
                            ::std::io::ErrorKind::InvalidData,
                        )),
                    )
                }
            };
            __bonsaidb_macros_decoder.finish()?;
            ::core::prelude::v1::Ok(__bonsaidb_macros_self_)
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::KeyEncoding<'__bonsaidb_macros_key, Self>
        for Test
    {
        type Error = ::bonsaidb::core::key::CompositeKeyError;
        const LENGTH: ::core::prelude::v1::Option<usize> = ::core::prelude::v1::None;
        fn describe<Visitor>(visitor: &mut Visitor)
        where
            Visitor: ::bonsaidb::core::key::KeyVisitor,
        {
            visitor.visit_composite(::bonsaidb::core::key::CompositeKind::Tuple, 3usize);
            visitor.visit_type(::bonsaidb::core::key::KeyKind::Unit);
            <i32>::describe(visitor);
            <String>::describe(visitor);
            <String>::describe(visitor);
            <i32>::describe(visitor);
        }
        fn as_ord_bytes(
            &'__bonsaidb_macros_key self,
        ) -> ::core::prelude::v1::Result<
            ::std::borrow::Cow<'__bonsaidb_macros_key, [u8]>,
            Self::Error,
        > {
            let mut __bonsaidb_macros_encoder =
                ::bonsaidb::core::key::CompositeKeyEncoder::default();
            const __bonsaidb_macros_discriminant0: isize = 0;
            const __bonsaidb_macros_discriminant1: isize = __bonsaidb_macros_discriminant0 + 1;
            const __bonsaidb_macros_discriminant2: isize = __bonsaidb_macros_discriminant1 + 1;
            match self {
                Self::A => __bonsaidb_macros_encoder.encode(&__bonsaidb_macros_discriminant0)?,
                Self::B(__bonsaidb_macros_field_0, __bonsaidb_macros_field_1) => {
                    __bonsaidb_macros_encoder.encode(&__bonsaidb_macros_discriminant1)?;
                    __bonsaidb_macros_encoder.encode(__bonsaidb_macros_field_0)?;
                    __bonsaidb_macros_encoder.encode(__bonsaidb_macros_field_1)?;
                }
                Self::C { a, b } => {
                    __bonsaidb_macros_encoder.encode(&__bonsaidb_macros_discriminant2)?;
                    __bonsaidb_macros_encoder.encode(a)?;
                    __bonsaidb_macros_encoder.encode(b)?;
                }
            }
            ::core::prelude::v1::Ok(::std::borrow::Cow::Owned(
                __bonsaidb_macros_encoder.finish(),
            ))
        }
    }
    match (&&[128, 0, 1], &Test::A.as_ord_bytes().unwrap().as_ref()) {
        (left_val, right_val) => {
            if !(*left_val == *right_val) {
                let kind = ::core::panicking::AssertKind::Eq;
                ::core::panicking::assert_failed(
                    kind,
                    &*left_val,
                    &*right_val,
                    ::core::option::Option::None,
                );
            }
        }
    };
    match (
        &&[129, 0, 0, 0, 0, 2, 97, 0, 1, 1],
        &Test::B(2, "a".into()).as_ord_bytes().unwrap().as_ref(),
    ) {
        (left_val, right_val) => {
            if !(*left_val == *right_val) {
                let kind = ::core::panicking::AssertKind::Eq;
                ::core::panicking::assert_failed(
                    kind,
                    &*left_val,
                    &*right_val,
                    ::core::option::Option::None,
                );
            }
        }
    };
    match (
        &&[130, 0, 98, 0, 0, 0, 0, 3, 1, 1],
        &Test::C {
            a: "b".into(),
            b: 3,
        }
        .as_ord_bytes()
        .unwrap()
        .as_ref(),
    ) {
        (left_val, right_val) => {
            if !(*left_val == *right_val) {
                let kind = ::core::panicking::AssertKind::Eq;
                ::core::panicking::assert_failed(
                    kind,
                    &*left_val,
                    &*right_val,
                    ::core::option::Option::None,
                );
            }
        }
    }
}
extern crate test;
#[cfg(test)]
#[rustc_test_marker = "enum_repr"]
pub const enum_repr: test::TestDescAndFn = test::TestDescAndFn {
    desc: test::TestDesc {
        name: test::StaticTestName("enum_repr"),
        ignore: false,
        ignore_message: ::core::option::Option::None,
        source_file: "crates/bonsaidb-macros/tests/key.rs",
        start_line: 88usize,
        start_col: 4usize,
        end_line: 88usize,
        end_col: 13usize,
        compile_fail: false,
        no_run: false,
        should_panic: test::ShouldPanic::No,
        test_type: test::TestType::IntegrationTest,
    },
    testfn: test::StaticTestFn(|| test::assert_test_result(enum_repr())),
};
fn enum_repr() {
    #[repr(u8)]
    enum Test1 {
        A = 1,
        B = 2,
    }
    #[automatically_derived]
    impl ::core::clone::Clone for Test1 {
        #[inline]
        fn clone(&self) -> Test1 {
            match self {
                Test1::A => Test1::A,
                Test1::B => Test1::B,
            }
        }
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for Test1 {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::write_str(
                f,
                match self {
                    Test1::A => "A",
                    Test1::B => "B",
                },
            )
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::Key<'__bonsaidb_macros_key> for Test1 {
        const CAN_OWN_BYTES: bool = false;
        type Owned = Test1;
        fn into_owned(self) -> Self::Owned {
            match self {
                Self::A => Test1::A,
                Self::B => Test1::B,
            }
        }
        fn from_ord_bytes<'__bonsaidb_macros_b>(
            mut __bonsaidb_macros_bytes: ::bonsaidb::core::key::ByteSource<
                '__bonsaidb_macros_key,
                '__bonsaidb_macros_b,
            >,
        ) -> ::core::prelude::v1::Result<Self, Self::Error> {
            const __bonsaidb_macros_discriminant0: u8 = 1;
            const __bonsaidb_macros_discriminant1: u8 = 2;
            ::core::prelude::v1::Ok(
                match <u8>::from_ord_bytes(__bonsaidb_macros_bytes)
                    .map_err(::bonsaidb::core::key::CompositeKeyError::new)?
                {
                    __bonsaidb_macros_discriminant0 => Self::A,
                    __bonsaidb_macros_discriminant1 => Self::B,
                    _ => {
                        return ::core::prelude::v1::Err(
                            ::bonsaidb::core::key::CompositeKeyError::from(::std::io::Error::from(
                                ::std::io::ErrorKind::InvalidData,
                            )),
                        )
                    }
                },
            )
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::KeyEncoding<'__bonsaidb_macros_key, Self>
        for Test1
    {
        type Error = ::bonsaidb::core::key::CompositeKeyError;
        const LENGTH: ::core::prelude::v1::Option<usize> =
            <u8 as ::bonsaidb::core::key::KeyEncoding<'__bonsaidb_macros_key>>::LENGTH;
        fn describe<Visitor>(visitor: &mut Visitor)
        where
            Visitor: ::bonsaidb::core::key::KeyVisitor,
        {
            <u8>::describe(visitor);
        }
        fn as_ord_bytes(
            &'__bonsaidb_macros_key self,
        ) -> ::core::prelude::v1::Result<
            ::std::borrow::Cow<'__bonsaidb_macros_key, [u8]>,
            Self::Error,
        > {
            const __bonsaidb_macros_discriminant0: u8 = 1;
            const __bonsaidb_macros_discriminant1: u8 = 2;
            match self {
                Self::A => __bonsaidb_macros_discriminant0.as_ord_bytes(),
                Self::B => __bonsaidb_macros_discriminant1.as_ord_bytes(),
            }
            .map_err(::bonsaidb::core::key::CompositeKeyError::new)
        }
    }
    # [key (enum_repr = u8)]
    enum Test2 {
        A = 2,
        B = 1,
    }
    #[automatically_derived]
    impl ::core::clone::Clone for Test2 {
        #[inline]
        fn clone(&self) -> Test2 {
            match self {
                Test2::A => Test2::A,
                Test2::B => Test2::B,
            }
        }
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for Test2 {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::write_str(
                f,
                match self {
                    Test2::A => "A",
                    Test2::B => "B",
                },
            )
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::Key<'__bonsaidb_macros_key> for Test2 {
        const CAN_OWN_BYTES: bool = false;
        type Owned = Test2;
        fn into_owned(self) -> Self::Owned {
            match self {
                Self::A => Test2::A,
                Self::B => Test2::B,
            }
        }
        fn from_ord_bytes<'__bonsaidb_macros_b>(
            mut __bonsaidb_macros_bytes: ::bonsaidb::core::key::ByteSource<
                '__bonsaidb_macros_key,
                '__bonsaidb_macros_b,
            >,
        ) -> ::core::prelude::v1::Result<Self, Self::Error> {
            const __bonsaidb_macros_discriminant0: u8 = 2;
            const __bonsaidb_macros_discriminant1: u8 = 1;
            ::core::prelude::v1::Ok(
                match <u8>::from_ord_bytes(__bonsaidb_macros_bytes)
                    .map_err(::bonsaidb::core::key::CompositeKeyError::new)?
                {
                    __bonsaidb_macros_discriminant0 => Self::A,
                    __bonsaidb_macros_discriminant1 => Self::B,
                    _ => {
                        return ::core::prelude::v1::Err(
                            ::bonsaidb::core::key::CompositeKeyError::from(::std::io::Error::from(
                                ::std::io::ErrorKind::InvalidData,
                            )),
                        )
                    }
                },
            )
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::KeyEncoding<'__bonsaidb_macros_key, Self>
        for Test2
    {
        type Error = ::bonsaidb::core::key::CompositeKeyError;
        const LENGTH: ::core::prelude::v1::Option<usize> =
            <u8 as ::bonsaidb::core::key::KeyEncoding<'__bonsaidb_macros_key>>::LENGTH;
        fn describe<Visitor>(visitor: &mut Visitor)
        where
            Visitor: ::bonsaidb::core::key::KeyVisitor,
        {
            <u8>::describe(visitor);
        }
        fn as_ord_bytes(
            &'__bonsaidb_macros_key self,
        ) -> ::core::prelude::v1::Result<
            ::std::borrow::Cow<'__bonsaidb_macros_key, [u8]>,
            Self::Error,
        > {
            const __bonsaidb_macros_discriminant0: u8 = 2;
            const __bonsaidb_macros_discriminant1: u8 = 1;
            match self {
                Self::A => __bonsaidb_macros_discriminant0.as_ord_bytes(),
                Self::B => __bonsaidb_macros_discriminant1.as_ord_bytes(),
            }
            .map_err(::bonsaidb::core::key::CompositeKeyError::new)
        }
    }
    match (
        &Test1::A.as_ord_bytes().unwrap(),
        &Test2::B.as_ord_bytes().unwrap(),
    ) {
        (left_val, right_val) => {
            if !(*left_val == *right_val) {
                let kind = ::core::panicking::AssertKind::Eq;
                ::core::panicking::assert_failed(
                    kind,
                    &*left_val,
                    &*right_val,
                    ::core::option::Option::None,
                );
            }
        }
    };
    match (&Test1::A.as_ord_bytes().unwrap().as_ref(), &&[1]) {
        (left_val, right_val) => {
            if !(*left_val == *right_val) {
                let kind = ::core::panicking::AssertKind::Eq;
                ::core::panicking::assert_failed(
                    kind,
                    &*left_val,
                    &*right_val,
                    ::core::option::Option::None,
                );
            }
        }
    };
    match (
        &Test1::B.as_ord_bytes().unwrap(),
        &Test2::A.as_ord_bytes().unwrap(),
    ) {
        (left_val, right_val) => {
            if !(*left_val == *right_val) {
                let kind = ::core::panicking::AssertKind::Eq;
                ::core::panicking::assert_failed(
                    kind,
                    &*left_val,
                    &*right_val,
                    ::core::option::Option::None,
                );
            }
        }
    };
    match (&Test1::B.as_ord_bytes().unwrap().as_ref(), &&[2]) {
        (left_val, right_val) => {
            if !(*left_val == *right_val) {
                let kind = ::core::panicking::AssertKind::Eq;
                ::core::panicking::assert_failed(
                    kind,
                    &*left_val,
                    &*right_val,
                    ::core::option::Option::None,
                );
            }
        }
    };
}
extern crate test;
#[cfg(test)]
#[rustc_test_marker = "enum_u64"]
pub const enum_u64: test::TestDescAndFn = test::TestDescAndFn {
    desc: test::TestDesc {
        name: test::StaticTestName("enum_u64"),
        ignore: false,
        ignore_message: ::core::option::Option::None,
        source_file: "crates/bonsaidb-macros/tests/key.rs",
        start_line: 117usize,
        start_col: 4usize,
        end_line: 117usize,
        end_col: 12usize,
        compile_fail: false,
        no_run: false,
        should_panic: test::ShouldPanic::No,
        test_type: test::TestType::IntegrationTest,
    },
    testfn: test::StaticTestFn(|| test::assert_test_result(enum_u64())),
};
fn enum_u64() {
    #[repr(u64)]
    enum Test {
        A = 0,
        B = u64::MAX - 1,
        C,
    }
    #[automatically_derived]
    impl ::core::clone::Clone for Test {
        #[inline]
        fn clone(&self) -> Test {
            match self {
                Test::A => Test::A,
                Test::B => Test::B,
                Test::C => Test::C,
            }
        }
    }
    #[automatically_derived]
    impl ::core::fmt::Debug for Test {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::write_str(
                f,
                match self {
                    Test::A => "A",
                    Test::B => "B",
                    Test::C => "C",
                },
            )
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::Key<'__bonsaidb_macros_key> for Test {
        const CAN_OWN_BYTES: bool = false;
        type Owned = Test;
        fn into_owned(self) -> Self::Owned {
            match self {
                Self::A => Test::A,
                Self::B => Test::B,
                Self::C => Test::C,
            }
        }
        fn from_ord_bytes<'__bonsaidb_macros_b>(
            mut __bonsaidb_macros_bytes: ::bonsaidb::core::key::ByteSource<
                '__bonsaidb_macros_key,
                '__bonsaidb_macros_b,
            >,
        ) -> ::core::prelude::v1::Result<Self, Self::Error> {
            const __bonsaidb_macros_discriminant0: u64 = 0;
            const __bonsaidb_macros_discriminant1: u64 = u64::MAX - 1;
            const __bonsaidb_macros_discriminant2: u64 = __bonsaidb_macros_discriminant1 + 1;
            ::core::prelude::v1::Ok(
                match <u64>::from_ord_bytes(__bonsaidb_macros_bytes)
                    .map_err(::bonsaidb::core::key::CompositeKeyError::new)?
                {
                    __bonsaidb_macros_discriminant0 => Self::A,
                    __bonsaidb_macros_discriminant1 => Self::B,
                    __bonsaidb_macros_discriminant2 => Self::C,
                    _ => {
                        return ::core::prelude::v1::Err(
                            ::bonsaidb::core::key::CompositeKeyError::from(::std::io::Error::from(
                                ::std::io::ErrorKind::InvalidData,
                            )),
                        )
                    }
                },
            )
        }
    }
    impl<'__bonsaidb_macros_key> ::bonsaidb::core::key::KeyEncoding<'__bonsaidb_macros_key, Self>
        for Test
    {
        type Error = ::bonsaidb::core::key::CompositeKeyError;
        const LENGTH: ::core::prelude::v1::Option<usize> =
            <u64 as ::bonsaidb::core::key::KeyEncoding<'__bonsaidb_macros_key>>::LENGTH;
        fn describe<Visitor>(visitor: &mut Visitor)
        where
            Visitor: ::bonsaidb::core::key::KeyVisitor,
        {
            <u64>::describe(visitor);
        }
        fn as_ord_bytes(
            &'__bonsaidb_macros_key self,
        ) -> ::core::prelude::v1::Result<
            ::std::borrow::Cow<'__bonsaidb_macros_key, [u8]>,
            Self::Error,
        > {
            const __bonsaidb_macros_discriminant0: u64 = 0;
            const __bonsaidb_macros_discriminant1: u64 = u64::MAX - 1;
            const __bonsaidb_macros_discriminant2: u64 = __bonsaidb_macros_discriminant1 + 1;
            match self {
                Self::A => __bonsaidb_macros_discriminant0.as_ord_bytes(),
                Self::B => __bonsaidb_macros_discriminant1.as_ord_bytes(),
                Self::C => __bonsaidb_macros_discriminant2.as_ord_bytes(),
            }
            .map_err(::bonsaidb::core::key::CompositeKeyError::new)
        }
    }
    match (
        &Test::C.as_ord_bytes().unwrap().as_ref(),
        &&[255, 255, 255, 255, 255, 255, 255, 255],
    ) {
        (left_val, right_val) => {
            if !(*left_val == *right_val) {
                let kind = ::core::panicking::AssertKind::Eq;
                ::core::panicking::assert_failed(
                    kind,
                    &*left_val,
                    &*right_val,
                    ::core::option::Option::None,
                );
            }
        }
    };
}
extern crate test;
#[cfg(test)]
#[rustc_test_marker = "lifetime"]
pub const lifetime: test::TestDescAndFn = test::TestDescAndFn {
    desc: test::TestDesc {
        name: test::StaticTestName("lifetime"),
        ignore: false,
        ignore_message: ::core::option::Option::None,
        source_file: "crates/bonsaidb-macros/tests/key.rs",
        start_line: 132usize,
        start_col: 4usize,
        end_line: 132usize,
        end_col: 12usize,
        compile_fail: false,
        no_run: false,
        should_panic: test::ShouldPanic::No,
        test_type: test::TestType::IntegrationTest,
    },
    testfn: test::StaticTestFn(|| test::assert_test_result(lifetime())),
};
fn lifetime() {
    struct Test<'a, 'b>(Cow<'a, str>, Cow<'b, str>);
    #[automatically_derived]
    impl<'a, 'b> ::core::clone::Clone for Test<'a, 'b> {
        #[inline]
        fn clone(&self) -> Test<'a, 'b> {
            Test(
                ::core::clone::Clone::clone(&self.0),
                ::core::clone::Clone::clone(&self.1),
            )
        }
    }
    #[automatically_derived]
    impl<'a, 'b> ::core::fmt::Debug for Test<'a, 'b> {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_tuple_field2_finish(f, "Test", &self.0, &&self.1)
        }
    }
    impl<'a, 'b, '__bonsaidb_macros_key> ::bonsaidb::core::key::Key<'__bonsaidb_macros_key>
        for Test<'a, 'b>
    where
        '__bonsaidb_macros_key: 'a,
        '__bonsaidb_macros_key: 'b,
    {
        const CAN_OWN_BYTES: bool = false;
        type Owned = Test<'static, 'static>;
        fn into_owned(self) -> Self::Owned {
            Test(self.0.into_owned(), self.1.into_owned())
        }
        fn from_ord_bytes<'__bonsaidb_macros_b>(
            mut __bonsaidb_macros_bytes: ::bonsaidb::core::key::ByteSource<
                '__bonsaidb_macros_key,
                '__bonsaidb_macros_b,
            >,
        ) -> ::core::prelude::v1::Result<Self, Self::Error> {
            let mut __bonsaidb_macros_decoder =
                ::bonsaidb::core::key::CompositeKeyDecoder::default_for(__bonsaidb_macros_bytes);
            let __bonsaidb_macros_self_ = Self(
                __bonsaidb_macros_decoder.decode()?,
                __bonsaidb_macros_decoder.decode()?,
            );
            __bonsaidb_macros_decoder.finish()?;
            ::core::prelude::v1::Ok(__bonsaidb_macros_self_)
        }
    }
    impl<'a, 'b, '__bonsaidb_macros_key>
        ::bonsaidb::core::key::KeyEncoding<'__bonsaidb_macros_key, Self> for Test<'a, 'b>
    where
        '__bonsaidb_macros_key: 'a,
        '__bonsaidb_macros_key: 'b,
    {
        type Error = ::bonsaidb::core::key::CompositeKeyError;
        const LENGTH: ::core::prelude::v1::Option<usize> = ::core::prelude::v1::None;
        fn describe<Visitor>(visitor: &mut Visitor)
        where
            Visitor: ::bonsaidb::core::key::KeyVisitor,
        {
            visitor.visit_composite(
                ::bonsaidb::core::key::CompositeKind::Struct(std::borrow::Cow::Borrowed(
                    std::any::type_name::<Self>(),
                )),
                2usize,
            );
            <Cow<'a, str>>::describe(visitor);
            <Cow<'b, str>>::describe(visitor);
        }
        fn as_ord_bytes(
            &'__bonsaidb_macros_key self,
        ) -> ::core::prelude::v1::Result<
            ::std::borrow::Cow<'__bonsaidb_macros_key, [u8]>,
            Self::Error,
        > {
            let mut __bonsaidb_macros_encoder =
                ::bonsaidb::core::key::CompositeKeyEncoder::default();
            __bonsaidb_macros_encoder.encode(&self.0)?;
            __bonsaidb_macros_encoder.encode(&self.1)?;
            ::core::prelude::v1::Ok(::std::borrow::Cow::Owned(
                __bonsaidb_macros_encoder.finish(),
            ))
        }
    }
    match (
        &&[97, 0, 98, 0, 1, 1],
        &Test("a".into(), "b".into())
            .as_ord_bytes()
            .unwrap()
            .as_ref(),
    ) {
        (left_val, right_val) => {
            if !(*left_val == *right_val) {
                let kind = ::core::panicking::AssertKind::Eq;
                ::core::panicking::assert_failed(
                    kind,
                    &*left_val,
                    &*right_val,
                    ::core::option::Option::None,
                );
            }
        }
    }
}
#[rustc_main]
pub fn main() -> () {
    extern crate test;
    test::test_main_static(&[
        &enum_repr,
        &enum_u64,
        &lifetime,
        &r#enum,
        &struct_struct,
        &transparent_structs,
        &tuple_struct,
        &unit_struct,
    ])
}
