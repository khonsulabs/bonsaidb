//! Macros `BonsaiDb`.

#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    missing_docs,
    // clippy::missing_docs_in_private_items,
    clippy::nursery,
    clippy::pedantic,
    future_incompatible,
    rust_2018_idioms,
)]
#![cfg_attr(doc, deny(rustdoc::all))]

use attribute_derive::Attribute;
use proc_macro2::TokenStream;
use proc_macro_error::{proc_macro_error, ResultExt};
use quote::quote;
use syn::{
    parse_macro_input, punctuated::Punctuated, token::Paren, DeriveInput, LitStr, Path, Type,
    TypeTuple,
};

#[derive(Attribute)]
#[attribute(ident = "collection")]
#[attribute(
    invalid_field = r#"Only `authority = "some-authority"`, `name = "some-name"`, `views = [SomeView, AnotherView]`, `serialization = Serialization` are supported attributes"#
)]
struct CollectionAttribute {
    #[attribute(
        missing = r#"You need to specify the collection authority via `#[collection(authority = "authority")]`"#
    )]
    authority: String,
    #[attribute(
        missing = r#"You need to specify the collection name via `#[collection(name = "name")]`"#
    )]
    name: String,
    #[attribute(default)]
    #[attribute(expected = r#"Specify the `views` like so: `view = [SomeView, AnotherView]`"#)]
    views: Vec<Type>,
    #[attribute(expected = r#"Specify the `serialization` like so: `serialization = Format` or `serialization = None` to disable deriving it"#)]
    serialization: Option<Path>,
}

/// Derives the `bonsaidb::core::schema::Collection` trait.
#[proc_macro_error]
/// `#[collection(authority = "Authority", name = "Name", views = [a, b, c])]`
#[proc_macro_derive(Collection, attributes(collection))]
pub fn collection_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let DeriveInput {
        attrs,
        ident,
        generics,
        ..
    } = parse_macro_input!(input as DeriveInput);

    let CollectionAttribute {
        authority,
        name,
        views,
        serialization,
    } = CollectionAttribute::from_attributes(attrs).unwrap_or_abort();

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let serialization = match serialization {
        Some(serialization) if serialization.is_ident("None") => TokenStream::new(),
        Some(serialization) => quote! {
            impl #impl_generics ::bonsaidb::core::schema::SerializedCollection for #ident #ty_generics #where_clause {
                type Contents = #ident #ty_generics;
                type Format = #serialization;

                fn format() -> Self::Format {
                    #serialization::default()
                }
            }
        },
        None => quote! {
            impl #impl_generics ::bonsaidb::core::schema::DefaultSerialization for #ident #ty_generics #where_clause {}
        },
    };

    quote! {
        impl #impl_generics ::bonsaidb::core::schema::Collection for #ident #ty_generics #where_clause {
            fn collection_name() -> ::bonsaidb::core::schema::CollectionName {
                ::bonsaidb::core::schema::CollectionName::new(#authority, #name)
            }
            fn define_views(schema: &mut ::bonsaidb::core::schema::Schematic) -> ::core::result::Result<(), ::bonsaidb::core::Error>{
                #( schema.define_view(#views)?; )*
                ::core::result::Result::Ok(())
            }
        }
        #serialization
    }
    .into()
}

#[derive(Attribute)]
#[attribute(ident = "view")]
#[attribute(
    invalid_field = r#"Only `collection = CollectionType`, `key = KeyType`, `name = "by-name"`, `value = ValueType` are supported attributes"#
)]
struct ViewAttribute {
    #[attribute(
        missing = r#"You need to specify the collection type via `#[view(collection = CollectionType)]`"#
    )]
    #[attribute(expected = r#"Specify the collection type like so: `collection = CollectionType`"#)]
    collection: Type,
    #[attribute(missing = r#"You need to specify the key type via `#[view(key = KeyType)]`"#)]
    #[attribute(expected = r#"Specify the key type like so: `key = KeyType`"#)]
    key: Type,
    name: Option<LitStr>,
    #[attribute(expected = r#"Specify the value type like so: `value = ValueType`"#)]
    value: Option<Type>,
}

/// Derives the `bonsaidb::core::schema::View` trait.
#[proc_macro_error]
/// `#[view(collection=CollectionType, key=KeyType, value=ValueType, name = "by-name")]`
/// `name` and `value` are optional
#[proc_macro_derive(View, attributes(view))]
pub fn view_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let DeriveInput {
        attrs,
        ident,
        generics,
        ..
    } = parse_macro_input!(input as DeriveInput);

    let ViewAttribute {
        collection,
        key,
        name,
        value,
    } = ViewAttribute::from_attributes(attrs).unwrap_or_abort();

    let value = value.unwrap_or_else(|| {
        Type::Tuple(TypeTuple {
            paren_token: Paren::default(),
            elems: Punctuated::new(),
        })
    });
    let name = name
        .as_ref()
        .map_or_else(|| ident.to_string(), LitStr::value);

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    quote! {
        impl #impl_generics ::bonsaidb::core::schema::View for #ident #ty_generics #where_clause {
            type Collection = #collection;
            type Key = #key;
            type Value = #value;

            fn name(&self) -> ::bonsaidb::core::schema::Name {
                ::bonsaidb::core::schema::Name::new(#name)
            }
        }
    }
    .into()
}

#[test]
fn ui() {
    use trybuild::TestCases;

    TestCases::new().compile_fail("tests/ui/*/*.rs");
}
