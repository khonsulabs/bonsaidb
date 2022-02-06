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
use proc_macro2::{Span, TokenStream};
use proc_macro_crate::{crate_name, FoundCrate};
use proc_macro_error::{proc_macro_error, ResultExt};
use quote::quote;
use syn::{
    parse_macro_input, parse_quote, punctuated::Punctuated, token::Paren, DeriveInput, Ident,
    LitStr, Path, Type, TypeTuple,
};

fn core_path() -> Path {
    match crate_name("bonsaidb")
        .or_else(|_| crate_name("bonsaidb_server"))
        .or_else(|_| crate_name("bonsaidb_local"))
        .or_else(|_| crate_name("bonsaidb_client"))
    {
        Ok(FoundCrate::Name(name)) => {
            let ident = Ident::new(&name, Span::call_site());
            parse_quote!(::#ident::core)
        }
        Ok(FoundCrate::Itself) => parse_quote!(crate),
        Err(_) => match crate_name("bonsaidb_core") {
            Ok(FoundCrate::Name(name)) => {
                let ident = Ident::new(&name, Span::call_site());
                parse_quote!(::#ident::core)
            }
            Ok(FoundCrate::Itself) => parse_quote!(crate),
            Err(_) => match () {
                () if cfg!(feature = "omnibus-path") => parse_quote!(::bonsaidb::core),
                () if cfg!(feature = "server-path") => parse_quote!(::bonsaidb_server::core),
                () if cfg!(feature = "local-path") => parse_quote!(::bonsaidb_local::core),
                () if cfg!(feature = "client-path") => parse_quote!(::bonsaidb_client::core),
                _ => parse_quote!(::bonsaidb_core),
            },
        },
    }
}

#[derive(Attribute)]
#[attribute(ident = "collection")]
#[attribute(
    invalid_field = r#"Only `authority = "some-authority"`, `name = "some-name"`, `views = [SomeView, AnotherView]`, `serialization = Serialization` are supported attributes"#
)]
struct CollectionAttribute {
    authority: Option<String>,
    #[attribute(
        missing = r#"You need to specify the collection name via `#[collection(name = "name")]`"#
    )]
    name: String,
    #[attribute(default)]
    #[attribute(expected = r#"Specify the `views` like so: `view = [SomeView, AnotherView]`"#)]
    views: Vec<Type>,
    #[attribute(
        expected = r#"Specify the `serialization` like so: `serialization = Format` or `serialization = None` to disable deriving it"#
    )]
    serialization: Option<Path>,
    #[attribute(expected = r#"Specify the the path to `core` like so: `core = bosaidb::core`"#)]
    core: Option<Path>,
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
        core,
    } = CollectionAttribute::from_attributes(attrs).unwrap_or_abort();

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let core = core.unwrap_or_else(core_path);

    let serialization = match serialization {
        Some(serialization) if serialization.is_ident("None") => TokenStream::new(),
        Some(serialization) => quote! {
            impl #impl_generics #core::schema::SerializedCollection for #ident #ty_generics #where_clause {
                type Contents = #ident #ty_generics;
                type Format = #serialization;

                fn format() -> Self::Format {
                    #serialization::default()
                }
            }
        },
        None => quote! {
            impl #impl_generics #core::schema::DefaultSerialization for #ident #ty_generics #where_clause {}
        },
    };

    let name = authority.map_or_else(
        || quote!(#core::schema::CollectionName::private(#name)),
        |authority| quote!(#core::schema::CollectionName::new(#authority, #name)),
    );

    quote! {
        impl #impl_generics #core::schema::Collection for #ident #ty_generics #where_clause {
            fn collection_name() -> #core::schema::CollectionName {
                #name
            }
            fn define_views(schema: &mut #core::schema::Schematic) -> ::core::result::Result<(), #core::Error>{
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
    #[attribute(
        expected = r#"Specify the collection type like so: `collection = CollectionType`"#
    )]
    collection: Type,
    #[attribute(missing = r#"You need to specify the key type via `#[view(key = KeyType)]`"#)]
    #[attribute(expected = r#"Specify the key type like so: `key = KeyType`"#)]
    key: Type,
    name: Option<LitStr>,
    #[attribute(expected = r#"Specify the value type like so: `value = ValueType`"#)]
    value: Option<Type>,
    #[attribute(expected = r#"Specify the the path to `core` like so: `core = bosaidb::core`"#)]
    core: Option<Path>,
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
        core,
    } = ViewAttribute::from_attributes(attrs).unwrap_or_abort();

    let core = core.unwrap_or_else(core_path);

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
        impl #impl_generics #core::schema::View for #ident #ty_generics #where_clause {
            type Collection = #collection;
            type Key = #key;
            type Value = #value;

            fn name(&self) -> #core::schema::Name {
                #core::schema::Name::new(#name)
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
