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

use proc_macro2::{Spacing, TokenStream};
use proc_macro_error::{abort, abort_call_site, proc_macro_error, ResultExt};
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input,
    punctuated::Punctuated,
    spanned::Spanned,
    token::Paren,
    DeriveInput, Error, Ident, Lit, LitStr, Meta, MetaList, MetaNameValue, NestedMeta, Path,
    Result, Type, TypeTuple,
};

/// Derives the `bonsaidb::core::schema::Collection` trait.
#[proc_macro_error]
/// `#[collection(authority = "Authority", name = "Name", views(a, b, c))]`
#[proc_macro_derive(Collection, attributes(collection))]
pub fn collection_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let DeriveInput {
        attrs,
        ident,
        generics,
        ..
    } = parse_macro_input!(input as DeriveInput);


    let mut name: Option<String> = None;
    let mut authority: Option<String> = None;
    let mut views: Vec<Path> = Vec::new();
    let mut serialization: Option<Path> = None;

    for attibute in attrs {
        if attibute.path.is_ident("collection") {
            if let Ok(Meta::List(MetaList { nested, .. })) = attibute.parse_meta() {
                for item in nested {
                    let span = item.span();
                    match item {
                        NestedMeta::Meta(Meta::NameValue(MetaNameValue {
                            path,
                            lit: Lit::Str(value),
                            ..
                        })) if path.is_ident("name") => name = Some(value.value()),
                        NestedMeta::Meta(Meta::NameValue(MetaNameValue {
                            path,
                            lit: Lit::Str(value),
                            ..
                        })) if path.is_ident("authority") => authority = Some(value.value()),
                        NestedMeta::Meta(Meta::List(MetaList { path, nested, .. }))
                            if path.is_ident("serialization") =>
                        {
                            match nested.len() {
                                0 => abort!(
                                    span,
                                    r#"You need to pass either a format type or `None` to `serialization`: `serialization(Format)`"#,
                                ),
                                2.. => abort!(
                                    span,
                                    r#"You can only specify a single format with `serialization` like so: `serialization(Format)`"#,
                                ),
                                _ => (),
                            }
                            serialization = nested
                                .into_iter()
                                .map(|meta| match meta {
                                    NestedMeta::Meta(Meta::Path(path)) => path,
                                    meta => abort!(
                            meta.span(),
                            r#"`{}` is not supported here, call `serialization` like so: `serialization(Format)`"#
                        ),
                                }).next();
                        }
                        NestedMeta::Meta(Meta::List(MetaList { path, nested, .. }))
                            if path.is_ident("views") =>
                        {
                            views = nested
                                .into_iter()
                                .map(|meta| match meta {
                                    NestedMeta::Meta(Meta::Path(path)) => path,
                                    meta => abort!(
                            meta.span(),
                            r#"`{}` is not supported here, call `views` like so: `views(SomeView, AnotherView)`"#
                        ),
                                })
                                .collect();
                        }
                        item => abort!(
                            item.span(),
                            r#"Only `authority="some-authority"`, `name="some-name"`, `views(SomeView, AnotherView)` are supported attributes"#
                        ),
                    }
                }
            }
        }
    }

    let authority = authority.unwrap_or_else(|| {
        abort_call_site!(
            r#"You need to specify the collection name via `#[collection(authority="authority")]`"#
        )
    });

    let name = name.unwrap_or_else(|| {
        abort_call_site!(
            r#"You need to specify the collection authority via `#[collection(name="name")]`"#
        )
    });

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

#[derive(Debug, Clone, Default)]
struct ViewAttribute {
    collection: Option<Type>,
    key: Option<Type>,
    name: Option<LitStr>,
    value: Option<Type>,
}

impl ViewAttribute {
    pub fn extend_with(&mut self, other: Self) -> Result<()> {
        match (&self.collection, other.collection) {
            (None, value @ Some(_)) => self.collection = value,
            (Some(first), Some(second)) => {
                let mut error =
                    Error::new_spanned(first, "`collection` type is specified multiple times");
                error.combine(Error::new_spanned(
                    second,
                    "`collection` type was already specified",
                ));
                return Err(error);
            }
            _ => (),
        }
        match (&self.key, other.key) {
            (None, value @ Some(_)) => self.key = value,
            (Some(first), Some(second)) => {
                let mut error = Error::new_spanned(first, "`key` type is specified multiple times");
                error.combine(Error::new_spanned(
                    second,
                    "`key` type was already specified",
                ));
                return Err(error);
            }
            _ => (),
        }
        match (&self.name, other.name) {
            (None, value @ Some(_)) => self.name = value,
            (Some(first), Some(second)) => {
                let mut error =
                    Error::new_spanned(first, "`name` type is specified multiple times");
                error.combine(Error::new_spanned(
                    second,
                    "`name` type was already specified",
                ));
                return Err(error);
            }
            _ => (),
        }
        Ok(())
    }
}

impl Parse for ViewAttribute {
    fn parse(input: ParseStream<'_>) -> Result<Self> {
        let mut view_attribute = Self::default();
        loop {
            let variable = Ident::parse(input)?;

            // Parse `=`
            input.step(|cursor| match cursor.punct() {
                Some((punct, rest))
                    if punct.as_char() == '=' && punct.spacing() == Spacing::Alone =>
                {
                    Ok(((), rest))
                }
                _ => Err(cursor.error("Expected assignment `=`")),
            })?;

            match variable.to_string().as_str() {
                "collection" => {
                    view_attribute.collection = Some(input.parse()?);
                }
                "key" => {
                    view_attribute.key = Some(input.parse()?);
                }
                "name" => {
                    view_attribute.name = Some(input.parse()?);
                }
                _ => {
                    return Err(Error::new(
                        variable.span(),
                        "Expected either `collection`, `key` or `name`",
                    ))
                }
            }

            if input.is_empty() {
                break;
            }

            // Parse `,`
            input.step(|cursor| match cursor.punct() {
                Some((punct, rest)) if punct.as_char() == ',' => Ok(((), rest)),
                _ => Err(cursor.error("Expected assignment `=`")),
            })?;
        }
        Ok(view_attribute)
    }
}

/// Derives the `bonsaidb::core::schema::View` trait.
#[proc_macro_error]
/// `#[view(collection(CollectionType), key(KeyType), value(ValueType), name = "by-name", version = 0)]`
#[proc_macro_derive(View, attributes(view))]
pub fn view_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let DeriveInput {
        attrs,
        ident,
        generics,
        ..
    } = parse_macro_input!(input as DeriveInput);

    let mut attributes = ViewAttribute::default();

    for attibute in attrs {
        if attibute.path.is_ident("view") {
            attributes
                .extend_with(attibute.parse_args().unwrap_or_abort())
                .unwrap_or_abort();
        }
    }

    let collection = attributes.collection.unwrap_or_else(|| {
        abort_call_site!(
            r#"You need to specify the collection type via `#[view(collection = CollectionType)]`"#
        )
    });
    let key = attributes.key.unwrap_or_else(|| {
        abort_call_site!(r#"You need to specify the key type via `#[view(key = KeyType)]`"#)
    });
    let value = attributes.value.unwrap_or_else(|| {
        Type::Tuple(TypeTuple {
            paren_token: Paren::default(),
            elems: Punctuated::new(),
        })
    });
    let name = attributes
        .name
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
