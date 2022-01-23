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
#![allow(clippy::option_if_let_else)]

use proc_macro::TokenStream;
use proc_macro_error::{abort, abort_call_site, proc_macro_error};
use quote::quote;
use syn::{
    parse_macro_input, spanned::Spanned, Data, DeriveInput, Lit, Meta, MetaList, MetaNameValue,
    NestedMeta, Path,
};

/// Derives the `bonsaidb_core::permissions::Action` trait.
#[proc_macro_error]
#[proc_macro_derive(Action)]
pub fn permissions_action_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let name = input.ident;

    let mut fields = Vec::new();
    match input.data {
        Data::Enum(data) => {
            for variant in data.variants.iter() {
                let ident = variant.ident.clone();
                let ident_as_string = ident.to_string();
                match variant.fields.len() {
                    0 => {
                        fields.push(quote! { Self::#ident => ActionName(vec![::std::borrow::Cow::Borrowed(#ident_as_string)]) });
                    }
                    1 => {
                        fields.push(quote! {
                            Self::#ident(subaction) => {
                                let mut name = Action::name(subaction);
                                name.0.insert(0, ::std::borrow::Cow::Borrowed(#ident_as_string));
                                name
                            }
                        });
                    }
                    _ => {
                        abort!(
                            variant.ident,
                            "For derive(Action), all enum variants may have at most 1 field"
                        )
                    }
                }
            }
        }
        _ => abort_call_site!("Action can only be derived for an enum."),
    }

    let expanded = quote! {
        impl Action for #name {
            fn name(&self) -> ActionName {
                match self {
                    #(
                        #fields
                    ),*
                }
            }
        }
    };

    TokenStream::from(expanded)
}

/// Derives the `bonsaidb::core::schema::Collection` trait.
#[proc_macro_error]
/// `#[collection(authority = "Authority", name = "Name", views(a, b, c))]`
#[proc_macro_derive(Collection, attributes(collection))]
pub fn collection_derive(input: TokenStream) -> TokenStream {
    let DeriveInput {
        attrs,
        ident,
        generics,
        ..
    } = parse_macro_input!(input as DeriveInput);

    let mut name: Option<String> = None;
    let mut authority: Option<String> = None;
    let mut view: Vec<Path> = Vec::new();

    for attibute in attrs {
        if attibute.path.is_ident("collection") {
            if let Ok(Meta::List(MetaList { nested, .. })) = attibute.parse_meta() {
                for item in nested {
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
                            if path.is_ident("views") =>
                        {
                            view = nested
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
            r#"You need to specify the collection name via `#[collection(name="name")]`"#
        )
    });

    quote! {
        impl ::bonsaidb::core::schema::Collection for #ident #generics {
            fn collection_name() -> ::core::result::Result<::bonsaidb::core::schema::CollectionName, ::bonsaidb::core::schema::InvalidNameError> {
                ::bonsaidb::core::schema::CollectionName::new(#authority, #name)
            }
            fn define_views(schema: &mut ::bonsaidb::core::schema::Schematic) -> ::core::result::Result<(), ::bonsaidb::core::Error>{
                #( schema.define_view(#view)?; )*
                ::core::result::Result::Ok(())
            }
        }
    }
    .into()
}

#[test]
fn ui() {
    use trybuild::TestCases;

    TestCases::new().compile_fail("tests/*/ui/*.rs");
}
