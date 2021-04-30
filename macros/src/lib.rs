//! Macros `PliantDb`.

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
#![cfg_attr(doc, deny(rustdoc))]
#![allow(
    clippy::missing_errors_doc, // TODO clippy::missing_errors_doc
    clippy::option_if_let_else,
)]

use proc_macro::TokenStream;
use proc_macro_error::{abort, abort_call_site, proc_macro_error};
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput};

/// Derives the `pliantdb_core::permissions::Action` trait.
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
                        fields.push(
                            quote! { Self::#ident => ActionName::from(vec![#ident_as_string]) },
                        );
                    }
                    1 => {
                        fields.push(quote! {
                            Self::#ident(subaction) => {
                                let mut name = Action::name(subaction);
                                name.0.insert(0, #ident_as_string);
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
