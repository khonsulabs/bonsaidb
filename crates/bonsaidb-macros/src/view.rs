use attribute_derive::Attribute;
use proc_macro2::TokenStream;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::token::Paren;
use syn::{DeriveInput, Ident, LitStr, Path, Type, TypeTuple};

use crate::{core_path, unwrap_or_abort};

#[derive(Attribute)]
#[attribute(ident = view)]
struct ViewAttribute {
    #[attribute(example = "CollectionType")]
    collection: Type,
    #[attribute(example = "KeyType")]
    key: Type,
    #[attribute(example = "\"by-name\"")]
    name: Option<LitStr>,
    #[attribute(example = "ValueType")]
    value: Option<Type>,
    #[attribute(example = "bosaidb::core")]
    core: Option<Path>,
    #[attribute(example = "Format or None")]
    serialization: Option<Path>,
}

pub fn derive(
    DeriveInput {
        attrs,
        ident,
        generics,
        ..
    }: DeriveInput,
) -> TokenStream {
    let ViewAttribute {
        collection,
        key,
        name,
        value,
        core,
        serialization,
    } = unwrap_or_abort!(ViewAttribute::from_attributes(&attrs));

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

    let serialization = match serialization {
        Some(serialization) if serialization.is_ident("None") => TokenStream::new(),
        Some(serialization) => quote! {
            impl #impl_generics #core::schema::SerializedView for #ident #ty_generics #where_clause {
                type Format = #serialization;

                fn format() -> Self::Format {
                    #serialization::default()
                }
            }
        },
        None => quote! {
            impl #impl_generics #core::schema::DefaultViewSerialization for #ident #ty_generics #where_clause {}
        },
    };

    quote! {
        impl #impl_generics #core::schema::View for #ident #ty_generics #where_clause {
            type Collection = #collection;
            type Key = #key;
            type Value = #value;

            fn name(&self) -> #core::schema::Name {
                #core::schema::Name::new(#name)
            }
        }
        #serialization
    }
}

#[derive(Attribute)]
#[attribute(ident = view_schema)]
struct ViewSchemaAttribute {
    #[attribute(example = "ViewType")]
    view: Option<Type>,
    #[attribute(example = "KeyType<'doc>")]
    mapped_key: Option<Type>,
    #[attribute(example = "\"by-name\"")]
    version: Option<u64>,
    #[attribute(example = "Lazy")]
    policy: Option<Ident>,
    #[attribute(example = "bosaidb::core")]
    core: Option<Path>,
}

pub fn derive_schema(
    DeriveInput {
        attrs,
        ident,
        generics,
        ..
    }: DeriveInput,
) -> TokenStream {
    let ViewSchemaAttribute {
        view,
        mapped_key,
        version,
        policy,
        core,
    } = unwrap_or_abort!(ViewSchemaAttribute::from_attributes(&attrs));

    let core = core.unwrap_or_else(core_path);

    let view = view.map_or_else(|| quote!(Self), |ty| quote!(#ty));

    let mapped_key = mapped_key.map_or_else(
        || quote!(<Self as #core::schema::View>::Key),
        |ty| quote!(#ty),
    );

    let version = version.map(|version| {
        quote!(fn version(&self) -> u64 {
            #version
        })
    });

    let policy = policy.map(|policy| {
        quote!(fn update_policy(&self) -> #core::schema::view::ViewUpdatePolicy {
            #core::schema::view::ViewUpdatePolicy::#policy
        })
    });

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    quote! {
        impl #impl_generics #core::schema::ViewSchema for #ident #ty_generics #where_clause {
            type View = #view;
            type MappedKey<'doc> = #mapped_key;

            #version
            #policy
        }
    }
}
