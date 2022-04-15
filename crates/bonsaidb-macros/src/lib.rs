//! Macros BonsaiDb.

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
use proc_macro_error::{abort, abort_call_site, proc_macro_error, ResultExt};
use quote::ToTokens;
use quote_use::{format_ident_namespaced as format_ident, quote_use as quote};
use syn::{
    parse_macro_input, parse_quote, punctuated::Punctuated, token::Paren, Data, DataEnum,
    DataStruct, DeriveInput, Expr, Field, Fields, FieldsNamed, FieldsUnnamed, Ident, Index, LitStr,
    Path, Token, Type, TypePath, TypeTuple, Variant,
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
        Ok(FoundCrate::Itself) => parse_quote!(crate::core),
        Err(_) => match crate_name("bonsaidb_core") {
            Ok(FoundCrate::Name(name)) => {
                let ident = Ident::new(&name, Span::call_site());
                parse_quote!(::#ident)
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
    invalid_field = r#"Only `authority = "some-authority"`, `name = "some-name"`, `views = [SomeView, AnotherView]`, `primary_key = u64`, `natural_id = |contents: &Self| Some(contents.id)`, serialization = SerializationFormat` and `core = bonsaidb::core` are supported attributes"#
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
    // TODO add some explanaition when it is possble to integrate the parse error in the printed
    // error message, for now the default error is probably more helpful
    encryption_key: Option<Expr>,
    encryption_required: bool,
    encryption_optional: bool,
    #[attribute(expected = r#"Specify the `primary_key` like so: `primary_key = u64`"#)]
    primary_key: Option<Type>,
    #[attribute(
        expected = r#"Specify the `natural_id` like so: `natural_id = function_name` or `natural_id = |doc| { .. }`"#
    )]
    natural_id: Option<Expr>,
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
        primary_key,
        natural_id,
        core,
        encryption_key,
        encryption_required,
        encryption_optional,
    } = CollectionAttribute::from_attributes(attrs).unwrap_or_abort();

    if encryption_required && encryption_key.is_none() {
        abort_call_site!("If `collection(encryption_required)` is set you need to provide an encryption key via `collection(encryption_key = EncryptionKey)`")
    }

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let core = core.unwrap_or_else(core_path);

    let primary_key = primary_key.unwrap_or_else(|| parse_quote!(u64));

    let serialization = match serialization {
        Some(serialization) if serialization.is_ident("None") => {
            if let Some(natural_id) = natural_id {
                abort!(
                    natural_id,
                    "`natural_id` must be manually implemented when using `serialization = None`"
                );
            }

            TokenStream::new()
        }
        Some(serialization) => {
            let natural_id = natural_id.map(|natural_id| {
                quote!(
                    fn natural_id(contents: &Self::Contents) -> Option<Self::PrimaryKey> {
                        #natural_id(contents)
                    }
                )
            });
            quote! {
                impl #impl_generics #core::schema::SerializedCollection for #ident #ty_generics #where_clause {
                    type Contents = #ident #ty_generics;
                    type Format = #serialization;

                    fn format() -> Self::Format {
                        #serialization::default()
                    }

                    #natural_id
                }
            }
        }
        None => {
            let natural_id = natural_id.map(|natural_id| {
                quote!(
                    fn natural_id(&self) -> Option<Self::PrimaryKey> {
                        (#natural_id)(self)
                    }
                )
            });
            quote! {
                impl #impl_generics #core::schema::DefaultSerialization for #ident #ty_generics #where_clause {
                    #natural_id
                }
            }
        }
    };

    let name = authority.map_or_else(
        || quote!(<#core::schema::CollectionName as #core::schema::Qualified>::private(#name)),
        |authority| quote!(<#core::schema::CollectionName as #core::schema::Qualified>::new(#authority, #name)),
    );

    let encryption = encryption_key.map(|encryption_key| {
        let encryption = if encryption_required || !encryption_optional {
            encryption_key.into_token_stream()
        } else {
            quote! {
                if #core::ENCRYPTION_ENABLED {
                    #encryption_key
                } else {
                    None
                }
            }
        };
        quote! {
            fn encryption_key() -> Option<#core::document::KeyId> {
                #encryption
            }
        }
    });

    quote! {
        impl #impl_generics #core::schema::Collection for #ident #ty_generics #where_clause {
            type PrimaryKey = #primary_key;

            fn collection_name() -> #core::schema::CollectionName {
                #name
            }
            fn define_views(schema: &mut #core::schema::Schematic) -> Result<(), #core::Error> {
                #( schema.define_view(#views)?; )*
                Ok(())
            }
            #encryption
        }
        #serialization
    }
    .into()
}

#[derive(Attribute)]
#[attribute(ident = "view")]
#[attribute(
    invalid_field = r#"Only `collection = CollectionType`, `key = KeyType`, `name = "by-name"`, `value = ValueType` and `serialization = SerializationFormat` and `core = bonsaidb::core` are supported attributes"#
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
    #[attribute(
        expected = r#"Specify the `serialization` like so: `serialization = Format` or `serialization = None` to disable deriving it"#
    )]
    serialization: Option<Path>,
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
        serialization,
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
    .into()
}

#[derive(Attribute)]
#[attribute(ident = "schema")]
#[attribute(
    invalid_field = r#"Only `name = "name"`, `authority = "authority"`, `collections = [SomeCollection, AnotherCollection]`, `include = [OtherSchema]`, and `core = bonsaidb::core` are supported attributes"#
)]
struct SchemaAttribute {
    #[attribute(missing = r#"You need to specify the schema name via `#[schema(name = "name")]`"#)]
    name: String,
    authority: Option<String>,
    #[attribute(default)]
    #[attribute(
        expected = r#"Specify the `collections` like so: `collections = [SomeCollection, AnotherCollection]`"#
    )]
    collections: Vec<Type>,
    #[attribute(default)]
    #[attribute(
        expected = r#"Specify other Schemas as plugins like so: `include = [SomeSchema, AnotherSchema]`"#
    )]
    include: Vec<Type>,
    #[attribute(expected = r#"Specify the the path to `core` like so: `core = bosaidb::core`"#)]
    core: Option<Path>,
}

/// Derives the `bonsaidb::core::schema::Schema` trait.
#[proc_macro_error]
/// `#[schema(name = "Name", authority = "Authority", collections = [A, B, C]), core = bonsaidb::core]`
/// `authority`, `collections` and `core` are optional
#[proc_macro_derive(Schema, attributes(schema))]
pub fn schema_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let DeriveInput {
        attrs,
        ident,
        generics,
        ..
    } = parse_macro_input!(input as DeriveInput);

    let SchemaAttribute {
        name,
        authority,
        collections,
        include,
        core,
    } = SchemaAttribute::from_attributes(attrs).unwrap_or_abort();

    let core = core.unwrap_or_else(core_path);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let name = authority.map_or_else(
        || quote!(<#core::schema::SchemaName as #core::schema::Qualified>::private(#name)),
        |authority| quote!(<#core::schema::SchemaName as #core::schema::Qualified>::new(#authority, #name)),
    );

    // For some reason, quote! does not like #include.
    let plugins = include;
    quote! {
        impl #impl_generics #core::schema::Schema for #ident #ty_generics #where_clause {
            fn schema_name() -> #core::schema::SchemaName {
                #name
            }

            fn define_collections(
                schema: &mut #core::schema::Schematic
            ) -> Result<(), #core::Error> {
                #( schema.define_collection::<#collections>()?; )*

                #( <#plugins as #core::schema::Schema>::define_collections(schema)?; )*

                Ok(())
            }
        }
    }
    .into()
}

#[derive(Attribute)]
#[attribute(ident = "key")]
#[attribute(
    invalid_field = r#"Only `allow_null_bytes`, `enum_repr = NumberType` and `core = bonsaidb::core` is supported"#
)]
struct KeyAttribute {
    #[attribute(expected = r#"Specify the the path to `core` like so: `core = bosaidb::core`"#)]
    core: Option<Path>,
    allow_null_bytes: bool,
    enum_repr: Option<Type>,
}

/// Derives the `bonsaidb::core::key::Key` trait.
///
/// `#[key(allow_null_bytes, enum_repr = u8, core = bonsaidb::core)]`, all parameters are optional
#[proc_macro_error]
#[proc_macro_derive(Key, attributes(key))]
pub fn key_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let DeriveInput {
        attrs,
        ident,
        generics,
        data,
        ..
    } = parse_macro_input!(input as DeriveInput);

    // Only relevant if it is an enum, get's the representation to use for the variant key
    let repr = attrs.iter().find_map(|attr| {
        attr.path
            .is_ident("repr")
            .then(|| attr.parse_args::<Ident>().ok())
            .flatten()
            .and_then(|ident| {
                matches!(
                    ident.to_string().as_ref(),
                    "u8" | "u16"
                        | "u32"
                        | "u64"
                        | "u128"
                        | "usize"
                        | "i8"
                        | "i16"
                        | "i32"
                        | "i64"
                        | "i128"
                        | "isize"
                )
                .then(|| ident)
            })
    });

    let KeyAttribute {
        core,
        allow_null_bytes,
        enum_repr,
    } = KeyAttribute::from_attributes(attrs).unwrap_or_abort();

    if matches!(data, Data::Struct(_)) && enum_repr.is_some() {
        // TODO better span when attribute-derive supports that
        abort!(enum_repr, "`enum_repr` is only usable with enums")
    }

    let repr: Type = enum_repr.unwrap_or_else(|| {
        Type::Path(TypePath {
            qself: None,
            path: repr.unwrap_or_else(|| format_ident!("isize")).into(),
        })
    });

    let allow_null_bytes = if allow_null_bytes {
        quote!($encoder.allow_null_bytes_in_variable_fields();)
    } else {
        quote!()
    };

    let core = core.unwrap_or_else(core_path);
    let (_, ty_generics, _) = generics.split_for_impl();
    let mut generics = generics.clone();
    generics
        .params
        .push(syn::GenericParam::Lifetime(parse_quote!('key)));
    let (impl_generics, _, where_clause) = generics.split_for_impl();

    let (encode_fields, decode_fields): (TokenStream, TokenStream) = match data {
        syn::Data::Struct(DataStruct { fields, .. }) => {
            let (encode_fields, decode_fields) = match fields {
                Fields::Named(FieldsNamed { named, .. }) => {
                    let (encode_fields, decode_fields): (TokenStream, TokenStream) = named
                        .into_iter()
                        .map(|Field { ident, .. }| {
                            let ident = ident.expect("named fields have idents");
                            (
                                quote!($encoder.encode(&self.#ident)?;),
                                quote!(#ident: $decoder.decode()?,),
                            )
                        })
                        .unzip();
                    (encode_fields, quote!( Self { #decode_fields }))
                }
                Fields::Unnamed(FieldsUnnamed { unnamed, .. }) => {
                    let (encode_fields, decode_fields): (TokenStream, TokenStream) = unnamed
                        .into_iter()
                        .enumerate()
                        .map(|(idx, _)| {
                            let idx = Index::from(idx);
                            (
                                quote!($encoder.encode(&self.#idx)?;),
                                quote!($decoder.decode()?,),
                            )
                        })
                        .unzip();
                    (encode_fields, quote!(Self(#decode_fields)))
                }
                Fields::Unit => abort_call_site!("unit structs are not supported"),
            };
            (encode_fields, quote!(let $self_ = #decode_fields;))
        }
        syn::Data::Enum(DataEnum { variants, .. }) => {
            let mut prev_ident = None;
            let (consts, (encode_variants, decode_variants)): (
                TokenStream,
                (TokenStream, TokenStream),
            ) = variants
                .into_iter()
                .enumerate()
                .map(
                    |(
                        idx,
                        Variant {
                            fields,
                            ident,
                            discriminant,
                            ..
                        },
                    )| {
                        let discriminant = discriminant.map_or_else(
                            || {
                                prev_ident
                                    .as_ref()
                                    .map_or_else(|| quote!(0), |ident| quote!(#ident + 1))
                            },
                            |(_, expr)| expr.to_token_stream(),
                        );

                        let const_ident = format_ident!("$discriminant{idx}");
                        let const_ = quote!(const #const_ident: #repr = #discriminant;);

                        let ret = (
                            const_,
                            match fields {
                                Fields::Named(FieldsNamed { named, .. }) => {
                                    let (idents, (encode_fields, decode_fields)): (
                                        Punctuated<_, Token![,]>,
                                        (TokenStream, TokenStream),
                                    ) = named
                                        .into_iter()
                                        .map(|Field { ident, .. }| {
                                            let ident = ident.expect("named fields have idents");
                                            (
                                                ident.clone(),
                                                (
                                                    quote!($encoder.encode(#ident)?;),
                                                    quote!(#ident: $decoder.decode()?,),
                                                ),
                                            )
                                        })
                                        .unzip();
                                    (
                                        quote! {
                                            Self::#ident{#idents} => {
                                                $encoder.encode(&#const_ident)?;
                                                #encode_fields
                                            },
                                        },
                                        quote! {
                                            #const_ident => Self::#ident{#decode_fields},
                                        },
                                    )
                                }
                                Fields::Unnamed(FieldsUnnamed { unnamed, .. }) => {
                                    let (idents, (encode_fields, decode_fields)): (
                                        Punctuated<_, Token![,]>,
                                        (TokenStream, TokenStream),
                                    ) = unnamed
                                        .into_iter()
                                        .enumerate()
                                        .map(|(idx, _)| {
                                            let ident = format_ident!("$field_{idx}");
                                            (
                                                ident.clone(),
                                                (
                                                    quote!($encoder.encode(#ident)?;),
                                                    quote!($decoder.decode()?,),
                                                ),
                                            )
                                        })
                                        .unzip();
                                    (
                                        quote! {
                                            Self::#ident(#idents) => {
                                                $encoder.encode(&#const_ident)?;
                                                #encode_fields
                                            },
                                        },
                                        quote! {
                                            #const_ident => Self::#ident(#decode_fields),
                                        },
                                    )
                                }
                                Fields::Unit => (
                                    quote!(Self::#ident => $encoder.encode(&#const_ident)?,),
                                    quote!(#const_ident => Self::#ident,),
                                ),
                            },
                        );
                        prev_ident = Some(const_ident);
                        ret
                    },
                )
                .unzip();
            (
                quote! {
                    #consts
                    match self{
                        #encode_variants
                    }
                },
                quote! {
                    use std::io::{self, ErrorKind};
                    #consts
                    let $self_ = match $decoder.decode::<#repr>()? {
                        #decode_variants
                        _ => return Err(#core::key::CompositeKeyError::from(io::Error::from(
                                ErrorKind::InvalidData,
                        )))
                    };
                },
            )
        }
        syn::Data::Union(_) => abort_call_site!("unions are not supported"),
    };

    quote! {
        use std::borrow::Cow;
        use std::io::{self, ErrorKind};

        impl #impl_generics #core::key::Key<'key> for #ident #ty_generics #where_clause {

            fn from_ord_bytes(mut $bytes: &'key [u8]) -> Result<Self, Self::Error> {

                let mut $decoder = #core::key::CompositeKeyDecoder::new($bytes);

                #decode_fields

                $decoder.finish()?;

                Ok($self_)
            }
        }

        impl #impl_generics #core::key::KeyEncoding<'key, Self> for #ident #ty_generics #where_clause {
            type Error = #core::key::CompositeKeyError;

            // TODO fixed width if possible
            const LENGTH: Option<usize> = None;

            fn as_ord_bytes(&'key self) -> Result<Cow<'key, [u8]>, Self::Error> {
                let mut $encoder = #core::key::CompositeKeyEncoder::default();

                #allow_null_bytes

                #encode_fields

                Ok(Cow::Owned($encoder.finish()))
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
