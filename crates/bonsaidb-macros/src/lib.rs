//! Macros BonsaiDb.

#![forbid(unsafe_code)]
#![warn(
    clippy::cargo,
    missing_docs,
    // clippy::missing_docs_in_private_items,
    clippy::pedantic,
    future_incompatible,
    rust_2018_idioms,
)]
#![allow(clippy::type_complexity)] // TODO: Refactor away from unzip()s
#![cfg_attr(doc, deny(rustdoc::all))]

use attribute_derive::{Attribute, ConvertParsed};
use proc_macro2::{Span, TokenStream};
use proc_macro_crate::{crate_name, FoundCrate};
use proc_macro_error::{
    abort, abort_call_site, proc_macro_error, Diagnostic, DiagnosticExt, Level, ResultExt,
    SpanRange,
};
use quote::ToTokens;
use quote_use::{
    format_ident_namespaced as format_ident, parse_quote_use as parse_quote, quote_use as quote,
};
use syn::punctuated::Punctuated;
use syn::token::Paren;
use syn::{
    parse_macro_input, Data, DataEnum, DataStruct, DeriveInput, Expr, Field, Fields, FieldsNamed,
    FieldsUnnamed, Ident, Index, LitStr, Path, Token, Type, TypePath, TypeTuple, Variant,
};

// -----------------------------------------------------------------------------
//     - Core Macros -
// -----------------------------------------------------------------------------

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

// TODO remove if proc_macro_error updates to synv2, or someone releases an updated fork
fn unwrap_or_abort<T>(result: Result<T, syn::Error>) -> T {
    result
        .map_err(|err| {
            // directly copied from https://docs.rs/proc-macro-error/latest/src/proc_macro_error/diagnostic.rs.html#291-349
            // licensed under MIT or Apache-2.0
            use proc_macro2::{Delimiter, TokenTree};

            fn gut_error(ts: &mut impl Iterator<Item = TokenTree>) -> Option<(SpanRange, String)> {
                let first = match ts.next() {
                    // compile_error
                    None => return None,
                    Some(tt) => tt.span(),
                };
                ts.next().unwrap(); // !

                let lit = match ts.next().unwrap() {
                    TokenTree::Group(group) => {
                        // Currently `syn` builds `compile_error!` invocations
                        // exclusively in `ident{"..."}` (braced) form which is not
                        // followed by `;` (semicolon).
                        //
                        // But if it changes to `ident("...");` (parenthesized)
                        // or `ident["..."];` (bracketed) form,
                        // we will need to skip the `;` as well.
                        // Highly unlikely, but better safe than sorry.

                        if group.delimiter() == Delimiter::Parenthesis
                            || group.delimiter() == Delimiter::Bracket
                        {
                            ts.next().unwrap(); // ;
                        }

                        match group.stream().into_iter().next().unwrap() {
                            TokenTree::Literal(lit) => lit,
                            _ => unreachable!(),
                        }
                    }
                    _ => unreachable!(),
                };

                let last = lit.span();
                let mut msg = lit.to_string();

                // "abc" => abc
                msg.pop();
                msg.remove(0);

                Some((SpanRange { first, last }, msg))
            }

            let mut ts = err.to_compile_error().into_iter();

            let (span_range, msg) = gut_error(&mut ts).unwrap();
            let mut res = Diagnostic::spanned_range(span_range, Level::Error, msg);

            while let Some((span_range, msg)) = gut_error(&mut ts) {
                res = res.span_range_error(span_range, msg);
            }

            res
        })
        .unwrap_or_abort()
}

#[derive(Attribute)]
#[attribute(ident = collection)]
struct CollectionAttribute {
    authority: Option<Expr>,
    #[attribute(example = "\"name\"")]
    name: String,
    #[attribute(optional, example = "[SomeView, AnotherView]")]
    views: Vec<Type>,
    #[attribute(example = "Format or None")]
    serialization: Option<Path>,
    #[attribute(example = "Some(KeyId::Master)")]
    encryption_key: Option<Expr>,
    encryption_required: bool,
    encryption_optional: bool,
    #[attribute(example = "u64")]
    primary_key: Option<Type>,
    #[attribute(example = "function_name or |doc| { .. }")]
    natural_id: Option<Expr>,
    #[attribute(example = "bosaidb::core")]
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
    } = unwrap_or_abort(CollectionAttribute::from_attributes(&attrs));

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
        || quote!(#core::schema::Qualified::private(#name)),
        |authority| quote!(#core::schema::Qualified::new(#authority, #name)),
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
#[attribute(ident = view)]
struct ViewAttribute {
    #[attribute(example = "CollectionType")]
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
    } = unwrap_or_abort(ViewAttribute::from_attributes(&attrs));

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
#[attribute(ident = schema)]
struct SchemaAttribute {
    #[attribute(example = "\"name\"")]
    name: String,
    #[attribute(example = "\"authority\"")]
    authority: Option<Expr>,
    #[attribute(optional, example = "[SomeCollection, AnotherCollection]")]
    collections: Vec<Type>,
    #[attribute(optional, example = "[SomeSchema, AnotherSchema]")]
    include: Vec<Type>,
    #[attribute(example = "bosaidb::core")]
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
    } = unwrap_or_abort(SchemaAttribute::from_attributes(&attrs));

    let core = core.unwrap_or_else(core_path);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let name = authority.map_or_else(
        || quote!(#core::schema::Qualified::private(#name)),
        |authority| quote!(#core::schema::Qualified::new(#authority, #name)),
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
#[attribute(ident = key)]
struct KeyAttribute {
    #[attribute(example = "bosaidb::core")]
    core: Option<Path>,
    #[attribute(default = NullHandling::Escape, example = "escape")]
    null_handling: NullHandling,
    can_own_bytes: bool,
    #[attribute(example = "u8")]
    enum_repr: Option<Type>,
    #[attribute(example = "\"name\"")]
    name: Option<String>,
}

enum NullHandling {
    Escape,
    Allow,
    Deny,
}

impl ConvertParsed for NullHandling {
    type Type = Ident;

    fn convert(value: Self::Type) -> syn::Result<Self> {
        if value == "escape" {
            Ok(NullHandling::Escape)
        } else if value == "allow" {
            Ok(NullHandling::Allow)
        } else if value == "deny" {
            Ok(NullHandling::Deny)
        } else {
            Err(syn::Error::new(
                Span::call_site(),
                "only `escape`, `allow`, and `deny` are allowed for `null_handling`",
            ))
        }
    }
}

/// Derives the `bonsaidb::core::key::Key` trait.
///
/// `#[key(null_handling = escape, enum_repr = u8, core = bonsaidb::core)]`, all parameters are optional
#[proc_macro_error]
#[proc_macro_derive(Key, attributes(key))]
pub fn key_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let DeriveInput {
        attrs,
        ident: derived_on,
        generics,
        data,
        ..
    } = parse_macro_input!(input as DeriveInput);

    // Only relevant if it is an enum, gets the representation to use for the variant key
    let repr = attrs.iter().find_map(|attr| {
        attr.path()
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
        null_handling,
        enum_repr,
        can_own_bytes,
        name,
    } = unwrap_or_abort(KeyAttribute::from_attributes(&attrs));

    let name = name.map_or_else(
        || quote!(std::any::type_name::<Self>()),
        |name| quote!(#name),
    );

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

    let (encoder_constructor, decoder_constructor) = match null_handling {
        NullHandling::Escape => (quote!(default), quote!(default_for)),
        NullHandling::Allow => (quote!(allowing_null_bytes), quote!(allowing_null_bytes)),
        NullHandling::Deny => (quote!(denying_null_bytes), quote!(denying_null_bytes)),
    };

    let core = core.unwrap_or_else(core_path);
    let (_, ty_generics, _) = generics.split_for_impl();
    let mut owned_generics = generics.clone();
    // For the `Owned` associated type, we need to replace any non-static
    // lifetimes with static ones.
    let static_ident = Ident::new("static", Span::call_site());
    for lifetime in owned_generics.lifetimes_mut() {
        lifetime.lifetime.ident = static_ident.clone();
    }
    let mut generics = generics.clone();
    let lifetimes: Vec<_> = generics.lifetimes().cloned().collect();
    let where_clause = generics.make_where_clause();
    for lifetime in lifetimes {
        where_clause.predicates.push(parse_quote!($'key: #lifetime));
    }
    generics
        .params
        .push(syn::GenericParam::Lifetime(parse_quote!($'key)));
    let (impl_generics, _, where_clause) = generics.split_for_impl();

    // Special case the implementation for 1
    // field -- just pass through to the
    // inner type so that this encoding is
    // completely transparent.
    if let Some((name, ty, map, into_owned)) = match &data {
        Data::Struct(DataStruct {
            fields: Fields::Named(FieldsNamed { named, .. }),
            ..
        }) if named.len() == 1 => {
            let name = &named[0].ident;
            Some((
                quote!(#name),
                named[0].ty.clone(),
                quote!(|value| Self { #name: value }),
                quote!({ #name: #core::key::Key::into_owned(self.#name) }),
            ))
        }
        Data::Struct(DataStruct {
            fields: Fields::Unnamed(FieldsUnnamed { unnamed, .. }),
            ..
        }) if unnamed.len() == 1 => Some((
            quote!(0),
            unnamed[0].ty.clone(),
            quote!(Self),
            quote!((#core::key::Key::into_owned(self.0))),
        )),
        _ => None,
    } {
        return quote! {
            # use std::{borrow::Cow, io::{self, ErrorKind}};
            # use #core::key::{ByteSource, KeyVisitor, IncorrectByteLength, Key, KeyEncoding};

            impl #impl_generics Key<$'key> for #derived_on #ty_generics #where_clause {
                const CAN_OWN_BYTES: bool = <#ty>::CAN_OWN_BYTES;

                type Owned = #derived_on #owned_generics;

                fn into_owned(self) -> Self::Owned {
                    #derived_on #into_owned
                }

                fn from_ord_bytes<$'b>(bytes: ByteSource<$'key, $'b>) -> Result<Self, Self::Error> {
                    <#ty>::from_ord_bytes(bytes).map(#map)
                }
            }

            impl #impl_generics KeyEncoding<Self> for #derived_on #ty_generics #where_clause {
                type Error = <#ty as KeyEncoding>::Error;

                const LENGTH: Option<usize> = <#ty>::LENGTH;

                fn describe<Visitor>(visitor: &mut Visitor)
                where
                    Visitor: KeyVisitor,
                {
                    <#ty>::describe(visitor)
                }

                fn as_ord_bytes(&self) -> Result<Cow<'_, [u8]>, Self::Error> {
                    self.#name.as_ord_bytes()
                }
            }
        }
        .into();
    }

    let (encode_fields, decode_fields, describe, into_owned, composite_kind, field_count): (
        TokenStream,
        TokenStream,
        TokenStream,
        TokenStream,
        TokenStream,
        usize,
    ) = match data {
        Data::Struct(DataStruct { fields, .. }) => {
            let (encode_fields, decode_fields, describe, into_owned, field_count) = match fields {
                Fields::Named(FieldsNamed { named, .. }) => {
                    let field_count = named.len();
                    let (encode_fields, (decode_fields, (describe, into_owned))): (
                        TokenStream,
                        (TokenStream, (TokenStream, TokenStream)),
                    ) = named
                        .into_iter()
                        .map(|Field { ident, ty, .. }| {
                            let ident = ident.expect("named fields have idents");
                            (
                                quote!($encoder.encode(&self.#ident)?;),
                                (
                                    quote!(#ident: $decoder.decode()?,),
                                    (
                                        quote!(<#ty>::describe(visitor);),
                                        quote!(#ident: #core::key::Key::into_owned(self.#ident),),
                                    )
                                ),
                            )
                        })
                        .unzip();
                    (
                        encode_fields,
                        quote!( Self { #decode_fields }),
                        describe,
                        quote!( Self { #into_owned }),
                        field_count,
                    )
                }
                Fields::Unnamed(FieldsUnnamed { unnamed, .. }) => {
                    let field_count = unnamed.len();
                    let (encode_fields, (decode_fields, (describe, into_owned))): (
                        TokenStream,
                        (TokenStream, (TokenStream, TokenStream)),
                    ) = unnamed
                        .into_iter()
                        .enumerate()
                        .map(|(idx, field)| {
                            let ty = field.ty;
                            let idx = Index::from(idx);
                            (
                                quote!($encoder.encode(&self.#idx)?;),
                                (
                                    quote!($decoder.decode()?,),
                                    (
                                        quote!(<#ty>::describe(visitor);),
                                        quote!(#core::key::Key::into_owned(self.#idx),)
                                    )
                                ),
                            )
                        })
                        .unzip();
                    (
                        encode_fields,
                        quote!(Self(#decode_fields)),
                        describe,
                        quote!(#derived_on(#into_owned)),
                        field_count,
                    )
                }
                Fields::Unit => {
                    return quote! {
                        # use std::{borrow::Cow, io::{self, ErrorKind}};
                        # use #core::key::{ByteSource, KeyVisitor, IncorrectByteLength, Key, KeyKind, KeyEncoding};

                        impl #impl_generics Key<$'key> for #derived_on #ty_generics #where_clause {
                            const CAN_OWN_BYTES: bool = false;

                            type Owned = #derived_on #owned_generics;

                            fn into_owned(self) -> Self::Owned {
                                #derived_on
                            }

                            fn from_ord_bytes<$'b>(bytes: ByteSource<$'key, $'b>) -> Result<Self, Self::Error> {
                                Ok(Self)
                            }
                        }

                        impl #impl_generics KeyEncoding<Self> for #derived_on #ty_generics #where_clause {
                            type Error = std::convert::Infallible;

                            const LENGTH: Option<usize> = Some(0);

                            fn describe<Visitor>(visitor: &mut Visitor)
                            where
                                Visitor: KeyVisitor,
                            {
                                visitor.visit_type(KeyKind::Unit);
                            }

                            fn as_ord_bytes(&self) -> Result<Cow<'_, [u8]>, Self::Error> {
                                Ok(Cow::Borrowed(&[]))
                            }
                        }
                    }.into()
                },
            };
            (
                encode_fields,
                quote!(let $self_ = #decode_fields;),
                describe,
                into_owned,
                quote!(#core::key::CompositeKind::Struct(std::borrow::Cow::Borrowed(#name))),
                field_count,
            )
        }
        Data::Enum(DataEnum { variants, .. }) => {
            let mut prev_ident = None;
            let field_count = variants.len();
            let all_variants_are_empty = variants.iter().all(|variant| variant.fields.is_empty());

            let (consts, (encode_variants, (decode_variants, (describe, into_owned)))): (
                TokenStream,
                (TokenStream, (TokenStream, (TokenStream, TokenStream))),
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
                                    let (
                                        idents,
                                        (encode_fields, (decode_fields, (describe, into_owned))),
                                    ): (
                                        Punctuated<_, Token![,]>,
                                        (TokenStream, (TokenStream, (TokenStream, TokenStream))),
                                    ) = named
                                        .into_iter()
                                        .map(|Field { ident, ty, .. }| {
                                            let ident = ident.expect("named fields have idents");
                                            (
                                                ident.clone(),
                                                (
                                                    quote!($encoder.encode(#ident)?;),
                                                    (
                                                        quote!(#ident: $decoder.decode()?,),
                                                        (
                                                            quote!(<#ty>::describe(visitor);),
                                                            quote!(#ident: #core::key::Key::into_owned(#ident),),
                                                        )
                                                    ),
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
                                        (
                                            quote! {
                                                #const_ident => Self::#ident{#decode_fields},
                                            },
                                            (
                                                describe,
                                                quote!(Self::#ident{#idents} => #derived_on::#ident{#into_owned},),
                                            )
                                        ),
                                    )
                                }
                                Fields::Unnamed(FieldsUnnamed { unnamed, .. }) => {
                                    let (idents, (encode_fields, (decode_fields, (describe, into_owned)))): (
                                        Punctuated<_, Token![,]>,
                                        (TokenStream, (TokenStream, (TokenStream, TokenStream))),
                                    ) = unnamed
                                        .into_iter()
                                        .enumerate()
                                        .map(|(idx, field)| {
                                            let ident = format_ident!("$field_{idx}");
                                            let ty = field.ty;
                                            (
                                                ident.clone(),
                                                (
                                                    quote!($encoder.encode(#ident)?;),
                                                    (
                                                        quote!($decoder.decode()?,),
                                                        (
                                                            quote!(<#ty>::describe(visitor);),
                                                            quote!(#core::key::Key::into_owned(#ident),),
                                                        )
                                                    ),
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
                                        (
                                            quote! {
                                                #const_ident => Self::#ident(#decode_fields),
                                            },
                                            (
                                                describe,
                                                quote!(Self::#ident(#idents) => #derived_on::#ident(#into_owned),),
                                            )
                                        ),
                                    )
                                }
                                Fields::Unit => {
                                    let encode = if all_variants_are_empty {
                                        quote!(Self::#ident => #const_ident.as_ord_bytes(),)
                                    } else {
                                        quote!(Self::#ident => $encoder.encode(&#const_ident)?,)
                                    };
                                    (
                                        encode,
                                        (
                                            quote!(#const_ident => Self::#ident,),
                                            (
                                                quote!(visitor.visit_type(#core::key::KeyKind::Unit);),
                                                quote!(Self::#ident => #derived_on::#ident,),
                                            )
                                        ),
                                    )
                                }
                            },
                        );
                        prev_ident = Some(const_ident);
                        ret
                    },
                )
                .unzip();

            if all_variants_are_empty {
                // Special case: if no enum variants have embedded values,
                // implement Key as a plain value, avoiding the composite key
                // overhead.
                return quote! {
                    # use std::{borrow::Cow, io::{self, ErrorKind}};
                    # use #core::key::{ByteSource, CompositeKeyDecoder, KeyVisitor, CompositeKeyEncoder, CompositeKeyError, Key, KeyEncoding};

                    impl #impl_generics Key<$'key> for #derived_on #ty_generics #where_clause {
                        const CAN_OWN_BYTES: bool = false;

                        type Owned = #derived_on #owned_generics;

                        fn into_owned(self) -> Self::Owned {
                            match self {
                                #into_owned
                            }
                        }

                        fn from_ord_bytes<$'b>(mut $bytes: ByteSource<$'key, $'b>) -> Result<Self, Self::Error> {
                            #consts
                            Ok(match <#repr>::from_ord_bytes($bytes).map_err(#core::key::CompositeKeyError::new)? {
                                #decode_variants
                                _ => return Err(#core::key::CompositeKeyError::from(io::Error::from(
                                        ErrorKind::InvalidData,
                                )))
                            })
                        }
                    }

                    impl #impl_generics KeyEncoding<Self> for #derived_on #ty_generics #where_clause {
                        type Error = CompositeKeyError;

                        const LENGTH: Option<usize> = <#repr as KeyEncoding>::LENGTH;

                        fn describe<Visitor>(visitor: &mut Visitor)
                        where
                            Visitor: KeyVisitor,
                        {
                            <#repr>::describe(visitor);
                        }

                        fn as_ord_bytes(& self) -> Result<Cow<'_, [u8]>, Self::Error> {
                            #consts
                            match self {
                                #encode_variants
                            }.map_err(#core::key::CompositeKeyError::new)
                        }
                    }
                }
                .into();
            }

            // At least one variant has a value, which means we need to encode a composite field.
            (
                quote! {
                    #consts
                    match self{
                        #encode_variants
                    }
                },
                quote! {
                    # use std::io::{self, ErrorKind};
                    #consts
                    let $self_ = match $decoder.decode::<#repr>()? {
                        #decode_variants
                        _ => return Err(#core::key::CompositeKeyError::from(io::Error::from(
                                ErrorKind::InvalidData,
                        )))
                    };
                },
                describe,
                quote! {
                    match self {
                        #into_owned
                    }
                },
                quote!(#core::key::CompositeKind::Tuple),
                field_count,
            )
        }
        Data::Union(_) => abort_call_site!("unions are not supported"),
    };

    quote! {
        # use std::{borrow::Cow, io::{self, ErrorKind}};
        # use #core::key::{ByteSource, CompositeKeyDecoder, KeyVisitor, CompositeKeyEncoder, CompositeKeyError, Key, KeyEncoding};

        impl #impl_generics Key<$'key> for #derived_on #ty_generics #where_clause {
            const CAN_OWN_BYTES: bool = #can_own_bytes;

            type Owned = #derived_on #owned_generics;

            fn into_owned(self) -> Self::Owned {
                #into_owned
            }

            fn from_ord_bytes<$'b>(mut $bytes: ByteSource<$'key, $'b>) -> Result<Self, Self::Error> {

                let mut $decoder = CompositeKeyDecoder::#decoder_constructor($bytes);

                #decode_fields

                $decoder.finish()?;

                Ok($self_)
            }
        }

        impl #impl_generics KeyEncoding<Self> for #derived_on #ty_generics #where_clause {
            type Error = CompositeKeyError;

            // TODO fixed width if possible
            const LENGTH: Option<usize> = None;

            fn describe<Visitor>(visitor: &mut Visitor)
            where
                Visitor: KeyVisitor,
            {
                visitor.visit_composite(#composite_kind, #field_count);
                #describe
            }

            fn as_ord_bytes(&self) -> Result<Cow<'_, [u8]>, Self::Error> {
                let mut $encoder = CompositeKeyEncoder::#encoder_constructor();

                #encode_fields

                Ok(Cow::Owned($encoder.finish()))
            }
        }
    }
    .into()
}

#[derive(Attribute)]
#[attribute(ident = api)]
struct ApiAttribute {
    #[attribute(example = "\"name\"")]
    name: String,
    #[attribute(example = "\"authority\"")]
    authority: Option<Expr>,
    #[attribute(example = "ResponseType")]
    response: Option<Type>,
    #[attribute(example = "ErrorType")]
    error: Option<Type>,
    #[attribute(example = "bosaidb::core")]
    core: Option<Path>,
}

/// Derives the `bonsaidb::core::api::Api` trait.
#[proc_macro_error]
/// `#[api(name = "Name", authority = "Authority", response = ResponseType, error = ErrorType, core = bonsaidb::core)]`
/// `authority`, `response`, `error` and `core` are optional
#[proc_macro_derive(Api, attributes(api))]
pub fn api_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let DeriveInput {
        attrs,
        ident,
        generics,
        ..
    } = parse_macro_input!(input as DeriveInput);

    let ApiAttribute {
        name,
        authority,
        response,
        error,
        core,
    } = unwrap_or_abort(ApiAttribute::from_attributes(&attrs));

    let core = core.unwrap_or_else(core_path);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let name = authority.map_or_else(
        || quote!(#core::schema::Qualified::private(#name)),
        |authority| quote!(#core::schema::Qualified::new(#authority, #name)),
    );

    let response = response.unwrap_or_else(|| parse_quote!(()));
    let error = error.unwrap_or_else(|| parse_quote!(#core::api::Infallible));

    quote! {
        # use #core::api::{Api, ApiName};

        impl #impl_generics Api for #ident #ty_generics #where_clause {
            type Response = #response;
            type Error = #error;

            fn name() -> ApiName {
                #name
            }
        }
    }
    .into()
}

// -----------------------------------------------------------------------------
//     - File Macros -
// -----------------------------------------------------------------------------

fn files_path() -> Path {
    match crate_name("bonsaidb") {
        Ok(FoundCrate::Name(name)) => {
            let ident = Ident::new(&name, Span::call_site());
            parse_quote!(::#ident::files)
        }
        Ok(FoundCrate::Itself) => parse_quote!(crate::files),
        Err(_) => match crate_name("bonsaidb_files") {
            Ok(FoundCrate::Name(name)) => {
                let ident = Ident::new(&name, Span::call_site());
                parse_quote!(::#ident)
            }
            Ok(FoundCrate::Itself) => parse_quote!(crate),
            Err(_) if cfg!(feature = "omnibus-path") => parse_quote!(::bonsaidb::files),
            Err(_) => parse_quote!(::bonsaidb_core),
        },
    }
}

#[derive(Attribute)]
#[attribute(ident = file_config)]
struct FileConfigAttribute {
    #[attribute(example = "MetadataType")]
    metadata: Option<Type>,
    #[attribute(example = "65_536")]
    block_size: Option<usize>,
    #[attribute(example = "\"authority\"")]
    authority: Option<Expr>,
    #[attribute(example = "\"files\"")]
    files_name: Option<String>,
    #[attribute(example = "\"blocks\"")]
    blocks_name: Option<String>,
    #[attribute(example = "bosaidb::core")]
    core: Option<Path>,
    #[attribute(example = "bosaidb::files")]
    files: Option<Path>,
}

/// Derives the `bonsaidb::files::FileConfig` trait.
#[proc_macro_error]
/// `#[api(metadata = MetadataType, block_size = 65_536, authority = "authority", files_name = "files", blocks_name = "blocks", core = bonsaidb::core, files = bosaidb::files)]`
/// all arguments are optional
#[proc_macro_derive(FileConfig, attributes(file_config))]
pub fn file_config_derive(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let DeriveInput {
        attrs,
        ident,
        generics,
        ..
    } = parse_macro_input!(input as DeriveInput);

    let FileConfigAttribute {
        metadata,
        block_size,
        authority,
        files_name,
        blocks_name,
        core,
        files,
    } = unwrap_or_abort(FileConfigAttribute::from_attributes(&attrs));

    let core = core.unwrap_or_else(core_path);
    let files = files.unwrap_or_else(files_path);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let (files_name, blocks_name) = match (authority, files_name, blocks_name) {
        (None, None, None) => (
            quote!(#files::BonsaiFiles::files_name()),
            quote!(#files::BonsaiFiles::blocks_name()),
        ),
        (Some(authority), Some(files_name), Some(blocks_name)) => (
            quote!(#core::schema::Qualified::new(#authority, #files_name)),
            quote!(#core::schema::Qualified::new(#authority, #blocks_name)),
        ),
        (None, Some(files_name), Some(blocks_name)) => (
            quote!(#core::schema::Qualified::private(#files_name)),
            quote!(#core::schema::Qualified::private(#blocks_name)),
        ),
        (Some(_), ..) => abort_call_site!(
            "if `authority` is specified, `files_name` and `blocks_name need to be provided as well"
        ),
        (_, Some(_), _) => abort_call_site!(
            "if `files_name` is specified, `blocks_name` needs to be provided as well"
        ),
        (_, _, Some(_)) => abort_call_site!(
            "if `blocks_name` is specified, `files_name` needs to be provided as well"
        ),
    };

    let metadata = metadata
        .unwrap_or_else(|| parse_quote!(<#files::BonsaiFiles as #files::FileConfig>::Metadata));
    let block_size = block_size.map_or_else(
        || quote!(<#files::BonsaiFiles as #files::FileConfig>::BLOCK_SIZE),
        |block_size| quote!(#block_size),
    );

    quote! {
        # use #files::FileConfig;
        # use #core::schema::CollectionName;

        impl #impl_generics FileConfig for #ident #ty_generics #where_clause {
            type Metadata = #metadata;
            const BLOCK_SIZE: usize = #block_size;

            fn files_name() -> CollectionName {
                #files_name
            }

            fn blocks_name() -> CollectionName {
                #blocks_name
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
