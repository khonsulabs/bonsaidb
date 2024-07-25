use generic_array::GenericArray;
use hpke::kem::{DhP256HkdfSha256, Kem as KemTrait};
use hpke::{Deserializable, Serializable};
use serde::de::Error;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use zeroize::Zeroize;

// Helpful aliases for the HPKE types we use in vault encryption

pub(crate) type VaultP256Kem = DhP256HkdfSha256;
pub(crate) type VaultP256PublicKey = <DhP256HkdfSha256 as KemTrait>::PublicKey;
pub(crate) type VaultP256PrivateKey = <DhP256HkdfSha256 as KemTrait>::PrivateKey;
pub(crate) type VaultP256EncappedKey = <DhP256HkdfSha256 as KemTrait>::EncappedKey;

// A previous version of hpke had serde impls. For backwards compatibility, we re-implement that
// here. All this is is casting to/from GenericArray, and using GenericArray's serde impl, just as
// the original did it:
// https://github.com/rozbb/rust-hpke/blob/57fce26b436f47846ee4f9a972ea0675786101c9/src/serde_impls.rs#L42-L74

// We put everything in its own module so we can use the `with` field attribute
// https://serde.rs/field-attrs.html#with

// Impl serde for $t: hpke::{Serializable, Deserializable}
macro_rules! impl_serde {
    ($modname:ident, $t:ty) => {
        pub(crate) mod $modname {
            use super::*;

            pub(crate) fn serialize<S: Serializer>(
                val: &$t,
                serializer: S,
            ) -> Result<S::Ok, S::Error> {
                let mut arr = val.to_bytes();
                let ret = arr.serialize(serializer);
                arr.zeroize();
                ret
            }

            pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
                deserializer: D,
            ) -> Result<$t, D::Error> {
                let mut arr = GenericArray::<u8, <$t as Serializable>::OutputSize>::deserialize(
                    deserializer,
                )?;
                let ret = <$t>::from_bytes(&arr).map_err(D::Error::custom);
                arr.zeroize();
                ret
            }
        }
    };
}

impl_serde!(serde_pubkey, VaultP256PublicKey);
impl_serde!(serde_privkey, VaultP256PrivateKey);
impl_serde!(serde_encapped_key, VaultP256EncappedKey);
