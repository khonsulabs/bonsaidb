use generic_array::GenericArray;
use hpke::{
    kem::{DhP256HkdfSha256, Kem as KemTrait},
    Deserializable, Serializable,
};
use serde::{de::Error, Deserialize, Deserializer, Serialize, Serializer};

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

pub(crate) mod serde_pubkey {
    use super::*;

    pub(crate) fn serialize<S: Serializer>(
        public_key: &VaultP256PublicKey,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let arr = public_key.to_bytes();
        arr.serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<VaultP256PublicKey, D::Error> {
        let arr =
            GenericArray::<u8, <VaultP256PublicKey as Serializable>::OutputSize>::deserialize(
                deserializer,
            )?;
        VaultP256PublicKey::from_bytes(&arr).map_err(D::Error::custom)
    }
}

pub(crate) mod serde_privkey {
    use super::*;
    use hpke::Serializable;

    pub(crate) fn serialize<S: Serializer>(
        private_key: &VaultP256PrivateKey,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let arr = private_key.to_bytes();
        arr.serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<VaultP256PrivateKey, D::Error> {
        let arr =
            GenericArray::<u8, <VaultP256PrivateKey as Serializable>::OutputSize>::deserialize(
                deserializer,
            )?;
        VaultP256PrivateKey::from_bytes(&arr).map_err(D::Error::custom)
    }
}

pub(crate) mod serde_encapped_key {
    use super::*;

    pub(crate) fn serialize<S: Serializer>(
        encapped_key: &VaultP256EncappedKey,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let arr = encapped_key.to_bytes();
        arr.serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<VaultP256EncappedKey, D::Error> {
        let arr =
            GenericArray::<u8, <VaultP256EncappedKey as Serializable>::OutputSize>::deserialize(
                deserializer,
            )?;
        VaultP256EncappedKey::from_bytes(&arr).map_err(D::Error::custom)
    }
}
