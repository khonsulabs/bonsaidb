use serde::{Deserialize, Serialize};

use crate::{
    connection::{IdentityId, SensitiveBytes},
    define_basic_unique_mapped_view,
    document::{CollectionDocument, Emit},
    key::time::TimestampAsNanoseconds,
    schema::Collection,
};

#[derive(Collection, Clone, Serialize, Deserialize, Debug)]
#[collection(name = "authentication-tokens", authority = "bonsaidb", core = crate)]
pub struct AuthenticationToken {
    pub identity: IdentityId,
    pub token: SensitiveBytes,
    pub created_at: TimestampAsNanoseconds,
}

#[cfg(feature = "token-authentication")]
mod implementation {
    use arc_bytes::serde::Bytes;
    use rand::{thread_rng, Rng};
    use zeroize::Zeroize;

    use super::AuthenticationToken;
    use crate::{
        admin::authentication_token,
        connection::{
            AsyncConnection, Connection, IdentityId, IdentityReference, SensitiveBytes,
            TokenChallengeAlgorithm,
        },
        document::CollectionDocument,
        key::time::TimestampAsNanoseconds,
        schema::SerializedCollection,
    };

    impl AuthenticationToken {
        fn random(identity: IdentityId) -> (u64, Self) {
            let mut rng = thread_rng();
            let id = rng.gen();
            let token = SensitiveBytes(Bytes::from(vec![rng.gen(); 32]));
            (
                id,
                Self {
                    identity,
                    token,
                    created_at: TimestampAsNanoseconds::now(),
                },
            )
        }

        pub fn create<C: Connection>(
            identity: &IdentityReference<'_>,
            database: &C,
        ) -> Result<CollectionDocument<Self>, crate::Error> {
            let identity_id = identity
                .resolve(database)?
                .ok_or(crate::Error::InvalidCredentials)?;
            loop {
                let (id, token) = Self::random(identity_id);
                match token.insert_into(&id, database) {
                    Err(err)
                        if err
                            .error
                            .is_unique_key_error::<authentication_token::ByToken, _>(database)
                            || err.error.conflicting_document::<Self>().is_some() =>
                    {
                        continue
                    }
                    other => break other.map_err(|err| err.error),
                }
            }
        }

        pub async fn create_async<C: AsyncConnection>(
            identity: IdentityReference<'_>,
            database: &C,
        ) -> Result<CollectionDocument<Self>, crate::Error> {
            let identity_id = identity
                .resolve_async(database)
                .await?
                .ok_or(crate::Error::InvalidCredentials)?;
            loop {
                let (id, token) = Self::random(identity_id);
                match token.insert_into_async(&id, database).await {
                    Err(err)
                        if err
                            .error
                            .is_unique_key_error::<authentication_token::ByToken, _>(database)
                            || err.error.conflicting_document::<Self>().is_some() =>
                    {
                        continue
                    }
                    other => break other.map_err(|err| err.error),
                }
            }
        }

        pub fn validate_challenge(
            &self,
            algorithm: TokenChallengeAlgorithm,
            server_timestamp: TimestampAsNanoseconds,
            nonce: &[u8],
            hash: &[u8],
        ) -> Result<(), crate::Error> {
            let TokenChallengeAlgorithm::Blake3 = algorithm;
            let computed_hash =
                Self::compute_challenge_response_blake3(&self.token, nonce, server_timestamp);
            let hash: [u8; blake3::OUT_LEN] = hash
                .try_into()
                .map_err(|_| crate::Error::InvalidCredentials)?;

            if computed_hash == hash {
                Ok(())
            } else {
                Err(crate::Error::InvalidCredentials)
            }
        }

        #[must_use]
        pub fn compute_challenge_response_blake3(
            token: &SensitiveBytes,
            nonce: &[u8],
            timestamp: TimestampAsNanoseconds,
        ) -> blake3::Hash {
            let context = format!("bonsaidb {timestamp} token-challenge");
            let mut key = blake3::derive_key(&context, &token.0);
            let hash = blake3::keyed_hash(&key, nonce);
            key.zeroize();
            hash
        }

        pub fn check_request_time(
            request_time: TimestampAsNanoseconds,
            request_time_check: &[u8],
            algorithm: TokenChallengeAlgorithm,
            token: &SensitiveBytes,
        ) -> Result<(), crate::Error> {
            match algorithm {
                TokenChallengeAlgorithm::Blake3 => {
                    let request_time_check: [u8; blake3::OUT_LEN] =
                        request_time_check
                            .try_into()
                            .map_err(|_| crate::Error::InvalidCredentials)?;
                    if Self::compute_request_time_hash_blake3(request_time, token)
                        == request_time_check
                    {
                        Ok(())
                    } else {
                        Err(crate::Error::InvalidCredentials)
                    }
                }
            }
        }

        pub(crate) fn compute_request_time_hash_blake3(
            request_time: TimestampAsNanoseconds,
            private_token: &SensitiveBytes,
        ) -> blake3::Hash {
            let context = format!("bonsaidb {request_time} token-authentication");
            let mut key = blake3::derive_key(&context, &private_token.0);
            let hash = blake3::keyed_hash(&key, &request_time.representation().to_be_bytes());
            key.zeroize();
            hash
        }
    }
}

impl AuthenticationToken {}

define_basic_unique_mapped_view! {
    ByToken, AuthenticationToken, 0, "by-token", SensitiveBytes, |token: CollectionDocument<AuthenticationToken>| token.header.emit_key(token.contents.token)
}
