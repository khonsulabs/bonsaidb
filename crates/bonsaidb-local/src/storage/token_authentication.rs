use std::sync::Arc;

use bonsaidb_core::admin::{AuthenticationToken, Role, User};
use bonsaidb_core::connection::{
    IdentityId, Session, SessionAuthentication, SessionId, TokenChallengeAlgorithm,
};
use bonsaidb_core::key::time::TimestampAsNanoseconds;
use bonsaidb_core::permissions::Permissions;
use bonsaidb_core::schema::SerializedCollection;
use parking_lot::Mutex;
use rand::{thread_rng, Rng};

use crate::storage::AuthenticatedSession;
use crate::{Database, Storage};

impl super::StorageInstance {
    pub(super) fn begin_token_authentication(
        &self,
        id: u64,
        request_time: TimestampAsNanoseconds,
        request_time_check: &[u8],
        algorithm: TokenChallengeAlgorithm,
        admin: &Database,
    ) -> Result<Storage, bonsaidb_core::Error> {
        // TODO alow configuration of timestamp sensitivity. 5 minutes chosen based on Kerberos and
        if request_time
            .duration_between(&TimestampAsNanoseconds::now())?
            .as_secs()
            > 300
        {
            return Err(bonsaidb_core::Error::InvalidCredentials); // TODO better error
        }
        let token = AuthenticationToken::get(&id, admin)?
            .ok_or(bonsaidb_core::Error::InvalidCredentials)?;
        AuthenticationToken::check_request_time(
            request_time,
            request_time_check,
            algorithm,
            &token.contents.token,
        )?;

        // Token authentication creates a temporary session for the token
        // challenge. The process of finishing token authentication will remove
        // the session.

        let mut sessions = self.data.sessions.write();
        sessions.last_session_id += 1;
        let session_id = SessionId(sessions.last_session_id);
        let nonce = thread_rng().gen::<[u8; 32]>();
        let session = Session {
            id: Some(session_id),
            authentication: SessionAuthentication::TokenChallenge {
                id,
                algorithm: TokenChallengeAlgorithm::Blake3,
                nonce,
                server_timestamp: TimestampAsNanoseconds::now(),
            },
            permissions: Permissions::default(), /* This session will have no permissions until it finishes token authentication */
        };
        let authentication = Arc::new(AuthenticatedSession {
            storage: Arc::downgrade(&self.data),
            session: Mutex::new(session.clone()),
        });
        sessions.sessions.insert(session_id, authentication.clone());

        Ok(Storage {
            instance: self.clone(),
            authentication: Some(authentication),
            effective_session: Some(Arc::new(session)),
        })
    }

    pub(super) fn finish_token_authentication(
        &self,
        session_id: SessionId,
        hash: &[u8],
        admin: &Database,
    ) -> Result<Storage, bonsaidb_core::Error> {
        // Remove the temporary session so that it can't be used multiple times.
        let session = {
            let mut sessions = self.data.sessions.write();
            sessions
                .sessions
                .remove(&session_id)
                .ok_or(bonsaidb_core::Error::InvalidCredentials)?
        };
        let session = session.session.lock();
        match &session.authentication {
            SessionAuthentication::TokenChallenge {
                id,
                algorithm,
                nonce,
                server_timestamp,
            } => {
                let token = AuthenticationToken::get(id, admin)?
                    .ok_or(bonsaidb_core::Error::InvalidCredentials)?;
                token
                    .contents
                    .validate_challenge(*algorithm, *server_timestamp, nonce, hash)?;
                match token.contents.identity {
                    IdentityId::User(id) => {
                        let user = User::get(&id, admin)?
                            .ok_or(bonsaidb_core::Error::InvalidCredentials)?;
                        self.assume_user(user, admin)
                    }
                    IdentityId::Role(id) => {
                        let role = Role::get(&id, admin)?
                            .ok_or(bonsaidb_core::Error::InvalidCredentials)?;
                        self.assume_role(role, admin)
                    }
                    _ => Err(bonsaidb_core::Error::InvalidCredentials),
                }
            }
            SessionAuthentication::None | SessionAuthentication::Identity(_) => {
                Err(bonsaidb_core::Error::InvalidCredentials)
            }
        }
    }
}
