use std::collections::hash_map::Entry;

use bonsaidb_core::{connection::SessionId, pubsub::Receiver};

use crate::{storage::SessionSubscriber, Database, Subscriber};

impl crate::storage::StorageInstance {
    pub(crate) fn register_subscriber(
        &self,
        session_id: Option<SessionId>,
        database: Database,
    ) -> Subscriber {
        let subscriber = self.relay().create_subscriber();
        let mut data = self.data.subscribers.write();
        let receiver = Receiver::new_stripping_prefixes(subscriber.receiver().clone());
        let id = loop {
            data.last_id = data.last_id.wrapping_add(1);
            let id = data.last_id;
            let entry = data.subscribers.entry(id);
            if matches!(entry, Entry::Vacant(_)) {
                entry.or_insert(SessionSubscriber {
                    session_id,
                    subscriber: subscriber.clone(),
                });
                break id;
            }
        };

        Subscriber {
            id,
            database,
            subscriber,
            receiver,
        }
    }
}
