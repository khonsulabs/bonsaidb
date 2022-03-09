use std::collections::hash_map::Entry;

use bonsaidb_core::Error;

use crate::{backend, storage::SessionSubscriber, Database, Subscriber};

impl<Backend: backend::Backend> crate::storage::StorageInstance<Backend> {
    pub(crate) fn register_subscriber(
        &self,
        session_id: Option<u64>,
        database: Database<Backend>,
    ) -> Subscriber<Backend> {
        let subscriber = self.relay().create_subscriber();
        let mut data = self.data.subscribers.write();
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
        }
    }

    pub(crate) fn subscribe_by_id(
        &self,
        subscriber_id: u64,
        topic: String,
        in_session_id: Option<u64>,
    ) -> Result<(), crate::Error> {
        let data = self.data.subscribers.read();
        if let Some(subscriber) = data.subscribers.get(&subscriber_id) {
            if subscriber.session_id == in_session_id {
                subscriber.subscriber.subscribe_to(topic);
                Ok(())
            } else {
                // TODO real errors? Generic message?
                Err(crate::Error::from(Error::Transport(String::from(
                    "session_id not from current session",
                ))))
            }
        } else {
            Err(crate::Error::from(Error::Transport(String::from(
                "invalid subscriber id",
            ))))
        }
    }

    pub(crate) fn unsubscribe_by_id(
        &self,
        subscriber_id: u64,
        topic: &str,
        in_session_id: Option<u64>,
    ) -> Result<(), crate::Error> {
        let data = self.data.subscribers.read();
        if let Some(subscriber) = data.subscribers.get(&subscriber_id) {
            if subscriber.session_id == in_session_id {
                subscriber.subscriber.unsubscribe_from(&topic);
                Ok(())
            } else {
                // TODO real errors? Generic message?
                Err(crate::Error::from(Error::Transport(String::from(
                    "session_id not from current session",
                ))))
            }
        } else {
            Err(crate::Error::from(Error::Transport(String::from(
                "invalid subscriber id",
            ))))
        }
    }

    pub(crate) fn unregister_subscriber_by_id(
        &self,
        subscriber_id: u64,
        in_session_id: Option<u64>,
    ) -> Result<(), crate::Error> {
        let mut data = self.data.subscribers.write();
        if let Some(subscriber) = data.subscribers.get(&subscriber_id) {
            if subscriber.session_id == in_session_id {
                data.subscribers.remove(&subscriber_id);
                Ok(())
            } else {
                // TODO real errors? Generic message?
                Err(crate::Error::from(Error::Transport(String::from(
                    "session_id not from current session",
                ))))
            }
        } else {
            Err(crate::Error::from(Error::Transport(String::from(
                "invalid subscriber id",
            ))))
        }
    }
}
