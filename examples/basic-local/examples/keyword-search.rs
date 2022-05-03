//! This example shows a naive approach to implementing text search. It works by
//! creating a View with the Key being each word contained within a document.
//! This example uses the Value type to store which field the match came from.
//!
//! While this is a naive approach, this can be used to build a fairly robust
//! search. After retrieving the query results from the view, analysis can be
//! done on the results to rank the matched documents based on how many times a
//! keyword hit, which fields it matched upon, etc.
//!
//! While this approach can be powerful, it pales in comparsion to full text
//! search capabilities. The tracking issue for adding full text indexes to
//! BonsaiDb is here: <https://github.com/khonsulabs/bonsaidb/issues/149>.
use std::{str::Chars, time::SystemTime};

use bonsaidb::{
    core::{
        document::{CollectionDocument, Emit},
        schema::{
            Collection, CollectionViewSchema, SerializedCollection, SerializedView, View,
            ViewMapResult,
        },
    },
    local::{
        config::{Builder, StorageConfiguration},
        Database,
    },
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Collection)]
#[collection(name = "messages", views = [MessagesByWords])]
struct Message {
    pub timestamp: SystemTime,
    pub subject: String,
    pub body: String,
}

impl Message {
    /// Returns a new message with the current timestamp.
    pub fn new(subject: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            timestamp: SystemTime::now(),
            subject: subject.into(),
            body: body.into(),
        }
    }
}

#[derive(View, Debug, Clone)]
#[view(name = "by-keyword", collection = Message, key = String, value = String)]
struct MessagesByWords;

impl CollectionViewSchema for MessagesByWords {
    type View = Self;

    fn map(
        &self,
        document: CollectionDocument<<Self::View as View>::Collection>,
    ) -> ViewMapResult<Self::View> {
        // Emit a key/value mapping for each word found in the subject and body.
        let subject_words =
            keywords(&document.contents.subject).map(|word| (word, String::from("subject")));
        let body_words = keywords(&document.contents.body).map(|word| (word, String::from("body")));
        subject_words
            .chain(body_words)
            .map(|(key, value)| document.header.emit_key_and_value(key, value))
            .collect()
    }
}

fn main() -> Result<(), bonsaidb::core::Error> {
    let db = Database::open::<Message>(StorageConfiguration::new("keyword-search.bonsaidb"))?;

    Message::new("Groceries", "Can you pick up some milk on the way home?").push_into(&db)?;
    Message::new("Re: Groceries", "2% milk? How are our eggs?").push_into(&db)?;
    Message::new("Re: Groceries", "Yes. We could use another dozen eggs.").push_into(&db)?;

    for result in &MessagesByWords::entries(&db)
        .with_key("eggs")
        .query_with_collection_docs()?
    {
        println!(
            "Contained `eggs` in field {} : {:?}",
            result.value, result.document
        );
    }

    for message in MessagesByWords::entries(&db)
        .with_key_prefix("doz")
        .query_with_collection_docs()?
        .documents
    {
        println!("Contained a word starting with `doz`: {message:?}");
    }

    Ok(())
}

/// Splits `source` into "words", where words are:
///
/// - Contiguous sequences of alphanumeric characters
/// - At least 4 characters long (Avoids "the", "and", etc, but it is *overly*
///   restrictive).
/// - Non-alphanumeric is always considered a word break and excluded from
///   results. This means that "m.d." will never yield any 'words' in this
///   algorithm, because 'm' and 'd' will be considered separate words and
///   excluded for being too short.
fn keywords(source: &str) -> impl Iterator<Item = String> + '_ {
    struct KeywordEmitter<'a> {
        chars: Chars<'a>,
    }

    impl<'a> Iterator for KeywordEmitter<'a> {
        type Item = String;

        fn next(&mut self) -> Option<Self::Item> {
            let mut word = String::new();
            for ch in &mut self.chars {
                if ch.is_alphanumeric() {
                    for ch in ch.to_lowercase() {
                        word.push(ch);
                    }
                } else if !word.is_empty() {
                    if word.len() > 3 {
                        return Some(word);
                    }
                    word.clear();
                }
            }

            (word.len() > 3).then(|| word)
        }
    }

    KeywordEmitter {
        chars: source.chars(),
    }
}

#[test]
fn runs() {
    main().unwrap()
}
