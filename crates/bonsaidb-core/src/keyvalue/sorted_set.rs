use std::{cmp::Ordering, collections::HashMap, ops::Deref};

use arc_bytes::{ArcBytes, OwnedBytes};
use serde::{ser::SerializeMap, Deserialize, Serialize};

#[derive(Default, Clone, Debug)]
pub struct SortedSet {
    members: HashMap<OwnedBytes, Score>,
    sorted_members: Vec<Entry>,
}

impl SortedSet {
    pub fn insert(&mut self, value: OwnedBytes, score: Score) -> Option<Score> {
        let entry = Entry { score, value };
        let existing_score = self
            .members
            .insert(entry.value.clone(), entry.score.clone());

        if existing_score.is_some() {
            let (remove_index, _) = self
                .sorted_members
                .iter()
                .enumerate()
                .find(|(_, member)| member.value == entry.value)
                .unwrap();
            self.sorted_members.remove(remove_index);
        }

        let insert_at = self
            .sorted_members
            .binary_search(&entry)
            .unwrap_or_else(|i| i);
        self.sorted_members.insert(insert_at, entry);

        existing_score
    }

    pub fn score(&self, value: &[u8]) -> Option<&Score> {
        self.members.get(value)
    }

    pub fn remove(&mut self, value: &[u8]) -> Option<Score> {
        let existing_score = self.members.remove(value);
        if existing_score.is_some() {
            let (remove_index, _) = self
                .sorted_members
                .iter()
                .enumerate()
                .find(|(_index, member)| member.value == value)
                .unwrap();
            self.sorted_members.remove(remove_index);
        }
        existing_score
    }
}

impl Deref for SortedSet {
    type Target = Vec<Entry>;

    fn deref(&self) -> &Self::Target {
        &self.sorted_members
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Entry {
    score: Score,
    value: OwnedBytes,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Score {
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    Bytes(OwnedBytes),
}

// We check that the float value on input is not a NaN.
impl Eq for Score {}

impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

#[allow(clippy::cast_precision_loss)]
impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Signed(a), Self::Signed(b)) => a.cmp(b),
            (Self::Signed(a), Self::Unsigned(b)) => {
                if let Ok(a) = u64::try_from(*a) {
                    a.cmp(b)
                } else {
                    Ordering::Less
                }
            }
            (Self::Unsigned(a), Self::Signed(b)) => {
                if let Ok(b) = u64::try_from(*b) {
                    a.cmp(&b)
                } else {
                    Ordering::Greater
                }
            }
            (Self::Unsigned(a), Self::Unsigned(b)) => a.cmp(b),
            (Self::Float(a), Self::Float(b)) => float_cmp(*a, *b),
            (Self::Float(a), Self::Signed(b)) => float_cmp(*a, *b as f64),
            (Self::Float(a), Self::Unsigned(b)) => float_cmp(*a, *b as f64),
            (Self::Signed(a), Self::Float(b)) => float_cmp(*a as f64, *b),
            (Self::Unsigned(a), Self::Float(b)) => float_cmp(*a as f64, *b),
            (Self::Bytes(a), Self::Bytes(b)) => a.cmp(b),
            (_, Self::Bytes(_)) => Ordering::Less,
            (Self::Bytes(_), _) => Ordering::Greater,
        }
    }
}

fn float_cmp(f1: f64, f2: f64) -> Ordering {
    let abs_diff = f1 - f2;
    if abs_diff < f64::EPSILON && abs_diff > -f64::EPSILON {
        Ordering::Equal
    } else if abs_diff < 0. {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}

#[test]
fn ord_tests() {
    use rand::seq::SliceRandom;
    let mut set = SortedSet::default();
    let mut rng = rand::thread_rng();
    let originals = vec![
        Score::Signed(-1),
        Score::Unsigned(0),
        Score::Signed(1),
        Score::Float(1.),
        Score::Float(1.5),
        Score::Unsigned(2),
        Score::Bytes(OwnedBytes::from(b"\x00")),
        Score::Bytes(OwnedBytes::from(b"\x01")),
    ];
    let mut shuffled = originals.clone();
    shuffled.shuffle(&mut rng);
    for (i, score) in shuffled.into_iter().enumerate() {
        set.insert(OwnedBytes::from(i.to_string().into_bytes()), score);
    }
    for (i, score) in originals.iter().enumerate() {
        assert_eq!(&set[i].score, score);
    }
}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Serialize for SortedSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.members.len()))?;
        for member in &self.sorted_members {
            map.serialize_entry(&member.value, &member.score)?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for SortedSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(Visitor)
    }
}

struct Visitor;

impl<'de> serde::de::Visitor<'de> for Visitor {
    type Value = SortedSet;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("sorted set entries")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let (mut members, mut sorted_members) = if let Some(size) = map.size_hint() {
            (HashMap::with_capacity(size), Vec::with_capacity(size))
        } else {
            (HashMap::default(), Vec::default())
        };

        while let Some((value, score)) = map.next_entry::<ArcBytes<'_>, Score>()? {
            let entry = Entry {
                value: OwnedBytes(value.into_owned()),
                score,
            };
            members.insert(entry.value.clone(), entry.score.clone());
            sorted_members.push(entry);
        }

        sorted_members.sort();

        Ok(SortedSet {
            members,
            sorted_members,
        })
    }
}

#[test]
fn basics() {
    let mut set = SortedSet::default();
    assert_eq!(
        set.insert(OwnedBytes::from(b"first"), Score::Unsigned(2)),
        None
    );
    assert_eq!(set.score(b"first"), Some(&Score::Unsigned(2)));
    assert_eq!(
        set.insert(OwnedBytes::from(b"first"), Score::Unsigned(1)),
        Some(Score::Unsigned(2))
    );
    assert_eq!(set.score(b"first"), Some(&Score::Unsigned(1)));
    assert_eq!(
        set.insert(OwnedBytes::from(b"second"), Score::Unsigned(1)),
        None
    );
    assert_eq!(set.score(b"second"), Some(&Score::Unsigned(1)));

    println!("With 2: {:?}", set);

    assert_eq!(set.insert(OwnedBytes::from(b"a"), Score::Unsigned(2)), None);
    assert_eq!(set.len(), 3);
    assert_eq!(set.score(b"a"), Some(&Score::Unsigned(2)));
    assert_eq!(set[0].value, b"first");
    assert_eq!(set[1].value, b"second");
    assert_eq!(set[2].value, b"a");
    assert_eq!(set.remove(b"first"), Some(Score::Unsigned(1)));
    assert_eq!(set.remove(b"first"), None);
    assert_eq!(set[0].value, b"second");
    assert_eq!(set[1].value, b"a");
}

#[test]
fn serialization() {
    let mut set = SortedSet::default();
    set.insert(OwnedBytes::from(b"a"), Score::Signed(2));
    set.insert(OwnedBytes::from(b"b"), Score::Unsigned(1));
    set.insert(OwnedBytes::from(b"c"), Score::Float(0.));
    let as_bytes = pot::to_vec(&set).unwrap();
    let deserialized = pot::from_slice::<SortedSet>(&as_bytes).unwrap();
    assert_eq!(deserialized.score(b"a"), set.score(b"a"));
    assert_eq!(deserialized.score(b"b"), set.score(b"b"));
    assert_eq!(deserialized.score(b"c"), set.score(b"c"));
    assert_eq!(deserialized[0].value, b"c");
    assert_eq!(deserialized[1].value, b"b");
    assert_eq!(deserialized[2].value, b"a");
}
