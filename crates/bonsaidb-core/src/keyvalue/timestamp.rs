use std::{
    borrow::Cow,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::key::{IncorrectByteLength, Key, KeyEncoding};

/// A timestamp relative to [`UNIX_EPOCH`].
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord, Default)]
pub struct Timestamp {
    /// The number of whole seconds since [`UNIX_EPOCH`].
    pub seconds: u64,
    /// The number of nanoseconds in the timestamp.
    pub nanos: u32,
}

impl Timestamp {
    /// The minimum representable Timestamp. This is equivalent to [`UNIX_EPOCH`].
    pub const MIN: Self = Self {
        seconds: 0,
        nanos: 0,
    };

    /// The maximum valid value of Timestamp.
    pub const MAX: Self = Self {
        seconds: u64::MAX,
        nanos: 999_999_999,
    };

    /// Returns the current timestamp according to the OS. Uses [`SystemTime::now()`].
    #[must_use]
    pub fn now() -> Self {
        Self::from(SystemTime::now())
    }
}

impl From<SystemTime> for Timestamp {
    fn from(time: SystemTime) -> Self {
        let duration_since_epoch = time
            .duration_since(UNIX_EPOCH)
            .expect("unrealistic system time");
        Self {
            seconds: duration_since_epoch.as_secs(),
            nanos: duration_since_epoch.subsec_nanos(),
        }
    }
}

impl From<Timestamp> for Duration {
    fn from(t: Timestamp) -> Self {
        Self::new(t.seconds, t.nanos)
    }
}

impl std::ops::Sub for Timestamp {
    type Output = Option<Duration>;

    fn sub(self, rhs: Self) -> Self::Output {
        Duration::from(self).checked_sub(Duration::from(rhs))
    }
}

impl std::ops::Add<Duration> for Timestamp {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        let mut nanos = self.nanos + rhs.subsec_nanos();
        let mut seconds = self.seconds.saturating_add(rhs.as_secs());
        while nanos > 1_000_000_000 {
            nanos -= 1_000_000_000;
            seconds = seconds.saturating_add(1);
        }
        Self { seconds, nanos }
    }
}

impl<'a> Key<'a> for Timestamp {
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() != 12 {
            return Err(IncorrectByteLength);
        }

        Ok(Self {
            seconds: u64::from_ord_bytes(&bytes[0..8])?,
            nanos: u32::from_ord_bytes(&bytes[8..12])?,
        })
    }
}

impl<'a> KeyEncoding<'a, Self> for Timestamp {
    type Error = IncorrectByteLength;

    const LENGTH: Option<usize> = Some(12);

    fn as_ord_bytes(&'a self) -> Result<std::borrow::Cow<'a, [u8]>, Self::Error> {
        let seconds_bytes: &[u8] = &self.seconds.to_be_bytes();
        let nanos_bytes = &self.nanos.to_be_bytes();
        Ok(Cow::Owned([seconds_bytes, nanos_bytes].concat()))
    }
}

#[test]
fn key_test() {
    let original = Timestamp::now();
    assert_eq!(
        Timestamp::from_ord_bytes(&original.as_ord_bytes().unwrap()).unwrap(),
        original
    );
}
