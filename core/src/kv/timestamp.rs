use std::{
    borrow::Cow,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::schema::Key;

/// A timestamp relative to [`UNIX_EPOCH`].
#[derive(Serialize, Deserialize, Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Ord)]
pub struct Timestamp {
    /// The number of whole seconds since [`UNIX_EPOCH`].
    pub seconds: u64,
    /// The number of nanoseconds in the timestamp.
    pub nanos: u32,
}

impl Timestamp {
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
        let mut seconds = self.seconds + rhs.as_secs();
        while nanos > 1_000_000_000 {
            nanos -= 1_000_000_000;
            seconds += 1;
        }
        Self { seconds, nanos }
    }
}

impl Key for Timestamp {
    fn as_big_endian_bytes(&self) -> anyhow::Result<std::borrow::Cow<'_, [u8]>> {
        let seconds_bytes: &[u8] = &self.seconds.to_be_bytes();
        let nanos_bytes = &self.nanos.to_be_bytes();
        Ok(Cow::Owned([seconds_bytes, nanos_bytes].concat()))
    }

    fn from_big_endian_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        if bytes.len() != 12 {
            anyhow::bail!("invalid length of stored bytes for Timestamp");
        }

        Ok(Self {
            seconds: u64::from_big_endian_bytes(&bytes[0..8])?,
            nanos: u32::from_big_endian_bytes(&bytes[8..12])?,
        })
    }
}
