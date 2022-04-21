use std::{
    borrow::Cow,
    fmt::Debug,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use ordered_varint::Variable;
use serde::{Deserialize, Serialize};

use crate::key::{
    time::limited::{BonsaiEpoch, UnixEpoch},
    Key, KeyEncoding,
};

impl<'a> Key<'a> for Duration {
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        let merged = u128::decode_variable(bytes).map_err(|_| TimeError::InvalidValue)?;
        let seconds = u64::try_from(merged >> 30).map_err(|_| TimeError::DeltaNotRepresentable)?;
        let nanos = u32::try_from(merged & (2_u128.pow(30) - 1)).unwrap();
        Ok(Self::new(seconds, nanos))
    }
}

impl<'a> KeyEncoding<'a, Self> for Duration {
    type Error = TimeError;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        let merged = u128::from(self.as_secs()) << 30 | u128::from(self.subsec_nanos());
        // It's safe to unwrap here, because under the hood ordered-varint can
        // only raise an error if the top bits are set. Since we only ever add
        // 94 bits, the top bits will not have any data set.
        Ok(Cow::Owned(merged.to_variable_vec().unwrap()))
    }
}

#[test]
fn duration_key_tests() {
    assert_eq!(
        Duration::ZERO,
        Duration::from_ord_bytes(&Duration::ZERO.as_ord_bytes().unwrap()).unwrap()
    );
    assert_eq!(
        Duration::MAX,
        Duration::from_ord_bytes(&Duration::MAX.as_ord_bytes().unwrap()).unwrap()
    );
}

impl<'a> Key<'a> for SystemTime {
    fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        let since_epoch = Duration::from_ord_bytes(bytes)?;
        UNIX_EPOCH
            .checked_add(since_epoch)
            .ok_or(TimeError::DeltaNotRepresentable)
    }
}

impl<'a> KeyEncoding<'a, Self> for SystemTime {
    type Error = TimeError;

    const LENGTH: Option<usize> = None;

    fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
        let since_epoch = self.duration_since(UNIX_EPOCH).unwrap();
        match since_epoch.as_ord_bytes()? {
            Cow::Owned(bytes) => Ok(Cow::Owned(bytes)),
            Cow::Borrowed(_) => unreachable!(),
        }
    }
}

#[test]
fn system_time_tests() {
    assert_eq!(
        UNIX_EPOCH,
        SystemTime::from_ord_bytes(&UNIX_EPOCH.as_ord_bytes().unwrap()).unwrap()
    );
    let now = SystemTime::now();
    assert_eq!(
        now,
        SystemTime::from_ord_bytes(&now.as_ord_bytes().unwrap()).unwrap()
    );
}

/// An error that indicates that the stored timestamp is unable to be converted
/// to the destination type without losing data.
#[derive(thiserror::Error, Debug)]
#[error("the stored timestamp is outside the allowed range")]
pub struct DeltaNotRepresentable;

/// Errors that can arise from parsing times serialized with [`Key`].
#[derive(thiserror::Error, Debug, Clone, Serialize, Deserialize)]
pub enum TimeError {
    /// An error that indicates that the stored timestamp is unable to be converted
    /// to the destination type without losing data.
    #[error("the stored timestamp is outside the allowed range")]
    DeltaNotRepresentable,
    /// The value stored was not encoded correctly.
    #[error("invalid value")]
    InvalidValue,
}

impl From<DeltaNotRepresentable> for TimeError {
    fn from(_: DeltaNotRepresentable) -> Self {
        Self::DeltaNotRepresentable
    }
}

impl From<std::io::Error> for TimeError {
    fn from(_: std::io::Error) -> Self {
        Self::InvalidValue
    }
}

/// Types for storing limited-precision Durations.
pub mod limited {
    use std::{
        borrow::Cow,
        fmt::{self, Debug, Display, Write},
        hash::Hash,
        marker::PhantomData,
        str::FromStr,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use derive_where::derive_where;
    use ordered_varint::Variable;
    use serde::{Deserialize, Serialize};

    use crate::key::{time::TimeError, Key, KeyEncoding};

    /// A [`Duration`] of time stored with a limited `Resolution`. This type may be
    /// preferred to [`std::time::Duration`] because `Duration` takes a full 12
    /// bytes to achieve its nanosecond resolution.
    ///
    /// Converting from [`Duration`] truncates the duration and performs no rounding.
    ///
    /// The `Resolution` type controls the storage size. The resolutions
    /// provided by BonsaiDb:
    ///
    /// - [`Weeks`]
    /// - [`Days`]
    /// - [`Hours`]
    /// - [`Minutes`]
    /// - [`Seconds`]
    /// - [`Milliseconds`]
    /// - [`Microseconds`]
    /// - [`Nanoseconds`]
    ///
    /// Other resolutions can be used by implementing [`TimeResolution`].
    #[derive_where(Hash, Eq, PartialEq, Ord, PartialOrd, Clone, Copy)]
    pub struct LimitedResolutionDuration<Resolution: TimeResolution> {
        representation: Resolution::Representation,
        _resolution: PhantomData<Resolution>,
    }

    /// A resolution of a time measurement.
    pub trait TimeResolution: Debug + Send + Sync {
        /// The in-memory and serialized representation for this resolution.
        type Representation: Variable
            + Serialize
            + for<'de> Deserialize<'de>
            + Display
            + Hash
            + Eq
            + PartialEq
            + Ord
            + PartialOrd
            + Clone
            + Copy
            + Send
            + Sync
            + Debug
            + Default;

        /// The label used when formatting times with this resolution.
        const FORMAT_SUFFIX: &'static str;

        /// Converts a [`Self::Representation`] to [`Duration`].
        fn repr_to_duration(value: Self::Representation) -> Result<SignedDuration, TimeError>;

        /// Converts a [`Duration`] to [`Self::Representation`].
        fn duration_to_repr(duration: SignedDuration) -> Result<Self::Representation, TimeError>;
    }

    /// A [`Duration`] that can be either negative or positive.
    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    pub enum SignedDuration {
        /// A duration representing a positive measurement of time.
        Positive(Duration),
        /// A duration representing a negative measurement of time.
        Negative(Duration),
    }

    impl<Resolution> LimitedResolutionDuration<Resolution>
    where
        Resolution: TimeResolution,
    {
        /// Returns a new instance with the `representation` provided, which
        /// conceptually is a unit of `Resolution`.
        pub fn new(representation: Resolution::Representation) -> Self {
            Self {
                representation,
                _resolution: PhantomData,
            }
        }

        /// Returns the internal representation of this duration.
        pub fn representation(&self) -> Resolution::Representation {
            self.representation
        }
    }

    impl<Resolution: TimeResolution> Debug for LimitedResolutionDuration<Resolution> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{:?}{}", self.representation, Resolution::FORMAT_SUFFIX)
        }
    }

    impl<Resolution: TimeResolution> Display for LimitedResolutionDuration<Resolution> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}{}", self.representation, Resolution::FORMAT_SUFFIX)
        }
    }

    impl<'a, Resolution> Key<'a> for LimitedResolutionDuration<Resolution>
    where
        Resolution: TimeResolution,
    {
        fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
            let representation = <Resolution::Representation as Variable>::decode_variable(bytes)
                .map_err(|_| TimeError::InvalidValue)?;

            Ok(Self {
                representation,
                _resolution: PhantomData,
            })
        }
    }

    impl<'a, Resolution> KeyEncoding<'a, Self> for LimitedResolutionDuration<Resolution>
    where
        Resolution: TimeResolution,
    {
        type Error = TimeError;

        const LENGTH: Option<usize> = None;

        fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
            self.representation
                .to_variable_vec()
                .map(Cow::Owned)
                .map_err(|_| TimeError::InvalidValue)
        }
    }

    impl<Resolution> Default for LimitedResolutionDuration<Resolution>
    where
        Resolution: TimeResolution,
    {
        fn default() -> Self {
            Self {
                representation: <Resolution::Representation as Default>::default(),
                _resolution: PhantomData,
            }
        }
    }

    impl<Resolution> Serialize for LimitedResolutionDuration<Resolution>
    where
        Resolution: TimeResolution,
    {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            self.representation.serialize(serializer)
        }
    }

    impl<'de, Resolution> Deserialize<'de> for LimitedResolutionDuration<Resolution>
    where
        Resolution: TimeResolution,
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            <Resolution::Representation as Deserialize<'de>>::deserialize(deserializer)
                .map(Self::new)
        }
    }

    impl<Resolution> TryFrom<SignedDuration> for LimitedResolutionDuration<Resolution>
    where
        Resolution: TimeResolution,
    {
        type Error = TimeError;

        fn try_from(duration: SignedDuration) -> Result<Self, Self::Error> {
            Resolution::duration_to_repr(duration).map(|representation| Self {
                representation,
                _resolution: PhantomData,
            })
        }
    }

    impl<Resolution> TryFrom<LimitedResolutionDuration<Resolution>> for SignedDuration
    where
        Resolution: TimeResolution,
    {
        type Error = TimeError;

        fn try_from(value: LimitedResolutionDuration<Resolution>) -> Result<Self, Self::Error> {
            Resolution::repr_to_duration(value.representation)
        }
    }

    impl<Resolution> TryFrom<Duration> for LimitedResolutionDuration<Resolution>
    where
        Resolution: TimeResolution,
    {
        type Error = TimeError;

        fn try_from(duration: Duration) -> Result<Self, Self::Error> {
            Self::try_from(SignedDuration::Positive(duration))
        }
    }

    impl<Resolution> TryFrom<LimitedResolutionDuration<Resolution>> for Duration
    where
        Resolution: TimeResolution,
    {
        type Error = TimeError;

        fn try_from(value: LimitedResolutionDuration<Resolution>) -> Result<Self, Self::Error> {
            match SignedDuration::try_from(value) {
                Ok(SignedDuration::Positive(value)) => Ok(value),
                _ => Err(TimeError::DeltaNotRepresentable),
            }
        }
    }

    /// A [`TimeResolution`] implementation that preserves nanosecond
    /// resolution. Internally, the number of microseconds is represented as an
    /// `i64`, allowing a range of +/- ~292.5 years.
    #[derive(Debug)]
    pub enum Nanoseconds {}

    impl TimeResolution for Nanoseconds {
        type Representation = i64;

        const FORMAT_SUFFIX: &'static str = "ns";

        fn repr_to_duration(value: Self::Representation) -> Result<SignedDuration, TimeError> {
            if let Ok(unsigned) = u64::try_from(value) {
                Ok(SignedDuration::Positive(Duration::from_nanos(unsigned)))
            } else if let Some(positive) = value
                .checked_abs()
                .and_then(|value| u64::try_from(value).ok())
            {
                Ok(SignedDuration::Negative(Duration::from_nanos(positive)))
            } else {
                Err(TimeError::DeltaNotRepresentable)
            }
        }

        fn duration_to_repr(duration: SignedDuration) -> Result<Self::Representation, TimeError> {
            match duration {
                SignedDuration::Positive(duration) => {
                    i64::try_from(duration.as_nanos()).map_err(|_| TimeError::DeltaNotRepresentable)
                }
                SignedDuration::Negative(duration) => i64::try_from(duration.as_nanos())
                    .map(|repr| -repr)
                    .map_err(|_| TimeError::DeltaNotRepresentable),
            }
        }
    }

    /// A [`TimeResolution`] implementation that truncates time measurements to
    /// microseconds. Internally, the number of microseconds is represented as
    /// an `i64`, allowing a range of +/- ~292,471 years.
    #[derive(Debug)]
    pub enum Microseconds {}

    impl TimeResolution for Microseconds {
        type Representation = i64;

        const FORMAT_SUFFIX: &'static str = "us";

        fn repr_to_duration(value: Self::Representation) -> Result<SignedDuration, TimeError> {
            if let Ok(unsigned) = u64::try_from(value) {
                Ok(SignedDuration::Positive(Duration::from_micros(unsigned)))
            } else if let Some(positive) = value
                .checked_abs()
                .and_then(|value| u64::try_from(value).ok())
            {
                Ok(SignedDuration::Negative(Duration::from_micros(positive)))
            } else {
                Err(TimeError::DeltaNotRepresentable)
            }
        }

        fn duration_to_repr(duration: SignedDuration) -> Result<Self::Representation, TimeError> {
            match duration {
                SignedDuration::Positive(duration) => i64::try_from(duration.as_micros())
                    .map_err(|_| TimeError::DeltaNotRepresentable),
                SignedDuration::Negative(duration) => {
                    let rounded_up = duration
                        .checked_add(Duration::from_nanos(999))
                        .ok_or(TimeError::DeltaNotRepresentable)?;
                    i64::try_from(rounded_up.as_micros())
                        .map(|repr| -repr)
                        .map_err(|_| TimeError::DeltaNotRepresentable)
                }
            }
        }
    }

    /// A [`TimeResolution`] implementation that truncates time measurements to
    /// milliseconds. Internally, the number of milliseconds is represented as
    /// an `i64`, allowing a range of +/- ~292.5 million years.
    #[derive(Debug)]
    pub enum Milliseconds {}

    impl TimeResolution for Milliseconds {
        type Representation = i64;

        const FORMAT_SUFFIX: &'static str = "ms";

        fn repr_to_duration(value: Self::Representation) -> Result<SignedDuration, TimeError> {
            if let Ok(unsigned) = u64::try_from(value) {
                Ok(SignedDuration::Positive(Duration::from_millis(unsigned)))
            } else if let Some(positive) = value
                .checked_abs()
                .and_then(|value| u64::try_from(value).ok())
            {
                Ok(SignedDuration::Negative(Duration::from_millis(positive)))
            } else {
                Err(TimeError::DeltaNotRepresentable)
            }
        }

        fn duration_to_repr(duration: SignedDuration) -> Result<Self::Representation, TimeError> {
            match duration {
                SignedDuration::Positive(duration) => i64::try_from(duration.as_millis())
                    .map_err(|_| TimeError::DeltaNotRepresentable),
                SignedDuration::Negative(duration) => {
                    let rounded_up = duration
                        .checked_add(Duration::from_nanos(999_999))
                        .ok_or(TimeError::DeltaNotRepresentable)?;
                    i64::try_from(rounded_up.as_millis())
                        .map(|repr| -repr)
                        .map_err(|_| TimeError::DeltaNotRepresentable)
                }
            }
        }
    }

    /// A [`TimeResolution`] implementation that truncates time measurements to
    /// seconds. Internally, the number of seconds is represented as an `i64`,
    /// allowing a range of +/- ~21 times the age of the universe.
    #[derive(Debug)]
    pub enum Seconds {}

    impl TimeResolution for Seconds {
        type Representation = i64;

        const FORMAT_SUFFIX: &'static str = "s";

        fn repr_to_duration(value: Self::Representation) -> Result<SignedDuration, TimeError> {
            if let Ok(unsigned) = u64::try_from(value) {
                Ok(SignedDuration::Positive(Duration::from_secs(unsigned)))
            } else if let Some(positive) = value
                .checked_abs()
                .and_then(|value| u64::try_from(value).ok())
            {
                Ok(SignedDuration::Negative(Duration::from_secs(positive)))
            } else {
                Err(TimeError::DeltaNotRepresentable)
            }
        }

        fn duration_to_repr(duration: SignedDuration) -> Result<Self::Representation, TimeError> {
            match duration {
                SignedDuration::Positive(duration) => {
                    i64::try_from(duration.as_secs()).map_err(|_| TimeError::DeltaNotRepresentable)
                }
                SignedDuration::Negative(duration) => {
                    let rounded_up = duration
                        .checked_add(Duration::from_nanos(999_999_999))
                        .ok_or(TimeError::DeltaNotRepresentable)?;
                    i64::try_from(rounded_up.as_secs())
                        .map(|repr| -repr)
                        .map_err(|_| TimeError::DeltaNotRepresentable)
                }
            }
        }
    }

    /// A [`TimeResolution`] implementation that truncates time measurements to
    /// minutes. Internally, the number of minutes is represented as an `i32`,
    /// allowing a range of +/- ~4,086 years.
    #[derive(Debug)]
    pub enum Minutes {}

    impl TimeResolution for Minutes {
        type Representation = i32;

        const FORMAT_SUFFIX: &'static str = "m";

        fn repr_to_duration(value: Self::Representation) -> Result<SignedDuration, TimeError> {
            if let Ok(unsigned) = u64::try_from(value) {
                Ok(SignedDuration::Positive(Duration::from_secs(unsigned * 60)))
            } else {
                let positive = u64::try_from(i64::from(value).abs()).unwrap();
                Ok(SignedDuration::Negative(Duration::from_secs(positive * 60)))
            }
        }

        fn duration_to_repr(duration: SignedDuration) -> Result<Self::Representation, TimeError> {
            match duration {
                SignedDuration::Positive(duration) => i32::try_from(duration.as_secs() / 60)
                    .map_err(|_| TimeError::DeltaNotRepresentable),
                SignedDuration::Negative(duration) => i32::try_from((duration.as_secs() + 59) / 60)
                    .map(|repr| -repr)
                    .map_err(|_| TimeError::DeltaNotRepresentable),
            }
        }
    }

    /// A [`TimeResolution`] implementation that truncates time measurements to
    /// hours. Internally, the number of hours is represented as an `i32`,
    /// allowing a range of +/- ~245,147 years.
    #[derive(Debug)]
    pub enum Hours {}

    impl TimeResolution for Hours {
        type Representation = i32;

        const FORMAT_SUFFIX: &'static str = "h";

        fn repr_to_duration(value: Self::Representation) -> Result<SignedDuration, TimeError> {
            if let Ok(unsigned) = u64::try_from(value) {
                Ok(SignedDuration::Positive(Duration::from_secs(
                    unsigned * 60 * 60,
                )))
            } else {
                let positive = u64::try_from(i64::from(value).abs()).unwrap();
                Ok(SignedDuration::Negative(Duration::from_secs(
                    positive * 60 * 60,
                )))
            }
        }

        fn duration_to_repr(duration: SignedDuration) -> Result<Self::Representation, TimeError> {
            const FACTOR: u64 = 60 * 60;
            match duration {
                SignedDuration::Positive(duration) => i32::try_from(duration.as_secs() / FACTOR)
                    .map_err(|_| TimeError::DeltaNotRepresentable),
                SignedDuration::Negative(duration) => {
                    i32::try_from((duration.as_secs() + FACTOR - 1) / FACTOR)
                        .map(|repr| -repr)
                        .map_err(|_| TimeError::DeltaNotRepresentable)
                }
            }
        }
    }

    /// A [`TimeResolution`] implementation that truncates time measurements to
    /// days. Internally, the number of days is represented as an `i32`,
    /// allowing a range of +/- ~5.88 million years.
    #[derive(Debug)]
    pub enum Days {}

    impl TimeResolution for Days {
        type Representation = i32;

        const FORMAT_SUFFIX: &'static str = "d";

        fn repr_to_duration(value: Self::Representation) -> Result<SignedDuration, TimeError> {
            if let Ok(unsigned) = u64::try_from(value) {
                Ok(SignedDuration::Positive(Duration::from_secs(
                    unsigned * 24 * 60 * 60,
                )))
            } else {
                let positive = u64::try_from(i64::from(value).abs()).unwrap();
                Ok(SignedDuration::Negative(Duration::from_secs(
                    positive * 24 * 60 * 60,
                )))
            }
        }

        fn duration_to_repr(duration: SignedDuration) -> Result<Self::Representation, TimeError> {
            const FACTOR: u64 = 24 * 60 * 60;
            match duration {
                SignedDuration::Positive(duration) => i32::try_from(duration.as_secs() / FACTOR)
                    .map_err(|_| TimeError::DeltaNotRepresentable),
                SignedDuration::Negative(duration) => {
                    Ok(i32::try_from((duration.as_secs() + FACTOR - 1) / FACTOR)
                        .map_or(i32::MIN, |repr| -repr))
                }
            }
        }
    }

    /// A [`TimeResolution`] implementation that truncates time measurements to
    /// weeks. Internally, the number of weeks is represented as an `i32`,
    /// allowing a range of +/- ~41.18 million years.
    #[derive(Debug)]
    pub enum Weeks {}

    impl TimeResolution for Weeks {
        type Representation = i32;

        const FORMAT_SUFFIX: &'static str = "w";

        fn repr_to_duration(value: Self::Representation) -> Result<SignedDuration, TimeError> {
            if let Ok(unsigned) = u64::try_from(value) {
                Ok(SignedDuration::Positive(Duration::from_secs(
                    unsigned * 7 * 24 * 60 * 60,
                )))
            } else {
                let positive = u64::try_from(i64::from(value).abs()).unwrap();
                Ok(SignedDuration::Negative(Duration::from_secs(
                    positive * 7 * 24 * 60 * 60,
                )))
            }
        }

        fn duration_to_repr(duration: SignedDuration) -> Result<Self::Representation, TimeError> {
            const FACTOR: u64 = 7 * 24 * 60 * 60;
            match duration {
                SignedDuration::Positive(duration) => i32::try_from(duration.as_secs() / FACTOR)
                    .map_err(|_| TimeError::DeltaNotRepresentable),
                SignedDuration::Negative(duration) => {
                    i32::try_from((duration.as_secs() + FACTOR - 1) / FACTOR)
                        .map(|repr| -repr)
                        .map_err(|_| TimeError::DeltaNotRepresentable)
                }
            }
        }
    }

    #[test]
    fn limited_resolution_duration_tests() {
        fn test_limited<Resolution: TimeResolution>(
            duration: Duration,
            expected_step: Resolution::Representation,
        ) {
            let limited = LimitedResolutionDuration::<Resolution>::try_from(duration).unwrap();
            assert_eq!(limited.representation, expected_step);
            let encoded = limited.as_ord_bytes().unwrap();
            println!("Encoded {:?} to {} bytes", limited, encoded.len());
            let decoded = LimitedResolutionDuration::from_ord_bytes(&encoded).unwrap();
            assert_eq!(limited, decoded);
        }

        fn test_eq_limited<Resolution: TimeResolution>(
            duration: Duration,
            expected_step: Resolution::Representation,
        ) {
            test_limited::<Resolution>(duration, expected_step);
            let limited = LimitedResolutionDuration::<Resolution>::try_from(duration).unwrap();
            assert_eq!(duration, Duration::try_from(limited).unwrap());
        }

        let truncating_seconds = 7 * 24 * 60 * 60 + 24 * 60 * 60 + 60 * 60 + 60 + 1;
        let truncating = Duration::new(u64::try_from(truncating_seconds).unwrap(), 987_654_321);
        test_limited::<Weeks>(truncating, 1);
        test_limited::<Days>(truncating, 8);
        test_limited::<Hours>(truncating, 8 * 24 + 1);
        test_limited::<Minutes>(truncating, 8 * 24 * 60 + 60 + 1);
        test_limited::<Seconds>(truncating, 8 * 24 * 60 * 60 + 60 * 60 + 60 + 1);
        test_limited::<Milliseconds>(truncating, truncating_seconds * 1_000 + 987);
        test_limited::<Microseconds>(truncating, truncating_seconds * 1_000_000 + 987_654);

        let forty_two_days = Duration::from_secs(42 * 24 * 60 * 60);
        test_eq_limited::<Weeks>(forty_two_days, 6);
        test_eq_limited::<Days>(forty_two_days, 42);
        test_eq_limited::<Hours>(forty_two_days, 42 * 24);
        test_eq_limited::<Minutes>(forty_two_days, 42 * 24 * 60);
        test_eq_limited::<Seconds>(forty_two_days, 42 * 24 * 60 * 60);
        test_eq_limited::<Milliseconds>(forty_two_days, 42 * 24 * 60 * 60 * 1_000);
        test_eq_limited::<Microseconds>(forty_two_days, 42 * 24 * 60 * 60 * 1_000_000);
    }

    /// A timestamp (moment in time) stored with a limited `Resolution`. This
    /// type may be preferred to [`std::time::SystemTime`] because `SystemTime`
    /// serializes with nanosecond resolution. Often this level of precision is
    /// not needed and less storage and memory can be used.
    ///
    /// This type stores the representation of the timestamp as a
    /// [`LimitedResolutionDuration`] relative to `Epoch`.
    ///
    /// The `Resolution` type controls the storage size. The resolutions
    /// provided by BonsaiDb:
    ///
    /// - [`Weeks`]
    /// - [`Days`]
    /// - [`Hours`]
    /// - [`Minutes`]
    /// - [`Seconds`]
    /// - [`Milliseconds`]
    /// - [`Microseconds`]
    /// - [`Nanoseconds`]
    ///
    /// Other resolutions can be used by implementing [`TimeResolution`].
    /// BonsaiDb provides two [`TimeEpoch`] implementations:
    ///
    /// - [`UnixEpoch`]
    /// - [`BonsaiEpoch`]
    #[derive_where(Hash, Eq, PartialEq, Ord, PartialOrd, Clone, Copy)]
    pub struct LimitedResolutionTimestamp<Resolution: TimeResolution, Epoch: TimeEpoch>(
        LimitedResolutionDuration<Resolution>,
        PhantomData<Epoch>,
    );

    impl<Resolution, Epoch> Default for LimitedResolutionTimestamp<Resolution, Epoch>
    where
        Resolution: TimeResolution,
        Epoch: TimeEpoch,
    {
        fn default() -> Self {
            Self(LimitedResolutionDuration::default(), PhantomData)
        }
    }

    impl<Resolution, Epoch> Serialize for LimitedResolutionTimestamp<Resolution, Epoch>
    where
        Resolution: TimeResolution,
        Epoch: TimeEpoch,
    {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            self.0.serialize(serializer)
        }
    }

    impl<'de, Resolution, Epoch> Deserialize<'de> for LimitedResolutionTimestamp<Resolution, Epoch>
    where
        Resolution: TimeResolution,
        Epoch: TimeEpoch,
    {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            LimitedResolutionDuration::deserialize(deserializer)
                .map(|duration| Self(duration, PhantomData))
        }
    }

    /// An epoch for [`LimitedResolutionTimestamp`].
    pub trait TimeEpoch: Sized + Send + Sync {
        /// The offset from [`UNIX_EPOCH`] for this epoch.
        fn epoch_offset() -> Duration;
    }

    impl<Resolution, Epoch> LimitedResolutionTimestamp<Resolution, Epoch>
    where
        Resolution: TimeResolution,
        Epoch: TimeEpoch,
    {
        /// Returns [`SystemTime::now()`] limited to `Resolution`. The timestamp
        /// will be truncated, not rounded.
        #[must_use]
        pub fn now() -> Self {
            Self::try_from(SystemTime::now()).expect("now should always be representable")
        }

        /// Returns the duration since another timestamp. This returns None if
        /// `other` is before `self`,
        pub fn duration_since(
            &self,
            other: &impl AnyTimestamp,
        ) -> Result<Option<Duration>, TimeError> {
            let self_delta = self.duration_since_unix_epoch()?;
            let other_delta = other.duration_since_unix_epoch()?;
            Ok(self_delta.checked_sub(other_delta))
        }

        /// Returns the absolute duration between `self` and `other`.
        pub fn duration_between(&self, other: &impl AnyTimestamp) -> Result<Duration, TimeError> {
            let self_delta = self.duration_since_unix_epoch()?;
            let other_delta = other.duration_since_unix_epoch()?;
            if self_delta < other_delta {
                Ok(other_delta - self_delta)
            } else {
                Ok(self_delta - other_delta)
            }
        }

        /// Returns the internal representation of this timestamp, which is a
        /// unit of `Resolution`.
        pub fn representation(&self) -> Resolution::Representation {
            self.0.representation()
        }

        /// Returns a new timestamp using the `representation` provided.
        pub fn from_representation(representation: Resolution::Representation) -> Self {
            Self::from(LimitedResolutionDuration::new(representation))
        }

        /// Converts this value to a a decimal string containing the number of
        /// seconds since the unix epoch (January 1, 1970 00:00:00 UTC).
        ///
        /// The resulting string can be parsed as well.
        ///
        /// ```rust
        /// use bonsaidb_core::key::time::limited::{
        ///     BonsaiEpoch, LimitedResolutionTimestamp, Milliseconds,
        /// };
        ///
        /// let now = LimitedResolutionTimestamp::<Milliseconds, BonsaiEpoch>::now();
        /// let timestamp = now.to_timestamp_string().unwrap();
        /// let parsed = timestamp.parse().unwrap();
        /// assert_eq!(now, parsed);
        /// ```
        ///
        /// The difference between this function and `to_string()`] is that
        /// `to_string()` will revert to using the underlying
        /// [`LimitedResolutionDuration`]'s `to_string()` if a value is unable
        /// to be converted to a value relative to the unix epoch.
        pub fn to_timestamp_string(&self) -> Result<String, TimeError> {
            let mut string = String::new();
            self.display(&mut string).map(|_| string)
        }

        fn display(&self, f: &mut impl Write) -> Result<(), TimeError> {
            let since_epoch = self.duration_since_unix_epoch()?;
            write!(f, "{}", since_epoch.as_secs()).map_err(|_| TimeError::InvalidValue)?;
            if since_epoch.subsec_nanos() > 0 {
                if since_epoch.subsec_nanos() % 1_000_000 == 0 {
                    // Rendering any precision beyond milliseconds will yield 0s
                    write!(f, ".{:03}", since_epoch.subsec_millis())
                        .map_err(|_| TimeError::InvalidValue)
                } else if since_epoch.subsec_nanos() % 1_000 == 0 {
                    write!(f, ".{:06}", since_epoch.subsec_micros())
                        .map_err(|_| TimeError::InvalidValue)
                } else {
                    write!(f, ".{:09}", since_epoch.subsec_nanos())
                        .map_err(|_| TimeError::InvalidValue)
                }
            } else {
                // No subsecond
                Ok(())
            }
        }
    }

    /// A timestamp that can report it sduration since the Unix Epoch.
    pub trait AnyTimestamp {
        /// Returns the [`Duration`] since January 1, 1970 00:00:00 UTC for this
        /// timestamp.
        fn duration_since_unix_epoch(&self) -> Result<Duration, TimeError>;
    }

    impl AnyTimestamp for SystemTime {
        fn duration_since_unix_epoch(&self) -> Result<Duration, TimeError> {
            Ok(self.duration_since(UNIX_EPOCH).unwrap())
        }
    }

    impl<Resolution, Epoch> AnyTimestamp for LimitedResolutionTimestamp<Resolution, Epoch>
    where
        Resolution: TimeResolution,
        Epoch: TimeEpoch,
    {
        fn duration_since_unix_epoch(&self) -> Result<Duration, TimeError> {
            let relative_offset = Resolution::repr_to_duration(self.0.representation)?;
            match relative_offset {
                SignedDuration::Positive(offset) => Epoch::epoch_offset()
                    .checked_add(offset)
                    .ok_or(TimeError::DeltaNotRepresentable),
                SignedDuration::Negative(offset) => Epoch::epoch_offset()
                    .checked_sub(offset)
                    .ok_or(TimeError::DeltaNotRepresentable),
            }
        }
    }

    impl<Resolution, Epoch> Debug for LimitedResolutionTimestamp<Resolution, Epoch>
    where
        Resolution: TimeResolution,
        Epoch: TimeEpoch,
    {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "LimitedResolutionTimestamp({self})")
        }
    }

    impl<Resolution, Epoch> Display for LimitedResolutionTimestamp<Resolution, Epoch>
    where
        Resolution: TimeResolution,
        Epoch: TimeEpoch,
    {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.display(f).or_else(|_| Display::fmt(&self.0, f))
        }
    }

    impl<Resolution, Epoch> FromStr for LimitedResolutionTimestamp<Resolution, Epoch>
    where
        Resolution: TimeResolution,
        Epoch: TimeEpoch,
    {
        type Err = TimeError;

        fn from_str(s: &str) -> Result<Self, Self::Err> {
            let mut parts = s.split('.');
            let seconds = parts.next().ok_or(TimeError::InvalidValue)?;
            let seconds = seconds
                .parse::<u64>()
                .map_err(|_| TimeError::InvalidValue)?;

            let duration = if let Some(subseconds_str) = parts.next() {
                if subseconds_str.len() > 9 || parts.next().is_some() {
                    return Err(TimeError::InvalidValue);
                }
                let subseconds = subseconds_str
                    .parse::<u32>()
                    .map_err(|_| TimeError::InvalidValue)?;

                let nanos =
                    subseconds * 10_u32.pow(u32::try_from(9 - subseconds_str.len()).unwrap());
                Duration::new(seconds, nanos)
            } else {
                Duration::from_secs(seconds)
            };

            let epoch = Epoch::epoch_offset();
            let duration = if duration < epoch {
                SignedDuration::Negative(epoch - duration)
            } else {
                SignedDuration::Positive(duration - epoch)
            };
            Ok(Self::from(
                LimitedResolutionDuration::<Resolution>::try_from(duration)?,
            ))
        }
    }

    #[test]
    fn timestamp_parse_tests() {
        fn test_roundtrip_parsing<Resolution: TimeResolution>() {
            let original = LimitedResolutionTimestamp::<Resolution, BonsaiEpoch>::now();
            let unix_timestamp = original.to_string();
            let parsed = unix_timestamp.parse().unwrap();
            assert_eq!(
                original, parsed,
                "{original} produced {unix_timestamp}, but parsed {parsed}"
            );
        }

        test_roundtrip_parsing::<Weeks>();
        test_roundtrip_parsing::<Days>();
        test_roundtrip_parsing::<Minutes>();
        test_roundtrip_parsing::<Seconds>();
        test_roundtrip_parsing::<Milliseconds>();
        test_roundtrip_parsing::<Microseconds>();
        test_roundtrip_parsing::<Nanoseconds>();
    }

    impl<'a, Resolution, Epoch> Key<'a> for LimitedResolutionTimestamp<Resolution, Epoch>
    where
        Resolution: TimeResolution,
        Epoch: TimeEpoch,
    {
        fn from_ord_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
            let duration = LimitedResolutionDuration::<Resolution>::from_ord_bytes(bytes)?;
            Ok(Self::from(duration))
        }
    }

    impl<'a, Resolution, Epoch> KeyEncoding<'a, Self> for LimitedResolutionTimestamp<Resolution, Epoch>
    where
        Resolution: TimeResolution,
        Epoch: TimeEpoch,
    {
        type Error = TimeError;

        const LENGTH: Option<usize> = None;

        fn as_ord_bytes(&'a self) -> Result<Cow<'a, [u8]>, Self::Error> {
            self.0.as_ord_bytes()
        }
    }

    impl<Resolution, Epoch> From<LimitedResolutionDuration<Resolution>>
        for LimitedResolutionTimestamp<Resolution, Epoch>
    where
        Resolution: TimeResolution,
        Epoch: TimeEpoch,
    {
        fn from(duration: LimitedResolutionDuration<Resolution>) -> Self {
            Self(duration, PhantomData)
        }
    }

    impl<Resolution, Epoch> From<LimitedResolutionTimestamp<Resolution, Epoch>>
        for LimitedResolutionDuration<Resolution>
    where
        Resolution: TimeResolution,
        Epoch: TimeEpoch,
    {
        fn from(time: LimitedResolutionTimestamp<Resolution, Epoch>) -> Self {
            time.0
        }
    }

    impl<Resolution, Epoch> TryFrom<SystemTime> for LimitedResolutionTimestamp<Resolution, Epoch>
    where
        Resolution: TimeResolution,
        Epoch: TimeEpoch,
    {
        type Error = TimeError;
        fn try_from(time: SystemTime) -> Result<Self, TimeError> {
            let epoch = UNIX_EPOCH
                .checked_add(Epoch::epoch_offset())
                .ok_or(TimeError::DeltaNotRepresentable)?;
            match time.duration_since(epoch) {
                Ok(duration) => {
                    LimitedResolutionDuration::try_from(SignedDuration::Positive(duration))
                        .map(Self::from)
                }
                Err(_) => match epoch.duration_since(time) {
                    Ok(duration) => {
                        LimitedResolutionDuration::try_from(SignedDuration::Negative(duration))
                            .map(Self::from)
                    }
                    Err(_) => Err(TimeError::DeltaNotRepresentable),
                },
            }
        }
    }

    impl<Resolution, Epoch> TryFrom<LimitedResolutionTimestamp<Resolution, Epoch>> for SystemTime
    where
        Resolution: TimeResolution,
        Epoch: TimeEpoch,
    {
        type Error = TimeError;

        fn try_from(
            time: LimitedResolutionTimestamp<Resolution, Epoch>,
        ) -> Result<Self, TimeError> {
            let since_epoch = SignedDuration::try_from(time.0)?;
            let epoch = UNIX_EPOCH
                .checked_add(Epoch::epoch_offset())
                .ok_or(TimeError::DeltaNotRepresentable)?;
            let time = match since_epoch {
                SignedDuration::Positive(since_epoch) => epoch.checked_add(since_epoch),
                SignedDuration::Negative(since_epoch) => epoch.checked_sub(since_epoch),
            };

            time.ok_or(TimeError::DeltaNotRepresentable)
        }
    }

    /// A [`TimeEpoch`] implementation that allows storing
    /// [`LimitedResolutionTimestamp`] relative to the "unix epoch": January 1,
    /// 1970 00:00:00 UTC.
    pub struct UnixEpoch;

    impl TimeEpoch for UnixEpoch {
        fn epoch_offset() -> Duration {
            Duration::ZERO
        }
    }

    /// A [`TimeEpoch`] implementation that allows storing
    /// [`LimitedResolutionTimestamp`] relative to the 10-year anniversary of
    /// BonsaiDb: March 20, 2031 04:31:47 UTC.
    ///
    /// ## Why use [`BonsaiEpoch`] instead of [`UnixEpoch`]?
    ///
    /// [`LimitedResolutionTimestamp`] uses [`ordered-varint::Variable`] to
    /// implement [`Key`], which encodes the underlying value in as few bytes as
    /// possible while still preserving the ordering required by [`Key`].
    ///
    /// Many applications are not storing timestamps that predate the
    /// application being developed. When there is a likelihood that timestamps
    /// are closer to "now" than they are to the unix timestamp (January 1, 1970
    /// 00:00:00 UTC), the [`BonsaiEpoch`] will consistently encode the
    /// underlying representation in fewer bytes than when using [`UnixEpoch`].
    ///
    /// We hope BonsaiDb is a viable database option for many years. By setting
    /// this epoch 10 years from the start of BonsaiDb, it allows the internal
    /// representation of timestamps to slowly decrease in size until the
    /// 10-year anniversary. Over the following 10 years, the size will grow
    /// back to the same size it was at its conception, and then slowly grow as
    /// needed from that point on.
    pub struct BonsaiEpoch;

    impl BonsaiEpoch {
        const EPOCH: Duration = Duration::new(1_931_747_507, 0);
    }

    impl TimeEpoch for BonsaiEpoch {
        fn epoch_offset() -> Duration {
            Self::EPOCH
        }
    }

    #[test]
    fn limited_resolution_timestamp_tests() {
        fn test_resolution<Resolution: TimeResolution>(resolution: Duration) {
            let now_in_seconds = LimitedResolutionTimestamp::<Resolution, UnixEpoch>::now();
            let as_system = SystemTime::try_from(now_in_seconds).unwrap();
            let as_limited =
                LimitedResolutionTimestamp::<Resolution, UnixEpoch>::try_from(as_system).unwrap();
            assert_eq!(as_limited, now_in_seconds);

            let now_in_seconds = LimitedResolutionTimestamp::<Resolution, BonsaiEpoch>::now();
            let as_system = SystemTime::try_from(now_in_seconds).unwrap();
            let as_limited =
                LimitedResolutionTimestamp::<Resolution, BonsaiEpoch>::try_from(as_system).unwrap();
            assert_eq!(as_limited, now_in_seconds);

            let slightly_before_epoch = UNIX_EPOCH + BonsaiEpoch::EPOCH
                - Duration::from_nanos(u64::try_from(resolution.as_nanos() / 2).unwrap());
            let unix_epoch_in_recent =
                LimitedResolutionTimestamp::<Resolution, BonsaiEpoch>::try_from(
                    slightly_before_epoch,
                )
                .unwrap();
            let as_system = SystemTime::try_from(unix_epoch_in_recent).unwrap();
            let as_limited =
                LimitedResolutionTimestamp::<Resolution, BonsaiEpoch>::try_from(as_system).unwrap();
            assert!(
                slightly_before_epoch
                    .duration_since(as_system)
                    .expect("timestamp should have been trunctated towards MIN")
                    < resolution
            );
            assert_eq!(as_limited, unix_epoch_in_recent);

            let slightly_after_epoch = UNIX_EPOCH
                + BonsaiEpoch::EPOCH
                + Duration::from_nanos(u64::try_from(resolution.as_nanos() / 2).unwrap());
            let unix_epoch_in_recent =
                LimitedResolutionTimestamp::<Resolution, BonsaiEpoch>::try_from(
                    slightly_after_epoch,
                )
                .unwrap();
            let as_system = SystemTime::try_from(unix_epoch_in_recent).unwrap();
            println!("{slightly_after_epoch:?} converted to {unix_epoch_in_recent} and back as {as_system:?}");
            let as_limited =
                LimitedResolutionTimestamp::<Resolution, BonsaiEpoch>::try_from(as_system).unwrap();
            assert!(
                slightly_after_epoch
                    .duration_since(as_system)
                    .expect("timestamp should have been truncated towards 0")
                    < resolution
            );
            assert_eq!(as_limited, unix_epoch_in_recent);
        }

        test_resolution::<Weeks>(Duration::from_secs(7 * 24 * 60 * 60));
        test_resolution::<Days>(Duration::from_secs(24 * 60 * 60));
        test_resolution::<Hours>(Duration::from_secs(60 * 60));
        test_resolution::<Minutes>(Duration::from_secs(60));
        test_resolution::<Seconds>(Duration::from_secs(1));
        test_resolution::<Milliseconds>(Duration::from_millis(1));
        test_resolution::<Microseconds>(Duration::from_micros(1));
        test_resolution::<Nanoseconds>(Duration::from_nanos(1));
    }

    #[test]
    fn serialization_tests() {
        fn test_serialization<Resolution: TimeResolution>() {
            let original = LimitedResolutionTimestamp::<Resolution, BonsaiEpoch>::now();
            let serialized = pot::to_vec(&original).unwrap();
            let deserialized = pot::from_slice(&serialized).unwrap();
            assert_eq!(original, deserialized);
        }

        test_serialization::<Weeks>();
        test_serialization::<Days>();
        test_serialization::<Hours>();
        test_serialization::<Minutes>();
        test_serialization::<Seconds>();
        test_serialization::<Milliseconds>();
        test_serialization::<Microseconds>();
        test_serialization::<Nanoseconds>();
    }
}

/// A signed duration of time represented in weeks (604,800 seconds).
/// Internally, the number of weeks is represented as an `i32`,
/// allowing a range of +/- ~41.18 million years.
pub type Weeks = limited::LimitedResolutionDuration<limited::Weeks>;

/// A signed duration of time represented in days (86,400 seconds). Internally,
/// the number of days is represented as an `i32`, allowing a range of +/- ~5.88
/// million years.
pub type Days = limited::LimitedResolutionDuration<limited::Days>;

/// A signed duration of time represented in hours (3,600 seconds). Internally,
/// the number of hours is represented as an `i32`, allowing a range of +/-
/// ~245,147 years.
pub type Hours = limited::LimitedResolutionDuration<limited::Hours>;

/// A signed duration of time represented in minutes (60 seconds). Internally,
/// the number of minutes is represented as an `i32`,
/// allowing a range of +/- ~4,086 years.
pub type Minutes = limited::LimitedResolutionDuration<limited::Minutes>;

/// A signed duration of time represented in seconds (with no partial
/// subseconds). Internally, the number of seconds is represented as an `i64`,
/// allowing a range of +/- ~21 times the age of the universe.
pub type Seconds = limited::LimitedResolutionDuration<limited::Seconds>;

/// A signed duration of time represented in milliseconds (1/1,000th of a
/// second). Internally, the number of milliseconds is represented as an `i64`,
/// allowing a range of +/- ~292.5 million years.
pub type Milliseconds = limited::LimitedResolutionDuration<limited::Milliseconds>;

/// A signed duration of time represented in microseconds (1/1,000,000th of a
/// second). Internally, the number of microseconds is represented as an `i64`,
/// allowing a range of +/- ~292,471 years.
pub type Microseconds = limited::LimitedResolutionDuration<limited::Microseconds>;

/// A signed duration of time represented in nanoseconds (1/1,000,000,000th of a
/// second). Internally, the number of microseconds is represented as an `i64`,
/// allowing a range of +/- ~292.5 years.
pub type Nanoseconds = limited::LimitedResolutionDuration<limited::Nanoseconds>;

/// A timestamp stored as the number of weeks (604,800 seconds) relative to
/// [`UnixEpoch`]. Internally, the number of weeks is represented as an `i32`,
/// allowing a range of +/- ~41.18 million years.
pub type WeeksSinceUnixEpoch = limited::LimitedResolutionTimestamp<limited::Weeks, UnixEpoch>;

/// A timestamp stored as the number of days (86,400 seconds) relative to
/// [`UnixEpoch`]. Internally, the number of days is represented as an `i32`,
/// allowing a range of +/- ~5.88 million years.
pub type DaysSinceUnixEpoch = limited::LimitedResolutionTimestamp<limited::Days, UnixEpoch>;

/// A timestamp stored as the number of hours (3,600 seconds) relative to
/// [`UnixEpoch`]. Internally, the number of hours is represented as an `i32`,
/// allowing a range of +/- ~245,147 years.
pub type HoursSinceUnixEpoch = limited::LimitedResolutionTimestamp<limited::Hours, UnixEpoch>;

/// A timestamp stored as the number of minutes (60 seconds) relative to
/// [`UnixEpoch`]. Internally, the number of minutes is represented as an `i32`,
/// allowing a range of +/- ~4,086 years.
pub type MinutesSinceUnixEpoch = limited::LimitedResolutionTimestamp<limited::Minutes, UnixEpoch>;

/// A timestamp stored as the number of seconds (with no partial subseconds)
/// relative to [`UnixEpoch`]. Internally, the number of seconds is represented
/// as an `i64`, allowing a range of +/- ~21 times the age of the universe.
pub type SecondsSinceUnixEpoch = limited::LimitedResolutionTimestamp<limited::Seconds, UnixEpoch>;

/// A timestamp stored as the number of milliseconds (1/1,000th of a second)
/// relative to [`UnixEpoch`]. Internally, the number of milliseconds is
/// represented as an `i64`, allowing a range of +/- ~292.5 million years.
pub type MillisecondsSinceUnixEpoch =
    limited::LimitedResolutionTimestamp<limited::Milliseconds, UnixEpoch>;

/// A timestamp stored as the number of microseconds (1/1,000,000th of a second)
/// relative to [`UnixEpoch`]. Internally, the number of microseconds is
/// represented as an `i64`, allowing a range of +/- ~292,471 years.
pub type MicrosecondsSinceUnixEpoch =
    limited::LimitedResolutionTimestamp<limited::Microseconds, UnixEpoch>;

/// A timestamp stored as the number of nanoseconds (1/1,000,000,000th of a
/// second) relative to [`UnixEpoch`]. Internally, the number of microseconds is
/// represented as an `i64`, allowing a range of +/- ~292.5 years.
pub type NanosecondsSinceUnixEpoch =
    limited::LimitedResolutionTimestamp<limited::Nanoseconds, UnixEpoch>;

/// A timestamp stored as the number of weeks (604,800 seconds) relative to
/// [`BonsaiEpoch`]. Internally, the number of weeks is represented as an `i32`,
/// allowing a range of +/- ~41.18 million years.
pub type TimestampAsWeeks = limited::LimitedResolutionTimestamp<limited::Weeks, BonsaiEpoch>;

/// A timestamp stored as the number of days (86,400 seconds) relative to
/// [`BonsaiEpoch`]. Internally, the number of days is represented as an `i32`,
/// allowing a range of +/- ~5.88 million years.
pub type TimestampAsDays = limited::LimitedResolutionTimestamp<limited::Days, BonsaiEpoch>;

/// A timestamp stored as the number of hours (3,600 seconds) relative to
/// [`BonsaiEpoch`]. Internally, the number of hours is represented as an `i32`,
/// allowing a range of +/- ~245,147 years.
pub type TimestampAsHours = limited::LimitedResolutionTimestamp<limited::Hours, BonsaiEpoch>;

/// A timestamp stored as the number of minutes (60 seconds) relative to
/// [`BonsaiEpoch`]. Internally, the number of minutes is represented as an `i32`,
/// allowing a range of +/- ~4,086 years.
pub type TimestampAsMinutes = limited::LimitedResolutionTimestamp<limited::Minutes, BonsaiEpoch>;

/// A timestamp stored as the number of seconds (with no partial subseconds)
/// relative to [`BonsaiEpoch`]. Internally, the number of seconds is represented
/// as an `i64`, allowing a range of +/- ~21 times the age of the universe.
pub type TimestampAsSeconds = limited::LimitedResolutionTimestamp<limited::Seconds, BonsaiEpoch>;

/// A timestamp stored as the number of milliseconds (1/1,000th of a second)
/// relative to [`BonsaiEpoch`]. Internally, the number of milliseconds is
/// represented as an `i64`, allowing a range of +/- ~292.5 million years.
pub type TimestampAsMilliseconds =
    limited::LimitedResolutionTimestamp<limited::Milliseconds, BonsaiEpoch>;

/// A timestamp stored as the number of microseconds (1/1,000,000th of a second)
/// relative to [`BonsaiEpoch`]. Internally, the number of microseconds is
/// represented as an `i64`, allowing a range of +/- ~292,471 years.
pub type TimestampAsMicroseconds =
    limited::LimitedResolutionTimestamp<limited::Microseconds, BonsaiEpoch>;

/// A timestamp stored as the number of nanoseconds (1/1,000,000,000th of a
/// second) relative to [`BonsaiEpoch`]. Internally, the number of microseconds is
/// represented as an `i64`, allowing a range of +/- ~292.5 years.
pub type TimestampAsNanoseconds =
    limited::LimitedResolutionTimestamp<limited::Nanoseconds, BonsaiEpoch>;
