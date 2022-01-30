use std::time::Duration;

use argon2::Algorithm;
use sysinfo::{System, SystemExt};

use crate::config::SystemDefault;

/// Password hashing configuration.
///
/// `BonsaiDb` uses [`argon2`](https://crates.io/crates/argon2) for its password hashing.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ArgonConfiguration {
    /// The number of concurrent hashing operations that are allowed to take place.
    pub hashers: u32,
    /// The algorithm variation to use. Most users should select
    /// [`Algorithm::Argon2id`].
    pub algorithm: Algorithm,
    /// The parameters for each hasher.
    pub params: ArgonParams,
}

impl SystemDefault for ArgonConfiguration {
    fn default_for(system: &System) -> Self {
        let cpu_count = u32::try_from(
            system
                .physical_core_count()
                .unwrap_or_else(|| system.processors().len()),
        )
        .expect("cpu count returned unexpectedly large value");
        let mut hashers = (cpu_count + 3) / 4;
        let max_hashers = u32::try_from(
            system.total_memory() / u64::from(TimedArgonParams::MINIMUM_RAM_PER_HASHER),
        )
        .unwrap_or(u32::MAX);
        // total_memory() can return 0, so we need to ensure max_hashers isn't
        // 0.
        if max_hashers > 0 && hashers > max_hashers {
            hashers = max_hashers;
        }

        ArgonConfiguration {
            hashers,
            algorithm: Algorithm::Argon2id,
            params: ArgonParams::default_for(system, hashers),
        }
    }
}

/// [Argon2id](https://crates.io/crates/argon2) base parameters.
#[derive(Debug, Clone)]
#[non_exhaustive]
#[must_use]
pub enum ArgonParams {
    /// Specific argon2 parameters.
    Params(argon2::ParamsBuilder),
    /// Automatic configuration based on execution time. This is measured during
    /// the first `set_password` operation.
    Timed(TimedArgonParams),
}

impl ArgonParams {
    /// Returns the default configuration based on the system information and
    /// number of hashers. See [`TimedArgonParams`] for more details.
    pub fn default_for(system: &System, hashers: u32) -> Self {
        ArgonParams::Timed(TimedArgonParams::default_for(system, hashers))
    }
}

/// Automatic configuration based on execution time. This is measured during the
/// first `set_password`
#[derive(Debug, Clone)]
#[must_use]
pub struct TimedArgonParams {
    /// The number of lanes (`p`) that the argon algorithm should use.
    pub lanes: u32,
    /// The amount of ram each hashing operation should utilize.
    pub ram_per_hasher: u32,
    /// The minimum execution time that hashing a password should consume.
    pub minimum_duration: Duration,
}

impl Default for TimedArgonParams {
    /// ## Default Values
    ///
    /// When using `TimedArgonParams::default()`, the settings are 4 lanes,
    /// [`Self::MINIMUM_RAM_PER_HASHER`] of RAM per hasher, and a minimum
    /// duration of 1 second.
    ///
    /// These settings are based on the smallest recommended memory settings
    /// from [RFC 9106, section 4 "Parameter Choice"][rfc]. The strength of
    /// Argon2 is derive largely by the amount of RAM dedicated to it, so the
    /// largest value acceptable should be chosen for `ram_per_hasher`.
    ///
    /// [rfc]: https://www.rfc-editor.org/rfc/rfc9106.html#name-parameter-choice
    fn default() -> Self {
        Self {
            lanes: 4,
            ram_per_hasher: Self::MINIMUM_RAM_PER_HASHER,
            minimum_duration: Duration::from_secs(1),
        }
    }
}

impl TimedArgonParams {
    /// The minimum amount of ram to allocate per hasher. This value is
    /// currently 64MB but will change as the [minimum recommendations][rfc] are
    /// changed.
    ///
    /// [rfc]: https://www.rfc-editor.org/rfc/rfc9106.html#name-parameter-choice
    pub const MINIMUM_RAM_PER_HASHER: u32 = 64 * 1024 * 1024;

    /// Returns the default configuration based on the system information and
    /// number of hashers.
    ///
    /// The total amount of RAM allocated will be the total system memory
    /// divided by 16. This allocated amount will be divided equally between the
    /// hashers. If this number is less than [`Self::MINIMUM_RAM_PER_HASHER`],
    /// [`Self::MINIMUM_RAM_PER_HASHER`] will be used instead.
    ///
    /// For example, if 4 hashers are used on a system with 16GB of RAM, a total
    /// of 1GB of RAM will be used between 4 hashers, yielding a
    /// `ram_per_hasher` value of 256MB.
    ///
    /// `lanes` defaults to 4.
    ///
    /// `minimum_duration` defaults to 1 second.
    ///
    /// These settings are based on the recommended settings from [RFC 9106,
    /// section 4 "Parameter Choice"][rfc], but scaling memory automatically
    /// based on a ratio that we feel is reasonable for most server
    /// configurations. The strength of Argon2 is derive largely by the amount
    /// of RAM dedicated to it, so the largest value acceptable should be chosen
    /// for `ram_per_hasher`.
    ///
    /// ## Debug Mode
    ///
    /// When running with `debug_assertions` the `ram_per_hasher` will be set to
    /// 32kb. This is due to how slow debug mode is for the hashing algorithm.
    /// These settings should not be used in production.
    ///
    /// [rfc]: https://www.rfc-editor.org/rfc/rfc9106.html#name-parameter-choice
    pub fn default_for(system: &System, hashers: u32) -> Self {
        let total_memory = u32::try_from(system.total_memory()).unwrap_or(u32::MAX);
        let max_memory = total_memory / 32;

        let ram_per_hasher = if cfg!(debug_assertions) {
            32 * 1024
        } else {
            (max_memory / hashers).max(Self::MINIMUM_RAM_PER_HASHER)
        };

        // Hypothetical Configurations used to determine these numbers:
        //
        // 1cpu, 512mb ram: 1 thread, 1 hasher, 32mb ram
        // 2cpus, 1GB ram: 1 thread, 1 hasher, 64mb ram
        // 16cpus, 16GB ram: 4 threads, 4 hashers, 64mb ram
        // 96cpus, 192GB ram: 24 threads, 24 hashers, ~510mb ram
        Self {
            lanes: 4,
            ram_per_hasher,
            minimum_duration: Duration::from_secs(1),
        }
    }
}
