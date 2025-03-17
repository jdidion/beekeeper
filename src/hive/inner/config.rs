#[cfg(feature = "local-batch")]
pub use self::local_batch::set_batch_limit_default;
#[cfg(feature = "local-batch")]
pub use self::local_batch::set_weight_limit_default;
#[cfg(feature = "retry")]
pub use self::retry::{
    set_max_retries_default, set_retries_default_disabled, set_retry_factor_default,
};

use super::Config;
use parking_lot::Mutex;
use std::sync::LazyLock;

const DEFAULT_NUM_THREADS: usize = 4;

pub static DEFAULTS: LazyLock<Mutex<Config>> = LazyLock::new(|| {
    let mut config = Config::empty();
    config.set_const_defaults();
    Mutex::new(config)
});

/// Sets the number of threads a `Builder` is configured with when using `Builder::default()`.
pub fn set_num_threads_default(num_threads: usize) {
    DEFAULTS.lock().num_threads.set(Some(num_threads));
}

/// Sets the number of threads a `Builder` is configured with when using `Builder::default()` to
/// the number of available CPU cores.
pub fn set_num_threads_default_all() {
    set_num_threads_default(num_cpus::get());
}

/// Resets all builder defaults to their original values.
pub fn reset_defaults() {
    let mut config = DEFAULTS.lock();
    config.set_const_defaults();
}

impl Config {
    /// Creates a new `Config` with all values unset.
    pub fn empty() -> Self {
        Self {
            num_threads: Default::default(),
            thread_name: Default::default(),
            thread_stack_size: Default::default(),
            #[cfg(feature = "affinity")]
            affinity: Default::default(),
            #[cfg(feature = "local-batch")]
            batch_limit: Default::default(),
            #[cfg(feature = "local-batch")]
            weight_limit: Default::default(),
            #[cfg(feature = "retry")]
            max_retries: Default::default(),
            #[cfg(feature = "retry")]
            retry_factor: Default::default(),
        }
    }

    /// Resets config values to their pre-configured defaults.
    fn set_const_defaults(&mut self) {
        self.num_threads.set(Some(DEFAULT_NUM_THREADS));
        #[cfg(feature = "local-batch")]
        self.set_batch_const_defaults();
        #[cfg(feature = "retry")]
        self.set_retry_const_defaults();
    }

    /// Converts fields into `Sync` variants to make this `Config` thread-safe.
    pub fn into_sync(self) -> Self {
        Self {
            num_threads: self.num_threads.into_sync_default(),
            thread_name: self.thread_name.into_sync(),
            thread_stack_size: self.thread_stack_size.into_sync(),
            #[cfg(feature = "affinity")]
            affinity: self.affinity.into_sync(),
            #[cfg(feature = "local-batch")]
            batch_limit: self.batch_limit.into_sync_default(),
            #[cfg(feature = "local-batch")]
            weight_limit: self.weight_limit.into_sync_default(),
            #[cfg(feature = "retry")]
            max_retries: self.max_retries.into_sync_default(),
            #[cfg(feature = "retry")]
            retry_factor: self.retry_factor.into_sync_default(),
        }
    }

    /// Converts fields into `Unsync` variants to enable them to be modified in a single-threaded
    /// context.
    pub fn into_unsync(self) -> Self {
        Self {
            num_threads: self.num_threads.into_unsync(),
            thread_name: self.thread_name.into_unsync(),
            thread_stack_size: self.thread_stack_size.into_unsync(),
            #[cfg(feature = "affinity")]
            affinity: self.affinity.into_unsync(),
            #[cfg(feature = "local-batch")]
            batch_limit: self.batch_limit.into_unsync(),
            #[cfg(feature = "local-batch")]
            weight_limit: self.weight_limit.into_unsync(),
            #[cfg(feature = "retry")]
            max_retries: self.max_retries.into_unsync(),
            #[cfg(feature = "retry")]
            retry_factor: self.retry_factor.into_unsync(),
        }
    }
}

impl Default for Config {
    /// Creates a new `Config` with default values. This simply clones `DEFAULTS`.
    fn default() -> Self {
        DEFAULTS.lock().clone()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
pub mod reset {
    /// Struct that resets the default values when `drop`ped.
    pub struct Reset;

    impl Drop for Reset {
        fn drop(&mut self) {
            super::reset_defaults();
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::Config;
    use super::reset::Reset;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_set_num_threads_default() {
        let reset = Reset;
        super::set_num_threads_default(2);
        let config = Config::default();
        assert_eq!(config.num_threads.get(), Some(2));
        // Dropping `Reset` should reset the defaults
        drop(reset);

        let reset = Reset;
        super::set_num_threads_default_all();
        let config = Config::default();
        assert_eq!(config.num_threads.get(), Some(num_cpus::get()));
        drop(reset);

        let config = Config::default();
        assert_eq!(config.num_threads.get(), Some(super::DEFAULT_NUM_THREADS));
    }
}

#[cfg(feature = "local-batch")]
mod local_batch {
    use super::{Config, DEFAULTS};

    const DEFAULT_BATCH_LIMIT: usize = 10;

    /// Sets the batch limit a `config` is configured with when using `Builder::default()`.
    pub fn set_batch_limit_default(batch_limit: usize) {
        DEFAULTS.lock().batch_limit.set(Some(batch_limit));
    }

    /// Sets the weight limit a `config` is configured with when using `Builder::default()`.
    pub fn set_weight_limit_default(weight_limit: u64) {
        DEFAULTS.lock().weight_limit.set(Some(weight_limit));
    }

    impl Config {
        pub(super) fn set_batch_const_defaults(&mut self) {
            self.batch_limit.set(Some(DEFAULT_BATCH_LIMIT));
            self.weight_limit.set(None);
        }
    }
}

#[cfg(feature = "retry")]
mod retry {
    use super::{Config, DEFAULTS};
    use std::time::Duration;

    const DEFAULT_MAX_RETRIES: u8 = 3;
    const DEFAULT_RETRY_FACTOR_SECS: u64 = 1;

    /// Sets the max number of retries a `config` is configured with when using `Builder::default()`.
    pub fn set_max_retries_default(num_retries: u8) {
        DEFAULTS.lock().max_retries.set(Some(num_retries));
    }

    /// Sets the retry factor a `config` is configured with when using `Builder::default()`.
    pub fn set_retry_factor_default(retry_factor: Duration) {
        DEFAULTS.lock().set_retry_factor_from(retry_factor);
    }

    /// Specifies that retries should be disabled by default when using `Builder::default()`.
    pub fn set_retries_default_disabled() {
        set_max_retries_default(0);
    }

    impl Config {
        pub fn get_retry_factor_duration(&self) -> Option<Duration> {
            self.retry_factor.get().map(Duration::from_nanos)
        }

        pub fn set_retry_factor_from(&mut self, duration: Duration) -> Option<Duration> {
            self.retry_factor
                .set(Some(duration.as_nanos() as u64))
                .map(Duration::from_nanos)
        }

        pub fn try_set_retry_factor_from(&self, duration: Duration) -> Option<Duration> {
            self.retry_factor
                .try_set(duration.as_nanos() as u64)
                .map(Duration::from_nanos)
                .ok()
        }

        pub(super) fn set_retry_const_defaults(&mut self) {
            self.max_retries.set(Some(DEFAULT_MAX_RETRIES));
            self.retry_factor.set(Some(
                Duration::from_secs(DEFAULT_RETRY_FACTOR_SECS).as_nanos() as u64,
            ));
        }
    }

    #[cfg(test)]
    #[cfg_attr(coverage_nightly, coverage(off))]
    mod tests {
        use super::Config;
        use crate::hive::inner::config::reset::Reset;
        use serial_test::serial;
        use std::time::Duration;

        #[test]
        #[serial]
        fn test_set_max_retries_default() {
            let reset = Reset;
            super::set_max_retries_default(1);
            let config = Config::default();
            assert_eq!(config.max_retries.get(), Some(1));
            // Dropping `Reset` should reset the defaults
            drop(reset);

            let reset = Reset;
            super::set_retries_default_disabled();
            let config = Config::default();
            assert_eq!(config.max_retries.get(), Some(0));
            drop(reset);

            let config = Config::default();
            assert_eq!(config.max_retries.get(), Some(super::DEFAULT_MAX_RETRIES));
        }

        #[test]
        #[serial]
        fn test_set_retry_factor_default() {
            let reset = Reset;
            super::set_retry_factor_default(Duration::from_secs(2));
            let config = Config::default();
            assert_eq!(
                config.get_retry_factor_duration(),
                Some(Duration::from_secs(2))
            );
            // Dropping `Reset` should reset the defaults
            drop(reset);
            let config = Config::default();
            assert_eq!(
                config.get_retry_factor_duration(),
                Some(Duration::from_secs(super::DEFAULT_RETRY_FACTOR_SECS))
            );
        }
    }
}
