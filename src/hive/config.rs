#[cfg(feature = "retry")]
pub use retry::{set_max_retries_default, set_retries_default_disabled, set_retry_factor_default};

use super::Config;
use parking_lot::Mutex;
use std::sync::LazyLock;

const DEFAULT_NUM_THREADS: usize = 4;

static DEFAULTS: LazyLock<Mutex<Config>> = LazyLock::new(|| {
    let mut config = Config::default();
    reset_config_defaults(&mut config);
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
    reset_config_defaults(&mut config);
}

fn reset_config_defaults(config: &mut Config) {
    config.num_threads.set(Some(DEFAULT_NUM_THREADS));
    #[cfg(feature = "retry")]
    retry::reset_config_defaults(config);
}

impl Config {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn with_defaults() -> Self {
        DEFAULTS.lock().clone()
    }

    pub fn into_sync(self) -> Self {
        Self {
            num_threads: self.num_threads.into_sync_default(),
            thread_name: self.thread_name.into_sync(),
            thread_stack_size: self.thread_stack_size.into_sync(),
            #[cfg(feature = "retry")]
            max_retries: self.max_retries.into_sync(),
            #[cfg(feature = "retry")]
            retry_factor: self.retry_factor.into_sync(),
            #[cfg(feature = "affinity")]
            affinity: self.affinity.into_sync(),
        }
    }

    pub fn into_unsync(self) -> Self {
        Self {
            num_threads: self.num_threads.into_unsync(),
            thread_name: self.thread_name.into_unsync(),
            thread_stack_size: self.thread_stack_size.into_unsync(),
            #[cfg(feature = "retry")]
            max_retries: self.max_retries.into_unsync(),
            #[cfg(feature = "retry")]
            retry_factor: self.retry_factor.into_unsync(),
            #[cfg(feature = "affinity")]
            affinity: self.affinity.into_unsync(),
        }
    }
}

#[cfg(test)]
pub mod reset {
    // Struct that resets the default values when `drop`ped.
    pub struct Reset;

    impl Drop for Reset {
        fn drop(&mut self) {
            super::reset_defaults();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::reset::Reset;
    use super::Config;
    use serial_test::serial;

    #[test]
    #[serial]
    fn test_set_num_threads_default() {
        let reset = Reset;
        super::set_num_threads_default(2);
        let config = Config::with_defaults();
        assert_eq!(config.num_threads.get(), Some(2));
        // Dropping `Reset` should reset the defaults
        drop(reset);

        let reset = Reset;
        super::set_num_threads_default_all();
        let config = Config::with_defaults();
        assert_eq!(config.num_threads.get(), Some(num_cpus::get()));
        drop(reset);

        let config = Config::with_defaults();
        assert_eq!(config.num_threads.get(), Some(super::DEFAULT_NUM_THREADS));
    }
}

#[cfg(feature = "retry")]
mod retry {
    use super::{Config, DEFAULTS};
    use std::time::Duration;

    const DEFAULT_MAX_RETRIES: u32 = 3;
    const DEFAULT_RETRY_FACTOR_SECS: u64 = 1;

    /// Sets the max number of retries a `config` is configured with when using `Config::with_defaults()`.
    pub fn set_max_retries_default(num_retries: u32) {
        DEFAULTS.lock().max_retries.set(Some(num_retries));
    }

    /// Sets the retry factor a `config` is configured with when using `Config::with_defaults()`.
    pub fn set_retry_factor_default(retry_factor: Duration) {
        DEFAULTS.lock().set_retry_factor_from(retry_factor);
    }

    /// Specifies that retries should be disabled by default when using `Config::with_defaults()`.
    pub fn set_retries_default_disabled() {
        set_max_retries_default(0);
    }

    pub fn reset_config_defaults(config: &mut Config) {
        config.max_retries.set(Some(DEFAULT_MAX_RETRIES));
        config.retry_factor.set(Some(
            Duration::from_secs(DEFAULT_RETRY_FACTOR_SECS).as_nanos() as u64,
        ));
    }

    impl Config {
        pub fn set_retry_factor_from(&mut self, duration: Duration) -> Option<u64> {
            self.retry_factor.set(Some(duration.as_nanos() as u64))
        }
    }

    #[cfg(test)]
    mod tests {
        use super::Config;
        use crate::hive::config::reset::Reset;
        use serial_test::serial;
        use std::time::Duration;

        impl Config {
            fn get_retry_factor_duration(&self) -> Option<Duration> {
                self.retry_factor.get().map(Duration::from_nanos)
            }
        }

        #[test]
        #[serial]
        fn test_set_max_retries_default() {
            let reset = Reset;
            super::set_max_retries_default(1);
            let config = Config::with_defaults();
            assert_eq!(config.max_retries.get(), Some(1));
            // Dropping `Reset` should reset the defaults
            drop(reset);

            let reset = Reset;
            super::set_retries_default_disabled();
            let config = Config::with_defaults();
            assert_eq!(config.max_retries.get(), Some(0));
            drop(reset);

            let config = Config::with_defaults();
            assert_eq!(config.max_retries.get(), Some(super::DEFAULT_MAX_RETRIES));
        }

        #[test]
        #[serial]
        fn test_set_retry_factor_default() {
            let reset = Reset;
            super::set_retry_factor_default(Duration::from_secs(2));
            let config = Config::with_defaults();
            assert_eq!(
                config.get_retry_factor_duration(),
                Some(Duration::from_secs(2))
            );
            // Dropping `Reset` should reset the defaults
            drop(reset);
            let config = Config::with_defaults();
            assert_eq!(
                config.get_retry_factor_duration(),
                Some(Duration::from_secs(super::DEFAULT_RETRY_FACTOR_SECS))
            );
        }
    }
}
