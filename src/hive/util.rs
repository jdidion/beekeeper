use crossbeam_utils::Backoff;
use std::sync::Arc;
use std::time::{Duration, Instant};

const MAX_WAIT: Duration = Duration::from_secs(10);

/// Utility function to loop (with exponential backoff) waiting for other references to `arc` to
/// drop so it can be unwrapped into its inner value.
///
/// If `arc` cannot be unwrapped with a certain amount of time (with an exponentially
/// increasing gap between each iteration), `arc` is returned as an error.
pub fn unwrap_arc<T>(mut arc: Arc<T>) -> Result<T, Arc<T>> {
    // wait for worker threads to drop, then take ownership of the shared data and convert it
    // into a Husk
    let mut backoff = None::<Backoff>;
    let mut start = None::<Instant>;
    loop {
        arc = match std::sync::Arc::try_unwrap(arc) {
            Ok(inner) => {
                return Ok(inner);
            }
            Err(arc) if start.is_none() => {
                let _ = start.insert(Instant::now());
                arc
            }
            Err(arc) if Instant::now() - start.unwrap() > MAX_WAIT => return Err(arc),
            Err(arc) => {
                backoff.get_or_insert_with(Backoff::new).spin();
                arc
            }
        };
    }
}
