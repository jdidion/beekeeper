use crate::hive::{HiveError, TaskResult};
use crate::task::Worker;

pub struct BatchResult<W: Worker> {
    successes: Vec<W::Output>,
    errors: Vec<HiveError<W>>,
}

impl<W: Worker> BatchResult<W> {
    pub(crate) fn new(successes: Vec<W::Output>, errors: Vec<HiveError<W>>) -> Self {
        Self { successes, errors }
    }

    pub fn len(&self) -> usize {
        self.successes.len() + self.errors.len()
    }

    pub fn successes(&self) -> &[W::Output] {
        &self.successes
    }

    pub fn num_successes(&self) -> usize {
        self.successes.len()
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn errors(&self) -> &[HiveError<W>] {
        &self.errors
    }

    pub fn num_errors(&self) -> usize {
        self.errors.len()
    }

    pub fn has_unprocessed(&self) -> bool {
        self.errors
            .iter()
            .any(|error| matches!(error, HiveError::Unprocessed { .. }))
    }

    /// Extracts any `HiveError::Unprocessed` errors from this `BatchResult` and returns the
    /// unprocessed inputs as a `Vec`.
    pub fn take_unprocessed(&mut self) -> Vec<W::Input> {
        let num_errors = self.errors.len();
        if num_errors == 0 {
            return Vec::new();
        }
        let num_unprocessed = self
            .errors
            .iter()
            .filter(|error| matches!(error, HiveError::Unprocessed { .. }))
            .count();
        if num_unprocessed == 0 {
            return Vec::new();
        }
        let errors = std::mem::replace(
            &mut self.errors,
            Vec::with_capacity(num_errors - num_unprocessed),
        );
        let mut unprocessed = Vec::with_capacity(num_unprocessed);
        for error in errors {
            if let HiveError::Unprocessed(input) = error {
                unprocessed.push(input);
            } else {
                self.errors.push(error)
            }
        }
        unprocessed
    }

    /// Returns the successes as a `Vec` if there are no errors, otherwise panics.
    pub fn unwrap(self) -> Vec<W::Output> {
        if self.has_errors() {
            panic!("BatchResult has errors");
        }
        self.successes
    }

    /// Returns a `std::result::Result`: `Ok(Vec<W::Output>)` if there are no errors, otherwise
    /// `Err(Vec<W::Error>)`. If `drop_unprocessed` is `true`, unprocessed inputs are discarded,
    /// otherwise they cause this method to panic.
    pub fn ok_or_unwrap_errors(
        self,
        drop_unprocessed: bool,
    ) -> Result<Vec<W::Output>, Vec<W::Error>> {
        if self.has_errors() {
            let failures = self
                .errors
                .into_iter()
                .filter_map(|error| match error.unwrap() {
                    Ok(_) if drop_unprocessed => None,
                    Ok(_) => panic!(),
                    Err(error) => Some(error),
                })
                .collect();
            Err(failures)
        } else {
            Ok(self.successes)
        }
    }

    pub fn into_parts(self) -> (Vec<W::Output>, Vec<HiveError<W>>) {
        (self.successes, self.errors)
    }
}

impl<W: Worker, I: IntoIterator<Item = TaskResult<W>>> From<I> for BatchResult<W> {
    fn from(value: I) -> Self {
        let (successes, failures) = value.into_iter().partition::<Vec<_>, _>(Result::is_ok);
        BatchResult::new(
            successes.into_iter().map(Result::ok).flatten().collect(),
            failures.into_iter().map(Result::err).flatten().collect(),
        )
    }
}
