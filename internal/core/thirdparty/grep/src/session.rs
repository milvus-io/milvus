use std::{
    mem::size_of,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use grep::{
    matcher::{Match, Matcher, NoCaptures},
    regex::RegexMatcher,
};

use crate::{BatchResult, Error, InputKind, LimitKind, Line, Plan, Span, TextResult};

/// Can be cloned and used concurrently with execution, without aliasing a
/// mutable Session. Cancellation is sticky and also affects later batches.
#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub struct Session {
    // Keep the plan alive even after the caller drops its reference. Each
    // session clones the matcher to get its own regex engine cache pool.
    _plan: Plan,
    matcher: RegexMatcher,
    cancellation: CancellationToken,
    poisoned: bool,
}

impl Session {
    pub fn new(plan: &Plan) -> Self {
        Self {
            _plan: plan.clone(),
            matcher: plan.matcher(),
            cancellation: CancellationToken::default(),
            poisoned: false,
        }
    }

    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    /// Synchronous, exclusive execution. Ordinary errors discard the batch but
    /// leave the session reusable; a recovered unwind poisons it permanently.
    /// `max_result_bytes` covers logical result data, not allocator overhead
    /// or temporary storage.
    pub fn execute_batch(
        &mut self,
        inputs: &[&[u8]],
        max_result_bytes: u64,
    ) -> Result<BatchResult, Error> {
        self.execute_accounted(inputs, max_result_bytes, ResultOverhead::default())
    }

    pub(crate) fn execute_accounted(
        &mut self,
        inputs: &[&[u8]],
        max_result_bytes: u64,
        overhead: ResultOverhead,
    ) -> Result<BatchResult, Error> {
        self.guarded(|session| session.execute(inputs, max_result_bytes, overhead))
    }

    fn guarded<T>(&mut self, f: impl FnOnce(&Self) -> Result<T, Error>) -> Result<T, Error> {
        if self.poisoned {
            return Err(Error::Internal("session is poisoned"));
        }
        match catch_unwind(AssertUnwindSafe(|| f(self))) {
            Ok(result) => result,
            Err(_payload) => {
                self.poisoned = true;
                Err(Error::Internal(
                    "panic during execution; session is poisoned",
                ))
            }
        }
    }

    fn execute(
        &self,
        inputs: &[&[u8]],
        max_result_bytes: u64,
        overhead: ResultOverhead,
    ) -> Result<BatchResult, Error> {
        if max_result_bytes == 0 {
            return Err(Error::InvalidConfiguration(
                "result byte budget must be positive",
            ));
        }
        let control = Control {
            cancellation: &self.cancellation,
        };
        control.check()?;
        let mut budget = ResultBudget {
            remaining: max_result_bytes,
        };
        budget.charge(size_of::<BatchResult>())?;
        budget.charge(overhead.batch)?;
        budget.charge_items::<TextResult>(inputs.len())?;
        budget.charge_product(overhead.text, inputs.len())?;
        let mut validated = Vec::new();
        validated.try_reserve_exact(inputs.len())?;
        for (index, input) in inputs.iter().enumerate() {
            control.check()?;
            let text = std::str::from_utf8(input).map_err(|source| Error::InvalidUtf8 {
                kind: InputKind::Text,
                index,
                source,
            })?;
            control.check()?;
            validated.push(text);
        }
        let mut texts = Vec::new();
        texts.try_reserve_exact(inputs.len())?;
        let matcher = CheckedMatcher {
            inner: &self.matcher,
            control: &control,
        };
        for text in validated {
            control.check()?;
            let mut lines = Vec::new();
            let mut start = 0;
            // Empty text has no logical lines. A terminal LF does not create
            // another line. CR, NUL and BOM are never normalized away.
            while start < text.len() {
                let end = line_end(text.as_bytes(), start, &control)?;
                let content = &text[start..end];
                let mut spans = Vec::new();
                matcher.try_find_iter_at(content.as_bytes(), 0, |m| -> Result<bool, Error> {
                    control.check()?;
                    if !content.is_char_boundary(m.start()) || !content.is_char_boundary(m.end()) {
                        if m.is_empty() {
                            return Ok(true);
                        }
                        return Err(Error::Internal(
                            "engine returned non-UTF-8 match boundaries",
                        ));
                    }
                    if spans.is_empty() {
                        budget.charge(size_of::<Line>())?;
                        budget.charge(overhead.line)?;
                        budget.charge(content.len())?;
                    }
                    budget.charge(size_of::<Span>())?;
                    budget.charge(overhead.span)?;
                    spans.try_reserve(1)?;
                    spans.push(Span {
                        start: m.start() as u64,
                        end: m.end() as u64,
                    });
                    Ok(true)
                })??;
                if !spans.is_empty() {
                    let mut owned = String::new();
                    owned.try_reserve_exact(content.len())?;
                    owned.push_str(content);
                    lines.try_reserve(1)?;
                    lines.push(Line {
                        content: owned.into_boxed_str(),
                        spans: spans.into_boxed_slice(),
                    });
                }
                control.check()?;
                // Avoid adding one past the end for an unterminated line.
                start = if end == text.len() { end } else { end + 1 };
            }
            texts.push(TextResult {
                lines: lines.into_boxed_slice(),
            });
        }
        let result = BatchResult {
            texts: texts.into_boxed_slice(),
        };
        // Success linearization: cancellation after this check cannot revoke
        // the owned result. All earlier errors drop the entire local result.
        control.check()?;
        Ok(result)
    }
}

struct Control<'a> {
    cancellation: &'a CancellationToken,
}

impl Control<'_> {
    fn check(&self) -> Result<(), Error> {
        if self.cancellation.is_cancelled() {
            return Err(Error::Cancelled);
        }
        Ok(())
    }
}

// Finding a line boundary is itself a scan. Bound this scan between checks;
// a single regex call and a single UTF-8 validation remain non-preemptible.
fn line_end(bytes: &[u8], start: usize, control: &Control<'_>) -> Result<usize, Error> {
    let mut offset = start;
    for chunk in bytes[start..].chunks(64 * 1024) {
        control.check()?;
        if let Some(i) = memchr::memchr(b'\n', chunk) {
            return Ok(offset + i);
        }
        offset += chunk.len();
    }
    Ok(bytes.len())
}

// Use the upstream Matcher iterator (including its empty-match suppression),
// but check around every find_at call, not just callbacks for successful hits.
struct CheckedMatcher<'a, 'b> {
    inner: &'a RegexMatcher,
    control: &'a Control<'b>,
}

impl Matcher for CheckedMatcher<'_, '_> {
    type Captures = NoCaptures;
    type Error = Error;
    fn find_at(&self, haystack: &[u8], at: usize) -> Result<Option<Match>, Error> {
        self.control.check()?;
        let found = self
            .inner
            .find_at(haystack, at)
            .map_err(|_| Error::Internal("infallible regex matcher returned an error"))?;
        self.control.check()?;
        Ok(found)
    }
    fn new_captures(&self) -> Result<NoCaptures, Error> {
        Ok(NoCaptures::new())
    }
}

/// Extra result views owned by an adapter, charged before producing results.
#[derive(Clone, Copy, Default)]
pub(crate) struct ResultOverhead {
    pub batch: usize,
    pub text: usize,
    pub line: usize,
    pub span: usize,
}

struct ResultBudget {
    remaining: u64,
}
impl ResultBudget {
    fn charge(&mut self, bytes: usize) -> Result<(), Error> {
        self.remaining = self
            .remaining
            .checked_sub(bytes as u64)
            .ok_or(Error::ResourceLimit(LimitKind::ResultBytes))?;
        Ok(())
    }
    fn charge_items<T>(&mut self, count: usize) -> Result<(), Error> {
        self.charge_product(size_of::<T>(), count)
    }
    fn charge_product(&mut self, item_size: usize, count: usize) -> Result<(), Error> {
        self.charge(
            count
                .checked_mul(item_size)
                .ok_or(Error::ResourceLimit(LimitKind::ResultBytes))?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CompileLimits, MatchOptions};

    fn session() -> Session {
        Session::new(
            &Plan::compile(
                &[b"a"],
                MatchOptions::default(),
                CompileLimits {
                    max_regex_bytes: 1_000_000,
                    max_dfa_cache_bytes: 1_000_000,
                },
            )
            .unwrap(),
        )
    }

    #[test]
    fn panic_poisoning_is_sticky() {
        let mut session = session();
        let error = session
            .guarded::<()>(|_| panic!("injected execution failure"))
            .unwrap_err();
        assert!(matches!(error, Error::Internal(_)));
        assert!(matches!(
            session.guarded(|_| Ok(())),
            Err(Error::Internal(_))
        ));
    }

    #[test]
    fn cancelled_control_rejects_no_match_calls() {
        let session = session();
        session.cancellation_token().cancel();
        let control = Control {
            cancellation: &session.cancellation,
        };
        let matcher = CheckedMatcher {
            inner: &session.matcher,
            control: &control,
        };
        assert!(matches!(
            matcher.find_at(b"no hits", 0),
            Err(Error::Cancelled)
        ));
        assert!(matches!(
            line_end(b"no newline", 0, &control),
            Err(Error::Cancelled)
        ));
    }

    #[test]
    fn result_size_arithmetic_overflow_is_a_limit_error() {
        let mut budget = ResultBudget {
            remaining: u64::MAX,
        };
        assert!(matches!(
            budget.charge_items::<Span>(usize::MAX),
            Err(Error::ResourceLimit(LimitKind::ResultBytes))
        ));
    }
}
