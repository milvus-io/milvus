use std::{
    cell::Cell,
    fmt,
    mem::size_of,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use grep::{
    matcher::{LineMatchKind, LineTerminator, Match, Matcher, NoCaptures},
    regex::RegexMatcher,
    searcher::{
        BinaryDetection, Searcher, SearcherBuilder, Sink, SinkContext, SinkError, SinkMatch,
    },
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
    searcher: Searcher,
    cancellation: CancellationToken,
    poisoned: bool,
}

impl Session {
    pub fn new(plan: &Plan) -> Self {
        let (before, after) = plan.context();
        let searcher = SearcherBuilder::new()
            .before_context(before.min(usize::MAX as u64) as usize)
            .after_context(after.min(usize::MAX as u64) as usize)
            .max_matches(plan.max_count())
            .line_number(true)
            .multi_line(false)
            .binary_detection(BinaryDetection::none())
            .encoding(None)
            .bom_sniffing(false)
            .build();
        Self {
            _plan: plan.clone(),
            matcher: plan.matcher(),
            searcher,
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
    /// or temporary storage. `overhead` includes adapter-owned result descriptors.
    pub(crate) fn execute_accounted(
        &mut self,
        inputs: &[&[u8]],
        max_result_bytes: u64,
        overhead: ResultOverhead,
    ) -> Result<BatchResult, Error> {
        self.guarded(|session| session.execute(inputs, max_result_bytes, overhead))
    }

    fn guarded<T>(&mut self, f: impl FnOnce(&mut Self) -> Result<T, Error>) -> Result<T, Error> {
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
        &mut self,
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
            failure: Cell::new(None),
        };
        for text in validated {
            control.check()?;
            let mut lines = Vec::new();
            matcher.search(
                &mut self.searcher,
                text.as_bytes(),
                ResultSink {
                    matcher: &matcher,
                    lines: &mut lines,
                    budget: &mut budget,
                    overhead,
                },
            )?;
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

// Charge context and matching lines identically before copying their content.
fn push_line(
    lines: &mut Vec<Line>,
    content: &str,
    line_number: u64,
    start: u64,
    spans: Vec<Span>,
    budget: &mut ResultBudget,
    overhead: ResultOverhead,
) -> Result<(), Error> {
    budget.charge(size_of::<Line>())?;
    budget.charge(overhead.line)?;
    budget.charge(content.len())?;
    let mut owned = String::new();
    owned.try_reserve_exact(content.len())?;
    owned.push_str(content);
    lines.try_reserve(1)?;
    lines.push(Line {
        line_number,
        line_start_offset: start,
        content: owned.into_boxed_str(),
        spans: spans.into_boxed_slice(),
    });
    Ok(())
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

// Searcher owns line traversal, context windows, deduplication, -m and offsets.
// This Sink only builds the Milvus result and accounts for its allocations.
struct ResultSink<'a, 'b, 'c> {
    matcher: &'a CheckedMatcher<'b, 'c>,
    lines: &'a mut Vec<Line>,
    budget: &'a mut ResultBudget,
    overhead: ResultOverhead,
}

impl ResultSink<'_, '_, '_> {
    fn emit(
        &mut self,
        bytes: &[u8],
        line_number: Option<u64>,
        offset: u64,
        matched: bool,
    ) -> Result<bool, Error> {
        self.matcher.control.check()?;
        let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
        // The full input was already validated; BOM sniffing/transcoding is off.
        let content = std::str::from_utf8(bytes)
            .map_err(|_| Error::Internal("searcher returned an invalid UTF-8 line"))?;
        let line_number = line_number.ok_or(Error::Internal("searcher omitted line number"))?;
        let mut spans = Vec::new();
        if matched {
            self.matcher
                .try_find_iter_at(bytes, 0, |m| -> Result<bool, Error> {
                    self.budget.charge(size_of::<Span>())?;
                    self.budget.charge(self.overhead.span)?;
                    spans.try_reserve(1)?;
                    spans.push(Span {
                        start: m.start() as u64,
                        end: m.end() as u64,
                    });
                    Ok(true)
                })
                .map_err(|_| self.matcher.take_error())??;
            if spans.is_empty() {
                return Err(Error::Internal(
                    "searcher returned a line without valid matches",
                ));
            }
        }
        push_line(
            self.lines,
            content,
            line_number,
            offset,
            spans,
            self.budget,
            self.overhead,
        )?;
        self.matcher.control.check()?;
        Ok(true)
    }
}

impl Sink for ResultSink<'_, '_, '_> {
    type Error = Error;
    fn matched(&mut self, _: &Searcher, mat: &SinkMatch<'_>) -> Result<bool, Error> {
        self.emit(
            mat.bytes(),
            mat.line_number(),
            mat.absolute_byte_offset(),
            true,
        )
    }
    fn context(&mut self, _: &Searcher, context: &SinkContext<'_>) -> Result<bool, Error> {
        self.emit(
            context.bytes(),
            context.line_number(),
            context.absolute_byte_offset(),
            false,
        )
    }
}

impl SinkError for Error {
    fn error_message<T: fmt::Display>(_: T) -> Self {
        // search_slice has fixed configuration and performs no external I/O.
        // Typed matcher errors are recovered by CheckedMatcher::search instead
        // of inferring their category from this upstream formatted message.
        Error::Internal("grep searcher failed")
    }
}

#[derive(Debug)]
struct MatcherFailure;
impl fmt::Display for MatcherFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("grep matcher failed")
    }
}

// Preserve the existing per-line anchor/UTF-8 contract and cooperative cancel
// checks while delegating line scanning and context handling to Searcher.
struct CheckedMatcher<'a, 'b> {
    inner: &'a RegexMatcher,
    control: &'a Control<'b>,
    failure: Cell<Option<Error>>,
}

impl CheckedMatcher<'_, '_> {
    fn capture<T>(&self, result: Result<T, Error>) -> Result<T, MatcherFailure> {
        result.map_err(|error| {
            self.failure.set(Some(error));
            MatcherFailure
        })
    }
    fn take_error(&self) -> Error {
        self.failure
            .take()
            .unwrap_or(Error::Internal("missing matcher error"))
    }
    fn search<S: Sink<Error = Error>>(
        &self,
        searcher: &mut Searcher,
        bytes: &[u8],
        sink: S,
    ) -> Result<(), Error> {
        searcher
            .search_slice(self, bytes, sink)
            .map_err(|error| self.failure.take().unwrap_or(error))
    }
}

impl Matcher for CheckedMatcher<'_, '_> {
    type Captures = NoCaptures;
    type Error = MatcherFailure;
    fn find_at(&self, haystack: &[u8], mut at: usize) -> Result<Option<Match>, MatcherFailure> {
        self.capture((|| loop {
            self.control.check()?;
            let found = self
                .inner
                .find_at(haystack, at)
                .map_err(|_| Error::Internal("infallible regex matcher returned an error"))?;
            self.control.check()?;
            let Some(m) = found else { return Ok(None) };
            let boundary = |i: usize| i == haystack.len() || haystack[i] & 0xc0 != 0x80;
            if boundary(m.start()) && boundary(m.end()) {
                return Ok(Some(m));
            }
            if !m.is_empty() {
                return Err(Error::Internal(
                    "engine returned non-UTF-8 match boundaries",
                ));
            }
            at = m.end() + 1;
        })())
    }
    fn new_captures(&self) -> Result<NoCaptures, MatcherFailure> {
        Ok(NoCaptures::new())
    }
    fn line_terminator(&self) -> Option<LineTerminator> {
        Some(LineTerminator::byte(b'\n'))
    }
    fn find_candidate_line(
        &self,
        haystack: &[u8],
    ) -> Result<Option<LineMatchKind>, MatcherFailure> {
        // Ask Searcher to verify each complete line. Delegating this to the
        // regex's whole-buffer fast path would change existing \A/\z semantics.
        self.capture(
            self.control
                .check()
                .map(|()| (!haystack.is_empty()).then_some(LineMatchKind::Candidate(0))),
        )
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
            failure: Cell::new(None),
        };
        assert!(matcher.find_at(b"no hits", 0).is_err());
        assert!(matches!(matcher.take_error(), Error::Cancelled));
        let mut searcher = SearcherBuilder::new()
            .before_context(1)
            .after_context(1)
            .build();
        let mut lines = Vec::new();
        let mut budget = ResultBudget {
            remaining: 1_000_000,
        };
        let sink = ResultSink {
            matcher: &matcher,
            lines: &mut lines,
            budget: &mut budget,
            overhead: ResultOverhead::default(),
        };
        assert!(matches!(
            matcher.search(&mut searcher, b"no hits", sink),
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
