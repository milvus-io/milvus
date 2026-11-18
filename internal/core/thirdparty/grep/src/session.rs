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
    matcher::{ByteSet, LineMatchKind, LineTerminator, Match, Matcher, NoCaptures},
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
        let matcher = plan.matcher();
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
            matcher,
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

fn collect_spans(
    matcher: &CheckedMatcher<'_, '_>,
    bytes: &[u8],
    budget: &mut ResultBudget,
    overhead: ResultOverhead,
) -> Result<Vec<Span>, Error> {
    // Enumerate on one complete logical line without LF.
    // Absolute anchors are line-local; an empty match at EOF is a valid span.
    let mut spans = Vec::new();
    matcher
        .try_find_iter_at(bytes, 0, |m| -> Result<bool, Error> {
            budget.charge(size_of::<Span>())?;
            budget.charge(overhead.span)?;
            spans.try_reserve(1)?;
            spans.push(Span {
                start: m.start() as u64,
                end: m.end() as u64,
            });
            Ok(true)
        })
        .map_err(|_| matcher.take_error())??;
    Ok(spans)
}

// Charge context and matching lines identically before copying their content.
fn push_line(
    lines: &mut Vec<Line>,
    content: &str,
    is_match: bool,
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
        is_match,
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
        spans: Vec<Span>,
        is_match: bool,
    ) -> Result<bool, Error> {
        self.matcher.control.check()?;
        let bytes = bytes.strip_suffix(b"\n").unwrap_or(bytes);
        // The full input was already validated; BOM sniffing/transcoding is off.
        let content = std::str::from_utf8(bytes)
            .map_err(|_| Error::Internal("searcher returned an invalid UTF-8 line"))?;
        let line_number = line_number.ok_or(Error::Internal("searcher omitted line number"))?;
        push_line(
            self.lines,
            content,
            is_match,
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
        let bytes = mat.bytes().strip_suffix(b"\n").unwrap_or(mat.bytes());
        let spans = collect_spans(self.matcher, bytes, self.budget, self.overhead)?;
        self.emit(
            mat.bytes(),
            mat.line_number(),
            mat.absolute_byte_offset(),
            spans,
            true,
        )
    }
    fn context(&mut self, _: &Searcher, context: &SinkContext<'_>) -> Result<bool, Error> {
        self.emit(
            context.bytes(),
            context.line_number(),
            context.absolute_byte_offset(),
            Vec::new(),
            false,
        )
    }
}

impl SinkError for Error {
    fn error_message<T: fmt::Display>(_: T) -> Self {
        // The reader is an in-memory slice and performs no external I/O.
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

// Preserve byte spans and cooperative cancellation while forwarding
// rg's search hints. In particular, rg disables fast line scanning for absolute
// anchors itself; overriding those hints would bypass its semantic safeguards.
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
        let result = searcher.search_slice(self, bytes, sink);
        result.map_err(|error| self.failure.take().unwrap_or(error))
    }
}

impl Matcher for CheckedMatcher<'_, '_> {
    type Captures = NoCaptures;
    type Error = MatcherFailure;
    fn find_at(&self, haystack: &[u8], at: usize) -> Result<Option<Match>, MatcherFailure> {
        self.capture((|| {
            self.control.check()?;
            let found = self
                .inner
                .find_at(haystack, at)
                .map_err(|_| Error::Internal("infallible regex matcher returned an error"))?;
            self.control.check()?;
            Ok(found)
        })())
    }
    fn new_captures(&self) -> Result<NoCaptures, MatcherFailure> {
        Ok(NoCaptures::new())
    }
    fn shortest_match_at(
        &self,
        haystack: &[u8],
        at: usize,
    ) -> Result<Option<usize>, MatcherFailure> {
        // Searcher only needs to know whether a line matches. Preserve the
        // regex engine's half-match fast path instead of finding a full span.
        self.capture((|| {
            self.control.check()?;
            let found = self
                .inner
                .shortest_match_at(haystack, at)
                .map_err(|_| Error::Internal("infallible regex matcher returned an error"))?;
            self.control.check()?;
            Ok(found)
        })())
    }
    fn line_terminator(&self) -> Option<LineTerminator> {
        self.inner.line_terminator()
    }
    fn non_matching_bytes(&self) -> Option<&ByteSet> {
        self.inner.non_matching_bytes()
    }
    fn find_candidate_line(
        &self,
        haystack: &[u8],
    ) -> Result<Option<LineMatchKind>, MatcherFailure> {
        self.capture((|| {
            self.control.check()?;
            let found = self
                .inner
                .find_candidate_line(haystack)
                .map_err(|_| Error::Internal("infallible regex matcher returned an error"))?;
            self.control.check()?;
            Ok(found)
        })())
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
    use crate::{CompileLimits, MatchOptions, PatternKind};

    #[derive(Default)]
    struct SelectedLines(Vec<(u64, u64, Vec<u8>, bool)>);

    impl Sink for &mut SelectedLines {
        type Error = std::io::Error;

        fn matched(&mut self, _: &Searcher, mat: &SinkMatch<'_>) -> std::io::Result<bool> {
            self.0.push((
                mat.line_number().unwrap(),
                mat.absolute_byte_offset(),
                mat.bytes()
                    .strip_suffix(b"\n")
                    .unwrap_or(mat.bytes())
                    .to_vec(),
                true,
            ));
            Ok(true)
        }

        fn context(&mut self, _: &Searcher, ctx: &SinkContext<'_>) -> std::io::Result<bool> {
            self.0.push((
                ctx.line_number().unwrap(),
                ctx.absolute_byte_offset(),
                ctx.bytes()
                    .strip_suffix(b"\n")
                    .unwrap_or(ctx.bytes())
                    .to_vec(),
                false,
            ));
            Ok(true)
        }
    }

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
    fn session_reuse_preserves_line_local_anchor_spans() {
        let plan = Plan::compile(
            &[br"\Aerror"],
            MatchOptions::default(),
            CompileLimits {
                max_regex_bytes: 1_000_000,
                max_dfa_cache_bytes: 1_000_000,
            },
        )
        .unwrap();
        let mut session = Session::new(&plan);
        let result = session
            .execute_accounted(
                &[b"error", b"error\n", b"miss", b"", b"\n"],
                1_000_000,
                ResultOverhead::default(),
            )
            .unwrap();
        assert_eq!(
            result
                .texts
                .iter()
                .map(|t| t.lines.len())
                .collect::<Vec<_>>(),
            vec![1, 1, 0, 0, 0]
        );
        let result = session
            .execute_accounted(
                &[b"prefix\nerror\nlast\n"],
                1_000_000,
                ResultOverhead::default(),
            )
            .unwrap();
        assert_eq!(result.texts[0].lines.len(), 1);
        assert_eq!(
            result.texts[0].lines[0].spans.as_ref(),
            &[Span { start: 0, end: 5 }]
        );
        assert!(result.texts[0].lines[0].is_match);
        let result = session
            .execute_accounted(&[b"error"], 1_000_000, ResultOverhead::default())
            .unwrap();
        assert_eq!(
            result.texts[0].lines[0].spans.as_ref(),
            &[Span { start: 0, end: 5 }]
        );
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
        for text in [b"no hits".as_slice(), b"a", b""] {
            assert!(matcher.shortest_match_at(text, 0).is_err());
            assert!(matches!(matcher.take_error(), Error::Cancelled));
            assert!(matcher.find_candidate_line(text).is_err());
            assert!(matches!(matcher.take_error(), Error::Cancelled));
        }
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
    fn confirmed_candidates_keep_native_scanning() {
        let session = session();
        let control = Control {
            cancellation: &session.cancellation,
        };
        let matcher = CheckedMatcher {
            inner: &session.matcher,
            control: &control,
            failure: Cell::new(None),
        };
        for _ in 0..32 {
            assert!(matches!(
                matcher.find_candidate_line(b"a").unwrap(),
                Some(LineMatchKind::Confirmed(_))
            ));
        }
    }

    #[test]
    fn candidate_scan_skips_nonmatching_lines() {
        let session = session();
        let control = Control {
            cancellation: &session.cancellation,
        };
        let matcher = CheckedMatcher {
            inner: &session.matcher,
            control: &control,
            failure: Cell::new(None),
        };
        assert!(matcher
            .find_candidate_line(b"miss\nmiss\n")
            .unwrap()
            .is_none());
        let candidate = matcher
            .find_candidate_line(b"miss\nmiss\na")
            .unwrap()
            .unwrap();
        let offset = match candidate {
            LineMatchKind::Candidate(offset) | LineMatchKind::Confirmed(offset) => offset,
        };
        assert!(
            offset >= 10,
            "candidate scan must skip the two nonmatching lines"
        );
    }

    #[test]
    fn line_selection_matches_unwrapped_rg_searcher() {
        let inputs = [
            "",
            "\n",
            "é\n中🙂\r\nerror\n\n",
            "error\0error",
            "miss",
            "prefix\nerror\nerror x\nerror\r\nerror",
            "error\nerror\nlast",
        ];
        let mut inputs: Vec<String> = inputs.iter().map(|text| text.to_string()).collect();
        inputs.push(format!(
            "{}{}{}",
            "error:1234\n".repeat(16),
            "miss\n".repeat(64),
            "error:1234\n".repeat(16)
        ));
        inputs.push(format!(
            "{}{}",
            "miss\n".repeat(64),
            "error:1234\n".repeat(32)
        ));
        for pattern_kind in [PatternKind::Regex, PatternKind::Literal] {
            for pattern in [
                "error",
                "",
                r"\Aerror",
                r"error\z",
                r"\Aerror\z",
                r"(?-m:^error$)",
                r"^error$",
                r"\b",
                r"\B",
                "a*",
                r"\w+error\w+",
                r"\berror:[0-9]{4}\b",
                r"(?-u:.)",
                r"(?-u:\xA9)",
                r"(?-u:[\x80-\xff])",
                r"(?-u:\b)error",
            ] {
                for (before, after) in [(0, 0), (1, 0), (0, 1), (2, 2)] {
                    for max_count in [None, Some(0), Some(1), Some(2)] {
                        let plan = Plan::compile(
                            &[pattern.as_bytes()],
                            MatchOptions {
                                pattern_kind,
                                before_context: before,
                                after_context: after,
                                max_count,
                                ..Default::default()
                            },
                            CompileLimits {
                                max_regex_bytes: 1_000_000,
                                max_dfa_cache_bytes: 1_000_000,
                            },
                        )
                        .unwrap();
                        let mut session = Session::new(&plan);
                        let bytes: Vec<_> = inputs.iter().map(|s| s.as_bytes()).collect();
                        let actual = session
                            .execute_accounted(&bytes, 1_000_000, ResultOverhead::default())
                            .unwrap();
                        // Use the raw upstream matcher: this oracle does not
                        // share CheckedMatcher's candidate or anchor handling.
                        let matcher = plan.matcher();
                        let mut searcher = SearcherBuilder::new()
                            .before_context(before as usize)
                            .after_context(after as usize)
                            .max_matches(max_count)
                            .line_number(true)
                            .binary_detection(BinaryDetection::none())
                            .encoding(None)
                            .bom_sniffing(false)
                            .build();
                        for (text, actual) in inputs.iter().zip(&actual.texts) {
                            let mut expected = SelectedLines::default();
                            searcher
                                .search_slice(&matcher, text.as_bytes(), &mut expected)
                                .unwrap();
                            let actual: Vec<_> = actual
                                .lines
                                .iter()
                                .map(|line| {
                                    (
                                        line.line_number,
                                        line.line_start_offset,
                                        line.content.as_bytes().to_vec(),
                                        line.is_match,
                                    )
                                })
                                .collect();
                            assert_eq!(actual, expected.0, "{pattern_kind:?}, {pattern:?}, {text:?}, {before}/{after}, {max_count:?}");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn max_count_anchor_after_context_preserves_type_and_spans() {
        for (pattern, second_is_match) in [
            ("error", true),
            (r"\Aerror", false),
            (r"error\z", false),
            (r"\Aerror\z", false),
        ] {
            let plan = Plan::compile(
                &[pattern.as_bytes()],
                MatchOptions {
                    max_count: Some(1),
                    after_context: 2,
                    ..Default::default()
                },
                CompileLimits {
                    max_regex_bytes: 1_000_000,
                    max_dfa_cache_bytes: 1_000_000,
                },
            )
            .unwrap();
            let result = Session::new(&plan)
                .execute_accounted(&[b"error\nerror\nx"], 1_000_000, ResultOverhead::default())
                .unwrap();
            let lines = &result.texts[0].lines;
            assert_eq!(lines.len(), 3, "{pattern}");
            for (i, (content, is_match)) in
                [("error", true), ("error", second_is_match), ("x", false)]
                    .into_iter()
                    .enumerate()
            {
                let line = &lines[i];
                assert_eq!(&*line.content, content, "{pattern}, line {i}");
                assert_eq!(line.line_number, i as u64 + 1);
                assert_eq!(line.line_start_offset, i as u64 * 6);
                assert_eq!(line.is_match, is_match, "{pattern}, line {i}");
                let expected_spans = if is_match {
                    vec![Span { start: 0, end: 5 }]
                } else {
                    vec![]
                };
                assert_eq!(&*line.spans, expected_spans, "{pattern}, line {i}");
            }
        }
    }

    #[test]
    fn session_reuse_after_budget_errors_preserves_results() {
        let inputs: Vec<&[u8]> = ["", "\n", "é\n中🙂\r\naaa\n\n", "a\0a", "miss", "aaa"]
            .iter()
            .map(|s| s.as_bytes())
            .collect();
        for pattern in ["a", "", "a*", r"\Aé", r"a\z", r"^$", r"\B"] {
            for max_count in [None, Some(0), Some(1), Some(2)] {
                let plan = Plan::compile(
                    &[pattern.as_bytes()],
                    MatchOptions {
                        max_count,
                        ..Default::default()
                    },
                    CompileLimits {
                        max_regex_bytes: 1_000_000,
                        max_dfa_cache_bytes: 1_000_000,
                    },
                )
                .unwrap();
                let mut reused = Session::new(&plan);
                for budget in [1, 256, 1024, 1_000_000] {
                    let overhead = ResultOverhead {
                        batch: 8,
                        text: 16,
                        line: 24,
                        span: 16,
                    };
                    let a = reused.execute_accounted(&inputs, budget, overhead);
                    let b = Session::new(&plan).execute_accounted(&inputs, budget, overhead);
                    match (a, b) {
                        (Ok(a), Ok(b)) => assert_eq!(a, b, "{pattern:?}, {max_count:?}, {budget}"),
                        (
                            Err(Error::ResourceLimit(LimitKind::ResultBytes)),
                            Err(Error::ResourceLimit(LimitKind::ResultBytes)),
                        ) => {}
                        (a, b) => panic!("{pattern:?}, {max_count:?}, {budget}: {a:?} != {b:?}"),
                    }
                }
                assert!(matches!(
                    reused.execute_accounted(
                        &[b"a", b"\xff"],
                        1_000_000,
                        ResultOverhead::default()
                    ),
                    Err(Error::InvalidUtf8 { .. })
                ));
                reused.cancellation_token().cancel();
                assert!(matches!(
                    reused.execute_accounted(&inputs, 1_000_000, ResultOverhead::default()),
                    Err(Error::Cancelled)
                ));
            }
        }
    }

    #[test]
    fn matcher_preserves_upstream_byte_offsets() {
        for pattern in [
            "",
            "a+",
            "a*",
            r"\Aé",
            r"é\z",
            r"\b",
            r"\B",
            ".",
            "不存在",
            r"(?-u:.)",
            r"(?-u:\xA9)",
        ] {
            let plan = Plan::compile(
                &[pattern.as_bytes()],
                MatchOptions::default(),
                CompileLimits {
                    max_regex_bytes: 1_000_000,
                    max_dfa_cache_bytes: 1_000_000,
                },
            )
            .unwrap();
            let session = Session::new(&plan);
            let control = Control {
                cancellation: &session.cancellation,
            };
            let matcher = CheckedMatcher {
                inner: &session.matcher,
                control: &control,
                failure: Cell::new(None),
            };
            for text in ["", "aaa", "é", "中🙂aé", " a ", "xyz"] {
                for at in 0..=text.len() {
                    let full = matcher.find_at(text.as_bytes(), at).unwrap();
                    let half = matcher.shortest_match_at(text.as_bytes(), at).unwrap();
                    assert_eq!(full, session.matcher.find_at(text.as_bytes(), at).unwrap());
                    assert_eq!(
                        half,
                        session
                            .matcher
                            .shortest_match_at(text.as_bytes(), at)
                            .unwrap()
                    );
                    assert_eq!(
                        half.is_some(),
                        full.is_some(),
                        "{pattern:?}, {text:?}, {at}"
                    );
                    if let Some(end) = half {
                        assert!(
                            end >= at && end <= text.len(),
                            "{pattern:?}, {text:?}, {at}: {end}"
                        );
                    }
                }
            }
            if pattern.is_empty() {
                // The upstream byte matcher accepts an empty match inside
                // this character; the wrapper preserves that byte position.
                assert_eq!(
                    session
                        .matcher
                        .shortest_match_at("é".as_bytes(), 1)
                        .unwrap(),
                    Some(1)
                );
                assert_eq!(
                    matcher.shortest_match_at("é".as_bytes(), 1).unwrap(),
                    Some(1)
                );
            }
        }
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
