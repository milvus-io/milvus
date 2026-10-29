use std::{
    mem::size_of,
    sync::{Arc, Barrier},
};

use crate::{
    session::ResultOverhead, BatchResult, Boundary, CaseMode, CompileLimits, Error, InputKind,
    LimitKind, Line, MatchOptions, PatternKind, Plan, Session, Span, TextResult,
};

fn compile_limits() -> CompileLimits {
    CompileLimits {
        max_regex_bytes: 8 * 1024 * 1024,
        max_dfa_cache_bytes: 1024 * 1024,
    }
}

fn execution_limits() -> u64 {
    32 * 1024 * 1024
}

fn plan(patterns: &[&str], options: MatchOptions) -> Plan {
    Plan::compile(
        &patterns.iter().map(|s| s.as_bytes()).collect::<Vec<_>>(),
        options,
        compile_limits(),
    )
    .unwrap()
}

fn search(patterns: &[&str], options: MatchOptions, inputs: &[&str]) -> BatchResult {
    Session::new(&plan(patterns, options))
        .execute_accounted(
            &inputs.iter().map(|s| s.as_bytes()).collect::<Vec<_>>(),
            execution_limits(),
            ResultOverhead::default(),
        )
        .unwrap()
}

fn rows(result: &TextResult) -> Vec<(&str, Vec<(u64, u64)>)> {
    result
        .lines
        .iter()
        .map(|line| {
            (
                line.content.as_ref(),
                line.spans.iter().map(|s| (s.start, s.end)).collect(),
            )
        })
        .collect()
}

#[test]
fn matching_lines_and_spans_remain_aligned_with_all_inputs() {
    let r = search(
        &["error"],
        MatchOptions::default(),
        &["done", "error: error\nnext", "错误：error", ""],
    );
    assert_eq!(r.texts.len(), 4);
    assert!(r.texts[0].lines.is_empty());
    assert_eq!(rows(&r.texts[1]), [("error: error", vec![(0, 5), (7, 12)])]);
    assert_eq!(rows(&r.texts[2]), [("错误：error", vec![(9, 14)])]);
    assert!(r.texts[3].lines.is_empty());
}

#[test]
fn combined_patterns_use_order_and_do_not_duplicate_occurrences() {
    let a = search(&["a", "ab", "a"], MatchOptions::default(), &["ab ab"]);
    assert_eq!(rows(&a.texts[0]), [("ab ab", vec![(0, 1), (3, 4)])]);
    let b = search(&["ab", "a"], MatchOptions::default(), &["ab ab"]);
    assert_eq!(rows(&b.texts[0]), [("ab ab", vec![(0, 2), (3, 5)])]);
    let c = search(&["aba"], MatchOptions::default(), &["ababa"]);
    assert_eq!(rows(&c.texts[0]), [("ababa", vec![(0, 3)])]);
}

#[test]
fn literals_case_and_inline_flags() {
    let opts = MatchOptions {
        pattern_kind: PatternKind::Literal,
        case_mode: CaseMode::Insensitive,
        ..Default::default()
    };
    let r = search(&["a.b", "(?-u)"], opts, &["A.B axb (?-u)", "a\nb"]);
    assert_eq!(
        rows(&r.texts[0]),
        [("A.B axb (?-u)", vec![(0, 3), (8, 13)])]
    );
    assert!(r.texts[1].lines.is_empty());
    let r = search(&["(?i:a)b"], MatchOptions::default(), &["Ab AB ab"]);
    assert_eq!(rows(&r.texts[0]), [("Ab AB ab", vec![(0, 2), (6, 8)])]);
    let r = search(
        &["é"],
        MatchOptions {
            case_mode: CaseMode::Insensitive,
            ..Default::default()
        },
        &["É é"],
    );
    assert_eq!(rows(&r.texts[0]), [("É é", vec![(0, 2), (3, 5)])]);
}

#[test]
fn word_and_line_boundaries_follow_the_builder() {
    let word = MatchOptions {
        boundary: Boundary::Word,
        ..Default::default()
    };
    let r = search(&["-2"], word, &["foo -2 bar x-2 -2x"]);
    assert_eq!(rows(&r.texts[0]), [("foo -2 bar x-2 -2x", vec![(4, 6)])]);
    let r = search(
        &["error"],
        MatchOptions {
            boundary: Boundary::Line,
            ..Default::default()
        },
        &["error\nerror extra\n error\nerror"],
    );
    assert_eq!(
        rows(&r.texts[0]),
        [("error", vec![(0, 5)]), ("error", vec![(0, 5)])]
    );
}

#[test]
fn lf_policy_preserves_cr_whitespace_nul_and_bom() {
    let r = search(
        &["error"],
        MatchOptions::default(),
        &["  error\t\r\n\0error\n\u{feff}error\nerror"],
    );
    assert_eq!(
        rows(&r.texts[0]),
        [
            ("  error\t\r", vec![(2, 7)]),
            ("\0error", vec![(1, 6)]),
            ("\u{feff}error", vec![(3, 8)]),
            ("error", vec![(0, 5)])
        ]
    );
    let r = search(&[r"\x00"], MatchOptions::default(), &["a\0b"]);
    assert_eq!(rows(&r.texts[0]), [("a\0b", vec![(1, 2)])]);
}

#[test]
fn empty_patterns_only_emit_character_boundaries() {
    let r = search(
        &[""],
        MatchOptions::default(),
        &["", "\n", "é中", "a\n", "\n\n"],
    );
    assert!(r.texts[0].lines.is_empty());
    assert_eq!(rows(&r.texts[1]), [("", vec![(0, 0)])]);
    assert_eq!(rows(&r.texts[2]), [("é中", vec![(0, 0), (2, 2), (5, 5)])]);
    assert_eq!(rows(&r.texts[3]), [("a", vec![(0, 0), (1, 1)])]);
    assert_eq!(rows(&r.texts[4]), [("", vec![(0, 0)]), ("", vec![(0, 0)])]);
}

#[test]
fn empty_after_nonempty_matches_follow_upstream_suppression() {
    let r = search(&["a*"], MatchOptions::default(), &["aé", "aa"]);
    assert_eq!(rows(&r.texts[0]), [("aé", vec![(0, 1), (3, 3)])]);
    assert_eq!(rows(&r.texts[1]), [("aa", vec![(0, 2)])]);
}

#[test]
fn anchors_use_whole_logical_lines_not_extracted_fragments() {
    for regex in [r"\Aerror\z", r"^error$", r"(?-m:^error$)"] {
        let r = search(
            &[regex],
            MatchOptions::default(),
            &["prefix\nerror\nerror x\nerror\r\nerror"],
        );
        assert_eq!(
            rows(&r.texts[0]),
            [("error", vec![(0, 5)]), ("error", vec![(0, 5)])],
            "{regex}"
        );
    }
    let r = search(
        &[r"\berror\b"],
        MatchOptions::default(),
        &["xerror error errory"],
    );
    assert_eq!(rows(&r.texts[0]), [("xerror error errory", vec![(7, 12)])]);
}

#[test]
fn patterns_never_cross_fields_or_lines() {
    for regex in [r"a.*b", r"(?s)a.*b", r"a\sb"] {
        let r = search(&[regex], MatchOptions::default(), &["a\nb", "a", "b"]);
        assert!(r.texts.iter().all(|t| t.lines.is_empty()));
    }
    assert!(matches!(
        Plan::compile(&[br"a\nb"], MatchOptions::default(), compile_limits()),
        Err(Error::PatternPolicy(_))
    ));
}

#[test]
fn syntax_and_unicode_policy_are_checked_before_engine_build() {
    for regex in ["[", r"(?=a)", r"(a)\1", r"\p{NotAProperty}"] {
        assert!(
            matches!(
                Plan::compile(
                    &[regex.as_bytes()],
                    MatchOptions::default(),
                    compile_limits()
                ),
                Err(Error::Syntax(_))
            ),
            "{regex}"
        );
    }
    for regex in ["(?-u)a", "(?i-u:a)", "(?-u:(?u:a))"] {
        assert!(matches!(
            Plan::compile(
                &[regex.as_bytes()],
                MatchOptions::default(),
                compile_limits()
            ),
            Err(Error::PatternPolicy(_))
        ));
    }
    for regex in [r"\(\?-u\)", "[(?-u)]", "(?u:a)"] {
        plan(&[regex], MatchOptions::default());
    }
    assert!(matches!(
        Plan::compile(
            &[b"(?P<x>a)", b"(?P<x>b)"],
            MatchOptions::default(),
            compile_limits()
        ),
        Err(Error::Syntax(_))
    ));
}

#[test]
fn normalized_pattern_utf8_and_control_bytes() {
    assert!(matches!(
        Plan::compile(&[&[0xff]], MatchOptions::default(), compile_limits()),
        Err(Error::InvalidUtf8 {
            kind: InputKind::Pattern,
            index: 0,
            ..
        })
    ));
    for kind in [PatternKind::Regex, PatternKind::Literal] {
        for bytes in [b"a\nb".as_slice(), b"a\0b"] {
            assert!(matches!(
                Plan::compile(
                    &[bytes],
                    MatchOptions {
                        pattern_kind: kind,
                        ..Default::default()
                    },
                    compile_limits()
                ),
                Err(Error::PatternPolicy(_))
            ));
        }
    }
}

#[test]
fn compile_limits_fail_without_guessing_upstream_error_text() {
    let mut limits = compile_limits();
    limits.max_regex_bytes = 1;
    let err = Plan::compile(&[b"[a-z]+"], MatchOptions::default(), limits).unwrap_err();
    assert!(matches!(err, Error::EngineBuild(_)));
    assert!(std::error::Error::source(&err).is_some());
}

#[test]
fn invalid_limits_and_missing_patterns_are_rejected() {
    assert!(matches!(
        Plan::compile(&[], MatchOptions::default(), compile_limits()),
        Err(Error::InvalidConfiguration(_))
    ));
    for i in 0..2 {
        let mut l = compile_limits();
        match i {
            0 => l.max_regex_bytes = 0,
            _ => l.max_dfa_cache_bytes = 0,
        }
        assert!(matches!(
            Plan::compile(&[b"a"], MatchOptions::default(), l),
            Err(Error::InvalidConfiguration(_))
        ));
    }
    let mut session = Session::new(&plan(&["a"], MatchOptions::default()));
    let limits = 0;
    assert!(matches!(
        session.execute_accounted(&[], limits, ResultOverhead::default()),
        Err(Error::InvalidConfiguration(_))
    ));
}

#[test]
fn invalid_later_text_fails_batch_before_matching_and_session_recovers() {
    let mut session = Session::new(&plan(&["a"], MatchOptions::default()));
    // Enough metadata, but no space for matching payload. Validate all inputs first.
    let limits = (size_of::<BatchResult>() + 2 * size_of::<TextResult>()) as u64;
    let error = session
        .execute_accounted(&[b"a", &[0xff]], limits, ResultOverhead::default())
        .unwrap_err();
    assert!(matches!(
        error,
        Error::InvalidUtf8 {
            kind: InputKind::Text,
            index: 1,
            ..
        }
    ));
    assert!(!session
        .execute_accounted(&[b"a"], execution_limits(), ResultOverhead::default())
        .unwrap()
        .texts[0]
        .lines
        .is_empty());
}

#[test]
fn output_budget_charges_metadata_content_and_every_span() {
    let mut session = Session::new(&plan(&["a"], MatchOptions::default()));
    let required = size_of::<BatchResult>()
        + size_of::<TextResult>()
        + size_of::<Line>()
        + 3
        + 2 * size_of::<Span>();
    let mut limits = required as u64;
    assert_eq!(
        rows(
            &session
                .execute_accounted(&[b"a a"], limits, ResultOverhead::default())
                .unwrap()
                .texts[0]
        ),
        [("a a", vec![(0, 1), (2, 3)])]
    );
    limits -= 1;
    assert!(matches!(
        session.execute_accounted(&[b"a a"], limits, ResultOverhead::default()),
        Err(Error::ResourceLimit(LimitKind::ResultBytes))
    ));
    assert!(session
        .execute_accounted(&[b"a"], execution_limits(), ResultOverhead::default())
        .is_ok());
}

#[test]
fn metadata_only_result_exhaustion() {
    let mut session = Session::new(&plan(&["a"], MatchOptions::default()));
    let limits = size_of::<BatchResult>() as u64;
    assert!(session
        .execute_accounted(&[], limits, ResultOverhead::default())
        .unwrap()
        .texts
        .is_empty());
    assert!(matches!(
        session.execute_accounted(&[b""], limits, ResultOverhead::default()),
        Err(Error::ResourceLimit(LimitKind::ResultBytes))
    ));
}

#[test]
fn ownership_and_sticky_cancellation_between_batches() {
    let plan = plan(&["a"], MatchOptions::default());
    let mut session = Session::new(&plan);
    drop(plan);
    let token = session.cancellation_token();
    let result = {
        let text = String::from("a");
        session
            .execute_accounted(
                &[text.as_bytes()],
                execution_limits(),
                ResultOverhead::default(),
            )
            .unwrap()
    };
    token.cancel();
    token.cancel();
    assert!(matches!(
        session.execute_accounted(&[], execution_limits(), ResultOverhead::default()),
        Err(Error::Cancelled)
    ));
    drop(session);
    drop(token);
    assert_eq!(rows(&result.texts[0]), [("a", vec![(0, 1)])]);
}

#[test]
fn independent_sessions_can_share_a_plan_across_threads() {
    let plan = plan(&["error"], MatchOptions::default());
    let jobs: Vec<_> = (0..4)
        .map(|i| {
            let plan = plan.clone();
            std::thread::spawn(move || {
                let mut session = Session::new(&plan);
                for _ in 0..10 {
                    let text = format!("{i} error");
                    let r = session
                        .execute_accounted(
                            &[text.as_bytes()],
                            execution_limits(),
                            ResultOverhead::default(),
                        )
                        .unwrap();
                    assert_eq!(rows(&r.texts[0]), [(text.as_str(), vec![(2, 7)])]);
                }
            })
        })
        .collect();
    drop(plan);
    for job in jobs {
        job.join().unwrap();
    }
}

#[test]
fn cancellation_can_be_requested_from_another_thread() {
    let mut session = Session::new(&plan(&["a"], MatchOptions::default()));
    let token = session.cancellation_token();
    let barrier = Arc::new(Barrier::new(2));
    let other = barrier.clone();
    let job = std::thread::spawn(move || {
        let text = "a\n".repeat(2_000_000);
        other.wait();
        session.execute_accounted(
            &[text.as_bytes()],
            execution_limits(),
            ResultOverhead::default(),
        )
    });
    barrier.wait();
    token.cancel();
    assert!(matches!(job.join().unwrap(), Err(Error::Cancelled)));
}

#[test]
fn large_input_preserves_cancellation() {
    let mut session = Session::new(&plan(&["needle"], MatchOptions::default()));
    let limits = 65536;
    let large = vec![b'x'; 9 << 20];
    assert!(session
        .execute_accounted(&[&large], limits, ResultOverhead::default())
        .is_ok());
    session.cancellation_token().cancel();
    assert!(matches!(
        session.execute_accounted(&[&large], limits, ResultOverhead::default()),
        Err(Error::Cancelled)
    ));
}

#[test]
fn many_patterns_have_no_separate_count_limit() {
    let patterns = vec![b"needle".as_slice(); 600];
    let plan = Plan::compile(&patterns, MatchOptions::default(), compile_limits()).unwrap();
    let mut session = Session::new(&plan);
    let result = session
        .execute_accounted(&[b"needle"], execution_limits(), ResultOverhead::default())
        .unwrap();
    assert!(!result.texts[0].lines.is_empty());
}
