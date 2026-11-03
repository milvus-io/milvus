use crate::{
    session::ResultOverhead, Boundary, CaseMode, CompileLimits, LiteralCondition as C,
    MatchOptions, PatternKind, Plan, Session,
};

fn compile(patterns: &[&str], options: MatchOptions) -> Plan {
    Plan::compile(
        &patterns.iter().map(|p| p.as_bytes()).collect::<Vec<_>>(),
        options,
        CompileLimits {
            max_regex_bytes: 8 * 1024 * 1024,
            max_dfa_cache_bytes: 1024 * 1024,
        },
    )
    .unwrap()
}

fn lit(value: &str) -> C {
    C::Literal(value.as_bytes().to_vec())
}

fn holds(condition: &C, value: &[u8]) -> bool {
    match condition {
        C::True => true,
        C::Literal(bytes) => memchr::memmem::find(value, bytes).is_some(),
        C::And(children) => children.iter().all(|child| holds(child, value)),
        C::Or(children) => children.iter().any(|child| holds(child, value)),
    }
}

fn check_bounds(condition: &C) {
    assert!(condition.node_count() <= C::MAX_NODES);
    match condition {
        C::True => {}
        C::Literal(bytes) => {
            assert!((2..=C::MAX_LITERAL_BYTES).contains(&bytes.len()));
            assert!(std::str::from_utf8(bytes).is_ok());
        }
        C::And(children) | C::Or(children) => {
            assert!(children.len() >= 2);
            assert!(!children.iter().any(C::is_true));
            for child in children {
                check_bounds(child);
            }
        }
    }
}

// The oracle is the actual line-oriented Grep session, not another extractor.
fn check_implication(plan: &Plan, texts: &[String]) -> usize {
    check_bounds(plan.literal_condition());
    let inputs = texts.iter().map(|text| text.as_bytes()).collect::<Vec<_>>();
    let output = Session::new(plan)
        .execute_accounted(&inputs, 16 * 1024 * 1024, ResultOverhead::default())
        .unwrap();
    let mut matches = 0;
    for (text, result) in texts.iter().zip(output.texts.iter()) {
        if !result.lines.is_empty() {
            matches += 1;
            assert!(
                holds(plan.literal_condition(), text.as_bytes()),
                "matcher hit {text:?} rejected by {:?}",
                plan.literal_condition(),
            );
        }
    }
    matches
}

#[test]
fn necessary_conditions_preserve_internal_literals_and_boolean_structure() {
    let cases = [
        ("error.*timeout", C::And(vec![lit("error"), lit("timeout")])),
        ("error|timeout", C::Or(vec![lit("error"), lit("timeout")])),
        (
            "(error|warn).*timeout",
            C::And(vec![C::Or(vec![lit("error"), lit("warn")]), lit("timeout")]),
        ),
        ("(error)?timeout", lit("timeout")),
        ("abc[12]def", C::Or(vec![lit("abc1def"), lit("abc2def")])),
        ("abcX?def", C::And(vec![lit("abc"), lit("def")])),
        (
            "(?:error)+.*timeout",
            C::And(vec![lit("error"), lit("timeout")]),
        ),
        ("error|\\d+", C::True),
        ("error|a", C::True),
        ("error|", C::True),
        ("(?:error)*", C::True),
        ("[a&&b]", C::True),
        ("", C::True),
    ];
    for (pattern, expected) in cases {
        let plan = compile(&[pattern], MatchOptions::default());
        assert_eq!(plan.literal_condition(), &expected, "{pattern}");
    }
    let plan = compile(&["error", "timeout"], MatchOptions::default());
    assert_eq!(
        plan.literal_condition(),
        &C::Or(vec![lit("error"), lit("timeout")])
    );
    let plan = compile(&["error", ""], MatchOptions::default());
    assert!(plan.literal_condition().is_true());
    let plan = compile(&["error", "error"], MatchOptions::default());
    assert_eq!(plan.literal_condition(), &lit("error"));
}

#[test]
fn fixed_strings_and_scoped_case_flags_keep_matcher_semantics() {
    let literal = MatchOptions {
        pattern_kind: PatternKind::Literal,
        ..Default::default()
    };
    let plan = compile(&["error.*timeout", "(?-u)"], literal);
    assert_eq!(
        plan.literal_condition(),
        &C::Or(vec![lit("error.*timeout"), lit("(?-u)")])
    );
    let patterns = [
        "(?i:error)",
        "(?i:k)s",
        "(?i:σ)ab",
        "(?i:ſ)ab",
        "(?i:é)x",
        "(?i:abc)(?-i:DEF)",
    ];
    let mut texts = vec![
        "Ks", "ks", "Ks", "KS", "Σab", "σab", "ςab", "Sab", "sab", "ſab", "Éx", "éx", "ABCDEF",
        "AbcDEF", "ABCdef",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    for mask in 0..32 {
        texts.push(
            "error"
                .chars()
                .enumerate()
                .map(|(i, c)| {
                    if mask & (1 << i) != 0 {
                        c.to_ascii_uppercase()
                    } else {
                        c
                    }
                })
                .collect(),
        );
    }
    for pattern in patterns {
        let plan = compile(&[pattern], MatchOptions::default());
        assert!(
            check_implication(&plan, &texts) > 0,
            "no matcher witness for {pattern}"
        );
    }
    let plan = compile(
        &["error"],
        MatchOptions {
            case_mode: CaseMode::Insensitive,
            ..literal
        },
    );
    assert_eq!(check_implication(&plan, &texts), 32);
}

#[test]
fn options_boundaries_newlines_and_multiple_patterns_match_actual_grep() {
    let patterns = ["error.*timeout", "(?i:warn)"];
    let texts = [
        "error---timeout",
        "timeout error",
        "warn",
        "WARN",
        "prewarnpost",
        "error\ntimeout",
        "first\nerror timeout\r\nWARN",
        "error.*timeout",
        "(?i:warn)",
        "",
        "\n",
        "éwarn中",
        "\0warn",
        "\u{feff}warn",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    for pattern_kind in [PatternKind::Regex, PatternKind::Literal] {
        for case_mode in [CaseMode::Sensitive, CaseMode::Insensitive] {
            for boundary in [Boundary::None, Boundary::Word, Boundary::Line] {
                let plan = compile(
                    &patterns,
                    MatchOptions {
                        pattern_kind,
                        case_mode,
                        boundary,
                        ..Default::default()
                    },
                );
                assert!(check_implication(&plan, &texts) > 0);
            }
        }
    }
    for pattern in [
        r"\x00foo",
        r"\bwarn\b",
        r"\Bwarn",
        r"(?m:^warn$)",
        r"(?s:error.*timeout)",
        r"[\s\S]*warn",
        r"(?:warn|)",
    ] {
        let mut corpus = texts.clone();
        corpus.push("\0foo".to_owned());
        assert!(
            check_implication(&compile(&[pattern], MatchOptions::default()), &corpus) > 0,
            "{pattern}"
        );
    }
}

#[test]
fn bounded_extraction_never_keeps_only_a_subset_of_alternatives() {
    let patterns = (0..180)
        .map(|i| format!("word{i:03}.*end{i:03}"))
        .collect::<Vec<_>>();
    let texts = (0..180)
        .map(|i| format!("word{i:03} -- end{i:03}"))
        .collect::<Vec<_>>();
    let plan = compile(
        &patterns.iter().map(String::as_str).collect::<Vec<_>>(),
        MatchOptions::default(),
    );
    assert_eq!(check_implication(&plan, &texts), texts.len());

    let and = patterns.join(".*");
    let witness = texts.join(" ");
    assert_eq!(
        check_implication(&compile(&[&and], MatchOptions::default()), &[witness]),
        1
    );

    let deep = format!("{}needle{}", "(".repeat(80), ")".repeat(80));
    let plan = compile(&[&deep], MatchOptions::default());
    assert!(plan.literal_condition().is_true());
    assert_eq!(check_implication(&plan, &["needle".to_owned()]), 1);

    let long = "中".repeat(50);
    let plan = compile(&[&long], MatchOptions::default());
    assert_eq!(plan.literal_condition(), &lit(&"中".repeat(21)));
    assert_eq!(check_implication(&plan, &[long]), 1);

    let literal = "(?-u)".repeat(1000);
    let plan = compile(
        &[&literal],
        MatchOptions {
            pattern_kind: PatternKind::Literal,
            ..Default::default()
        },
    );
    assert!(plan.literal_condition().is_true());
    assert_eq!(check_implication(&plan, &[literal]), 1);
}

#[test]
fn generated_small_regexes_obey_implication_on_exhaustive_texts() {
    let mut texts = vec![String::new()];
    let mut level = vec![String::new()];
    for _ in 0..4 {
        level = level
            .iter()
            .flat_map(|prefix| ['a', 'b', '1', 'é', '\n'].map(|c| format!("{prefix}{c}")))
            .collect();
        texts.extend(level.iter().cloned());
    }
    let atoms = ["a", "ab", "é", "[ab]", "[12]", "a?", "b+", "(?:a|)"];
    let mut matches = 0;
    for a in atoms {
        for b in atoms {
            for pattern in [
                format!("{a}{b}"),
                format!("(?:{a})|(?:{b})"),
                format!("{a}.*{b}"),
                format!("(?:{a})?{b}"),
            ] {
                matches +=
                    check_implication(&compile(&[&pattern], MatchOptions::default()), &texts);
            }
        }
    }
    assert!(
        matches > 1000,
        "generated corpus must exercise actual matcher hits"
    );
}

#[test]
fn retaining_conditions_does_not_relax_validation_or_plan_ownership() {
    let limits = CompileLimits {
        max_regex_bytes: 8 * 1024 * 1024,
        max_dfa_cache_bytes: 1024 * 1024,
    };
    for pattern in ["(", "(?-u:a)", r"\n", "literal\nLF", "literal\0NUL"] {
        assert!(
            Plan::compile(&[pattern.as_bytes()], MatchOptions::default(), limits).is_err(),
            "{pattern:?}"
        );
    }
    assert!(Plan::compile(
        &[b"[a-z]{1000}"],
        MatchOptions::default(),
        CompileLimits {
            max_regex_bytes: 1,
            ..limits
        }
    )
    .is_err());
    let owned = String::from("needle");
    let plan = compile(&[&owned], MatchOptions::default());
    let clone = plan.clone();
    drop(owned);
    drop(plan);
    assert_eq!(clone.literal_condition(), &lit("needle"));
    assert_eq!(check_implication(&clone, &["a needle here".to_owned()]), 1);
}
