use std::sync::Arc;

use grep::regex::{RegexMatcher, RegexMatcherBuilder};
use regex_syntax::ast::{self, Ast, Flag, GroupKind, Visitor};
use regex_syntax::hir::Hir;

use crate::{Error, InputKind, LimitKind, LiteralCondition};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PatternKind {
    #[default]
    Regex,
    Literal,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CaseMode {
    #[default]
    Sensitive,
    Insensitive,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Boundary {
    #[default]
    None,
    Word,
    Line,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MatchOptions {
    pub pattern_kind: PatternKind,
    pub case_mode: CaseMode,
    pub boundary: Boundary,
    /// Maximum matching lines per input. None is unlimited; Some(0) matches nothing.
    pub max_count: Option<u64>,
    pub before_context: u64,
    pub after_context: u64,
}

/// Strictly positive service limits. Engine limits are approximate and do not
/// bound aggregate RSS or compilation time. No service defaults are invented.
#[derive(Clone, Copy, Debug)]
pub struct CompileLimits {
    pub max_regex_bytes: usize,
    pub max_dfa_cache_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct Plan(Arc<PlanData>);

#[derive(Debug)]
struct PlanData {
    matcher: RegexMatcher,
    max_count: Option<u64>,
    before_context: u64,
    after_context: u64,
    condition: LiteralCondition,
    condition_payload: Vec<u8>,
}

impl Plan {
    /// Compile once from a typed specification. Pattern order is preserved.
    /// No caller storage is retained. Execution retains the plan for reuse;
    /// standalone validators may drop it immediately after a successful build.
    pub fn compile(
        patterns: &[&[u8]],
        options: MatchOptions,
        limits: CompileLimits,
    ) -> Result<Self, Error> {
        if limits.max_regex_bytes == 0 || limits.max_dfa_cache_bytes == 0 {
            return Err(Error::InvalidConfiguration(
                "compile limits must be positive",
            ));
        }
        if patterns.is_empty() {
            return Err(Error::InvalidConfiguration(
                "at least one pattern is required",
            ));
        }
        let mut strings = Vec::new();
        strings.try_reserve_exact(patterns.len())?;
        for (index, pattern) in patterns.iter().enumerate() {
            let pattern = std::str::from_utf8(pattern).map_err(|source| Error::InvalidUtf8 {
                kind: InputKind::Pattern,
                index,
                source,
            })?;
            if pattern.contains(['\n', '\0']) {
                return Err(Error::PatternPolicy(
                    "literal LF and NUL are not permitted in patterns",
                ));
            }
            strings.push(pattern);
        }

        let condition = if options.pattern_kind == PatternKind::Regex {
            // Reuse the HIR already needed for validation. Drop it before
            // constructing the engine; the plan retains only the bounded tree.
            LiteralCondition::extract(&prepare_hir(&strings, options)?)
        } else {
            literal_condition(&strings, options)
        };
        let mut builder = RegexMatcherBuilder::new();
        builder
            .unicode(true)
            .multi_line(true)
            .line_terminator(Some(b'\n'))
            .case_insensitive(options.case_mode == CaseMode::Insensitive)
            .fixed_strings(options.pattern_kind == PatternKind::Literal)
            .word(options.boundary == Boundary::Word)
            .whole_line(options.boundary == Boundary::Line)
            .size_limit(limits.max_regex_bytes)
            .dfa_size_limit(limits.max_dfa_cache_bytes);
        let matcher = builder.build_many(&strings).map_err(|err| {
            match err.kind() {
                grep::regex::ErrorKind::NotAllowed(_) => {
                    Error::PatternPolicy("pattern requires a line terminator")
                }
                // Syntax and size-limit causes can both be lost upstream.
                // Preserve the source, never guess based on its text.
                _ => Error::EngineBuild(err),
            }
        })?;
        let condition_payload = crate::condition_proto::encode(&condition)?;
        Ok(Self(Arc::new(PlanData {
            matcher,
            max_count: options.max_count,
            before_context: options.before_context,
            after_context: options.after_context,
            condition,
            condition_payload,
        })))
    }

    /// Index-independent necessary condition. Even a useful condition cannot
    /// replace this plan's matcher or establish candidate exactness.
    pub fn literal_condition(&self) -> &LiteralCondition {
        &self.0.condition
    }

    /// Cached versioned Protobuf bytes, borrowed from this plan. Encoding
    /// budgets may weaken the extracted condition to True; they never weaken
    /// the matcher. This view contains binary data, including possible NULs.
    pub fn condition_payload(&self) -> &[u8] {
        &self.0.condition_payload
    }

    pub(crate) fn matcher(&self) -> RegexMatcher {
        self.0.matcher.clone()
    }

    pub(crate) fn context(&self) -> (u64, u64) {
        (self.0.before_context, self.0.after_context)
    }

    pub(crate) fn max_count(&self) -> Option<u64> {
        self.0.max_count
    }
}

fn literal_condition(patterns: &[&str], options: MatchOptions) -> LiteralCondition {
    // Literal mode previously skipped HIR construction altogether. Bound the
    // additional parsing work, including large lists of empty patterns. Failure
    // here disables pruning; only the authoritative matcher may reject input.
    const MAX_LITERAL_HIR_BYTES: usize = 4096;
    let bounded = patterns.iter().try_fold(0usize, |size, pattern| {
        let next = size
            .checked_add(pattern.len().checked_mul(2)?)?
            .checked_add(5)?;
        (next <= MAX_LITERAL_HIR_BYTES).then_some(next)
    });
    if bounded.is_none() {
        return LiteralCondition::True;
    }
    prepare_hir(patterns, options)
        .map(|hir| LiteralCondition::extract(&hir))
        .unwrap_or(LiteralCondition::True)
}

fn prepare_hir(patterns: &[&str], options: MatchOptions) -> Result<Hir, Error> {
    // Match the upstream build_many grouping so scoped flags and duplicate
    // capture names are validated in the same combined expression.
    let capacity = patterns
        .iter()
        .try_fold(0usize, |n, p| {
            let bytes = if options.pattern_kind == PatternKind::Literal {
                p.len().checked_mul(2)?
            } else {
                p.len()
            };
            n.checked_add(bytes)?.checked_add(5)
        })
        .ok_or(Error::ResourceLimit(LimitKind::PatternBytes))?;
    let mut combined = String::new();
    combined.try_reserve_exact(capacity)?;
    for (i, pattern) in patterns.iter().enumerate() {
        if i > 0 {
            combined.push('|');
        }
        combined.push_str("(?:");
        if options.pattern_kind == PatternKind::Literal {
            regex_syntax::escape_into(pattern, &mut combined);
        } else {
            combined.push_str(pattern);
        }
        combined.push(')');
    }
    let ast = ast::parse::Parser::new()
        .parse(&combined)
        .map_err(|e| Error::Syntax(Box::new(e.into())))?;
    if options.pattern_kind == PatternKind::Regex {
        ast::visit(&ast, UnicodePolicy)?;
    }
    // Validate Unicode classes and other HIR-level syntax before the upstream
    // builder stringifies its errors. Matching still uses grep-regex itself.
    regex_syntax::hir::translate::TranslatorBuilder::new()
        .utf8(false)
        .unicode(true)
        .multi_line(true)
        .case_insensitive(options.case_mode == CaseMode::Insensitive)
        .build()
        .translate(&combined, &ast)
        .map_err(|e| Error::Syntax(Box::new(e.into())))
}

struct UnicodePolicy;
impl Visitor for UnicodePolicy {
    type Output = ();
    type Err = Error;
    fn finish(self) -> Result<(), Error> {
        Ok(())
    }
    fn visit_pre(&mut self, ast: &Ast) -> Result<(), Error> {
        let flags = match ast {
            Ast::Flags(set) => Some(&set.flags),
            Ast::Group(group) => match &group.kind {
                GroupKind::NonCapturing(flags) => Some(flags),
                _ => None,
            },
            _ => None,
        };
        if flags.is_some_and(|f| f.flag_state(Flag::Unicode) == Some(false)) {
            return Err(Error::PatternPolicy(
                "inline Unicode disabling is not supported",
            ));
        }
        Ok(())
    }
}
