use std::sync::Arc;

use grep::regex::{RegexMatcher, RegexMatcherBuilder};
use regex_syntax::ast::{self, Ast, Flag, GroupKind, Visitor};

use crate::{Error, InputKind, LimitKind};

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
}

/// Strictly positive service limits. Engine limits are approximate and do not
/// bound aggregate RSS or compilation time. No service defaults are invented.
#[derive(Clone, Copy, Debug)]
pub struct CompileLimits {
    pub max_regex_bytes: usize,
    pub max_dfa_cache_bytes: usize,
}

#[derive(Clone, Debug)]
pub struct Plan(Arc<RegexMatcher>);

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

        if options.pattern_kind == PatternKind::Regex {
            validate_regex(&strings, options)?;
        }
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
        Ok(Self(Arc::new(matcher)))
    }

    pub(crate) fn matcher(&self) -> RegexMatcher {
        (*self.0).clone()
    }
}

fn validate_regex(patterns: &[&str], options: MatchOptions) -> Result<(), Error> {
    // Match the upstream build_many grouping so scoped flags and duplicate
    // capture names are validated in the same combined expression.
    let capacity = patterns
        .iter()
        .try_fold(0usize, |n, p| n.checked_add(p.len())?.checked_add(5))
        .ok_or(Error::ResourceLimit(LimitKind::PatternBytes))?;
    let mut combined = String::new();
    combined.try_reserve_exact(capacity)?;
    for (i, pattern) in patterns.iter().enumerate() {
        if i > 0 {
            combined.push('|');
        }
        combined.push_str("(?:");
        combined.push_str(pattern);
        combined.push(')');
    }
    let ast = ast::parse::Parser::new()
        .parse(&combined)
        .map_err(|e| Error::Syntax(Box::new(e.into())))?;
    ast::visit(&ast, UnicodePolicy)?;
    // Validate Unicode classes and other HIR-level syntax before the upstream
    // builder stringifies its errors. Matching still uses grep-regex itself.
    regex_syntax::hir::translate::TranslatorBuilder::new()
        .utf8(false)
        .unicode(true)
        .multi_line(true)
        .case_insensitive(options.case_mode == CaseMode::Insensitive)
        .build()
        .translate(&combined, &ast)
        .map_err(|e| Error::Syntax(Box::new(e.into())))?;
    Ok(())
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
