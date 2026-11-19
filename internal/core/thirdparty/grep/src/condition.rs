//! Necessary substring conditions, independent of an index or a matcher.

use regex_syntax::hir::{Class, Hir, HirKind};

/// A match implies this condition holds on the original value. The converse
/// is not true: order, boundaries and final acceptance belong to the matcher.
/// `True` means no pruning. Literal atoms are case-sensitive byte substrings.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LiteralCondition {
    True,
    Literal(Vec<u8>),
    And(Vec<Self>),
    Or(Vec<Self>),
}

impl LiteralCondition {
    // Match the shared C++ RegexLiteralCondition limits. Index-specific gram
    // length restrictions are deliberately not applied during compilation.
    pub const MAX_NODES: usize = 127;
    pub const MAX_LITERAL_BYTES: usize = 64;

    pub fn is_true(&self) -> bool {
        matches!(self, Self::True)
    }

    pub fn node_count(&self) -> usize {
        match self {
            Self::And(children) | Self::Or(children) => {
                1 + children.iter().map(Self::node_count).sum::<usize>()
            }
            _ => 1,
        }
    }

    pub(crate) fn extract(hir: &Hir) -> Self {
        Analyzer {
            remaining: MAX_VISITS,
        }
        .visit(hir, 0)
        .condition
    }

    fn literal(bytes: &[u8]) -> Self {
        // A prefix is still necessary. Trim a partial UTF-8 codepoint (or an
        // invalid suffix) so the future NGRAM consumer can tokenize the atom.
        let prefix = &bytes[..bytes.len().min(Self::MAX_LITERAL_BYTES)];
        let length = match std::str::from_utf8(prefix) {
            Ok(_) => prefix.len(),
            Err(error) => error.valid_up_to(),
        };
        if length < 2 {
            Self::True
        } else {
            Self::Literal(prefix[..length].to_vec())
        }
    }

    fn combine(self, other: Self, and: bool) -> Self {
        if self.is_true() || other.is_true() {
            if !and {
                return Self::True;
            }
            return if self.is_true() { other } else { self };
        }
        if self == other {
            return self;
        }
        // Save the left constraint before consuming either tree. Dropping an
        // AND constraint is safe; dropping an OR branch never is.
        if self.node_count() + other.node_count() + 1 > Self::MAX_NODES {
            return if and { self } else { Self::True };
        }
        let mut children = Vec::new();
        for node in [self, other] {
            match node {
                Self::And(inner) if and => children.extend(inner),
                Self::Or(inner) if !and => children.extend(inner),
                node => children.push(node),
            }
        }
        let mut unique = Vec::new();
        for child in children {
            if !unique.contains(&child) {
                unique.push(child);
            }
        }
        if unique.len() == 1 {
            unique.pop().unwrap()
        } else if and {
            Self::And(unique)
        } else {
            Self::Or(unique)
        }
    }
}

const MAX_VISITS: usize = 4096;
const MAX_DEPTH: usize = 64;
const MAX_SEQUENCES: usize = 32;

// A bounded finite cover of the bytes consumed by a node, NOT an exactness
// flag. Assertions can be ignored in this cover. It is used only to join
// adjacent literals/small classes before deriving substring conditions.
type Sequences = Vec<Vec<u8>>;

struct Analysis {
    condition: LiteralCondition,
    sequences: Option<Sequences>,
}

impl Analysis {
    fn unknown() -> Self {
        Self {
            condition: LiteralCondition::True,
            sequences: None,
        }
    }

    fn finite(sequences: Sequences) -> Self {
        Self {
            condition: sequence_condition(&sequences),
            sequences: Some(sequences),
        }
    }
}

fn sequence_condition(sequences: &[Vec<u8>]) -> LiteralCondition {
    let mut conditions = sequences
        .iter()
        .map(|bytes| LiteralCondition::literal(bytes));
    let Some(first) = conditions.next() else {
        return LiteralCondition::True;
    };
    conditions.fold(first, |left, right| left.combine(right, false))
}

fn product(left: &[Vec<u8>], right: &[Vec<u8>]) -> Option<Sequences> {
    if left.len().checked_mul(right.len())? > MAX_SEQUENCES {
        return None;
    }
    let mut joined = Vec::new();
    for a in left {
        for b in right {
            if a.len() + b.len() > LiteralCondition::MAX_LITERAL_BYTES {
                return None;
            }
            let mut bytes = a.clone();
            bytes.extend_from_slice(b);
            if !joined.contains(&bytes) {
                joined.push(bytes);
            }
        }
    }
    Some(joined)
}

struct Analyzer {
    remaining: usize,
}

impl Analyzer {
    fn visit(&mut self, hir: &Hir, depth: usize) -> Analysis {
        if self.remaining == 0 {
            return Analysis::unknown();
        }
        self.remaining -= 1;
        if depth >= MAX_DEPTH {
            return Analysis::unknown();
        }
        match hir.kind() {
            HirKind::Empty | HirKind::Look(_) => Analysis::finite(vec![vec![]]),
            HirKind::Literal(literal) => {
                if literal.0.len() <= LiteralCondition::MAX_LITERAL_BYTES {
                    Analysis::finite(vec![literal.0.to_vec()])
                } else {
                    Analysis {
                        condition: LiteralCondition::literal(&literal.0),
                        sequences: None,
                    }
                }
            }
            HirKind::Class(class) => class_sequences(class)
                .map(Analysis::finite)
                .unwrap_or_else(Analysis::unknown),
            HirKind::Capture(capture) => self.visit(&capture.sub, depth + 1),
            HirKind::Repetition(repetition) => {
                if repetition.min == 0 {
                    return Analysis::unknown();
                }
                let mut inner = self.visit(&repetition.sub, depth + 1);
                if repetition.min != 1 || repetition.max != Some(1) {
                    // One occurrence supplies necessary substrings, but is
                    // not a cover of all the bytes consumed by a repetition.
                    inner.sequences = None;
                }
                inner
            }
            HirKind::Concat(children) => self.concat(children, depth + 1),
            HirKind::Alternation(children) => self.alternation(children, depth + 1),
        }
    }

    fn concat(&mut self, children: &[Hir], depth: usize) -> Analysis {
        let mut condition = LiteralCondition::True;
        let mut pending: Option<Sequences> = None;
        let mut whole = true;
        for child in children {
            if self.remaining == 0 {
                whole = false;
                break; // Omit only AND constraints on exhaustion.
            }
            let analysis = self.visit(child, depth);
            if let Some(sequences) = analysis.sequences {
                pending = Some(match pending.take() {
                    None => sequences,
                    Some(previous) => match product(&previous, &sequences) {
                        Some(joined) => joined,
                        None => {
                            whole = false;
                            condition = condition.combine(sequence_condition(&previous), true);
                            sequences
                        }
                    },
                });
            } else {
                whole = false;
                if let Some(previous) = pending.take() {
                    condition = condition.combine(sequence_condition(&previous), true);
                }
                condition = condition.combine(analysis.condition, true);
            }
        }
        if let Some(sequences) = &pending {
            condition = condition.combine(sequence_condition(sequences), true);
        }
        Analysis {
            condition,
            sequences: if whole { pending } else { None },
        }
    }

    fn alternation(&mut self, children: &[Hir], depth: usize) -> Analysis {
        let mut condition = None;
        let mut sequences: Option<Sequences> = Some(Vec::new());
        for child in children {
            if self.remaining == 0 {
                return Analysis::unknown(); // Never retain an incomplete OR.
            }
            let analysis = self.visit(child, depth);
            condition = Some(match condition {
                None => analysis.condition,
                Some(previous) => LiteralCondition::combine(previous, analysis.condition, false),
            });
            sequences = match (sequences, analysis.sequences) {
                (Some(mut all), Some(branch)) if all.len() + branch.len() <= MAX_SEQUENCES => {
                    all.extend(branch);
                    Some(all)
                }
                _ => None,
            };
            if sequences.is_none() && condition.as_ref().is_some_and(LiteralCondition::is_true) {
                return Analysis::unknown();
            }
        }
        Analysis {
            condition: condition.unwrap_or(LiteralCondition::True),
            sequences,
        }
    }
}

fn class_sequences(class: &Class) -> Option<Sequences> {
    let mut sequences = Vec::new();
    match class {
        Class::Unicode(class) => {
            for range in class.iter() {
                let count = range.end() as u32 - range.start() as u32 + 1;
                if count as usize > MAX_SEQUENCES - sequences.len() {
                    return None;
                }
                for scalar in range.start() as u32..=range.end() as u32 {
                    if let Some(character) = char::from_u32(scalar) {
                        sequences.push(character.encode_utf8(&mut [0; 4]).as_bytes().to_vec());
                    }
                }
            }
        }
        Class::Bytes(class) => {
            for range in class.iter() {
                let count = usize::from(range.end()) - usize::from(range.start()) + 1;
                if count > MAX_SEQUENCES - sequences.len() {
                    return None;
                }
                sequences.extend((range.start()..=range.end()).map(|byte| vec![byte]));
            }
        }
    }
    // An impossible class may conservatively disable pruning.
    if sequences.is_empty() {
        None
    } else {
        Some(sequences)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn true_and_budget_fallbacks_are_directionally_safe() {
        let atom = LiteralCondition::literal(b"needle");
        for and in [false, true] {
            let expected = if and {
                atom.clone()
            } else {
                LiteralCondition::True
            };
            assert_eq!(atom.clone().combine(LiteralCondition::True, and), expected);
            assert_eq!(LiteralCondition::True.combine(atom.clone(), and), expected);
        }
        let mut and = LiteralCondition::True;
        let mut or = LiteralCondition::literal(b"first");
        for i in 0..200 {
            let atom = LiteralCondition::literal(format!("literal{i:03}").as_bytes());
            and = and.combine(atom.clone(), true);
            or = or.combine(atom, false);
            assert!(and.node_count() <= LiteralCondition::MAX_NODES);
            assert!(or.node_count() <= LiteralCondition::MAX_NODES);
        }
        assert!(!and.is_true());
        assert!(or.is_true());
    }

    #[test]
    fn work_exhaustion_drops_and_constraints_but_never_or_branches() {
        let hir = regex_syntax::Parser::new().parse("prefix.*suffix").unwrap();
        let mut analyzer = Analyzer { remaining: 2 };
        let result = analyzer.visit(&hir, 0);
        assert_eq!(analyzer.remaining, 0);
        assert_eq!(result.condition, LiteralCondition::literal(b"prefix"));
        assert!(result.sequences.is_none());

        let hir = regex_syntax::Parser::new()
            .parse("alpha.*omega|beta.*theta")
            .unwrap();
        let result = Analyzer { remaining: 2 }.visit(&hir, 0);
        assert!(result.condition.is_true());
        assert!(result.sequences.is_none());
    }
}
