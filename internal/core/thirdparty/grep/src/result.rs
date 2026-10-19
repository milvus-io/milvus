/// A content-relative UTF-8 byte interval [start, end).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Span {
    pub start: u64,
    pub end: u64,
}

/// An entire matching line, excluding only its terminal LF. Spans never refer
/// to the original field coordinates. An empty match is represented explicitly.
#[derive(Debug, Eq, PartialEq)]
pub struct Line {
    pub content: Box<str>,
    pub spans: Box<[Span]>,
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct TextResult {
    pub lines: Box<[Line]>,
}

/// Owns all data independently of the input, session and plan. One entry per
/// input, including unselected fields. There are no partial successful results.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct BatchResult {
    pub texts: Box<[TextResult]>,
}
