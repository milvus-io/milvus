/// A content-relative UTF-8 byte interval [start, end).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Span {
    pub start: u64,
    pub end: u64,
}

/// A matching or context line, excluding only its terminal LF. Spans never refer
/// to the original field coordinates. An empty match is represented explicitly.
#[derive(Debug, Eq, PartialEq)]
pub struct Line {
    /// One-based logical line number within the original field.
    pub line_number: u64,
    /// Zero-based UTF-8 byte offset of this line in the original field.
    pub line_start_offset: u64,
    pub content: Box<str>,
    /// Empty for context; nonempty for a matching line (including zero-width hits).
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
