/// A content-relative byte interval [start, end). Endpoints may lie inside
/// a UTF-8 code point; the full line content remains valid UTF-8.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Span {
    pub start: u64,
    pub end: u64,
}

/// A matching or context line, including its original terminal LF. Spans never refer
/// to the original field coordinates. An empty match is represented explicitly.
#[derive(Debug, Eq, PartialEq)]
pub struct Line {
    /// One-based logical line number within the original field.
    pub line_number: u64,
    /// Zero-based UTF-8 byte offset of this line in the original field.
    pub line_start_offset: u64,
    pub content: Box<str>,
    /// Set by line selection, independently of span enumeration.
    pub is_match: bool,
    /// Empty for context. Matching lines enumerate their line-local spans,
    /// including zero-length matches at the end of the line.
    pub spans: Box<[Span]>,
}

#[derive(Debug, Default, Eq, PartialEq)]
pub struct TextResult {
    pub matched: bool,
    /// Populated only for summary modes; details do not compute a summary.
    pub count: u64,
    pub complete: bool,
    pub lines: Box<[Line]>,
}

/// Owns all data independently of the input, session and plan. One entry per
/// input, including unselected fields. There are no partial successful results.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct BatchResult {
    pub texts: Box<[TextResult]>,
}
