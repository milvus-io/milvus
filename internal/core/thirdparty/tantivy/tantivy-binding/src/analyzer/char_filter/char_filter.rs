pub(crate) trait CharFilter: Send + Sync {
    fn apply(&self, input: FilteredText) -> FilteredText;
    fn box_clone(&self) -> BoxCharFilter;
}

pub(crate) type BoxCharFilter = Box<dyn CharFilter>;

impl Clone for BoxCharFilter {
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FilteredText {
    pub(crate) text: String,
    corrections: OffsetCorrections,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum OffsetMappingMode {
    #[default]
    SourceSpan,
    Boundary,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum OffsetCorrections {
    SourceSpan(Vec<SpanOffsetCorrection>),
    Boundary(Vec<BoundaryOffsetCorrection>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SpanOffsetCorrection {
    // Tuple (a, b, c, d) maps [a, a + b) to
    // [a + d, a + b + c + d).
    filtered_start: usize,
    filtered_len: usize,
    length_delta: isize,
    start_delta: isize,
}

impl SpanOffsetCorrection {
    fn new(
        filtered_start: usize,
        filtered_len: usize,
        original_start: usize,
        original_end: usize,
    ) -> Self {
        debug_assert!(original_start <= original_end);
        SpanOffsetCorrection {
            filtered_start,
            filtered_len,
            length_delta: signed_delta(original_end - original_start, filtered_len),
            start_delta: signed_delta(original_start, filtered_start),
        }
    }

    fn filtered_end(&self) -> usize {
        self.filtered_start + self.filtered_len
    }

    fn original_start(&self) -> usize {
        add_delta(self.filtered_start, self.start_delta)
    }

    fn original_end(&self) -> usize {
        add_delta(self.filtered_end(), self.start_delta + self.length_delta)
    }

    fn post_delta(&self) -> isize {
        self.start_delta + self.length_delta
    }

    fn correct_start(&self, offset: usize) -> usize {
        if offset < self.filtered_end() {
            self.original_start()
        } else {
            add_delta(offset, self.post_delta())
        }
    }

    fn correct_end(&self, offset: usize) -> usize {
        debug_assert!(self.filtered_start < offset);
        if offset <= self.filtered_end() {
            self.original_end()
        } else {
            add_delta(offset, self.post_delta())
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BoundaryOffsetCorrection {
    filtered_offset: usize,
    original_offset: usize,
}

impl Default for FilteredText {
    fn default() -> Self {
        FilteredText::new("")
    }
}

impl FilteredText {
    pub(crate) fn new(text: &str) -> Self {
        Self::with_offset_mode(text, OffsetMappingMode::default())
    }

    pub(crate) fn with_offset_mode(text: &str, mode: OffsetMappingMode) -> Self {
        FilteredText {
            text: text.to_string(),
            corrections: match mode {
                OffsetMappingMode::SourceSpan => OffsetCorrections::SourceSpan(Vec::new()),
                OffsetMappingMode::Boundary => OffsetCorrections::Boundary(Vec::new()),
            },
        }
    }

    pub(crate) fn correct_offsets(&self, offset_from: usize, offset_to: usize) -> (usize, usize) {
        debug_assert!(offset_from <= offset_to);

        match &self.corrections {
            OffsetCorrections::SourceSpan(corrections) => {
                Self::correct_source_span_offsets(corrections, offset_from, offset_to)
            }
            OffsetCorrections::Boundary(corrections) => {
                Self::correct_boundary_offsets(corrections, offset_from, offset_to)
            }
        }
    }

    fn correct_source_span_offsets(
        corrections: &[SpanOffsetCorrection],
        offset_from: usize,
        offset_to: usize,
    ) -> (usize, usize) {
        let correction_at_or_before = |offset| {
            corrections
                .partition_point(|correction| correction.filtered_start <= offset)
                .checked_sub(1)
        };

        if offset_from >= offset_to {
            let offset = correction_at_or_before(offset_from).map_or(offset_from, |index| {
                corrections[index].correct_start(offset_from)
            });
            return (offset, offset);
        }

        let start_index = correction_at_or_before(offset_from);
        let corrected_from = start_index.map_or(offset_from, |index| {
            corrections[index].correct_start(offset_from)
        });

        // Token spans are normally short. Reuse the start lookup and walk only
        // the correction records crossed by this token.
        let mut end_index = start_index;
        let mut next_index = start_index.map_or(0, |index| index + 1);
        while next_index < corrections.len() && corrections[next_index].filtered_start < offset_to {
            end_index = Some(next_index);
            next_index += 1;
        }
        let corrected_to =
            end_index.map_or(offset_to, |index| corrections[index].correct_end(offset_to));

        (corrected_from, corrected_to)
    }

    fn correct_boundary_offsets(
        corrections: &[BoundaryOffsetCorrection],
        offset_from: usize,
        offset_to: usize,
    ) -> (usize, usize) {
        let start_index = corrections
            .partition_point(|correction| correction.filtered_offset <= offset_from)
            .checked_sub(1);
        let corrected_from = Self::correct_boundary_offset(corrections, start_index, offset_from);

        // Token spans are normally short, so reuse the start lookup for the end.
        let mut end_index = start_index;
        let mut next_index = start_index.map_or(0, |index| index + 1);
        while next_index < corrections.len() && corrections[next_index].filtered_offset <= offset_to
        {
            end_index = Some(next_index);
            next_index += 1;
        }
        let corrected_to = Self::correct_boundary_offset(corrections, end_index, offset_to);
        (corrected_from, corrected_to)
    }

    fn correct_boundary_offset(
        corrections: &[BoundaryOffsetCorrection],
        correction_index: Option<usize>,
        offset: usize,
    ) -> usize {
        correction_index.map_or(offset, |index| {
            let correction = &corrections[index];
            correction.original_offset + (offset - correction.filtered_offset)
        })
    }

    /// Replacements must be sorted, non-overlapping, and aligned to UTF-8 byte
    /// boundaries in the current filtered text.
    pub(crate) fn replace_ranges<S: AsRef<str>>(
        self,
        replacements: Vec<(usize, usize, S)>,
    ) -> Self {
        debug_assert!(
            Self::valid_replacements(&self.text, &replacements),
            "char filter replacements must be sorted, non-overlapping, and valid UTF-8 ranges"
        );

        if replacements.is_empty() {
            return self;
        }

        let correction_capacity = self.correction_count() + replacements.len();
        let mut output = FilteredText {
            text: String::with_capacity(self.text.len()),
            corrections: match &self.corrections {
                OffsetCorrections::SourceSpan(_) => {
                    OffsetCorrections::SourceSpan(Vec::with_capacity(correction_capacity))
                }
                OffsetCorrections::Boundary(_) => {
                    OffsetCorrections::Boundary(Vec::with_capacity(correction_capacity * 2))
                }
            },
        };

        let mut cursor = 0;
        let mut correction_index = 0;
        for (start, end, replacement) in replacements {
            self.push_original_segment(cursor, start, &mut correction_index, &mut output);
            self.push_replacement(start, end, replacement.as_ref(), &mut output);
            cursor = end;
        }
        self.push_original_segment(cursor, self.text.len(), &mut correction_index, &mut output);

        output
    }

    fn correction_count(&self) -> usize {
        match &self.corrections {
            OffsetCorrections::SourceSpan(corrections) => corrections.len(),
            OffsetCorrections::Boundary(corrections) => corrections.len(),
        }
    }

    fn valid_replacements<S>(text: &str, replacements: &[(usize, usize, S)]) -> bool {
        let mut cursor = 0;
        for (start, end, _) in replacements {
            if *start < cursor || *start > *end || *end > text.len() {
                return false;
            }
            if !text.is_char_boundary(*start) || !text.is_char_boundary(*end) {
                return false;
            }
            cursor = *end;
        }
        true
    }

    fn push_original_segment(
        &self,
        start: usize,
        end: usize,
        correction_index: &mut usize,
        output: &mut FilteredText,
    ) {
        let output_start = output.text.len();
        output.text.push_str(&self.text[start..end]);

        match (&self.corrections, &mut output.corrections) {
            (
                OffsetCorrections::SourceSpan(corrections),
                OffsetCorrections::SourceSpan(output_corrections),
            ) => {
                if start >= end {
                    return;
                }

                while *correction_index < corrections.len() {
                    let correction = &corrections[*correction_index];
                    let correction_end = correction.filtered_end();

                    if correction.filtered_len == 0 {
                        if correction.filtered_start < start {
                            *correction_index += 1;
                            continue;
                        }
                        if correction.filtered_start >= end {
                            break;
                        }

                        Self::push_span_correction(
                            output_corrections,
                            output_start + correction.filtered_start - start,
                            0,
                            correction.original_start(),
                            correction.original_end(),
                        );
                        *correction_index += 1;
                        continue;
                    }

                    if correction_end <= start {
                        *correction_index += 1;
                        continue;
                    }
                    if correction.filtered_start >= end {
                        break;
                    }

                    let copied_start = correction.filtered_start.max(start);
                    let copied_end = correction_end.min(end);
                    Self::push_span_correction(
                        output_corrections,
                        output_start + copied_start - start,
                        copied_end - copied_start,
                        correction.original_start(),
                        correction.original_end(),
                    );

                    if correction_end <= end {
                        *correction_index += 1;
                    } else {
                        break;
                    }
                }
            }
            (
                OffsetCorrections::Boundary(corrections),
                OffsetCorrections::Boundary(output_corrections),
            ) => {
                let original_start = Self::correct_boundary_offset_at(corrections, start);
                Self::push_boundary_correction(output_corrections, output_start, original_start);

                let first =
                    corrections.partition_point(|correction| correction.filtered_offset <= start);
                let last =
                    corrections.partition_point(|correction| correction.filtered_offset <= end);
                for correction in &corrections[first..last] {
                    Self::push_boundary_correction(
                        output_corrections,
                        output_start + correction.filtered_offset - start,
                        correction.original_offset,
                    );
                }
            }
            _ => unreachable!("offset mapping mode must remain unchanged"),
        }
    }

    fn push_replacement(
        &self,
        start: usize,
        end: usize,
        replacement: &str,
        output: &mut FilteredText,
    ) {
        let output_start = output.text.len();

        match (&self.corrections, &mut output.corrections) {
            (
                OffsetCorrections::SourceSpan(_),
                OffsetCorrections::SourceSpan(output_corrections),
            ) => {
                let (original_start, original_end) = self.correct_offsets(start, end);
                output.text.push_str(replacement);
                Self::push_span_correction(
                    output_corrections,
                    output_start,
                    replacement.len(),
                    original_start,
                    original_end,
                );
            }
            (
                OffsetCorrections::Boundary(corrections),
                OffsetCorrections::Boundary(output_corrections),
            ) => {
                if replacement.is_empty() {
                    let corrected = Self::correct_boundary_offset_at(corrections, end);
                    Self::push_boundary_correction(output_corrections, output_start, corrected);
                    return;
                }

                let source_boundaries = self.char_boundaries(start, end);
                let replacement_boundaries = char_boundaries(replacement);
                let source_char_count = source_boundaries.len().saturating_sub(1);
                let replacement_char_count = replacement_boundaries.len().saturating_sub(1);
                let mut source_correction_index = corrections
                    .partition_point(|correction| {
                        correction.filtered_offset <= source_boundaries[0]
                    })
                    .checked_sub(1);
                let mut next_correction_index =
                    source_correction_index.map_or(0, |index| index + 1);

                output.text.push_str(replacement);
                for (boundary_index, boundary) in replacement_boundaries.iter().enumerate() {
                    let source_boundary_index = if boundary_index == replacement_char_count {
                        source_char_count
                    } else if source_char_count == 0 {
                        0
                    } else if replacement_char_count > source_char_count
                        && boundary_index >= source_char_count
                    {
                        source_char_count - 1
                    } else {
                        boundary_index.min(source_char_count)
                    };
                    let source_boundary = source_boundaries[source_boundary_index];
                    while next_correction_index < corrections.len()
                        && corrections[next_correction_index].filtered_offset <= source_boundary
                    {
                        source_correction_index = Some(next_correction_index);
                        next_correction_index += 1;
                    }
                    let corrected = Self::correct_boundary_offset(
                        corrections,
                        source_correction_index,
                        source_boundary,
                    );
                    Self::push_boundary_correction(
                        output_corrections,
                        output_start + boundary,
                        corrected,
                    );
                }
            }
            _ => unreachable!("offset mapping mode must remain unchanged"),
        }
    }

    fn push_span_correction(
        corrections: &mut Vec<SpanOffsetCorrection>,
        filtered_start: usize,
        filtered_len: usize,
        original_start: usize,
        original_end: usize,
    ) {
        debug_assert!(corrections.last().map_or(true, |correction| {
            correction.filtered_start <= filtered_start
        }));
        corrections.push(SpanOffsetCorrection::new(
            filtered_start,
            filtered_len,
            original_start,
            original_end,
        ));
    }

    fn correct_boundary_offset_at(
        corrections: &[BoundaryOffsetCorrection],
        offset: usize,
    ) -> usize {
        let index = corrections
            .partition_point(|correction| correction.filtered_offset <= offset)
            .checked_sub(1);
        Self::correct_boundary_offset(corrections, index, offset)
    }

    fn push_boundary_correction(
        corrections: &mut Vec<BoundaryOffsetCorrection>,
        filtered_offset: usize,
        original_offset: usize,
    ) {
        if corrections
            .last()
            .is_some_and(|last| last.filtered_offset == filtered_offset)
        {
            corrections.pop();
        }

        let expected_original_offset = corrections.last().map_or(filtered_offset, |last| {
            last.original_offset + (filtered_offset - last.filtered_offset)
        });
        if original_offset != expected_original_offset {
            corrections.push(BoundaryOffsetCorrection {
                filtered_offset,
                original_offset,
            });
        }
    }

    fn char_boundaries(&self, start: usize, end: usize) -> Vec<usize> {
        let mut boundaries = Vec::new();
        boundaries.push(start);
        for (offset, _) in self.text[start..end].char_indices().skip(1) {
            boundaries.push(start + offset);
        }
        boundaries.push(end);
        boundaries
    }
}

fn char_boundaries(text: &str) -> Vec<usize> {
    let mut boundaries = Vec::new();
    boundaries.push(0);
    for (offset, _) in text.char_indices().skip(1) {
        boundaries.push(offset);
    }
    boundaries.push(text.len());
    boundaries
}

fn signed_delta(value: usize, base: usize) -> isize {
    if value >= base {
        isize::try_from(value - base).expect("offset delta must fit in isize")
    } else {
        -isize::try_from(base - value).expect("offset delta must fit in isize")
    }
}

fn add_delta(offset: usize, delta: isize) -> usize {
    if delta >= 0 {
        offset
            .checked_add(delta as usize)
            .expect("corrected offset must fit in usize")
    } else {
        offset
            .checked_sub((-delta) as usize)
            .expect("corrected offset must not be negative")
    }
}

#[cfg(test)]
mod tests {
    use super::{FilteredText, OffsetMappingMode};

    #[test]
    fn test_replace_ranges_with_longer_replacement() {
        let text = FilteredText::new("a-b");
        let filtered = text.replace_ranges(vec![(1, 2, " and ".to_string())]);

        assert_eq!(filtered.text, "a and b");
        assert_eq!(filtered.correct_offsets(0, 1), (0, 1));
        assert_eq!(filtered.correct_offsets(2, 5), (1, 2));
        assert_eq!(filtered.correct_offsets(6, 7), (2, 3));
    }

    #[test]
    fn test_expansion_maps_each_token_to_the_full_source_span() {
        let text = FilteredText::new("ab");
        let filtered = text.replace_ranges(vec![(0, 2, "x y".to_string())]);

        assert_eq!(filtered.text, "x y");
        assert_eq!(filtered.correct_offsets(0, 1), (0, 2));
        assert_eq!(filtered.correct_offsets(2, 3), (0, 2));
        assert_eq!(filtered.correct_offsets(0, 3), (0, 2));
    }

    #[test]
    fn test_equal_length_replacement_preserves_source_provenance() {
        let text = FilteredText::new("ab");
        let filtered = text.replace_ranges(vec![(0, 2, "xy".to_string())]);

        assert_eq!(filtered.text, "xy");
        assert_eq!(filtered.correct_offsets(0, 1), (0, 2));
        assert_eq!(filtered.correct_offsets(1, 2), (0, 2));
        assert_eq!(filtered.correction_count(), 1);
    }

    #[test]
    fn test_boundary_mode_preserves_common_character_boundaries() {
        let text = FilteredText::with_offset_mode("ab", OffsetMappingMode::Boundary);
        let filtered = text.replace_ranges(vec![(0, 2, "x y".to_string())]);

        assert_eq!(filtered.text, "x y");
        assert_eq!(filtered.correct_offsets(0, 1), (0, 1));
        assert_eq!(filtered.correct_offsets(2, 3), (1, 2));
        assert_eq!(filtered.correct_offsets(0, 3), (0, 2));
    }

    #[test]
    fn test_boundary_mode_uses_utf8_character_boundaries() {
        let text = FilteredText::with_offset_mode("中", OffsetMappingMode::Boundary);
        let filtered = text.replace_ranges(vec![(0, 3, "x y".to_string())]);

        assert_eq!(filtered.text, "x y");
        assert_eq!(filtered.correct_offsets(0, 1), (0, 0));
        assert_eq!(filtered.correct_offsets(2, 3), (0, 3));
        assert_eq!(filtered.correct_offsets(0, 3), (0, 3));
    }

    #[test]
    fn test_boundary_mode_handles_multibyte_replacement() {
        let text = FilteredText::with_offset_mode("a", OffsetMappingMode::Boundary);
        let filtered = text.replace_ranges(vec![(0, 1, "中英".to_string())]);

        assert_eq!(filtered.text, "中英");
        assert_eq!(filtered.correct_offsets(0, 3), (0, 0));
        assert_eq!(filtered.correct_offsets(3, 6), (0, 1));
    }

    #[test]
    fn test_boundary_mode_handles_multibyte_contraction() {
        let text = FilteredText::with_offset_mode("中英", OffsetMappingMode::Boundary);
        let filtered = text.replace_ranges(vec![(0, 6, "xy".to_string())]);

        assert_eq!(filtered.text, "xy");
        assert_eq!(filtered.correct_offsets(0, 1), (0, 3));
        assert_eq!(filtered.correct_offsets(1, 2), (3, 6));
    }

    #[test]
    fn test_boundary_mode_composes_across_filters() {
        let text = FilteredText::with_offset_mode("中", OffsetMappingMode::Boundary);
        let first = text.replace_ranges(vec![(0, 3, "x y".to_string())]);
        let second = first.replace_ranges(vec![(0, 1, "p q".to_string())]);

        assert_eq!(second.text, "p q y");
        assert_eq!(second.correct_offsets(0, 1), (0, 0));
        assert_eq!(second.correct_offsets(2, 3), (0, 0));
        assert_eq!(second.correct_offsets(4, 5), (0, 3));
    }

    #[test]
    fn test_replace_ranges_with_empty_replacement() {
        let text = FilteredText::new("a--b");
        let filtered = text.replace_ranges(vec![(1, 3, "".to_string())]);

        assert_eq!(filtered.text, "ab");
        assert_eq!(filtered.correct_offsets(0, 1), (0, 1));
        assert_eq!(filtered.correct_offsets(1, 2), (3, 4));
        assert_eq!(filtered.correct_offsets(0, 2), (0, 4));
    }

    #[test]
    fn test_replace_ranges_with_utf8_text() {
        let text = FilteredText::new("中-文");
        let filtered = text.replace_ranges(vec![(3, 4, "".to_string())]);

        assert_eq!(filtered.text, "中文");
        assert_eq!(filtered.correct_offsets(0, 3), (0, 3));
        assert_eq!(filtered.correct_offsets(3, 6), (4, 7));
        assert_eq!(filtered.correct_offsets(0, 6), (0, 7));
    }

    #[test]
    fn test_offset_corrections_compose_across_filters() {
        let text = FilteredText::new("a-b");
        let first = text.replace_ranges(vec![(1, 2, " and ".to_string())]);
        let second = first.replace_ranges(vec![(1, 6, "_".to_string())]);

        assert_eq!(second.text, "a_b");
        assert_eq!(second.correct_offsets(0, 1), (0, 1));
        assert_eq!(second.correct_offsets(1, 2), (1, 2));
        assert_eq!(second.correct_offsets(2, 3), (2, 3));
    }

    #[test]
    fn test_offset_corrections_compose_for_partial_replacement_copies() {
        let text = FilteredText::new("ab");
        let first = text.replace_ranges(vec![(0, 1, "x y".to_string())]);
        let second = first.replace_ranges(vec![(0, 1, "p q".to_string())]);

        assert_eq!(second.text, "p q yb");
        assert_eq!(second.correct_offsets(0, 1), (0, 1));
        assert_eq!(second.correct_offsets(2, 3), (0, 1));
        assert_eq!(second.correct_offsets(4, 5), (0, 1));
        assert_eq!(second.correct_offsets(5, 6), (1, 2));
    }

    #[test]
    fn test_end_correction_scans_replacements_crossed_by_token() {
        let text = FilteredText::new("ab");
        let filtered =
            text.replace_ranges(vec![(0, 1, "x y".to_string()), (1, 2, "u v".to_string())]);

        assert_eq!(filtered.text, "x yu v");
        assert_eq!(filtered.correct_offsets(0, 1), (0, 1));
        assert_eq!(filtered.correct_offsets(2, 3), (0, 1));
        assert_eq!(filtered.correct_offsets(3, 4), (1, 2));
        assert_eq!(filtered.correct_offsets(5, 6), (1, 2));
        assert_eq!(filtered.correct_offsets(0, 4), (0, 2));
    }

    #[test]
    fn test_adjacent_deletion_and_replacement_share_filtered_start() {
        let text = FilteredText::new("abc");
        let filtered = text.replace_ranges(vec![(1, 2, "".to_string()), (2, 3, "x y".to_string())]);

        assert_eq!(filtered.text, "ax y");
        assert_eq!(filtered.correct_offsets(0, 1), (0, 1));
        assert_eq!(filtered.correct_offsets(1, 2), (2, 3));
        assert_eq!(filtered.correct_offsets(3, 4), (2, 3));
        assert_eq!(filtered.correct_offsets(0, 4), (0, 3));
    }

    #[test]
    fn test_valid_replacements_rejects_invalid_ranges() {
        assert!(FilteredText::valid_replacements(
            "a-b",
            &[(1, 2, " ".to_string())]
        ));
        assert!(!FilteredText::valid_replacements(
            "abc",
            &[(2, 3, "".to_string()), (1, 2, "".to_string())]
        ));
        assert!(!FilteredText::valid_replacements(
            "abc",
            &[(2, 1, "".to_string())]
        ));
        assert!(!FilteredText::valid_replacements(
            "中文",
            &[(1, 3, "".to_string())]
        ));
    }
}
