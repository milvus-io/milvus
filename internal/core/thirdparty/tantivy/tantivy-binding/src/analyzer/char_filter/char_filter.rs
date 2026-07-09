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
    offset_map: Vec<usize>,
}

impl FilteredText {
    pub(crate) fn new(text: &str) -> Self {
        FilteredText {
            text: text.to_string(),
            offset_map: (0..=text.len()).collect(),
        }
    }

    pub(crate) fn correct_offset(&self, offset: usize) -> usize {
        self.offset_map
            .get(offset)
            .copied()
            .unwrap_or_else(|| *self.offset_map.last().unwrap_or(&0))
    }

    /// Replacements must be sorted, non-overlapping, and aligned to UTF-8 byte
    /// boundaries in the current filtered text.
    pub(crate) fn replace_ranges(&self, replacements: Vec<(usize, usize, String)>) -> Self {
        debug_assert!(
            Self::valid_replacements(&self.text, &replacements),
            "char filter replacements must be sorted, non-overlapping, and valid UTF-8 ranges"
        );

        if replacements.is_empty() {
            return self.clone();
        }

        let mut text = String::with_capacity(self.text.len());
        let mut offset_map = Vec::with_capacity(self.text.len() + 1);
        offset_map.push(self.correct_offset(0));

        let mut cursor = 0;
        for (start, end, replacement) in replacements {
            if start < cursor {
                continue;
            }
            self.push_original_segment(cursor, start, &mut text, &mut offset_map);
            self.push_replacement(start, end, &replacement, &mut text, &mut offset_map);
            cursor = end;
        }
        self.push_original_segment(cursor, self.text.len(), &mut text, &mut offset_map);

        FilteredText { text, offset_map }
    }

    fn valid_replacements(text: &str, replacements: &[(usize, usize, String)]) -> bool {
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
        output: &mut String,
        output_offsets: &mut Vec<usize>,
    ) {
        if let Some(last) = output_offsets.last_mut() {
            *last = self.correct_offset(start);
        }
        output.push_str(&self.text[start..end]);
        for offset in (start + 1)..=end {
            output_offsets.push(self.correct_offset(offset));
        }
    }

    fn push_replacement(
        &self,
        start: usize,
        end: usize,
        replacement: &str,
        output: &mut String,
        output_offsets: &mut Vec<usize>,
    ) {
        if let Some(last) = output_offsets.last_mut() {
            *last = self.correct_offset(start);
        }

        if replacement.is_empty() {
            if let Some(last) = output_offsets.last_mut() {
                *last = self.correct_offset(end);
            }
            return;
        }

        output.push_str(replacement);

        let source_boundaries = self.char_boundaries(start, end);
        let replacement_boundaries = char_boundaries(replacement);
        let source_char_count = source_boundaries.len().saturating_sub(1);
        let replacement_char_count = replacement_boundaries.len().saturating_sub(1);

        let mut previous_boundary = 0;
        for (boundary_index, boundary) in replacement_boundaries.iter().enumerate().skip(1) {
            let source_boundary_index = if boundary_index == replacement_char_count {
                source_char_count
            } else if replacement_char_count == 0 {
                0
            } else {
                source_char_count * boundary_index / replacement_char_count
            };
            let corrected = self.correct_offset(source_boundaries[source_boundary_index]);

            for _ in (previous_boundary + 1)..=*boundary {
                output_offsets.push(corrected);
            }
            previous_boundary = *boundary;
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

#[cfg(test)]
mod tests {
    use super::FilteredText;

    #[test]
    fn test_replace_ranges_with_longer_replacement() {
        let text = FilteredText::new("a-b");
        let filtered = text.replace_ranges(vec![(1, 2, " and ".to_string())]);

        assert_eq!(filtered.text, "a and b");
        assert_eq!(filtered.correct_offset(0), 0);
        assert_eq!(filtered.correct_offset(1), 1);
        assert_eq!(filtered.correct_offset(6), 2);
        assert_eq!(filtered.correct_offset(7), 3);
    }

    #[test]
    fn test_replace_ranges_with_empty_replacement() {
        let text = FilteredText::new("a--b");
        let filtered = text.replace_ranges(vec![(1, 3, "".to_string())]);

        assert_eq!(filtered.text, "ab");
        assert_eq!(filtered.correct_offset(0), 0);
        assert_eq!(filtered.correct_offset(1), 3);
        assert_eq!(filtered.correct_offset(2), 4);
    }

    #[test]
    fn test_replace_ranges_with_utf8_text() {
        let text = FilteredText::new("中-文");
        let filtered = text.replace_ranges(vec![(3, 4, "".to_string())]);

        assert_eq!(filtered.text, "中文");
        assert_eq!(filtered.correct_offset(0), 0);
        assert_eq!(filtered.correct_offset(3), 4);
        assert_eq!(filtered.correct_offset(6), 7);
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
