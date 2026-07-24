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
    corrections: Vec<OffsetCorrection>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OffsetCorrection {
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
        FilteredText {
            text: text.to_string(),
            corrections: Vec::new(),
        }
    }

    pub(crate) fn correct_offset(&self, offset: usize) -> usize {
        let offset = offset.min(self.text.len());
        match self
            .corrections
            .binary_search_by_key(&offset, |correction| correction.filtered_offset)
        {
            Ok(index) => self.corrections[index].original_offset,
            Err(0) => offset,
            Err(index) => {
                let correction = &self.corrections[index - 1];
                correction.original_offset + (offset - correction.filtered_offset)
            }
        }
    }

    /// Replacements must be sorted, non-overlapping, and aligned to UTF-8 byte
    /// boundaries in the current filtered text.
    pub(crate) fn replace_ranges(self, replacements: Vec<(usize, usize, String)>) -> Self {
        debug_assert!(
            Self::valid_replacements(&self.text, &replacements),
            "char filter replacements must be sorted, non-overlapping, and valid UTF-8 ranges"
        );

        if replacements.is_empty() {
            return self;
        }

        let mut output = FilteredText {
            text: String::with_capacity(self.text.len()),
            corrections: Vec::with_capacity(replacements.len() * 2),
        };

        let mut cursor = 0;
        for (start, end, replacement) in replacements {
            self.push_original_segment(cursor, start, &mut output);
            self.push_replacement(start, end, &replacement, &mut output);
            cursor = end;
        }
        self.push_original_segment(cursor, self.text.len(), &mut output);

        output
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

    fn push_original_segment(&self, start: usize, end: usize, output: &mut FilteredText) {
        let output_start = output.text.len();
        output.push_correction(output_start, self.correct_offset(start));
        output.text.push_str(&self.text[start..end]);

        for correction in self.corrections.iter().filter(|correction| {
            correction.filtered_offset > start && correction.filtered_offset <= end
        }) {
            output.push_correction(
                output_start + correction.filtered_offset - start,
                correction.original_offset,
            );
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

        if replacement.is_empty() {
            output.push_correction(output_start, self.correct_offset(end));
            return;
        }

        output.push_correction(output_start, self.correct_offset(start));
        output.text.push_str(replacement);

        let source_boundaries = self.char_boundaries(start, end);
        let replacement_boundaries = char_boundaries(replacement);
        let source_char_count = source_boundaries.len().saturating_sub(1);
        let replacement_char_count = replacement_boundaries.len().saturating_sub(1);

        for (boundary_index, boundary) in replacement_boundaries.iter().enumerate().skip(1) {
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
            let corrected = self.correct_offset(source_boundaries[source_boundary_index]);
            output.push_correction(output_start + boundary, corrected);
        }
    }

    fn push_correction(&mut self, filtered_offset: usize, original_offset: usize) {
        if let Some(last) = self.corrections.last() {
            if last.filtered_offset == filtered_offset {
                self.corrections.pop();
            }
        }

        let expected_original_offset = self.corrections.last().map_or(filtered_offset, |last| {
            last.original_offset + (filtered_offset - last.filtered_offset)
        });
        if original_offset == expected_original_offset {
            return;
        }

        self.corrections.push(OffsetCorrection {
            filtered_offset,
            original_offset,
        });
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
    fn test_expansion_preserves_common_character_boundaries() {
        let text = FilteredText::new("ab");
        let filtered = text.replace_ranges(vec![(0, 2, "x y".to_string())]);

        assert_eq!(filtered.text, "x y");
        assert_eq!(filtered.correct_offset(0), 0);
        assert_eq!(filtered.correct_offset(1), 1);
        assert_eq!(filtered.correct_offset(2), 1);
        assert_eq!(filtered.correct_offset(3), 2);
    }

    #[test]
    fn test_equal_length_replacement_needs_no_offset_corrections() {
        let text = FilteredText::new("ab");
        let filtered = text.replace_ranges(vec![(0, 2, "xy".to_string())]);

        assert_eq!(filtered.text, "xy");
        assert!(filtered.corrections.is_empty());
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
    fn test_offset_corrections_compose_across_filters() {
        let text = FilteredText::new("a-b");
        let first = text.replace_ranges(vec![(1, 2, " and ".to_string())]);
        let second = first.replace_ranges(vec![(1, 6, "_".to_string())]);

        assert_eq!(second.text, "a_b");
        assert_eq!(second.correct_offset(0), 0);
        assert_eq!(second.correct_offset(1), 1);
        assert_eq!(second.correct_offset(2), 2);
        assert_eq!(second.correct_offset(3), 3);
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
