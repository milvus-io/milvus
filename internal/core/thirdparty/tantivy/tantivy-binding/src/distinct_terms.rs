//! Index-assisted exact DISTINCT primitive for one untokenized keyword field.
//!
//! For each segment this walks the field's term dictionary and keeps a term
//! only if at least one of its postings maps to a live document whose Milvus
//! row offset is in the caller-supplied effective row set. The postings walk
//! for a term stops at the first such hit. Terms already confirmed by an
//! earlier segment are skipped, so values repeated across segments are checked
//! once and the per-segment results form a union.
//!
//! The primitive is deliberately standalone:
//! - The caller decides visibility. The effective row set must already exclude
//!   rows removed by filters, Milvus deletes and MVCC. Tantivy-level deletions
//!   (the segment alive bitset) are honoured here as well.
//! - It is exact. Raw dictionary membership or `doc_freq` are never used as a
//!   shortcut, because they count deleted and filtered-out rows.
//! - NULL values are not indexed as terms, so no NULL is produced. Query-level
//!   NULL synthesis, C++ routing and cost-based path selection are out of scope.

// No caller is wired in yet: routing from C++ is a follow-up (see #53800).
#![allow(dead_code)]

use std::collections::HashSet;

use tantivy::schema::{FieldType, IndexRecordOption};
use tantivy::SegmentReader;

use crate::error::{Result, TantivyBindingError};
use crate::index_reader::IndexReaderWrapper;

/// The rows a DISTINCT result may come from, in Milvus row-offset space.
///
/// Layout matches a little-endian `u64`-word bitmap (Milvus `TargetBitmap`):
/// row `r` is visible iff bit `r % 64` of `words[r / 64]` is set and
/// `r < num_rows`.
#[derive(Clone, Copy, Debug)]
pub struct EffectiveRowSet<'a> {
    words: &'a [u64],
    num_rows: usize,
}

impl<'a> EffectiveRowSet<'a> {
    pub fn new(words: &'a [u64], num_rows: usize) -> Result<Self> {
        if (words.len() as u128) * 64 < num_rows as u128 {
            return Err(TantivyBindingError::InternalError(format!(
                "effective row set has {} words, which cannot cover {} rows",
                words.len(),
                num_rows
            )));
        }
        Ok(EffectiveRowSet { words, num_rows })
    }

    #[inline]
    pub fn contains(&self, row: i64) -> bool {
        if row < 0 || (row as u64) >= self.num_rows as u64 {
            return false;
        }
        let row = row as usize;
        (self.words[row >> 6] >> (row & 63)) & 1 == 1
    }
}

/// Work counters, useful for checking the early-stop behaviour and for
/// comparing against a scan/hash baseline.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DistinctTermsStats {
    pub segments: usize,
    /// Terms whose postings were read.
    pub terms_checked: usize,
    /// Terms skipped because an earlier segment already confirmed them.
    pub terms_skipped: usize,
    /// Postings entries examined across all checked terms.
    pub docs_checked: usize,
}

#[derive(Clone, Debug, Default)]
pub struct DistinctKeywordTerms {
    /// Distinct visible values, sorted by byte order.
    pub values: Vec<String>,
    pub stats: DistinctTermsStats,
}

impl IndexReaderWrapper {
    fn ensure_untokenized_keyword(&self) -> Result<()> {
        let schema = self.index.schema();
        match schema.get_field_entry(self.field).field_type() {
            FieldType::Str(options) => match options.get_indexing_options() {
                Some(indexing) if indexing.tokenizer() == "raw" => Ok(()),
                _ => Err(TantivyBindingError::InternalError(format!(
                    "distinct keyword terms requires an untokenized keyword field; `{}` is tokenized or not indexed",
                    self.field_name
                ))),
            },
            other => Err(TantivyBindingError::InternalError(format!(
                "distinct keyword terms requires a keyword field; `{}` has type {:?}",
                self.field_name,
                other.value_type()
            ))),
        }
    }

    /// Exact distinct values of the keyword field among the visible rows.
    pub fn distinct_keyword_terms(&self, rows: &EffectiveRowSet) -> Result<DistinctKeywordTerms> {
        self.ensure_untokenized_keyword()?;
        let searcher = self.reader.searcher();
        let mut found: HashSet<Vec<u8>> = HashSet::new();
        let mut stats = DistinctTermsStats::default();
        for segment_reader in searcher.segment_readers() {
            self.collect_segment_distinct_terms(segment_reader, rows, &mut found, &mut stats)?;
        }

        let mut values = found
            .into_iter()
            .map(|bytes| {
                String::from_utf8(bytes).map_err(|e| {
                    TantivyBindingError::InternalError(format!(
                        "keyword term is not valid UTF-8: {e}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        values.sort_unstable();
        Ok(DistinctKeywordTerms { values, stats })
    }

    /// Adds the visible terms of one segment to `found`. Exposed per segment
    /// so callers can union results from several readers themselves.
    pub(crate) fn collect_segment_distinct_terms(
        &self,
        segment_reader: &SegmentReader,
        rows: &EffectiveRowSet,
        found: &mut HashSet<Vec<u8>>,
        stats: &mut DistinctTermsStats,
    ) -> Result<()> {
        stats.segments += 1;
        let inverted_index = segment_reader.inverted_index(self.field)?;
        // Same docID mapping as the query collectors: the `doc_id` fast field
        // when present, otherwise the Tantivy doc id is the Milvus row offset
        // (user-specified doc ids, or the legacy single-segment layout).
        let doc_id_column = if self.id_field.is_some() {
            Some(segment_reader.fast_fields().i64("doc_id")?)
        } else {
            None
        };
        let alive_bitset = segment_reader.alive_bitset();

        let mut stream = inverted_index.terms().stream()?;
        while stream.advance() {
            let term = stream.key();
            if found.contains(term) {
                stats.terms_skipped += 1;
                continue;
            }
            stats.terms_checked += 1;

            let mut postings = inverted_index
                .read_block_postings_from_terminfo(stream.value(), IndexRecordOption::Basic)?;
            'postings: loop {
                let docs = postings.docs();
                if docs.is_empty() {
                    break;
                }
                for &doc in docs {
                    stats.docs_checked += 1;
                    if alive_bitset.is_some_and(|alive| !alive.is_alive(doc)) {
                        continue;
                    }
                    let row = match &doc_id_column {
                        Some(column) => match column.first(doc) {
                            Some(row) => row,
                            None => continue,
                        },
                        None => doc as i64,
                    };
                    if rows.contains(row) {
                        found.insert(term.to_vec());
                        break 'postings;
                    }
                }
                postings.advance();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Instant;

    use tantivy::merge_policy::NoMergePolicy;
    use tantivy::schema::{
        IndexRecordOption, Schema, TextFieldIndexing, TextOptions, FAST, INDEXED, TEXT,
    };
    use tantivy::{doc, Index, Term};

    use super::{DistinctKeywordTerms, EffectiveRowSet};
    use crate::data_type::TantivyDataType;
    use crate::index_reader::IndexReaderWrapper;
    use crate::index_writer::IndexWriterWrapper;
    use crate::util::set_bitset;
    use crate::TantivyIndexVersion;

    const MIN_MEMORY_BUDGET: usize = 15_000_000;

    /// Small deterministic generator so fixtures are reproducible without
    /// adding a dependency.
    struct XorShift(u64);
    impl XorShift {
        fn next(&mut self) -> u64 {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            x
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn bitmap(num_rows: usize, mut visible: impl FnMut(usize) -> bool) -> Vec<u64> {
        let mut words = vec![0u64; num_rows.div_ceil(64)];
        for row in 0..num_rows {
            if visible(row) {
                words[row >> 6] |= 1 << (row & 63);
            }
        }
        words
    }

    /// Scan/hash oracle: distinct values of the live, visible rows.
    fn scan_oracle(rows: &[(i64, String, bool)], set: &EffectiveRowSet) -> Vec<String> {
        rows.iter()
            .filter(|(row, _, deleted)| !deleted && set.contains(*row))
            .map(|(_, value, _)| value.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Builds a Milvus keyword index through the production V7 writer with one
    /// committed segment per batch. Returns the reader and the (row, value)
    /// fixture.
    fn build_milvus_keyword_index(
        dir: &std::path::Path,
        user_specified_doc_id: bool,
        batches: &[Vec<String>],
    ) -> (IndexReaderWrapper, Vec<(i64, String, bool)>) {
        let mut writer = IndexWriterWrapper::new(
            "kw",
            TantivyDataType::Keyword,
            dir.to_str().unwrap().to_string(),
            1,
            MIN_MEMORY_BUDGET,
            TantivyIndexVersion::V7,
            user_specified_doc_id,
            false, // no background merge: keep one segment per commit
        )
        .unwrap();
        let mut fixture = Vec::new();
        let mut offset = 0i64;
        for batch in batches {
            for value in batch {
                writer.add(value.as_str(), Some(offset)).unwrap();
                fixture.push((offset, value.clone(), false));
                offset += 1;
            }
            writer.commit().unwrap();
        }
        let reader = writer.create_reader(set_bitset).unwrap();
        reader.reload().unwrap();
        (reader, fixture)
    }

    fn random_batches(
        rng: &mut XorShift,
        batches: usize,
        rows: usize,
        card: u64,
    ) -> Vec<Vec<String>> {
        (0..batches)
            .map(|_| {
                (0..rows)
                    .map(|_| format!("v{:06}", rng.below(card)))
                    .collect()
            })
            .collect()
    }

    fn assert_matches_oracle(
        reader: &IndexReaderWrapper,
        fixture: &[(i64, String, bool)],
        rng: &mut XorShift,
    ) {
        let num_rows = fixture.len();
        // empty, very selective, selective, broad, full
        for per_mille in [0u64, 1, 50, 500, 1000] {
            let words = bitmap(num_rows, |_| rng.below(1000) < per_mille);
            let set = EffectiveRowSet::new(&words, num_rows).unwrap();
            let got = reader.distinct_keyword_terms(&set).unwrap();
            assert_eq!(
                got.values,
                scan_oracle(fixture, &set),
                "mismatch at visibility {per_mille}/1000"
            );
        }
    }

    #[test]
    fn matches_scan_oracle_across_segments_with_doc_id_field() {
        let dir = tempfile::tempdir().unwrap();
        let mut rng = XorShift(0x5eed_0001);
        let batches = random_batches(&mut rng, 4, 3_000, 500);
        let (reader, fixture) = build_milvus_keyword_index(dir.path(), false, &batches);
        assert!(reader.reader.searcher().segment_readers().len() >= 2);
        assert_matches_oracle(&reader, &fixture, &mut rng);
    }

    #[test]
    fn matches_scan_oracle_with_user_specified_doc_id() {
        let dir = tempfile::tempdir().unwrap();
        let mut rng = XorShift(0x5eed_0002);
        let batches = random_batches(&mut rng, 3, 2_000, 2_000);
        let (reader, fixture) = build_milvus_keyword_index(dir.path(), true, &batches);
        assert!(reader.reader.searcher().segment_readers().len() >= 2);
        assert_matches_oracle(&reader, &fixture, &mut rng);
    }

    #[test]
    fn cross_segment_repeats_are_checked_once_and_stop_at_first_hit() {
        let dir = tempfile::tempdir().unwrap();
        // The same 10 values in each of 3 segments, 100 rows per value.
        let batch: Vec<String> = (0..1_000).map(|i| format!("k{}", i % 10)).collect();
        let (reader, fixture) =
            build_milvus_keyword_index(dir.path(), false, &[batch.clone(), batch.clone(), batch]);
        let words = bitmap(fixture.len(), |_| true);
        let set = EffectiveRowSet::new(&words, fixture.len()).unwrap();

        let DistinctKeywordTerms { values, stats } = reader.distinct_keyword_terms(&set).unwrap();
        assert_eq!(values.len(), 10);
        assert_eq!(stats.segments, 3);
        // Confirmed once in the first segment, skipped in the other two.
        assert_eq!(stats.terms_checked, 10);
        assert_eq!(stats.terms_skipped, 20);
        // Every row is visible, so each term stops at its first posting.
        assert_eq!(stats.docs_checked, 10);
    }

    #[test]
    fn value_visible_only_in_later_segment_is_found() {
        let dir = tempfile::tempdir().unwrap();
        let first: Vec<String> = vec!["a".into(), "b".into()];
        let second: Vec<String> = vec!["a".into(), "c".into()];
        let (reader, fixture) = build_milvus_keyword_index(dir.path(), false, &[first, second]);
        // Only row 2 ("a" in the second segment) and row 3 ("c") are visible.
        let words = bitmap(fixture.len(), |row| row >= 2);
        let set = EffectiveRowSet::new(&words, fixture.len()).unwrap();
        let got = reader.distinct_keyword_terms(&set).unwrap();
        assert_eq!(got.values, vec!["a".to_string(), "c".to_string()]);
    }

    #[test]
    fn honours_tantivy_deletes() {
        let mut builder = Schema::builder();
        let kw = builder.add_text_field(
            "kw",
            TextOptions::default().set_indexing_options(
                TextFieldIndexing::default()
                    .set_tokenizer("raw")
                    .set_fieldnorms(false)
                    .set_index_option(IndexRecordOption::Basic),
            ),
        );
        let doc_id = builder.add_i64_field("doc_id", FAST);
        let pk = builder.add_i64_field("pk", INDEXED);
        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_with_num_threads(1, MIN_MEMORY_BUDGET).unwrap();
        writer.set_merge_policy(Box::new(NoMergePolicy));

        let mut rng = XorShift(0x5eed_0003);
        let mut fixture = Vec::new();
        for row in 0..4_000i64 {
            let value = format!("d{:03}", rng.below(300));
            writer
                .add_document(doc!(kw => value.clone(), doc_id => row, pk => row))
                .unwrap();
            fixture.push((row, value, false));
            if row % 1_000 == 999 {
                writer.commit().unwrap();
            }
        }
        // Delete about a third of the rows, including every row of some values.
        for (row, value, deleted) in fixture.iter_mut() {
            if *row % 3 == 0 || value.as_str() < "d010" {
                writer.delete_term(Term::from_field_i64(pk, *row));
                *deleted = true;
            }
        }
        writer.commit().unwrap();

        let reader = IndexReaderWrapper::from_index(Arc::new(index), set_bitset).unwrap();
        reader.reload().unwrap();
        let searcher = reader.reader.searcher();
        assert!(searcher.segment_readers().len() >= 2);
        assert!(searcher
            .segment_readers()
            .iter()
            .any(|s| s.num_deleted_docs() > 0));

        assert_matches_oracle(&reader, &fixture, &mut rng);
        let words = bitmap(fixture.len(), |_| true);
        let set = EffectiveRowSet::new(&words, fixture.len()).unwrap();
        let got = reader.distinct_keyword_terms(&set).unwrap();
        assert!(got.values.iter().all(|v| v.as_str() >= "d010"));
    }

    #[test]
    fn handles_empty_and_unicode_values() {
        let dir = tempfile::tempdir().unwrap();
        let batch: Vec<String> = vec!["".into(), "向量".into(), "café".into(), "".into()];
        let (reader, fixture) = build_milvus_keyword_index(dir.path(), false, &[batch]);
        let words = bitmap(fixture.len(), |_| true);
        let set = EffectiveRowSet::new(&words, fixture.len()).unwrap();
        let got = reader.distinct_keyword_terms(&set).unwrap();
        assert_eq!(got.values, scan_oracle(&fixture, &set));
        assert_eq!(got.values.len(), 3);
    }

    #[test]
    fn rejects_tokenized_and_non_keyword_fields() {
        let mut builder = Schema::builder();
        let title = builder.add_text_field("title", TEXT);
        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_with_num_threads(1, MIN_MEMORY_BUDGET).unwrap();
        writer.add_document(doc!(title => "hello world")).unwrap();
        writer.commit().unwrap();
        let reader = IndexReaderWrapper::from_index(Arc::new(index), set_bitset).unwrap();
        let words = [u64::MAX];
        let set = EffectiveRowSet::new(&words, 1).unwrap();
        assert!(reader.distinct_keyword_terms(&set).is_err());

        let mut builder = Schema::builder();
        let number = builder.add_i64_field("number", INDEXED);
        let index = Index::create_in_ram(builder.build());
        let mut writer = index.writer_with_num_threads(1, MIN_MEMORY_BUDGET).unwrap();
        writer.add_document(doc!(number => 1i64)).unwrap();
        writer.commit().unwrap();
        let reader = IndexReaderWrapper::from_index(Arc::new(index), set_bitset).unwrap();
        assert!(reader.distinct_keyword_terms(&set).is_err());
    }

    #[test]
    fn effective_row_set_bounds() {
        assert!(EffectiveRowSet::new(&[0], 65).is_err());
        let words = [u64::MAX, 1];
        let set = EffectiveRowSet::new(&words, 65).unwrap();
        assert!(set.contains(0) && set.contains(63) && set.contains(64));
        assert!(!set.contains(-1) && !set.contains(65) && !set.contains(i64::MAX));
        // Rows past num_rows are invisible even when the bit is set.
        let set = EffectiveRowSet::new(&words, 10).unwrap();
        assert!(!set.contains(10) && !set.contains(64));
    }

    /// Low/high cardinality x broad/selective comparison against a scan/hash
    /// baseline over the same visible rows. Not a CI test; run with
    /// `cargo test --release --lib distinct_terms -- --ignored --nocapture`.
    #[test]
    #[ignore]
    fn compare_with_scan_hash_baseline() {
        const ROWS: usize = 1_000_000;
        const SEGMENTS: usize = 4;
        println!(
            "| cardinality | visible rows | distinct | index path | scan/hash | terms checked | postings read |"
        );
        println!("|---|---|---|---|---|---|---|");
        for card in [100u64, 100_000] {
            let dir = tempfile::tempdir().unwrap();
            let mut rng = XorShift(0x5eed_0004 ^ card);
            let batches = random_batches(&mut rng, SEGMENTS, ROWS / SEGMENTS, card);
            let (reader, fixture) = build_milvus_keyword_index(dir.path(), false, &batches);
            let values: Vec<&str> = fixture.iter().map(|(_, v, _)| v.as_str()).collect();
            for (label, per_mille) in [("90% (broad)", 900u64), ("0.1% (selective)", 1)] {
                let words = bitmap(ROWS, |_| rng.below(1000) < per_mille);
                let set = EffectiveRowSet::new(&words, ROWS).unwrap();

                let start = Instant::now();
                let got = reader.distinct_keyword_terms(&set).unwrap();
                let index_time = start.elapsed();

                // Baseline: decode each visible row's value and hash it.
                let start = Instant::now();
                let mut seen = std::collections::HashSet::new();
                for (row, value) in values.iter().enumerate() {
                    if set.contains(row as i64) {
                        seen.insert(*value);
                    }
                }
                let scan_time = start.elapsed();

                assert_eq!(got.values.len(), seen.len());
                println!(
                    "| {card} | {label} | {} | {:.2?} | {:.2?} | {} | {} |",
                    got.values.len(),
                    index_time,
                    scan_time,
                    got.stats.terms_checked,
                    got.stats.docs_checked
                );
            }
        }
    }
}
