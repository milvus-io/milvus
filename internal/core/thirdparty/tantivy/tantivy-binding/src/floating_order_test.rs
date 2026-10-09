use std::collections::HashSet;
use std::ffi::c_void;
use std::ops::Bound;
use std::sync::Arc;

use tempfile::tempdir;

use crate::data_type::TantivyDataType;
use crate::index_reader::IndexReaderWrapper;
use crate::index_writer::IndexWriterWrapper;
use crate::util::{canonical_f64, set_bitset};
use crate::TantivyIndexVersion;

fn nan_values() -> [f64; 3] {
    [
        f64::from_bits(0x7ff8_0000_0000_0001),
        f64::from_bits(0xfff8_0000_0000_0002),
        f64::from_bits(0x7ff0_0000_0000_0001),
    ]
}

fn float_values() -> Vec<f64> {
    let mut values = nan_values().to_vec();
    values.extend([f64::NEG_INFINITY, -1.0, -0.0, 0.0, 1.0, f64::INFINITY]);
    values
}

#[test]
fn test_nan_canonical_encoding_both_versions() {
    for nan in nan_values() {
        let value = canonical_f64(nan);
        assert_eq!(value.to_bits(), 0x7fff_ffff_ffff_ffff);
        let term = tantivy::Term::from_field_f64(tantivy::schema::Field::from_field_id(0), value);
        assert_eq!(term.serialized_value_bytes(), &[0xff; 8]);
        assert!(term.value().as_f64().unwrap().is_nan());
        let term =
            tantivy_5::Term::from_field_f64(tantivy_5::schema::Field::from_field_id(0), value);
        assert_eq!(term.serialized_value_bytes(), &[0xff; 8]);
        assert!(term.value().as_f64().unwrap().is_nan());
    }
    for value in [f64::NEG_INFINITY, -1.0, 0.0, 1.0, f64::INFINITY] {
        assert_eq!(canonical_f64(value).to_bits(), value.to_bits());
    }
    for zero in [-0.0, 0.0] {
        assert_eq!(canonical_f64(zero).to_bits(), 0.0f64.to_bits());
        let field = tantivy::schema::Field::from_field_id(0);
        assert_eq!(
            tantivy::Term::from_field_f64(field, canonical_f64(zero)),
            tantivy::Term::from_field_f64(field, 0.0)
        );
        let field = tantivy_5::schema::Field::from_field_id(0);
        assert_eq!(
            tantivy_5::Term::from_field_f64(field, canonical_f64(zero)),
            tantivy_5::Term::from_field_f64(field, 0.0)
        );
    }
}

#[test]
fn test_nan_writer_and_queries_version7() {
    let dir = tempdir().unwrap();
    let mut writer = IndexWriterWrapper::new(
        "value",
        TantivyDataType::F64,
        dir.path().to_str().unwrap().into(),
        1,
        50_000_000,
        TantivyIndexVersion::V7,
        false,
        false,
    )
    .unwrap();
    for (row, value) in float_values().into_iter().enumerate() {
        writer.add(value, Some(row as i64)).unwrap();
    }
    writer.add_array([nan_values()[1], 1.0], Some(9)).unwrap();
    writer.commit().unwrap();
    let reader = writer.create_reader(set_bitset).unwrap();
    let mut hits = HashSet::<u32>::new();
    for nan in nan_values() {
        reader
            .terms_query_f64(&[nan], &mut hits as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(hits, HashSet::from([0, 1, 2, 9]));
        hits.clear();
        reader
            .lower_bound_range_query_f64(nan, true, &mut hits as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(hits, HashSet::from([0, 1, 2, 9]));
        hits.clear();
        reader
            .lower_bound_range_query_f64(nan, false, &mut hits as *mut _ as *mut c_void)
            .unwrap();
        assert!(hits.is_empty());
        reader
            .upper_bound_range_query_f64(nan, false, &mut hits as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(hits, HashSet::from([3, 4, 5, 6, 7, 8, 9]));
        hits.clear();
        reader
            .upper_bound_range_query_f64(nan, true, &mut hits as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(hits.len(), 10);
        hits.clear();
        reader
            .range_query_f64(nan, nan, true, true, &mut hits as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(hits, HashSet::from([0, 1, 2, 9]));
        hits.clear();
    }
    for zero in [-0.0, 0.0] {
        reader
            .terms_query_f64(&[zero], &mut hits as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(hits, HashSet::from([5, 6]));
        hits.clear();
        reader
            .range_query_f64(zero, zero, true, true, &mut hits as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(hits, HashSet::from([5, 6]));
        hits.clear();
        reader
            .upper_bound_range_query_f64(zero, false, &mut hits as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(hits, HashSet::from([3, 4]));
        hits.clear();
        reader
            .lower_bound_range_query_f64(zero, false, &mut hits as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(hits, HashSet::from([0, 1, 2, 7, 8, 9]));
        hits.clear();
    }
    reader
        .lower_bound_range_query_f64(f64::INFINITY, false, &mut hits as *mut _ as *mut c_void)
        .unwrap();
    assert_eq!(hits, HashSet::from([0, 1, 2, 9]));
    hits.clear();
    let mut terms = vec![nan_values()[1]; 10_000];
    terms.push(1.0);
    reader
        .terms_query_f64(&terms, &mut hits as *mut _ as *mut c_void)
        .unwrap();
    assert_eq!(hits, HashSet::from([0, 1, 2, 7, 9]));
    hits.clear();
    reader
        .range_query_f64(
            f64::NEG_INFINITY,
            f64::INFINITY,
            true,
            true,
            &mut hits as *mut _ as *mut c_void,
        )
        .unwrap();
    assert_eq!(hits, HashSet::from([3, 4, 5, 6, 7, 8, 9]));
}

#[test]
fn test_nan_writer_version5_and_single_segment() {
    for single_segment in [false, true] {
        let dir = tempdir().unwrap();
        let path = dir.path().to_str().unwrap().to_string();
        let mut writer = if single_segment {
            IndexWriterWrapper::new_with_single_segment("value", TantivyDataType::F64, path)
                .unwrap()
        } else {
            IndexWriterWrapper::new(
                "value",
                TantivyDataType::F64,
                path,
                1,
                50_000_000,
                TantivyIndexVersion::V5,
                false,
                false,
            )
            .unwrap()
        };
        for (row, value) in float_values().into_iter().enumerate() {
            writer
                .add(
                    value,
                    if single_segment {
                        None
                    } else {
                        Some(row as i64)
                    },
                )
                .unwrap();
        }
        writer
            .add_array(
                [nan_values()[1], 1.0],
                if single_segment { None } else { Some(9) },
            )
            .unwrap();
        writer.finish().unwrap();
        let index = tantivy_5::Index::open_in_dir(dir.path()).unwrap();
        let reader = index.reader().unwrap();
        let field = index.schema().get_field("value").unwrap();
        for nan in nan_values() {
            let query = tantivy_5::query::TermQuery::new(
                tantivy_5::Term::from_field_f64(field, canonical_f64(nan)),
                tantivy_5::schema::IndexRecordOption::Basic,
            );
            assert_eq!(
                reader
                    .searcher()
                    .search(&query, &tantivy_5::collector::Count)
                    .unwrap(),
                4
            );
        }
        for zero in [-0.0, 0.0] {
            let zero = canonical_f64(zero);
            let query = tantivy_5::query::TermQuery::new(
                tantivy_5::Term::from_field_f64(field, zero),
                tantivy_5::schema::IndexRecordOption::Basic,
            );
            assert_eq!(
                reader
                    .searcher()
                    .search(&query, &tantivy_5::collector::Count)
                    .unwrap(),
                2
            );
            for (lower, upper, expected) in [
                (Bound::Included(zero), Bound::Included(zero), 2),
                (Bound::Unbounded, Bound::Excluded(zero), 2),
                (Bound::Excluded(zero), Bound::Unbounded, 6),
            ] {
                let query =
                    tantivy_5::query::RangeQuery::new_f64_bounds("value".into(), lower, upper);
                assert_eq!(
                    reader
                        .searcher()
                        .search(&query, &tantivy_5::collector::Count)
                        .unwrap(),
                    expected
                );
            }
        }
        let query = tantivy_5::query::RangeQuery::new_f64_bounds(
            "value".into(),
            Bound::Excluded(f64::INFINITY),
            Bound::Unbounded,
        );
        assert_eq!(
            reader
                .searcher()
                .search(&query, &tantivy_5::collector::Count)
                .unwrap(),
            4
        );
        let query = tantivy_5::query::RangeQuery::new_f64_bounds(
            "value".into(),
            Bound::Unbounded,
            Bound::Excluded(canonical_f64(f64::NAN)),
        );
        assert_eq!(
            reader
                .searcher()
                .search(&query, &tantivy_5::collector::Count)
                .unwrap(),
            7
        );
    }
}

fn check_json_nan_query_boundaries(fast: bool) {
    use tantivy::schema::{OwnedValue, Schema, FAST, STRING};
    let mut schema = Schema::builder();
    let field = schema.add_json_field("json", if fast { STRING | FAST } else { STRING });
    let index = tantivy::Index::create_in_ram(schema.build());
    let mut writer = index.writer_with_num_threads(1, 50_000_000).unwrap();
    for value in float_values() {
        let mut doc = tantivy::TantivyDocument::default();
        doc.add_field_value(
            field,
            &OwnedValue::Object(vec![("a".into(), OwnedValue::F64(canonical_f64(value)))]),
        );
        writer.add_document(doc).unwrap();
    }
    for (path, value) in [
        ("a", OwnedValue::I64(-3)),
        ("a", OwnedValue::Str("NaN".into())),
        ("b", OwnedValue::F64(canonical_f64(f64::NAN))),
    ] {
        let mut doc = tantivy::TantivyDocument::default();
        doc.add_field_value(field, &OwnedValue::Object(vec![(path.into(), value)]));
        writer.add_document(doc).unwrap();
    }
    writer.commit().unwrap();
    let reader = IndexReaderWrapper::from_index(Arc::new(index), set_bitset).unwrap();
    let mut hits = HashSet::<u32>::new();
    for nan in nan_values() {
        reader
            .json_term_query_f64("a", nan, &mut hits as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(hits.len(), 3);
        hits.clear();
        reader
            .json_terms_query_f64("a", &[nan; 10], &mut hits as *mut _ as *mut c_void)
            .unwrap();
        assert_eq!(hits.len(), 3);
        hits.clear();
        reader
            .json_range_query(
                "a",
                f64::NEG_INFINITY,
                nan,
                true,
                false,
                false,
                false,
                &mut hits as *mut _ as *mut c_void,
            )
            .unwrap();
        assert_eq!(hits.len(), 6);
        hits.clear();
        reader
            .json_range_query(
                "a",
                nan,
                nan,
                false,
                false,
                true,
                true,
                &mut hits as *mut _ as *mut c_void,
            )
            .unwrap();
        assert_eq!(hits.len(), 3);
        hits.clear();
        reader
            .json_range_query(
                "a",
                nan,
                0.0,
                false,
                true,
                true,
                false,
                &mut hits as *mut _ as *mut c_void,
            )
            .unwrap();
        assert_eq!(hits.len(), 3);
        hits.clear();
        reader
            .json_range_query(
                "a",
                nan,
                0.0,
                false,
                true,
                false,
                false,
                &mut hits as *mut _ as *mut c_void,
            )
            .unwrap();
        assert!(hits.is_empty());
    }
}

#[test]
fn test_json_nan_query_boundaries() {
    for fast in [false, true] {
        check_json_nan_query_boundaries(fast);
    }
}
