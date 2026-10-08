# MEP: Sorted high-cardinality ordinary Array indexes

- **Created:** 2026-10-08
- **Status:** Draft
- **Component:** Index, DataNode, QueryNode, Coordinator

## Summary

Use sorted parent-row postings for ordinary Array HYBRID indexes at high cardinality, gated on negotiated scalar engine version 6. Preserve BITMAP for low cardinality and existing index read compatibility.

## Motivation

Scalar HYBRID already defaults to BITMAP for low cardinality and STL_SORT for high cardinality. Ordinary Array fields still hardcode INVERTED for high cardinality because the sorted scalar builders previously treated each row as a single value. A configured high-cardinality STL_SORT therefore does not apply to ordinary arrays.

Use STL_SORT for newly built high-cardinality ordinary Array HYBRID indexes once the negotiated scalar index engine version reaches 6. Keep low-cardinality BITMAP, explicit INVERTED indexes, old physical-index readers, and versions <=5 unchanged. This change does not expose direct Array STL_SORT creation through the API.

## Design Details

### Parent-row semantics

Ordinary Array sorted indexes store element-to-parent-row postings. Count and result bitmaps remain in the parent-row domain. This differs from nested struct-array sub-field indexes, which flatten elements into distinct offsets.

String indexes retain their sorted unique values and posting lists; duplicate elements of one row contribute only one posting. Numeric indexes store sorted value/parent-row pairs. Numeric range queries use forward matching because complementing nonmatching element postings can incorrectly remove a row that also contains a matching element.

Non-null empty arrays remain valid rows with no postings. Row nulls remain false in positive membership and negative membership. Invalid nullable elements and floating NaNs do not contribute matching postings. Any valid NaN forces HYBRID to use the NaN-free sorted path on capable readers, including at low cardinality. All numeric membership queries discard NaN targets; range predicates with a NaN bound match no rows. Scalar reverse lookup cannot reconstruct a whole array and returns no scalar value; HasRawData remains false, so the raw array column stays loaded for operations requiring full array contents.

ARRAY_CONTAINS_ANY uses the union of matching row postings. ARRAY_CONTAINS_ALL uses the existing intersection of per-element match bitmaps. Negation and empty query behavior remain the expression evaluator's responsibility.

### NaN query semantics

A successfully produced floating NaN is a valid non-null value, whether it comes from a scalar field, a valid array/Struct member, or a successful JSON numeric cast. It never enters STL_SORT ordered entries. Equality and IN cannot match NaN; ordered comparisons with either operand NaN are false. Inequality, NOT IN and logical NOT follow the same floating comparisons and validity masks as a raw scan. IS NULL is false and IS NOT NULL is true for a valid NaN. An invalid member or source NULL remains invalid even if its ignored payload contains NaN.

Contains ANY treats a NaN target as an equality that cannot match. Contains ALL with any NaN target cannot match; it must not discard that target during deduplication. Scan and index evaluation must agree for NaN-only and mixed finite/NaN targets, irrespective of target ordering, and both skip invalid member payloads.

Full JSON flat indexes currently record exact-path scalar presence rather than array-container validity. Contains queries with a NaN target therefore use raw evaluation to preserve the same validity mask and negation semantics; finite-target flat queries retain their existing execution path. This does not add container metadata or repair historical indexes.

A raw JSON string such as "NaN" remains a string. Only a successful STRING_TO_DOUBLE projection is a numeric NaN; unconverted string comparisons retain their source-type semantics. Genuine parse failures remain cast failures.

### Persistence and rolling upgrades

Packed V3 indexes already persist row count, validity, postings, and offset data. Reuse that layout and the existing memory/mmap synchronous and asynchronous loaders. The legacy numeric BinarySet path gains an optional validity entry to preserve empty-array row validity; old numeric indexes without this entry retain their existing reader behavior.

Advertise scalar engine version 6 through the existing QueryNode session capability. DataCoord negotiates the minimum reader capability across QueryNodes, and DataNode clamps build versions against the supported maximum. Versions <=5 continue to build ordinary Array HYBRID high-cardinality indexes as INVERTED. Nested indexes retain their existing version-4 gate. Existing physical indexes retain their stored type and are not migrated or repaired. Writers incapable of preserving a valid NaN source row reject that build instead of putting NaN into sorted entries. The version-6 nan_rows sidecar stores row offsets only, never NaN values.

Nested inverted indexes with nullable members store null offsets in the element domain and carry an explicit marker. New member-null metadata is gated on reader capability 6; an older build target rejects it. Existing markerless metadata keeps its previous interpretation. Bitmap and sorted nested indexes preserve element offsets and skip invalid-member postings. Ordinary inverted indexes use logical-to-physical row access for compact nullable arrays.

Typed JSON scalar paths retain HYBRID selection. JSON ARRAY_* AUTO requests route to their supported inverted array projection. Array-projection validity follows the source container: empty arrays and arrays with no matching member values remain valid; missing paths, JSON null and non-array values are invalid for contains predicates. This mask is independent of the existing EXISTS policy and is persisted in the existing inverted null-offset metadata, so positive and negated contains predicates agree with scans. Full JSON keeps the flat multi-type index; neither is sent through a single-valued sorted projection. A successful STRING_TO_DOUBLE conversion that yields NaN retains a valid numeric projection and follows the same NaN comparison and null semantics as a floating field. Invalid strings remain cast failures; the source JSON value and path existence are preserved.

## Empty-index behavior

A nested field with no element slots keeps the existing DataIsEmpty contract. ScalarIndexCreator consumes that signal, skips uploading an index, and QueryNode retains the binlog/raw query path. Nullable members with actual element slots are different: their offsets and validity are preserved by the index.

## Test Plan

Test the HYBRID selection matrix across old/new versions and nested/ordinary arrays. Compare membership and range bitmaps against row-level expectations with duplicated elements, mixed matching/nonmatching elements, null rows, empty arrays, and floating NaNs. Exercise legacy and packed memory/mmap reloads, including asynchronous packed reads. Verify nested nullable members and hidden NaN payloads for sorted, bitmap, and inverted indexes, across single-segment and multi-segment writers. Check unsupported-version failures before upload and assert that every sorted entry is non-NaN. Run sealed expression evaluation against raw-scan results for ARRAY_CONTAINS, ANY, and ALL. Test QueryNode version-5/version-6 coexistence and minimum-version negotiation.

The patch changes future builds after capability negotiation. Rebuilding indexes on an existing production instance is a separate operational action.
