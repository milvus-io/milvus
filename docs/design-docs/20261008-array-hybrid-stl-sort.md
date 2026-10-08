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

Non-null empty arrays remain valid rows with no postings. Row nulls remain false in positive membership and negative membership. Floating NaNs do not contribute matching postings. Any valid NaN forces HYBRID to use the NaN-free sorted path on capable readers, including at low cardinality. Numeric equality cannot match NaN targets; range predicates with a NaN bound match no rows. Scalar reverse lookup cannot reconstruct a whole array and returns no scalar value; HasRawData remains false, so the raw array column stays loaded for operations requiring full array contents.

ARRAY_CONTAINS_ANY uses the union of matching row postings. ARRAY_CONTAINS_ALL uses the existing intersection of per-element match bitmaps. Negation and empty query behavior remain the expression evaluator's responsibility.

### NaN query semantics

A successfully produced floating NaN is a valid non-null value, whether it comes from a scalar field, a valid array/Struct member, or a successful JSON numeric cast. It never enters STL_SORT ordered entries. Equality and IN cannot match NaN; ordered comparisons with either operand NaN are false. Inequality, NOT IN and logical NOT follow the same floating comparisons and validity masks as a raw scan. IS NULL is false and IS NOT NULL is true for a valid NaN. An invalid member or source NULL remains invalid even if its ignored payload contains NaN.

Scalar and nested sorted indexes represent an unindexed valid NaN using their existing source-validity bitmap and row-to-sorted-offset mapping: validity is true and the sorted offset is -1. Invalid slots remain false; finite values have a nonnegative sorted offset. No NaN value or separate NaN-row list is stored. Reverse lookup recognizes a valid floating slot with offset -1. Ordinary arrays retain their parent-row postings and raw array contents instead of inferring individual elements from this mapping.

Positive range queries use finite postings when the scalar/nested index contains unindexed NaNs. A runtime-only flag, derived from source validity and posting count, keeps the existing complement optimization for indexes without such slots. NOT IN continues to subtract matching postings from source validity.

Contains ANY treats a NaN target as an equality that cannot match. Contains ALL with any NaN target cannot match; it must not discard that target during deduplication. Scan and index evaluation must agree for NaN-only and mixed finite/NaN targets, irrespective of target ordering. This change does not redefine finite NULL-member payload handling.

JSON contains queries with a NaN target use raw evaluation so NaN comparisons and negation follow the source expression across scalar and array projections. Finite-target queries retain their existing execution path. This does not add container or NULL-member metadata or repair historical indexes.

A raw JSON string such as "NaN" remains a string. Only a successful STRING_TO_DOUBLE projection is a numeric NaN; unconverted string comparisons retain their source-type semantics. Genuine parse failures remain cast failures.

### Persistence and rolling upgrades

Packed V3 indexes already persist row count, validity, postings, and offset data. Reuse that layout and the existing memory/mmap synchronous and asynchronous loaders, preserving source validity and -1 offsets for unindexed slots. The legacy numeric BinarySet path saves source validity for arrays/nested fields or unindexed valid NaNs, and reconstructs missing sorted offsets as -1. Finite primitive fields retain their existing validity reconstruction. This preserves valid NaNs and empty-array row validity; old numeric indexes without the validity entry retain their existing reader behavior.

Advertise scalar engine version 6 through the existing QueryNode session capability. DataCoord negotiates the minimum reader capability across QueryNodes, and DataNode clamps build versions against the supported maximum. Versions <=5 continue to build ordinary Array HYBRID high-cardinality indexes as INVERTED. Nested indexes retain their existing version-4 gate. Existing physical indexes retain their stored type and are not migrated or repaired. Writers incapable of preserving a valid NaN source row reject that build instead of putting NaN into sorted entries. Version 6 protects readers that understand a source-valid floating slot with no sorted offset; older readers assume every valid scalar slot has a posting.

Independent fixes for finite NULL members, inverted NULL-offset domains, and compact nullable inverted-array access are outside this change. A NaN hidden in an invalid payload is excluded from ordered entries without being recorded as a source-valid NaN. Existing finite NULL-member behavior is retained.

Typed JSON scalar paths retain HYBRID selection. JSON ARRAY_* AUTO requests route to their supported inverted array projection. Full JSON keeps the flat multi-type index; neither is sent through a single-valued sorted projection. A successful STRING_TO_DOUBLE conversion that yields NaN retains a valid numeric projection and follows the same NaN comparison and null semantics as a floating field. Invalid strings remain cast failures; the source JSON value and path existence are preserved.

## Empty-index behavior

A nested field with no element slots keeps the existing DataIsEmpty contract. ScalarIndexCreator consumes that signal, skips uploading an index, and QueryNode retains the binlog/raw query path. NaN slots with actual source elements retain their existing offset domain; valid NaNs and ignored NaN payloads are tested separately.

## Test Plan

Test the HYBRID selection matrix across old/new versions and nested/ordinary arrays. Compare membership and range bitmaps against row-level expectations with duplicated elements, mixed matching/nonmatching elements, null rows, empty arrays, and floating NaNs. Exercise legacy and packed memory/mmap reloads, including asynchronous packed reads. Verify valid NaNs and ignored NaN payloads, across single-segment and multi-segment writers. Check unsupported-version failures before upload and assert that every sorted entry is non-NaN. Run sealed expression evaluation against raw-scan results for ARRAY_CONTAINS, ANY, and ALL. Test QueryNode version-5/version-6 coexistence and minimum-version negotiation.

The patch changes future builds after capability negotiation. Rebuilding indexes on an existing production instance is a separate operational action.
