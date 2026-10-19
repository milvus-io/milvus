# Persist complete JSON path null offsets in packed scalar indexes

- Date: 2026-10-19
- Status: Implementation
- Branches: master, 3.0, 2.6
- Scope: scalar JSON path INVERTED indexes stored in the V3 packed format
- Related: [#50976](https://github.com/milvus-io/milvus/issues/50976), [#50979](https://github.com/milvus-io/milvus/pull/50979), [#51135](https://github.com/milvus-io/milvus/pull/51135)

## Problem

A JSON path may be missing, null, or incompatible with the index cast type.
These rows are UNKNOWN for comparisons, including negated comparisons. They
must be excluded independently of whether the JSON path EXISTS.

Historical JsonInvertedIndex artifacts record only whole-field null rows in
`index_null_offset`. The 2.6 reader therefore reconstructs comparable rows with
a broad Tantivy query whenever IsNotNull is called. Newer 3.0 and master builders
already project JSON into nullable scalar input and persist invalid offsets,
but an unmarked historical V3 artifact cannot safely be assumed complete.

## Persistent contract

Add the optional boolean `json_null_offsets_complete` to the existing packed
`__meta__` object. A true value certifies that the existing `index_null_offset`
entry contains every non-comparable row for this scalar JSON path and cast.
The entry remains a sorted native-size_t offset array. No additional bitmap or
offset entry, packed format revision, scalar engine version, or coordinator
parameter is introduced.

The flag is independent of `has_null`:

| Complete flag | has_null | Meaning |
| --- | --- | --- |
| true | false | Complete, every row is comparable; no offset entry |
| true | true | Complete offsets are present |
| absent or false | either | Completeness is unknown; recover once when loading |

Only scalar JSON INVERTED writers emit the flag. ARRAY casts, JSON flat indexes,
HYBRID selectors, NGRAM, and ordinary scalar indexes retain their existing behavior.

## Comparable-value semantics

For BOOL and VARCHAR, a successfully extracted/cast scalar value is comparable.
False and the empty string are valid values. For numeric casts, the comparable
set matches the historical inclusive Tantivy range from lowest() to max().
Consequently NaN and infinities produced by STRING_TO_DOUBLE are not comparable.
Missing paths, JSON nulls, whole-field nulls, type mismatches, and failed casts
are all recorded in the complete offset set.

INVERTED JSON projection in all three branches must obey this same contract.
Other JSON index families and ARRAY element conversion are outside this change.
The newer nullable projection previously accepted non-finite cast results;
3.0 and master now exclude them when building a scalar JSON INVERTED index.
This explicitly aligns that edge case with the historical finite-range mask.

## Build and write

Collect complete offsets during JSON extraction; do not query Tantivy to build
the new metadata. Reuse the existing null-offset entry when writing V3.

On 2.6, preserve whole-field-null offsets separately during construction only
so the legacy Serialize()/Upload() path continues to write its historical
representation. Its non-exist sidecar fallback must not start treating cast
failures as missing paths. No new sidecar is added to scattered-file indexes.

On 3.0 and master, reuse the existing nullable projection and scalar builder.
Publish the completeness flag from the JSON wrapper/artifact, not from a
generic inverted writer. Keep the current validity bitmap materialization.

## Load and query

For scalar JSON INVERTED V3 artifacts:

1. Load the existing Tantivy files, null offsets, and independent non-exist data.
2. If the completeness flag is true, use the persisted offsets directly.
3. Otherwise execute the historical type-specific comparable query once and
   move its result into the existing validity representation. Discard any
   bitmap derived from incomplete offsets before allocating the replacement.
   Do not expand the recovery bitmap into an additional offset vector.
4. Finalize validity and memory accounting before publishing the reader. On
   master, run recovery on the existing local I/O executor because a Tantivy
   query can fault in mmap pages.

No query-time lazy initialization, mutex, global cache, or repeated new/old
dispatch is needed for loaded packed indexes. IsNull, IsNotNull, and negated
predicates use the normalized validity representation. Loading the same old
artifact again pays the recovery cost again; remote artifacts are not rewritten.
Recovery errors propagate through the existing load failure path; never mark
incomplete data complete after an error.

Scattered-file layout and compatibility fallbacks retain their existing
contract. The finite-number normalization above also applies to newly built
3.0/master scattered indexes, because extraction is shared across formats.
Legacy scattered-file read paths are unchanged, including the 2.6 query-time
Tantivy fallback.

## EXISTS and compatibility

`json_index_non_exist_offsets` and `has_non_exist` retain their meaning.
A cast failure can be non-comparable while EXISTS remains true. Never derive
non-exist offsets from the newly completed null set.

An old V3 reader ignores the optional metadata field and reads the same entry
encoding. The 2.6 V3 loader uses has_non_exist independently; its scattered-file
fallback that copies null offsets into non-exist offsets is not used here.
Cross-version tests must still verify predicate and EXISTS results, not merely
successful deserialization.

Deploy the same metadata key and semantics on master, 3.0, and 2.6. Existing
engine-version constants remain unchanged. Older unmarked artifacts remain
loadable, including those built by the newer nullable-projection implementation.

## Branch implementation map

| Branch | Build/write | Read/normalization |
| --- | --- | --- |
| 2.6 | JsonInvertedIndex build callback and WriteEntries | JsonInvertedIndex::LoadEntries; base null-offset predicates |
| 3.0 | JsonScalarIndexWrapper over InvertedIndexTantivy | wrapper LoadEntries; existing sealed validity bitmap |
| master | JsonBuildMaterializer and JsonProjectedIndexArtifact | InvertedIndexLoader packed plan/finish; InvertedIndexReader validity |

## Verification

Exercise the real packed serializer/loader with marked complete artifacts and
unmarked historical fixtures whose null entry deliberately omits missing/cast
failures. Removing only the flag from a new complete artifact is insufficient
to test backward compatibility.

Cover BOOL, DOUBLE, VARCHAR; false/empty string; all valid; all invalid; missing
and JSON null; nullable whole field; cast failure; STRING_TO_DOUBLE finite
limits/NaN/infinities; independent EXISTS; NOT IN and validity inversion.
Verify an all-valid new artifact emits the flag without an offset payload.
Keep ARRAY and ordinary scalar behavior covered. Audit old readers against the
new entry/metadata contract and retain the 2.6 scattered-file serialization
regression cases. Exercise both sync/async packed loading on master and reject
a malformed completeness flag without publishing a reader.

Audit every producer and consumer of the new flag, JSON projected validity,
and null/non-exist offsets. Validate that old recovery runs before publication
and that failure cannot publish a partially initialized reader. Run focused
C++ tests plus formatting and the repository lint gate before any remote push.
Record unavailable build/runtime checks explicitly. No measured latency benefit
is claimed without a benchmark.
