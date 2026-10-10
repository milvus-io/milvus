# Index contract behavior tests

## Scope

Exercise the public behavior of `internal/core/src/index/contracts/` through
the `index_tests` target. Cases are organized by interface and by observable
promise, not by concrete index class. A production backend is selected only to
provide a real reader or growing owner. The existing `~/milvus` tests are a
read-only case inventory; assertions use the refactored contracts.

## Test layers

1. **Shared reader cases** build and open an index through the typed registry,
   then query only `IIndexReaderBase` and its advertised mixins. The same input
   and oracle run on every profile with the required capability. Capability
   absence is a routing decision; a load failure is a test failure.
2. **Lifecycle cases** build once from borrowed complete input, serialize to
   named entries, load independently, and verify that artifacts and readers
   outlive caller input. Optional direct conversion consumes the artifact and
   keeps the reader's dependencies alive.
3. **Growing cases** check the shared publication protocol with a minimal owner
   because `IGrowingIndex` implements pinning and replacement itself. A real
   appendable owner checks borrowed input, commit, flush, frozen coverage,
   validity, and old pin behavior.
4. **Vector cases** build and load a real vector reader, then use only
   `IIndexReaderBase` and `IVectorReader` for logical IDs, filtering, search,
   retrieval, metadata, and empty physical state. A method with backend
   dependent support is tested on a profile that declares or demonstrates that
   support; unsupported operations are not inferred from the presence of the
   unified interface.

## Dataset rules

`ScalarDataSets`, `JsonDataSets`, and `TextAndCandidateDataSets` are the shared
catalogs. A new case first chooses an existing dataset with the same logical
values, null pattern, input shape, and coordinate domain. A new catalog entry
is justified only when one of those properties is missing; one dataset should
then serve every interface that needs that property. The build tests use the
same scalar catalog. Vector and growing fixtures remain local because they
carry physical vectors or append generations that the scalar catalogs cannot
represent.

For every interface method, case design spans the applicable dimensions:
ordinary match/miss, empty input or result, duplicate or repeated calls,
boundary values, nullable versus all-valid versus all-null, multiple batches,
row versus element domain, post-build ownership, and unsupported/failure
behavior. Tests assert a complete result bitmap or value sequence rather than
only checking that a call returned.

## Contract matrix

| Contract | Observable behavior |
|---|---|
| `BuilderRegistry`, `LoaderRegistry`, `LoaderEntry` | Typed lookup, unknown and duplicate keys, metadata only caps, independent create/load, parameter forwarding, and factory errors |
| `IArtifactBuilder`, scalar/vector input | Complete borrowed input, multiple and empty batches, null validity, one shot artifact ownership, serialization after caller input dies, and invalid input rejection |
| `IReaderConvertible` | Optional consuming conversion, no implicit I/O, shell destruction, failure propagation, and reader dependency lifetime |
| `IIndexReaderBase`, `ReaderCaps` | Count in declared coordinate domain, type, capability/mixin agreement, resource values, and metadata derived caps matching the opened reader |
| `IScalarPredicateReader` | IN, NOT IN, all comparison and interval bounds, duplicates, empty keys, null exclusion, bit count, nested element offsets |
| `INullReader` | Exact complementary masks, all valid/all null, packed boundaries, empty batches, independent returned bitmap, and own coordinate domain |
| `IScalarValueReader` | Owned `Lookup`, null, repeated and unordered `Gather`, callback ordering/validity, and borrowed view copied inside callback |
| `IPatternMatchReader` | Each operator, literal escaping, exact versus candidate result, and per call routing guard |
| `ITextMatchReader` | Match, phrase and fuzzy semantics, analyzer variants, null exclusion, result size |
| `INgramReader`, `ISpatialReader` | Eligible calls return candidate supersets, never drop true rows, retain Count/domain, and require consumer refinement |
| `IJsonIndexReader`, `JsonResolvedReader` | Path/cast vocabulary, unsupported versus absent path, owns or borrows resolved reader, moved handle lifetime, ordinary predicate behavior after resolution |
| `IVectorReader` | Search and filtered logical IDs, metadata, nullable mapping, owned value retrieval, prepared parameters, and empty physical state; per operation support checked for iterators, refine, sparse and embedding lists |
| `IGrowingIndex`, `GrowingIndexSnapshotPin`, `IAppendable` | Empty and published pin distinction, immutable published count/coverage, copy/move lifetime, monotonic replacement, failure preservation, append input ownership, query commit, and flush |

## Method and edge case ledger

| Interface and methods | Case dimensions and oracle |
|---|---|
| `IScalarPredicateReader::In`, `NotIn` | Empty, absent, duplicate, mixed and all-hit keys; NULL rows remain excluded; all-valid/all-null, single row, batches, high cardinality, nested element offsets, NUL-containing strings; repeated result mutation cannot change later queries |
| `IScalarPredicateReader::Range` (unary) | All six comparison operators, min/max, equality and inequality, negative zero, infinities, nullable rows, row/element domains; bitmap independence |
| `IScalarPredicateReader::Range` (interval) | Four inclusive/exclusive endpoint pairs, equal/reversed endpoints, infinity, all-null, batches, nested offsets; bitmap independence |
| `INullReader::IsNull`, `IsNotNull` | Complementary exact Count-sized masks for mixed, absent-validity, all-null, single-row, packed-bit boundary, batches and nested elements; mutate each returned mask and requery |
| `IScalarValueReader::Lookup`, `Gather` | Owned strings after reader destruction; null lookup; all offsets and packed-bit boundary; Gather empty, ordered/unaligned/repeated offsets, callback validity and one visit per request, NUL/Unicode copied during callback |
| `IPatternMatchReader::ShouldUseForOp`, `PatternMatchIsExact`, `PatternMatch` | Per-call guard at short/empty literal, stable per-operation exact/candidate declaration, SQL LIKE versus literal prefix/postfix/inner and regex, escaping/NUL/Unicode, NULL, high cardinality, candidate superset and independent bitmaps |
| `ITextMatchReader::MatchQuery`, `PhraseMatchQuery`, `FuzzyMatchQuery` | Token and analyzer cases, threshold 0/above token count, phrase order/slop, edit distance 0/1/2, empty query, all-null/empty reader, exact Count-sized bitmap |
| `INgramReader::CanHandle`, `Candidates` | Literal length threshold, pattern operation, UTF-8, empty input, initial mask, candidate superset and repeated AND idempotence |
| `ISpatialReader::Candidates` | Each spatial relation, MBR superset retains true hits, null rows, empty candidate result, result size and repeatability |
| `IJsonIndexReader::CastTypesOf`, `Resolve`, `Exists`; `JsonResolvedReader` | Supported versus unsupported path/cast, escaped path, supported but absent path, null comparable values, owned/borrowed/empty handles, simultaneous resolves and move lifetime |
| `IIndexReaderBase` getters and `ReaderCaps` | Metadata-derived versus opened caps, mixin consistency, row/element count, value type, repeated resource reads, single row/multibatch/all-null profiles |
| `IVectorReader::Search`, `Iterators`, `CalcDistByIDs` | Logical IDs, nullable masking, explicit filter, zero topk/wrong dimension rejection, prepared params, supported result shape or explicit runtime unsupported, empty physical index |
| `IVectorReader::GetVector`, `GetSparseVector`, `GetEmbListByIds` | Dense/sparse owned retrieval, empty requests, unsupported cross-type request, null-only index, list terminal offset and valid empty list versus null parent |
| `IVectorReader` metadata and validity | Metric/type/dim/refine/raw capability, logical validity versus physical count, all-null rows, frozen growing prefix |
| `IGrowingIndex` and `GrowingIndexSnapshotPin` | Empty versus published-empty pin, independent coverage and reader count, copy/move, old pin after publication/owner destruction, rejected replacement and monotonic coverage |
| `IAppendable`, `CommitIfNeeded`, `Flush` | Borrowed text input copied before return, accepted replay, row gap rejection, null versus empty value, unpublished tail, flush/idempotence, vector null-only tail and immutable earlier prefix |
| `LoaderEntry`, `LoaderRegistry` | Empty/partial/full entry, metadata only `derive_caps`, unknown/duplicate registration, synchronous and asynchronous load, create error/null loader/load error with preserved classification |
| `BuilderRegistry<Input>` | Unknown and duplicate family, separate tables for different input types, same family with distinct input shape, unchanged typed parameters, independent instances, null/throwing factory, concurrent registration and lookup |
| `IArtifactBuilder<Input>::InputSpec`, `Build` | Stable side input declaration; borrowed values survive multiple/empty batches and input destruction; all-null, explicit all-valid, omitted-validity, scalar/array/vector shapes; invalid non-nullable input and invalid prepared-file path rejection |
| `IReaderConvertible::FromArtifact`, `IntoReader` | Null artifact, absent capability, successful ownership transfer, conversion throw/null result, non-convertible real artifact, direct RAM conversion, persistence before consumption |

Invalid scalar lookup offsets, empty scalar builder input, and NaN order have no
uniform expectation in the interfaces or across the registered reader profiles.
The tests do not assign invented behavior to those inputs.
The R-Tree builder rejects an empty geometry dataset, so an empty spatial
reader cannot be opened through the public build and load path.

The vector suite builds, persists, and loads real HNSW input with a declared
resident scalar side input. It also checks that the caller's input can die
before serialization and that the loaded reader still answers queries. The
DiskANN suite checks prepared-file rejection and exercises a real scalar
sidecar build/load when the backend supports it; otherwise that case skips.
The vector query suite covers nonempty embedding lists through the public
builder/loader path.

## Verification

Build `index_tests` with unit tests enabled, run the new contract suites by
filter, and run the complete target. Report new failures separately from the
existing baseline. A passing helper-only test is not evidence for a production
reader: the query and lifecycle suites must cross the registry and loader path.
