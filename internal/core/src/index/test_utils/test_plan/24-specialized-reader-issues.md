# 剩余专用读取器和本地产物生产问题

Date: 2026-09-15

Production code was not changed. Runtime evidence is the framework-owned full
run `/tmp/segcore-index-test-run-20260915-041720/index-tests-full.xml`.

## 损坏的持久化 Tantivy payload 被分类为 UnexpectedError

The following independent corruption tests retain the expected persisted-data
classification `ErrorCode::DataFormatBroken` (2024), but runtime returns
`ErrorCode::UnexpectedError` (2001):

- `InvertedIndexArtifactTest.InvalidEnginePayloadIsRejected`
- `TextIndexArtifactTest.InvalidEngineFilesAreRejected`

Both tests preserve the V3 metadata and file inventory while replacing every
declared Tantivy engine file with nonempty invalid bytes. The Text and Inverted
loaders validate the inventory, materialize the files, and explicitly classify
a false `tantivy_index_exist` result as `DataFormatBroken`
(`TextIndexLoader.cpp:310-312`, `InvertedIndexLoader.cpp:338-340`). In this
runtime shape that check succeeds, after which both loaders construct
`TantivyIndexWrapper`. Its persisted-reader constructor calls
`tantivy_load_index` and checks the returned failure with unclassified
`AssertInfo` (`thirdparty/tantivy/tantivy-wrapper.h:124-135`), producing the
observed default `UnexpectedError`.

Malformed persisted engine bytes are external artifact data, so the contract
expectation remains `DataFormatBroken`. Changing either test to accept
`UnexpectedError`, weakening the payload, or stopping at an earlier inventory
error would hide the loader-boundary classification defect. A production fix
would need to preserve system failures while mapping the binding's corrupt
persisted-index result to `DataFormatBroken`; that change is outside this
test-only task.

## 锚定 JsonFlat regex 跨 Rust FFI 边界中止

Runtime evidence is the framework-owned second full run
`/tmp/segcore-index-test-run-20260915-041720/full-run-2.log`. It reaches
`JsonReaders/JsonIndexReaderTest.RoutesAndQueriesExpectedJsonValues/JsonFlatV7_JsonEmployees_RegexPattern`
with the legal raw regex `^(Alice|Bob)$`, then aborts the process. The test and
expectation are retained unchanged.

The public `PatternMatchReader` contract states that `RegexMatch` takes a raw
regular expression and declares no anchor restriction
(`contracts/query/IPatternMatchReader.h:50-55`). Anchors are also established
ordinary syntax in pinned-master direct pattern coverage, for example
`BitmapIndexTest.cpp::TestPatternMatchFunc` uses `^1.*`. The JsonFlat resolved
child advertises `pattern_match` and `exact`; rejecting an engine-specific
unsupported form would therefore at least have to be a controlled query error,
not process termination.

The observed cross-layer path is:

1. `JsonIndexReaderTest.cpp:811-818` registers the `/profile/name/first`
   VARCHAR Regex query with expected rows Alice and Bob. `AddPatternCase` calls
   the resolved child's `PatternMatch` at line 380.
2. `JsonFlatIndexReader.cpp:1051-1074` accepts `PatternOp::RegexMatch` and sends
   the raw owned pattern to `TantivyIndexWrapper::json_regex_query`.
3. `thirdparty/tantivy/tantivy-wrapper.h:1553-1566` crosses to
   `tantivy_json_regex_query`; `tantivy-binding/src/index_reader_c.rs:670-680`
   is an `extern "C"` entry and calls the Rust reader directly.
4. `tantivy-binding/src/index_reader.rs:734-742` invokes
   `RegexQuery::from_pattern_with_json_path`. The locked Tantivy revision is
   `96f3335ab5f061926c5b44cf246e81243e1dedc5`
   (`tantivy-binding/Cargo.lock:3006,3963`). Its
   `src/query/regex_query.rs:82-99` says the JSON-path automaton does not support
   `^` or `$`, appends the raw pattern to the encoded JSON path, and calls
   `Regex::new(regex_text).unwrap()` at line 96.
5. The constructor returns `Err(NoEmpty)` for this combined pattern. `unwrap`
   panics, and Rust reports `panic in a function that cannot unwind` at the C
   entry before aborting.

The second full run first confirmed the abort on `JsonFlatV7` nullable heap.
Framework-owned isolated runs then confirmed the same exit status 134 for all
four V7 profiles:

- `crash-0.log`: `JsonFlatV7`
- `crash-1.log`: `JsonFlatV7NonNull`
- `crash-2.log`: `JsonFlatV7Mmap`
- `crash-3.log`: `JsonFlatV7NonNullMmap`

Their adjacent `.status` files each contain `134`; the mmap runs also have
before/after cleanup manifests. No production fix, test pattern weakening,
backend restriction, or expected-result change is made in this test-only task.
