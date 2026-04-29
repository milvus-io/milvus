# Scalar Index V3 Backport Plan

Base refs checked on 2026-04-29:

- `origin/2.6`: `fe9e0c8935`
- `origin/master`: `8afbe311b1`
- `git rev-list --left-right --count origin/2.6...origin/master`: `753 1346`
- `git cherry -v origin/2.6 origin/master`: 427 master-side commits already have patch-equivalent changes in 2.6, 919 remain master-only.

## Must Backport

These commits define the scalar-index V3 behavior that 2.6 needs to match master: scalar index version propagation and cluster negotiation, unified packed scalar index format, V3 default bump, and the V3 HYBRID high-cardinality index selection.

| Order | Commit | PR | Why |
|---:|---|---|---|
| 1 | `f5f5c2591ec397681663f9a6cdaf0574e27598fb` | [#47215](https://github.com/milvus-io/milvus/pull/47215) | Proto/load metadata changes required to carry scalar index version through build/load APIs. |
| 2 | `d4dc4b824fae236779116b3f9ea60cb40e2b2a74` | [#47342](https://github.com/milvus-io/milvus/pull/47342) | Fixes scalar index version propagation from DataCoord metadata through QueryCoord/QueryNode into C++ load config. |
| 3 | `a7f996619f3495d673c56a26dca7ba29c185e678` | [#47408](https://github.com/milvus-io/milvus/pull/47408) | Adds version-gated HYBRID low/high-cardinality index selection; V3 high-cardinality default is `STL_SORT`. |
| 4 | `c87f15698f0545e7861da4f064082778f609e328` | [#47690](https://github.com/milvus-io/milvus/pull/47690) | Introduces scalar index unified packed file format V3 and V3 upload/load support for scalar index implementations. |
| 5 | `bae515b7bfe1e06366708e890f471b2ba5c0b271` | [#48249](https://github.com/milvus-io/milvus/pull/48249) | Adds scalar index version management parity with vector indexes: max version, target version, force rebuild, and clamping. |
| 6 | `1a84e8d89267c926acbf7ce7b931e99ee0d9c10b` | [#48509](https://github.com/milvus-io/milvus/pull/48509) | Review fixes for version manager locking, clamp logging, and target/current semantics. |
| 7 | `a27401f5aecfd48b6f39ec137f5f5693508c9d87` | [#48510](https://github.com/milvus-io/milvus/pull/48510) | Routes TextMatchIndex through `ScalarIndexCreator`, aligning text scalar indexes with the unified build/load path. |
| 8 | `86bdb309567d38df76c1727d0958853a44d3b2a8` | [#48552](https://github.com/milvus-io/milvus/pull/48552) | Bumps `CurrentScalarIndexEngineVersion` and `MaximumScalarIndexEngineVersion` to 3 and persists scalar index version in snapshots. |
| 9 | `a1bb66ebfee1b2497fd2c4a07fdae32598dde228` | [#48839](https://github.com/milvus-io/milvus/pull/48839) | Removes the old `kScalarIndexUseV3` test/global switch and normalizes C++ scalar index tests around V3 APIs. |
| 10 | `84c052e5661ffec017b8a40d50291e0bb10c06e1` | [#49006](https://github.com/milvus-io/milvus/pull/49006) | Clarifies engine-version vs file-format semantics and renames V3 entry points to `LoadUnified` / `UploadUnified`. |

## Applied On 2.6

The selected commits have been cherry-picked onto local `2.6` with conflict fixes for the 2.6 code layout and API surface.

| Order | Master commit | Local 2.6 commit | PR |
|---:|---|---|---|
| 1 | `f5f5c2591ec397681663f9a6cdaf0574e27598fb` | `7582aa8f51` | [#47215](https://github.com/milvus-io/milvus/pull/47215) |
| 2 | `d4dc4b824fae236779116b3f9ea60cb40e2b2a74` | `cef2ada380` | [#47342](https://github.com/milvus-io/milvus/pull/47342) |
| 3 | `a7f996619f3495d673c56a26dca7ba29c185e678` | `c1038f3b1d` | [#47408](https://github.com/milvus-io/milvus/pull/47408) |
| 4 | `c87f15698f0545e7861da4f064082778f609e328` | `428f715bff` | [#47690](https://github.com/milvus-io/milvus/pull/47690) |
| 5 | `bae515b7bfe1e06366708e890f471b2ba5c0b271` | `53056ae467` | [#48249](https://github.com/milvus-io/milvus/pull/48249) |
| 6 | `1a84e8d89267c926acbf7ce7b931e99ee0d9c10b` | `85b0892b52` | [#48509](https://github.com/milvus-io/milvus/pull/48509) |
| 7 | `a27401f5aecfd48b6f39ec137f5f5693508c9d87` | `51a6c57eae` | [#48510](https://github.com/milvus-io/milvus/pull/48510) |
| 8 | `86bdb309567d38df76c1727d0958853a44d3b2a8` | `4836628c75` | [#48552](https://github.com/milvus-io/milvus/pull/48552) |
| 9 | `a1bb66ebfee1b2497fd2c4a07fdae32598dde228` | `7a7d0689ba` | [#48839](https://github.com/milvus-io/milvus/pull/48839) |
| 10 | `84c052e5661ffec017b8a40d50291e0bb10c06e1` | `e40e8d14ac` | [#49006](https://github.com/milvus-io/milvus/pull/49006) |

## Related, Not In This Initial Scalar Backport

These are master-only and mention V3, but are StorageV3 manifest/delta or general query/retrieve behavior rather than the scalar index V3 format/version contract. Keep them out unless a conflict or test failure proves a dependency.

| Commit | PR | Reason not selected initially |
|---|---|---|
| `a5a298ba915fcfcd590e5bc084e20574e620eaf7` | [#48716](https://github.com/milvus-io/milvus/pull/48716) | V3 segment text/json stats dual-write behavior, tied to StorageV3 manifest tasks. |
| `4fb3220770fed6473eee027c68ab970917c7cabe` | [#48753](https://github.com/milvus-io/milvus/pull/48753) | Snapshot restore fix for StorageV3 segments with text/json stats. |
| `d13ddfd871814e18bcded60f98fa2aa7611a0a6b` | [#48751](https://github.com/milvus-io/milvus/pull/48751) | StorageV3 manifest support in cache-opt field loading. |
| `45aa4db9646b168ec517cf5d8a82c0634904603d` | [#49034](https://github.com/milvus-io/milvus/pull/49034) | StorageV3 delta-log loading path. |
| `deabc7a4e1650c77978e82a70d715905a5b32b31` | [#49044](https://github.com/milvus-io/milvus/pull/49044) | Follow-up StorageV3 delta-log path reconstruction fix. |
| `41b66e0826330d803fb16beb6282da96624880d2` | [#49196](https://github.com/milvus-io/milvus/pull/49196) | V2 legacy text/json stat basePath normalization. |
| `693eb2a265ad53a5153aa7d7540bc873b59ec0e3` | [#49238](https://github.com/milvus-io/milvus/pull/49238) | Query execution path optimization; useful but not required for V3 format compatibility. |
| `df51830b58c4e4517f622520330b08f4f2eccf2b` | [#49127](https://github.com/milvus-io/milvus/pull/49127) | Sealed retrieve policy toggle for index-has-raw-data fields; independent of scalar index V3 file/version compatibility. |
