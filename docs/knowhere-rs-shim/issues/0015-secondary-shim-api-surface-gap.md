## Summary

- After the first-round header wrappers and sparse-row model landed, remote `internal/core` compilation advanced into more Milvus subsystems.
- The next blockers are no longer generic missing headers, but a second layer of knowhere API surface assumptions:
  - `knowhere/comp/knowhere_check.h`
  - `knowhere/cluster/cluster_factory.h`
  - `knowhere::RefineType`
  - `DataSet::SetIsSparse(bool)`
  - sparse row byte-view compatibility for `row->data()`

## Impact

- Blocks `index`, `segcore`, `query`, `exec`, `storage`, and `clustering` compilation.
- Confirms the stage-1 integration is still constrained by compatibility-surface completeness, not by remote bootstrap or `knowhere-rs` runtime logic.

## Evidence

- Remote build errors now include:
  - `fatal error: knowhere/comp/knowhere_check.h: No such file or directory`
  - `fatal error: knowhere/cluster/cluster_factory.h: No such file or directory`
  - `error: ‘RefineType’ in namespace ‘knowhere’ does not name a type`
  - `error: ... DataSet ... has no member named ‘SetIsSparse’`
  - `error: invalid static_cast from type '...SparseRow<float>::Element*' to type 'const uint8_t*'`
- Representative inclusion and usage sites:
  - `internal/core/src/index/IndexFactory.cpp`
  - `internal/core/src/segcore/check_vec_index_c.cpp`
  - `internal/core/src/segcore/SegcoreConfig.h`
  - `internal/core/src/clustering/KmeansClustering.cpp`
  - `internal/core/src/storage/Event.cpp`

## Resolution Direction

- Keep fixes inside the shim only.
- Add minimal header-only definitions that satisfy Milvus compile-time contracts without expanding stage-1 runtime scope:
  - permissive `KnowhereCheck`
  - stub `ClusterFactory` / `ClusterNode` / `Cluster<T>`
  - lightweight `RefineType` enum
  - sparse flag plumbing on `DataSet`
  - sparse-row `data()` as a raw byte view rather than typed-element pointer
