# MEP: Online Scalar Index Replacement

- **Created:** 2026-06-18
- **Author(s):** @zhengbuqian
- **Status:** Proposal
- **Component:** Proxy, DataCoord, QueryCoord, QueryNode
- **Released:** TBD

## Summary

Introduce an online `ReplaceIndex` operation for scalar indexes. The operation changes the user-visible logical scalar index without requiring users to release and reload a loaded collection.

The core design separates:

- **Logical index:** the public index users address by collection, field, JSON path, and index name.
- **Physical generation:** the actual DataCoord index metadata and segment-index files identified by `index_id`.

After replacement is accepted, Milvus exposes only the latest current generation through public APIs. Older generations become hidden retired generations. They are not build targets and are not public, but their files remain available while loaded or lazy-loaded segments may still reference them.

## Motivation

Scalar index replacement is useful when users need to change scalar index type or parameters on a loaded collection. Today the practical workflow is disruptive:

1. Release the collection.
2. Drop or alter the old scalar index.
3. Create the new scalar index.
4. Wait for index build.
5. Load the collection again.

For workloads that depend on continuous search and query availability, this is too expensive.

Scalar indexes are different from vector indexes for load correctness. Sealed segment load normally does not require scalar index completion. A segment can still be loaded and searched without a scalar index, but query performance may be lower. This allows an online replacement model where QueryCoord updates loaded segments as the new scalar generation becomes available.

However, Milvus supports lazy loading and eviction of indexes. A loaded segment may keep only old index metadata and file paths, then reload the old scalar index later. Therefore old scalar index files must not be physically garbage collected immediately after a replacement is accepted.

## Goals

- Add a public `ReplaceIndex` API for scalar indexes.
- Keep search and query available while a scalar index is being replaced.
- Hide old generations from public APIs immediately after replacement is accepted.
- Allow the replacement to keep the same index name or rename the logical index.
- Allow repeated replacement of the same field/path, producing generation N+1.
- Keep retired generation files GC-protected while loaded or lazy-loaded segments may still reference them.
- Let QueryCoord update loaded segments to the newest current generation when that generation is available.

## Non-Goals

- Supporting vector index replacement in this proposal.
- Making old retired generations user-addressable.
- Providing request-id based idempotency for repeated `ReplaceIndex` calls.
- Guaranteeing that every fresh load uses an old retired generation as a durable fallback forever.
- Changing scalar index query semantics. Missing scalar indexes remain a performance concern, not a correctness concern.

## Public Interfaces

### MilvusService

```proto
service MilvusService {
  rpc ReplaceIndex(ReplaceIndexRequest) returns (common.Status) {}
}

message ReplaceIndexRequest {
  option (common.privilege_ext_obj) = {
    object_type: Collection
    object_privilege: PrivilegeCreateIndex
    object_name_index: 3
  };
  common.MsgBase base = 1;
  string db_name = 2;
  string collection_name = 3;
  string field_name = 4;
  string new_index_name = 5;
  repeated common.KeyValuePair extra_params = 6;
}
```

The public request does not include an old index name. The target is resolved by collection, field, and optional JSON path in index params. The replacement applies to the current logical index for that field/path.

### Authorization

Milvus proxy RBAC is driven by the `common.privilege_ext_obj` request-message annotation. `ReplaceIndexRequest` must carry a collection-level index mutation privilege so the proxy interceptor performs the same authorization path as existing index DDL.

The v1 request uses `PrivilegeCreateIndex` with `object_name_index: 3`, matching `CreateIndex` and `AlterIndex`, because `ReplaceIndex` creates a new current physical index generation. It must not be left without a privilege annotation; otherwise the proxy privilege interceptor treats the message as having no privilege metadata and skips the permission check.

### Client API

The Go SDK exposes a `ReplaceIndex` option similar to `CreateIndex`:

```go
task, err := client.ReplaceIndex(ctx,
    milvusclient.NewReplaceIndexOption(collectionName, fieldName, index.NewBitmapIndex()).
        WithNewIndexName("score_bitmap_idx"))
if err != nil {
    return err
}
return task.Await(ctx)
```

`ReplaceIndex` returns after the DDL is accepted. The returned task waits for the latest current generation to finish.

## Metadata Model

Each physical index row keeps existing index metadata plus generation metadata:

```proto
enum IndexGenerationState {
  IndexGenerationStateNone = 0;
  Current = 1;
  Retired = 2;
}

message IndexInfo {
  int64 indexID = ...;
  string index_name = ...;
  repeated common.KeyValuePair type_params = ...;
  repeated common.KeyValuePair index_params = ...;
  IndexGenerationState generation_state = ...;
  int64 generation = ...;
}
```

The logical index key is:

```text
(collectionID, fieldID, normalized JSON path)
```

For non-JSON scalar fields, the JSON path is empty.

Generation rules:

- Existing indexes without generation metadata are treated as `Current`, generation `1`.
- Exactly one non-deleted generation for a logical key should be `Current`.
- Older generations are `Retired`.
- `Retired` is separate from `IsDeleted`.
- Only `IsDeleted=true` makes an index eligible for existing index GC.

This avoids pairwise replacement metadata such as `replaces_index_id` and `replaced_by_index_id`, which does not model generation 3 replacing generation 2 while generation 1 may still be referenced by some loaded segments.

## ReplaceIndex Admission

DataCoord handles replacement as an atomic metadata transition:

1. Resolve the current generation for the logical key.
2. Validate the target field is scalar.
3. Validate replacement index params with normal scalar index validation.
4. Validate the new index name does not collide with another current logical index in the same collection. Retired generations are ignored for this name check because they are hidden physical rows, not public logical indexes.
5. Allocate a new physical `index_id`.
6. Set the previous current generation to `Retired`.
7. Create the new physical generation as `Current` with `generation = max(existing generation) + 1`.

Reusing the current index name is allowed. Providing a different name is a logical rename: the old public name disappears immediately and the new public name appears immediately.

## Public Metadata Behavior

Public APIs expose only current generations:

- `DescribeIndex`
- `GetIndexStatistics`
- `ListIndexes`
- name-based `DropIndex`
- name-based `AlterIndex`

Retired generations are hidden from users. Users cannot drop or alter a retired generation by old name after replacement.

`DropIndex` on the current logical index drops all generations for the logical key. Retired generation files become physically removable only after the same reference-safety condition described in the GC section.

## Build Scheduling

DataCoord schedules new build work only for current generations.

For retired generations:

- do not create new segment-index tasks;
- keep already-finished segment-index files available;
- already-running tasks may finish harmlessly, but they are not required for forward progress.

New flushed or compacted segments only target the latest current generation.

## Segment Index Selection

For `GetIndexInfos`, DataCoord returns at most one physical generation for each logical key on each segment:

1. Prefer the newest finished generation.
2. Usually this is the current generation.
3. If current is not finished for the segment, return the newest finished retired generation.
4. If no generation is finished, return no scalar index for that logical key.

This is the key fallback rule. It lets fresh load or segment reopen use the best available scalar index files without exposing retired generations as public indexes.

## QueryCoord and QueryNode Behavior

QueryCoord receives both current and retired generations from an internal index listing API.

`IndexChecker` behavior:

- A missing current generation on a loaded segment means the segment should be reopened or updated when DataCoord reports current generation files for that segment.
- A loaded retired generation is not redundant while the current generation is unavailable on that segment.
- Once the current generation is loaded on that same segment, retired generations for the same logical key become redundant and can be dropped from the segment.

QueryNode must treat physical index IDs as the actual loaded index identity. The same field may temporarily have an old retired physical scalar index while DataCoord is building the new current one.

## GC and File Retention

Retired generations are hidden, but not GC-eligible.

The existing index GC path removes metadata and files for indexes marked `IsDeleted=true`. A retired generation must not be marked `IsDeleted=true` just because a newer current generation exists.

Safe physical deletion requires proof that no loaded or lazy-loaded segment can still reference the retired `index_id`.

The target release condition is:

1. the current generation has finished for all relevant eligible sealed segments; and
2. QueryCoord distribution, or an explicit QueryCoord/QueryNode acknowledgement, proves no loaded segment still references the retired `index_id`.

Until that condition is met:

- the retired index metadata remains persisted;
- segment-index files remain available;
- public APIs continue to hide the retired generation.

If exact proof is not available, Milvus should retain retired generations conservatively rather than risk lazy-load failures.

## Repeated Replacement

Repeated replacement of the same logical key is allowed.

Example:

```text
gen1 Current
ReplaceIndex -> gen1 Retired, gen2 Current
ReplaceIndex -> gen1 Retired, gen2 Retired, gen3 Current
```

Build scheduling targets only gen3 after gen3 is current. Segment load selection uses the newest finished generation available for each segment:

```text
gen3 finished -> use gen3
else gen2 finished -> use gen2
else gen1 finished -> use gen1
else no scalar index
```

This avoids an artificial "replacement in progress" public state and avoids rejecting a natural second replacement request.

## DDL Semantics

### DropIndex

Dropping the current logical index drops all generations for the logical key.

Retired generations are not addressable by old public name. A drop request using an old name after rename should follow normal not-found/no-op behavior and must not delete a hidden retired generation for a different current logical name.

### AlterIndex

`AlterIndex` applies only to the current public generation. Retired generations are immutable fallback file sources.

If an alter changes build-affecting params while the current generation is building, Milvus should either reject that alter or follow existing `AlterIndex` rules. It should not mutate retired generations.

### CreateIndex

`CreateIndex` on the same logical key follows the existing "one public index per field/path" rule against the current generation.

`CreateIndex` on a different field or JSON path is independent, subject to normal index name collision checks.

The DataCoord write-path helpers that admit or deduplicate `CreateIndex` must be generation-aware. Helpers such as `canCreateIndex`, `HasSameReq`, and index-name collision checks should only consider the current generation for a logical key, plus non-generated legacy indexes treated as current. Retired generations remain `IsDeleted=false` for GC safety, but they must not make a later `CreateIndex` or idempotent retry fail as if multiple public indexes existed on the field.

## Failure and Retry

If the current generation build fails for some segment:

- public index state should reflect the current generation's aggregate state;
- already loaded segments may continue using retired generations where available;
- fresh load may use the newest finished retired generation if current is not finished;
- users may issue another `ReplaceIndex`, creating generation N+1;
- users may `DropIndex` to remove the logical index and all generations.

If the client times out after the replacement DDL commits, a repeated request creates another generation. Request-id based idempotency is outside the scope of this proposal.

## Restart Recovery

DataCoord persists current and retired generations.

After DataCoord restart:

- public APIs expose only current generations;
- build scheduling resumes only current generations;
- `GetIndexInfos` can still return retired fallback files when current files are unavailable;
- retired generation GC protection remains until reference safety is proven.

## Concurrency Scenarios

| Scenario | Expected Behavior |
|---|---|
| Replace same field/path while previous generation is still building | Allow. New request creates generation N+1 and makes prior current retired. |
| Replace different scalar fields in same collection | Allow. DDL admission may serialize, but generations are independent. |
| Replace different JSON paths on same JSON field | Allow if logical keys and names do not collide. |
| Replace vector index | Reject in this proposal. |
| Drop current logical index during replacement | Drop all generations for that logical key, with physical GC delayed until references are gone. |
| Drop old name after replacement rename | Not found or no-op; must not target hidden retired generations. |
| Search/query during replacement | Continue. Loaded segments can use old retired scalar index until current generation is ready and loaded. |
| Release/load during replacement | Allow. Load uses newest finished generation per logical key. |
| Drop collection during replacement | Collection drop wins and cascades all current and retired generations. |

## Implementation Plan

1. Add public `ReplaceIndex` proto and SDK option.
2. Add DataCoord/internal index proto fields for `generation_state` and `generation`.
3. Extend the index metastore model with generation metadata.
4. Implement DataCoord `ReplaceIndex` as an atomic retire-plus-create transition.
5. Filter retired generations from public metadata APIs.
6. Make DataCoord write-path admission and idempotency helpers current-generation-aware, including `canCreateIndex`, `HasSameReq`, and index-name collision checks.
7. Add internal listing that includes retired generations for QueryCoord.
8. Schedule builds only for current generations.
9. Change `GetIndexInfos` to select newest finished generation per logical key.
10. Make QueryCoord `IndexChecker` generation-aware.
11. Keep retired generations out of existing `IsDeleted` GC until reference safety is proven.
12. Add tests for metadata transitions, write-path admission with retired generations, load selection, QueryCoord reopen behavior, repeated replacement, and continuous-search e2e.

## Test Plan

- Unit: existing index with no generation metadata is treated as current generation 1.
- Unit: replace gen1 with gen2; gen1 becomes retired, gen2 becomes current.
- Unit: replace gen2 with gen3 before gen2 finishes; gen1/gen2 are retired, gen3 is current.
- Unit: public describe/list/stat APIs hide retired generations.
- Unit: `CreateIndex` admission and idempotency ignore retired generations and compare only against the current logical generation.
- Unit: name-collision checks allow reuse of a retired generation's old name only when it is no longer the current public logical name, while still rejecting collisions with other current logical indexes.
- Unit: build scheduler targets only current generations.
- Unit: `GetIndexInfos` returns gen3 if finished, otherwise gen2, otherwise gen1, otherwise none.
- Unit: QueryCoord does not drop a loaded retired generation until current is loaded on the same segment.
- Unit: DropIndex on current logical index expands to all generations for that logical key.
- E2E: loaded collection continues search during scalar replacement.
- E2E: gen1 -> gen2 -> gen3 while continuous searches run.
- Future GC test: retired generation remains protected while QueryCoord reports references, and becomes deletable after references vanish.

## Open Questions

- What is the exact reference-proof mechanism for retired generation GC: QueryCoord distribution scan, explicit QueryNode acknowledgement, or both?
- Should already-running retired-generation build tasks be cancelled when a newer generation becomes current, or simply allowed to finish and be ignored?
- Should repeated `ReplaceIndex` support request-id idempotency in a later API revision?
