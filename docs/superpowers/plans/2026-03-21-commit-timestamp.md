# commit_timestamp Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `commit_timestamp` to `SegmentInfo` and `SegmentLoadInfo`, then make all temporal decisions (delete apply, snapshot visibility) use `commit_timestamp` when non-zero instead of `start_position.timestamp`.

**Architecture:** A new `uint64 commit_timestamp` field is added to both `SegmentInfo` (DataCoord) and `SegmentLoadInfo` (QueryCoord→QueryNode). When non-zero, this field overrides `start_position.Timestamp` as the effective transaction time for the segment — ensuring import segments (committed at `T_commit`) are immune to DML deletes that happened before `T_commit`. After compaction rewrites all row timestamps, output segments naturally have `commit_timestamp = 0`.

**Tech Stack:** Go, protobuf (proto3), Milvus internal packages (`merr`, `pkg/v2/log`, `pkg/v2/proto/datapb`, `pkg/v2/proto/querypb`)

---

## File Map

| File | Change |
|------|--------|
| `pkg/proto/data_coord.proto` | Add `uint64 commit_timestamp = 35` to `SegmentInfo` |
| `pkg/proto/query_coord.proto` | Add `uint64 commit_timestamp = 25` to `SegmentLoadInfo` |
| `pkg/proto/datapb/*.pb.go` | Auto-generated — run `make generated-proto-without-cpp` |
| `pkg/proto/querypb/*.pb.go` | Auto-generated — run `make generated-proto-without-cpp` |
| `internal/datacoord/meta.go` | Add `UpdateCommitTimestamp` operator |
| `internal/datacoord/meta_test.go` | Test `UpdateCommitTimestamp` |
| `internal/querycoordv2/utils/types.go` | Copy `commit_timestamp` into `SegmentLoadInfo` |
| `internal/querycoordv2/utils/types_test.go` | Test the copy |
| `internal/querynodev2/delegator/delegator_data.go` | Helper `segmentEffectiveTs`; 4 call-sites |
| `internal/querynodev2/delegator/delegator_data_test.go` | Test delete buffer uses effective ts |
| `internal/datacoord/handler.go` | `GenSnapshot` filter uses `effectiveTs` |
| `internal/datacoord/handler_test.go` | Test snapshot excludes uncommitted segment |

---

## Chunk 1: Proto + Generated Code

### Task 1: Add `commit_timestamp` to protos and regenerate

**Files:**
- Modify: `pkg/proto/data_coord.proto`
- Modify: `pkg/proto/query_coord.proto`
- Generated: `pkg/proto/datapb/*.pb.go`, `pkg/proto/querypb/*.pb.go`

- [ ] **Step 1: Add field to `SegmentInfo`**

  In `pkg/proto/data_coord.proto`, after line 449 (`int32 schema_version = 34;`), add:

  ```proto
  // commit_timestamp is the transaction timestamp for import segments.
  // When non-zero, it overrides start_position.Timestamp for all temporal
  // decisions (delete filtering, snapshot visibility). Zero means normal segment.
  // Cleared to 0 when segment data is rewritten by compaction.
  uint64 commit_timestamp = 35;
  ```

- [ ] **Step 2: Add field to `SegmentLoadInfo`**

  In `pkg/proto/query_coord.proto`, after line 407 (`string manifest_path = 24;`), add:

  ```proto
  // commit_timestamp mirrors data_coord.SegmentInfo.commit_timestamp.
  // QueryNode uses it for delete-buffer pinning and ListAfter calls.
  uint64 commit_timestamp = 25;
  ```

- [ ] **Step 3: Regenerate proto**

  ```bash
  make generated-proto-without-cpp
  ```

  Expected: no compilation errors; new `GetCommitTimestamp()` getters appear in generated `.pb.go` files.

- [ ] **Step 4: Verify generated getters exist**

  ```bash
  grep -n "CommitTimestamp" pkg/proto/datapb/data_coord.pb.go | head -5
  grep -n "CommitTimestamp" pkg/proto/querypb/query_coord.pb.go | head -5
  ```

  Expected: both grep commands return results showing `GetCommitTimestamp`.

- [ ] **Step 5: Commit**

  ```bash
  git add pkg/proto/data_coord.proto pkg/proto/query_coord.proto \
          pkg/proto/datapb/ pkg/proto/querypb/
  git commit -s -m "feat: add commit_timestamp field to SegmentInfo and SegmentLoadInfo protos

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```

---

## Chunk 2: Meta Operator

### Task 2: Add `UpdateCommitTimestamp` operator in DataCoord meta

**Files:**
- Modify: `internal/datacoord/meta.go`
- Test: `internal/datacoord/meta_test.go`

- [ ] **Step 1: Write the failing test**

  In `internal/datacoord/meta_test.go`, inside `TestUpdateSegmentsInfo` (around line 1029) or as a new sub-test, add:

  ```go
  t.Run("update commit timestamp", func(t *testing.T) {
      meta, err := newMemoryMeta(t)
      assert.NoError(t, err)
      meta.AddSegment(context.Background(), &SegmentInfo{
          SegmentInfo: &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Flushed},
      })

      const testTs uint64 = 1234567890

      err = meta.UpdateSegmentsInfo(context.TODO(), UpdateCommitTimestamp(1, testTs))
      assert.NoError(t, err)

      seg := meta.GetSegment(context.TODO(), 1)
      assert.Equal(t, testTs, seg.GetCommitTimestamp())

      // verify clearing to zero works
      err = meta.UpdateSegmentsInfo(context.TODO(), UpdateCommitTimestamp(1, 0))
      assert.NoError(t, err)
      seg = meta.GetSegment(context.TODO(), 1)
      assert.Equal(t, uint64(0), seg.GetCommitTimestamp())
  })
  ```

- [ ] **Step 2: Run to confirm it fails**

  ```bash
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
    ./internal/datacoord/... -run TestUpdateSegmentsInfo -v 2>&1 | tail -10
  ```

  Expected: FAIL — `UpdateCommitTimestamp undefined`.

- [ ] **Step 3: Implement `UpdateCommitTimestamp`**

  In `internal/datacoord/meta.go`, after the `UpdateIsImporting` function (~line 1291):

  ```go
  // UpdateCommitTimestamp sets the commit_timestamp on an import segment.
  // A non-zero value marks the segment as committed at that transaction time,
  // overriding start_position.Timestamp for all temporal decisions.
  // Pass 0 to clear the field (e.g., after compaction rewrites all row timestamps).
  func UpdateCommitTimestamp(segmentID int64, ts uint64) UpdateOperator {
      return func(modPack *updateSegmentPack) bool {
          segment := modPack.Get(segmentID)
          if segment == nil {
              log.Ctx(context.TODO()).Warn("meta update: update commit timestamp failed - segment not found",
                  zap.Int64("segmentID", segmentID))
              return false
          }
          segment.CommitTimestamp = ts
          return true
      }
  }
  ```

- [ ] **Step 4: Run the test to confirm it passes**

  ```bash
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
    ./internal/datacoord/... -run TestUpdateSegmentsInfo -v 2>&1 | tail -10
  ```

  Expected: PASS.

- [ ] **Step 5: Commit**

  ```bash
  git add internal/datacoord/meta.go internal/datacoord/meta_test.go
  git commit -s -m "feat: add UpdateCommitTimestamp operator to datacoord meta

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```

---

## Chunk 3: QueryCoord — Propagate to SegmentLoadInfo

### Task 3: Copy `commit_timestamp` when building `SegmentLoadInfo`

**Files:**
- Modify: `internal/querycoordv2/utils/types.go`
- Test: `internal/querycoordv2/utils/types_test.go` (create if it doesn't exist, otherwise add to existing)

- [ ] **Step 1: Write the failing test**

  Find or create `internal/querycoordv2/utils/types_test.go`. Add:

  ```go
  package utils

  import (
      "testing"

      "github.com/stretchr/testify/assert"

      "github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
      "github.com/milvus-io/milvus/pkg/v2/proto/datapb"
  )

  func TestPackSegmentLoadInfo_CommitTimestamp(t *testing.T) {
      const commitTs uint64 = 99999

      seg := &datapb.SegmentInfo{
          ID:              1,
          CollectionID:    10,
          PartitionID:     100,
          InsertChannel:   "ch1",
          CommitTimestamp: commitTs,
          StartPosition:   &msgpb.MsgPosition{Timestamp: 1000},
      }

      checkpoint := &msgpb.MsgPosition{Timestamp: 2000}
      loadInfo := PackSegmentLoadInfo(seg, checkpoint, nil)
      assert.Equal(t, commitTs, loadInfo.GetCommitTimestamp())
  }
  ```

  The function is `PackSegmentLoadInfo` (not `BuildSegmentLoadInfo`) and takes `*datapb.SegmentInfo` directly — not the `*datacoord.SegmentInfo` wrapper. Import `msgpb` from `go-api/v2/msgpb`, not from `pkg/v2/proto/msgpb` (which doesn't contain `MsgPosition`).

- [ ] **Step 2: Run to confirm it fails**

  ```bash
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
    ./internal/querycoordv2/utils/... -run TestPackSegmentLoadInfo_CommitTimestamp -v 2>&1 | tail -10
  ```

  Expected: FAIL — `GetCommitTimestamp()` returns 0, not `commitTs`.

- [ ] **Step 3: Add the field copy in `types.go`**

  In `internal/querycoordv2/utils/types.go`, inside the `loadInfo := &querypb.SegmentLoadInfo{...}` literal (around line 75–94), add:

  ```go
  CommitTimestamp: segment.GetCommitTimestamp(),
  ```

- [ ] **Step 4: Run the test to confirm it passes**

  ```bash
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
    ./internal/querycoordv2/utils/... -run TestPackSegmentLoadInfo_CommitTimestamp -v 2>&1 | tail -10
  ```

  Expected: PASS.

- [ ] **Step 5: Commit**

  ```bash
  git add internal/querycoordv2/utils/types.go internal/querycoordv2/utils/types_test.go
  git commit -s -m "feat: propagate commit_timestamp from SegmentInfo to SegmentLoadInfo

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```

---

## Chunk 4: QueryNode — Delete Buffer Uses Effective Timestamp

### Task 4: Use `commit_timestamp` in delete-buffer pin/list calls

**Context:** When a segment loads, the delegator pins the delete buffer at
`start_position.Timestamp` so deletes from that point forward are retained and
applied. For import segments with `commit_timestamp = T_commit`, only deletes
from `T_commit` onwards should apply — deletes before `T_commit` happened
"before" the data was officially committed.

**Files:**
- Modify: `internal/querynodev2/delegator/delegator_data.go`
- Test: `internal/querynodev2/delegator/delegator_data_test.go`

- [ ] **Step 1: Write failing test**

  In `internal/querynodev2/delegator/delegator_data_test.go`, add a test that
  verifies that when a `SegmentLoadInfo` has a non-zero `commit_timestamp`, the
  delegator applies only deletes at or after `commit_timestamp`, not earlier ones.

  Search the test file for existing patterns like `TestProcessDelete` or
  segment-loading tests to understand the test setup. Then add:

  ```go
  func TestLoadSegments_CommitTimestampFiltersDeletes(t *testing.T) {
      // This test verifies that for an import segment with commit_timestamp=T_commit,
      // delete records with ts < T_commit are NOT applied (only ts >= T_commit are applied).

      // The observable behavior: deleteBuffer.ListAfter(effectiveTs) where effectiveTs
      // uses commit_timestamp. We test this by examining the effective ts used to
      // query the delete buffer.
      //
      // Use segmentEffectiveTs helper directly:
      info := &querypb.SegmentLoadInfo{
          SegmentID:       1,
          StartPosition:   &msgpb.MsgPosition{Timestamp: 1000},
          CommitTimestamp: 5000,
      }
      assert.Equal(t, uint64(5000), segmentEffectiveTs(info))

      // Without commit_timestamp: falls back to start_position.Timestamp
      infoNormal := &querypb.SegmentLoadInfo{
          SegmentID:     2,
          StartPosition: &msgpb.MsgPosition{Timestamp: 1000},
      }
      assert.Equal(t, uint64(1000), segmentEffectiveTs(infoNormal))
  }
  ```

- [ ] **Step 2: Run to confirm it fails**

  ```bash
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
    ./internal/querynodev2/delegator/... -run TestLoadSegments_CommitTimestampFiltersDeletes -v 2>&1 | tail -10
  ```

  Expected: FAIL — `segmentEffectiveTs undefined`.

- [ ] **Step 3: Add `segmentEffectiveTs` helper and update call sites**

  In `internal/querynodev2/delegator/delegator_data.go`:

  **a) Add the helper function** (near the top of the file, after imports):

  ```go
  // segmentEffectiveTs returns the timestamp used for delete-buffer pin/list operations.
  // For import segments with a non-zero commit_timestamp, deletes before T_commit
  // predated the segment's official commit and should not apply.
  // For normal segments (commit_timestamp == 0), start_position.Timestamp is used.
  func segmentEffectiveTs(info *querypb.SegmentLoadInfo) uint64 {
      if ts := info.GetCommitTimestamp(); ts != 0 {
          return ts
      }
      return info.GetStartPosition().GetTimestamp()
  }
  ```

  **b) Update call site 1 — Pin (line ~475):**
  ```go
  // Before:
  sd.deleteBuffer.Pin(info.GetStartPosition().GetTimestamp(), info.GetSegmentID())

  // After:
  sd.deleteBuffer.Pin(segmentEffectiveTs(info), info.GetSegmentID())
  ```

  **c) Update call site 2 — Unpin (line ~479):**
  ```go
  // Before:
  sd.deleteBuffer.Unpin(info.GetStartPosition().GetTimestamp(), info.GetSegmentID())

  // After:
  sd.deleteBuffer.Unpin(segmentEffectiveTs(info), info.GetSegmentID())
  ```

  **d) Update call site 3 — Phase 1 ListAfter (line ~810):**
  ```go
  // Before:
  records := sd.deleteBuffer.ListAfter(info.GetStartPosition().GetTimestamp())

  // After:
  records := sd.deleteBuffer.ListAfter(segmentEffectiveTs(info))
  ```

  **e) Update call site 4 — Phase 3 catchUpTs (line ~873):**
  ```go
  // Before:
  catchUpTs := info.GetStartPosition().GetTimestamp()

  // After:
  catchUpTs := segmentEffectiveTs(info)
  ```

  **Note — line ~854 is intentionally excluded.** That line uses
  `info.GetStartPosition().GetTimestamp()` only inside a `zap.Time(...)` log
  field for diagnostic output. It reflects the structural start position of the
  segment (useful for debugging) and is not a temporal decision point. Changing
  it would make logs confusing without any correctness benefit.

- [ ] **Step 4: Run the test to confirm it passes**

  ```bash
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
    ./internal/querynodev2/delegator/... -run TestLoadSegments_CommitTimestampFiltersDeletes -v 2>&1 | tail -10
  ```

  Expected: PASS.

- [ ] **Step 5: Run the full delegator test suite to catch regressions**

  ```bash
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
    ./internal/querynodev2/delegator/... 2>&1 | tail -20
  ```

  Expected: all tests PASS.

- [ ] **Step 6: Commit**

  ```bash
  git add internal/querynodev2/delegator/delegator_data.go \
          internal/querynodev2/delegator/delegator_data_test.go
  git commit -s -m "feat: use commit_timestamp for delete buffer pin/list in delegator

  Import segments with commit_timestamp use it instead of start_position.Timestamp
  so only deletes at or after T_commit are applied to the segment.

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```

---

## Chunk 5: DataCoord — GenSnapshot Uses Effective Timestamp

### Task 5: Respect `commit_timestamp` in `GenSnapshot` segment filter

**Context:** `handler.go:GenSnapshot` includes segments whose
`start_position.Timestamp < snapshotTs`. For import segments, the effective time
is `commit_timestamp`. A segment committed at `T_commit > snapshotTs` must not
appear in a snapshot taken before `T_commit`.

**Files:**
- Modify: `internal/datacoord/handler.go`
- Test: `internal/datacoord/handler_test.go`

- [ ] **Step 1: Write failing test**

  In `internal/datacoord/handler_test.go`, find the existing `GenSnapshot`
  tests (search for `TestGenSnapshot` or similar). Add a new sub-test:

  ```go
  t.Run("import segment excluded when snapshotTs < commit_timestamp", func(t *testing.T) {
      // Segment has start_position.Ts=1000 (would normally pass the ts<snapshot filter)
      // but commit_timestamp=5000. A snapshot at ts=3000 should NOT include it.
      const segID int64 = 999
      const startTs uint64 = 1000
      const commitTs uint64 = 5000
      const snapshotTs uint64 = 3000

      // Set up meta with the segment
      // (adapt setup to match the existing test helper pattern in this file)
      seg := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
          ID:              segID,
          CollectionID:    1,
          State:           commonpb.SegmentState_Flushed,
          Binlogs:         []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{}}}, },
          StartPosition:   &msgpb.MsgPosition{Timestamp: startTs},
          CommitTimestamp: commitTs,
      }}
      // ... (add segment to meta, call GenSnapshot, assert segID absent)
  })

  t.Run("import segment included when snapshotTs >= commit_timestamp", func(t *testing.T) {
      // Same segment, snapshot at ts=6000 >= commitTs=5000, should be included
      // ... assert segID present
  })
  ```

  Note: adapt setup to use the same helper used by existing handler tests. Look
  at how the test constructs the `handler` and `meta` objects.

- [ ] **Step 2: Run to confirm it fails**

  ```bash
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
    ./internal/datacoord/... -run TestGenSnapshot -v 2>&1 | tail -15
  ```

  Expected: the new sub-tests FAIL (import segment incorrectly included at
  `snapshotTs=3000` even though `commit_timestamp=5000`).

- [ ] **Step 3: Add `segmentEffectiveTs` helper in `handler.go` and update filter**

  In `internal/datacoord/handler.go`, add a package-level helper (near other
  helpers, before or after `GenSnapshot`):

  ```go
  // segmentEffectiveTs returns the timestamp that governs when a segment's data
  // is considered "real" in the system. For import segments with a non-zero
  // commit_timestamp, that field takes precedence over start_position.Timestamp.
  func segmentEffectiveTs(info *datapb.SegmentInfo) uint64 {
      if ts := info.GetCommitTimestamp(); ts != 0 {
          return ts
      }
      return info.GetStartPosition().GetTimestamp()
  }
  ```

  Then update the `SelectSegments` filter in `GenSnapshot` (line ~738):

  ```go
  // Before:
  return segmentHasData && info.GetStartPosition().GetTimestamp() < snapshotTs && info.GetState() != commonpb.SegmentState_Dropped && !info.GetIsImporting()

  // After:
  return segmentHasData && segmentEffectiveTs(info.SegmentInfo) < snapshotTs && info.GetState() != commonpb.SegmentState_Dropped && !info.GetIsImporting()
  ```

  Note: `info` inside `SelectSegments` is a `*SegmentInfo` (wrapper), so use
  `info.SegmentInfo` to get the underlying `*datapb.SegmentInfo`.

- [ ] **Step 4: Run the test to confirm it passes**

  ```bash
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
    ./internal/datacoord/... -run TestGenSnapshot -v 2>&1 | tail -15
  ```

  Expected: all sub-tests PASS.

- [ ] **Step 5: Run full datacoord suite**

  ```bash
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
    ./internal/datacoord/... 2>&1 | tail -20
  ```

  Expected: all tests PASS.

- [ ] **Step 6: Commit**

  ```bash
  git add internal/datacoord/handler.go internal/datacoord/handler_test.go
  git commit -s -m "feat: GenSnapshot uses commit_timestamp for import segment visibility

  Import segments with a non-zero commit_timestamp are excluded from snapshots
  taken before T_commit, ensuring consistent primary/secondary visibility.

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```

---

## Chunk 6: Static Check + Final Verification

### Task 6: Pass static check and full test suites

- [ ] **Step 1: Run static check**

  ```bash
  make static-check
  ```

  Fix any lint errors (unused imports, missing error checks, etc.) before proceeding.

- [ ] **Step 2: Run all affected test suites**

  ```bash
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/datacoord/... 2>&1 | tail -5
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/querycoordv2/... 2>&1 | tail -5
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/querynodev2/... 2>&1 | tail -5
  ```

  Expected: all PASS.

- [ ] **Step 3: Commit any static-check fixes**

  If step 1 required fixes:

  ```bash
  git add -u
  git commit -s -m "fix: address static-check findings for commit_timestamp

  Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
  ```
