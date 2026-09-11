# DataNode CPU/Memory Placement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** DataCoord estimates a CPU + memory requirement for every task it dispatches, DataNodes report CPU/memory totals and what they have accepted, and the scheduler places tasks on both dimensions instead of one scalar slot.

**Architecture:** Estimation lives only in DataCoord (`GetTaskResource()` on every task family, cached once, shipped verbatim in each request). DataNode only books what the request says (add on accept, subtract on completion, at the same points the scalar `usingSlots` is booked) and reports `total − Σaccepted` in `QuerySlot`. A new `nodePicker` filters on memory, scores on memory/CPU/balance, and falls through to the untouched scalar max-heap for workers that do not report the new fields.

**Tech Stack:** Go 1.2x, protobuf (protoc from the main checkout), mockery v2.53.3, testify + mockey, paramtable.

**Spec:** `docs/superpowers/specs/2026-08-27-datanode-cpu-memory-slots-design.md`

## Global Constraints

- Branch `enhance/datanode-cpu-memory-slots-3.0`, worktree `/home/zc/work/milvus-worktrees/dn-cpu-mem-3.0`, base `upstream/3.0` @ `b4870e2a34`. Never touch `/home/zc/work/milvus`.
- Every Go test run goes through the wrapper (it sets `PKG_CONFIG_PATH`/`LD_LIBRARY_PATH` to the borrowed core, unsets proxies, and adds `-tags dynamic,test -gcflags="all=-N -l" -count=1`):
  `bash /tmp/claude-1000/-home-zc-work-milvus/a8e23eec-da58-49e1-9340-a33b42be6487/scratchpad/gotest.sh <pkgs...> [-run X -v]`
  Call it `GOTEST` below. For builds use `go build -tags dynamic,test` with the same two env vars (`B=/home/zc/work/nightly-wt/fix-issue-52191-2/internal/core/output`).
- The scalar slot chain (`GetTaskSlot`, `slot_usage`/`task_slot`, `available_slots`, `usingSlots`, `pickNode` heap) is NOT modified. New code sits beside it.
- DataNode never estimates. A request without the new fields books zero.
- No task ever reports a zero requirement from DataCoord: memory is clamped to `dataCoord.taskResource.minTaskMemory` (64MB), CPU to `dataCoord.taskResource.defaultCPU` (1).
- Memory in bytes; CPU in whole cores. `taskcommon.Resource{CPU, Memory int64}` is the one shared type (`pkg/taskcommon/resource.go`).
- Logging: `mlog` only, real ctx first. Errors: `merr` only. Import order std → third-party → milvus (gci).
- Every commit: `git commit -s` with the trailer below; the developer's Signed-off-by must come last.
  ```
  Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
  Claude-Session: https://claude.ai/code/session_01V39kgspQN9aawcNuoRgkER
  ```
- Generated files (`*.pb.go`, `mock_*.go`) are regenerated, never hand-edited.
- Coverage of touched code ≥ 90%.

---

## File Structure

| File | Responsibility |
|---|---|
| `pkg/taskcommon/resource.go` (+`_test.go`) | `Resource` value type shared by coordinator and worker |
| `pkg/proto/data_coord.proto`, `pkg/proto/worker.proto` (+ generated `datapb/*.pb.go`, `workerpb/*.pb.go`) | wire fields |
| `pkg/util/paramtable/component_param.go`, `configs/milvus.yaml` | `dataCoord.taskResource.*` knobs |
| `internal/datacoord/task_resource.go` (+`_test.go`) | the only pricing code: per-family formulas, size fallbacks, `resourceCache` |
| `internal/datacoord/task/task.go`, `mock_task.go` | `GetTaskResource()` on the `Task` interface |
| `internal/datacoord/compaction_task_{mix,l0,clustering,bump_schema_version}.go` | compaction families implement + ship |
| `internal/datacoord/task_{index,stats,analyze}.go` | index/stats/analyze implement + ship |
| `internal/datacoord/import_task_{import,preimport}.go`, `import_util.go`, `copy_segment_task.go`, `task_refresh_external_collection.go` | remaining families implement + ship |
| `internal/datacoord/session/cluster.go` | `WorkerSlots` carries the four new report fields |
| `internal/datacoord/task/node_picker.go` (+`_test.go`), `global_scheduler.go` | two-tier placement |
| `internal/datanode/compactor/{compactor,executor,*_compactor,sort_compaction}.go`, `mock_compactor.go` | compaction bookkeeping |
| `internal/datanode/index/{task,scheduler,task_index,task_stats,task_analyze}.go` | index-queue bookkeeping |
| `internal/datanode/importv2/{task,scheduler,task_*.go}`, `mock_task.go` | import bookkeeping |
| `internal/datanode/services.go`, `pkg/metrics/datanode_metrics.go` | `QuerySlot` report + gauge |

---

### Task 1: `taskcommon.Resource` and the wire fields

**Files:**
- Create: `pkg/taskcommon/resource.go`, `pkg/taskcommon/resource_test.go`
- Modify: `pkg/proto/data_coord.proto` (QuerySlotResponse L1471, CompactionPlan L812, PreImportRequest L1038, ImportRequest L1064, CopySegmentRequest L1199), `pkg/proto/worker.proto` (CreateJobRequest L81, AnalyzeRequest L153, CreateStatsRequest L175)
- Regenerate: `pkg/proto/datapb/data_coord.pb.go`, `pkg/proto/workerpb/worker.pb.go`

**Interfaces:**
- Produces: `taskcommon.Resource{CPU, Memory int64}` with `IsZero() bool`, `Add(Resource) Resource`, `Sub(Resource) Resource` (clamps each field at 0), `String() string`.
- Produces proto getters: `QuerySlotResponse.GetTotalCpu/GetAvailableCpu/GetTotalMemory/GetAvailableMemory`; `GetCpu()/GetMemory()` on `CompactionPlan`, `CreateJobRequest`, `AnalyzeRequest`, `CreateStatsRequest`, `PreImportRequest`, `ImportRequest`, `CopySegmentRequest`.

- [ ] **Step 1: Write the failing test**

`pkg/taskcommon/resource_test.go`:
```go
package taskcommon

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResource(t *testing.T) {
	assert.True(t, Resource{}.IsZero())
	assert.False(t, Resource{CPU: 1}.IsZero())
	assert.False(t, Resource{Memory: 1}.IsZero())

	sum := Resource{CPU: 1, Memory: 10}.Add(Resource{CPU: 2, Memory: 20})
	assert.Equal(t, Resource{CPU: 3, Memory: 30}, sum)

	diff := Resource{CPU: 3, Memory: 30}.Sub(Resource{CPU: 1, Memory: 10})
	assert.Equal(t, Resource{CPU: 2, Memory: 20}, diff)

	// Sub never goes negative: a release that exceeds what was booked clamps to zero.
	clamped := Resource{CPU: 1, Memory: 10}.Sub(Resource{CPU: 5, Memory: 50})
	assert.Equal(t, Resource{}, clamped)

	assert.Equal(t, "cpu=2 memory=20", Resource{CPU: 2, Memory: 20}.String())
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pkg && go test ./taskcommon/ -run TestResource -v`
Expected: FAIL — `undefined: Resource`

- [ ] **Step 3: Write the implementation**

`pkg/taskcommon/resource.go`:
```go
package taskcommon

import "fmt"

// Resource is what one task is expected to occupy on a worker for its whole
// run, or what a worker has in total / has left. CPU is whole cores; Memory is
// bytes. It is estimated only by DataCoord; a worker only adds and subtracts it.
type Resource struct {
	CPU    int64
	Memory int64
}

func (r Resource) IsZero() bool {
	return r.CPU == 0 && r.Memory == 0
}

func (r Resource) Add(o Resource) Resource {
	return Resource{CPU: r.CPU + o.CPU, Memory: r.Memory + o.Memory}
}

// Sub subtracts o and clamps each dimension at zero, so a release that exceeds
// what was booked (a request that changed mid-flight) cannot drive the ledger
// negative.
func (r Resource) Sub(o Resource) Resource {
	return Resource{CPU: max(r.CPU-o.CPU, 0), Memory: max(r.Memory-o.Memory, 0)}
}

func (r Resource) String() string {
	return fmt.Sprintf("cpu=%d memory=%d", r.CPU, r.Memory)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd pkg && go test ./taskcommon/ -run TestResource -v`
Expected: PASS

- [ ] **Step 5: Add the proto fields**

`pkg/proto/data_coord.proto` — replace the `QuerySlotResponse` message with:
```proto
message QuerySlotResponse {
  common.Status status = 1;
  int64 available_slots = 2;
  string version = 3; // Kept for wire compatibility; new schedulers do not use it
  // Two-dimensional report. A worker that predates it leaves total_* at zero,
  // and a coordinator that sees total_memory == 0 places on available_slots.
  // available_* = total_* - sum(cpu/memory of accepted, unfinished tasks).
  int64 total_cpu = 4;
  int64 available_cpu = 5;
  int64 total_memory = 6;   // bytes
  int64 available_memory = 7; // bytes
}
```
Append to `CompactionPlan` (after `repeated schema.FunctionSchema functions = 31;`):
```proto
  // Estimated by DataCoord; the worker books these on accept and releases on completion.
  int64 cpu = 32;
  int64 memory = 33; // bytes
```
Append to `PreImportRequest` (after `plugin_context = 12;`): `int64 cpu = 13; int64 memory = 14;`
Append to `ImportRequest` (after `use_loon_ffi = 17;`): `int64 cpu = 18; int64 memory = 19;`
Append to `CopySegmentRequest` (after `external_spec = 8;`): `int64 cpu = 9; int64 memory = 10;`

`pkg/proto/worker.proto`:
Append to `CreateJobRequest` (after `index_store_path_version = 36;`): `int64 cpu = 37; int64 memory = 38;`
Append to `AnalyzeRequest` (after `plugin_context = 19;`): `int64 cpu = 20; int64 memory = 21;`
Append to `CreateStatsRequest` (after `file_resources = 32;`): `int64 cpu = 33; int64 memory = 34;`

Each appended pair carries the same comment as CompactionPlan (`// Estimated by DataCoord; ...`, `// bytes`).

- [ ] **Step 6: Regenerate the two pb.go files**

Write `/tmp/claude-1000/-home-zc-work-milvus/a8e23eec-da58-49e1-9340-a33b42be6487/scratchpad/regen_proto.sh`:
```bash
#!/bin/bash
set -e
B=/home/zc/work/milvus-worktrees/dn-cpu-mem-3.0
mkdir -p $B/cmake_build/thirdparty
[ -d $B/cmake_build/thirdparty/milvus-proto ] || cp -r /home/zc/work/mv-copyseg-nits/cmake_build/thirdparty/milvus-proto $B/cmake_build/thirdparty/
export PATH=/home/zc/work/milvus/bin:$(go env GOPATH)/bin:$PATH
export LD_LIBRARY_PATH=/home/zc/work/milvus/cmake_build/lib:$LD_LIBRARY_PATH
cd $B/pkg/proto
for spec in "data_coord.proto datapb" "worker.proto workerpb"; do
  set -- $spec
  /home/zc/work/milvus/cmake_build/bin/protoc \
    --proto_path=/home/zc/work/milvus/cmake_build/include \
    --proto_path=$B/cmake_build/thirdparty/milvus-proto/proto \
    --proto_path=. \
    --go_out=paths=source_relative:./$2 \
    --go-grpc_out=require_unimplemented_servers=false,paths=source_relative:./$2 \
    $1
done
```
Run it. If `/home/zc/work/mv-copyseg-nits/cmake_build/thirdparty/milvus-proto` is missing, use any other worktree's `cmake_build/thirdparty/milvus-proto` (`ls /home/zc/work/*/cmake_build/thirdparty/milvus-proto -d`).

Verify: `git diff --stat` shows only the two `.proto` and two `.pb.go` files; `git diff pkg/proto/datapb/data_coord.pb.go | grep '^[-+]' | grep -v rawDesc | grep -i 'protoc-gen-go\|protoc '` prints nothing (no generator-version churn). If it does, do NOT commit — find a matching milvus-proto per the memory note.

- [ ] **Step 7: Build to prove the wire compiles**

Run: `cd pkg && go build ./... && cd .. && go build -tags dynamic,test ./internal/datacoord/session/ ./internal/datanode/compactor/`
Expected: no output.

- [ ] **Step 8: Commit**

```bash
git add pkg/taskcommon/resource.go pkg/taskcommon/resource_test.go pkg/proto/data_coord.proto pkg/proto/worker.proto pkg/proto/datapb/data_coord.pb.go pkg/proto/workerpb/worker.pb.go
git commit -s -m "enhance: add cpu/memory resource fields to task and QuerySlot protos

Introduce taskcommon.Resource and carry a per-task cpu/memory estimate on
every DataNode task request, plus a total/available cpu+memory report on
QuerySlotResponse. The scalar slot fields are kept untouched as the
compatibility tier.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01V39kgspQN9aawcNuoRgkER"
```

---

### Task 2: `dataCoord.taskResource.*` configuration

**Files:**
- Modify: `pkg/util/paramtable/component_param.go` (struct decl after L5592 `AnalyzeTaskSlotUsage`; init after `p.AnalyzeTaskSlotUsage.Init(base.mgr)` ≈ L7140), `pkg/util/paramtable/component_param_test.go`, `configs/milvus.yaml` (after the `dataCoord.slot` block L859-867)

**Interfaces:**
- Produces on `DataCoordConfig`: `TaskResourceVectorIndexCPU`, `TaskResourceAnalyzeCPU`, `TaskResourceClusteringCompactionCPU`, `TaskResourceDefaultCPU` (`GetAsInt64`), `TaskResourceIndexMemoryFactor`, `TaskResourceStatsMemoryFactor`, `TaskResourceL0CompactionMemoryFactor`, `TaskResourceAnalyzeMemoryFactor` (`GetAsFloat`), `TaskResourceClusteringCompactionMemory`, `TaskResourceMinTaskMemory` (`GetAsSize`, bytes).

- [ ] **Step 1: Write the failing test**

Append to `pkg/util/paramtable/component_param_test.go` inside the existing `TestComponentParam` → `t.Run("test dataCoordConfig", ...)` block (find it with `grep -n 'test dataCoordConfig' pkg/util/paramtable/component_param_test.go`), at the end of that closure:
```go
		assert.Equal(t, int64(8), Params.TaskResourceVectorIndexCPU.GetAsInt64())
		assert.Equal(t, int64(8), Params.TaskResourceAnalyzeCPU.GetAsInt64())
		assert.Equal(t, int64(8), Params.TaskResourceClusteringCompactionCPU.GetAsInt64())
		assert.Equal(t, int64(1), Params.TaskResourceDefaultCPU.GetAsInt64())
		assert.Equal(t, 2.0, Params.TaskResourceIndexMemoryFactor.GetAsFloat())
		assert.Equal(t, 2.0, Params.TaskResourceStatsMemoryFactor.GetAsFloat())
		assert.Equal(t, 2.0, Params.TaskResourceL0CompactionMemoryFactor.GetAsFloat())
		assert.Equal(t, 2.0, Params.TaskResourceAnalyzeMemoryFactor.GetAsFloat())
		assert.Equal(t, int64(32)<<30, Params.TaskResourceClusteringCompactionMemory.GetAsSize())
		assert.Equal(t, int64(64)<<20, Params.TaskResourceMinTaskMemory.GetAsSize())
```
(`Params` inside that closure is `params.DataCoordCfg`; check the closure's local name and use it.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd pkg && go test ./util/paramtable/ -run 'TestComponentParam' -v 2>&1 | tail -5`
Expected: compile error `undefined ... TaskResourceVectorIndexCPU`.

- [ ] **Step 3: Add the struct fields and the ParamItems**

After `AnalyzeTaskSlotUsage ParamItem \`refreshable:"true"\`` (L5592):
```go

	// Two-dimensional task pricing (dataCoord.taskResource.*). CPU in cores,
	// memory as a multiplier of the input size or an absolute size.
	TaskResourceVectorIndexCPU             ParamItem `refreshable:"true"`
	TaskResourceAnalyzeCPU                 ParamItem `refreshable:"true"`
	TaskResourceClusteringCompactionCPU    ParamItem `refreshable:"true"`
	TaskResourceDefaultCPU                 ParamItem `refreshable:"true"`
	TaskResourceIndexMemoryFactor          ParamItem `refreshable:"true"`
	TaskResourceStatsMemoryFactor          ParamItem `refreshable:"true"`
	TaskResourceL0CompactionMemoryFactor   ParamItem `refreshable:"true"`
	TaskResourceAnalyzeMemoryFactor        ParamItem `refreshable:"true"`
	TaskResourceClusteringCompactionMemory ParamItem `refreshable:"true"`
	TaskResourceMinTaskMemory              ParamItem `refreshable:"true"`
```

After `p.AnalyzeTaskSlotUsage.Init(base.mgr)`:
```go

	p.TaskResourceVectorIndexCPU = ParamItem{
		Key:          "dataCoord.taskResource.vectorIndexCPU",
		Version:      "3.0.1",
		DefaultValue: "8",
		Doc:          "cpu cores a vector index build task is expected to use; used by DataCoord to place tasks across DataNodes",
		Export:       true,
	}
	p.TaskResourceVectorIndexCPU.Init(base.mgr)

	p.TaskResourceAnalyzeCPU = ParamItem{
		Key:          "dataCoord.taskResource.analyzeCPU",
		Version:      "3.0.1",
		DefaultValue: "8",
		Doc:          "cpu cores an analyze task is expected to use",
		Export:       true,
	}
	p.TaskResourceAnalyzeCPU.Init(base.mgr)

	p.TaskResourceClusteringCompactionCPU = ParamItem{
		Key:          "dataCoord.taskResource.clusteringCompactionCPU",
		Version:      "3.0.1",
		DefaultValue: "8",
		Doc:          "cpu cores a clustering compaction task is expected to use",
		Export:       true,
	}
	p.TaskResourceClusteringCompactionCPU.Init(base.mgr)

	p.TaskResourceDefaultCPU = ParamItem{
		Key:          "dataCoord.taskResource.defaultCPU",
		Version:      "3.0.1",
		DefaultValue: "1",
		Doc:          "cpu cores every other task type (scalar index, stats, mix/l0/sort compaction, import, copy segment) is expected to use",
		Export:       true,
	}
	p.TaskResourceDefaultCPU.Init(base.mgr)

	p.TaskResourceIndexMemoryFactor = ParamItem{
		Key:          "dataCoord.taskResource.indexMemoryFactor",
		Version:      "3.0.1",
		DefaultValue: "2",
		Doc:          "memory of an index build task = indexed field size * this factor",
		Export:       true,
	}
	p.TaskResourceIndexMemoryFactor.Init(base.mgr)

	p.TaskResourceStatsMemoryFactor = ParamItem{
		Key:          "dataCoord.taskResource.statsMemoryFactor",
		Version:      "3.0.1",
		DefaultValue: "2",
		Doc:          "memory of a stats task (text match, bm25, json key index) or a sort compaction = segment size * this factor",
		Export:       true,
	}
	p.TaskResourceStatsMemoryFactor.Init(base.mgr)

	p.TaskResourceL0CompactionMemoryFactor = ParamItem{
		Key:          "dataCoord.taskResource.l0CompactionMemoryFactor",
		Version:      "3.0.1",
		DefaultValue: "2",
		Doc:          "memory of an l0 compaction task = total delta log size of its input segments * this factor",
		Export:       true,
	}
	p.TaskResourceL0CompactionMemoryFactor.Init(base.mgr)

	p.TaskResourceAnalyzeMemoryFactor = ParamItem{
		Key:          "dataCoord.taskResource.analyzeMemoryFactor",
		Version:      "3.0.1",
		DefaultValue: "2",
		Doc:          "memory of an analyze task = raw vector data size (rows * dim * element size) * this factor",
		Export:       true,
	}
	p.TaskResourceAnalyzeMemoryFactor.Init(base.mgr)

	p.TaskResourceClusteringCompactionMemory = ParamItem{
		Key:          "dataCoord.taskResource.clusteringCompactionMemory",
		Version:      "3.0.1",
		DefaultValue: "32g",
		Doc:          "memory a clustering compaction task is expected to use",
		Export:       true,
	}
	p.TaskResourceClusteringCompactionMemory.Init(base.mgr)

	p.TaskResourceMinTaskMemory = ParamItem{
		Key:          "dataCoord.taskResource.minTaskMemory",
		Version:      "3.0.1",
		DefaultValue: "64m",
		Doc:          "lower bound of the memory estimate of any task, so that no task is ever placed as if it were free",
		Export:       true,
	}
	p.TaskResourceMinTaskMemory.Init(base.mgr)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd pkg && go test ./util/paramtable/ -run 'TestComponentParam' -v 2>&1 | tail -5`
Expected: PASS

- [ ] **Step 5: Regenerate `configs/milvus.yaml`**

Run: `unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy; go run ./cmd/tools/config gen-yaml && mv milvus.yaml configs/milvus.yaml`
Then `git diff configs/milvus.yaml` must show ONLY a new `taskResource:` block under `dataCoord:` (right after the `slot:` block) with the ten keys and their docs. If the tool needs cgo and fails, hand-write the block instead:
```yaml
  taskResource:
    vectorIndexCPU: 8 # cpu cores a vector index build task is expected to use; used by DataCoord to place tasks across DataNodes
    analyzeCPU: 8 # cpu cores an analyze task is expected to use
    clusteringCompactionCPU: 8 # cpu cores a clustering compaction task is expected to use
    defaultCPU: 1 # cpu cores every other task type (scalar index, stats, mix/l0/sort compaction, import, copy segment) is expected to use
    indexMemoryFactor: 2 # memory of an index build task = indexed field size * this factor
    statsMemoryFactor: 2 # memory of a stats task (text match, bm25, json key index) or a sort compaction = segment size * this factor
    l0CompactionMemoryFactor: 2 # memory of an l0 compaction task = total delta log size of its input segments * this factor
    analyzeMemoryFactor: 2 # memory of an analyze task = raw vector data size (rows * dim * element size) * this factor
    clusteringCompactionMemory: 32g # memory a clustering compaction task is expected to use
    minTaskMemory: 64m # lower bound of the memory estimate of any task, so that no task is ever placed as if it were free
```

- [ ] **Step 6: Commit**

```bash
git add pkg/util/paramtable/component_param.go pkg/util/paramtable/component_param_test.go configs/milvus.yaml
git commit -s -m "enhance: add dataCoord.taskResource config params

CPU constants and memory factors DataCoord uses to price tasks in two
dimensions. Defaults: vector index / analyze / clustering compaction take 8
cores, everything else 1; index memory = 2x field size, stats and sort
compaction = 2x segment size, l0 = 2x delta size, analyze = 2x raw vector
size, clustering = 32GB, floor 64MB.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01V39kgspQN9aawcNuoRgkER"
```

---

### Task 3: pricing helpers and the `Task` interface

**Files:**
- Create: `internal/datacoord/task_resource.go`, `internal/datacoord/task_resource_test.go`
- Modify: `internal/datacoord/task/task.go` (interface), `internal/datacoord/task/global_scheduler_test.go` (4 mock expectations at L285, L370, L485, L521)
- Regenerate: `internal/datacoord/task/mock_task.go`

**Interfaces:**
- Produces on `task.Task`: `GetTaskResource() taskcommon.Resource`.
- Produces in package `datacoord`:
  - `func indexTaskResource(fieldSize int64, isVectorIndex bool) taskcommon.Resource`
  - `func statsTaskResource(segmentSize int64) taskcommon.Resource`
  - `func mixCompactionTaskResource() taskcommon.Resource`
  - `func l0CompactionTaskResource(deltaSize int64) taskcommon.Resource`
  - `func clusteringCompactionTaskResource() taskcommon.Resource`
  - `func analyzeTaskResource(rawDataSize int64) taskcommon.Resource`
  - `func importTaskResource(bufferSize int64) taskcommon.Resource`
  - `func lightweightTaskResource() taskcommon.Resource`
  - `func defaultTaskResource() taskcommon.Resource` (= defaultCPU, minTaskMemory; the "could not price" answer)
  - `func estimateSegmentSize(segment *SegmentInfo, schema *schemapb.CollectionSchema) int64`
  - `func estimateFieldSize(segment *SegmentInfo, schema *schemapb.CollectionSchema, fieldID int64) int64`
  - `type resourceCache struct` with `func (c *resourceCache) get(compute func() (taskcommon.Resource, bool)) taskcommon.Resource` — caches only when `compute` returns `ok=true`.

- [ ] **Step 1: Write the failing tests**

`internal/datacoord/task_resource_test.go`:
```go
package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	testMiB = int64(1) << 20
	testGiB = int64(1) << 30
)

func testResourceSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
			{FieldID: 102, Name: "str", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "64"}}},
		},
	}
}

func TestTaskResource_Formulas(t *testing.T) {
	paramtable.Init()
	defaultCPU := Params.DataCoordCfg.TaskResourceDefaultCPU.GetAsInt64()
	minMem := Params.DataCoordCfg.TaskResourceMinTaskMemory.GetAsSize()

	assert.Equal(t, taskcommon.Resource{CPU: 8, Memory: 2 * testGiB}, indexTaskResource(testGiB, true))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2 * testGiB}, indexTaskResource(testGiB, false))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2 * testGiB}, statsTaskResource(testGiB))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: Params.DataCoordCfg.SegmentMaxSize.GetAsInt64() * testMiB}, mixCompactionTaskResource())
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: 2 * testGiB}, l0CompactionTaskResource(testGiB))
	assert.Equal(t, taskcommon.Resource{CPU: 8, Memory: 32 * testGiB}, clusteringCompactionTaskResource())
	assert.Equal(t, taskcommon.Resource{CPU: 8, Memory: 2 * testGiB}, analyzeTaskResource(testGiB))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: testGiB}, importTaskResource(testGiB))
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: minMem}, lightweightTaskResource())
	assert.Equal(t, taskcommon.Resource{CPU: defaultCPU, Memory: minMem}, defaultTaskResource())

	// Nothing is ever priced below the floor: a 0-byte input still costs minTaskMemory.
	assert.Equal(t, minMem, indexTaskResource(0, true).Memory)
	assert.Equal(t, minMem, statsTaskResource(0).Memory)
	assert.Equal(t, minMem, l0CompactionTaskResource(0).Memory)
	assert.Equal(t, minMem, analyzeTaskResource(0).Memory)
	assert.Equal(t, minMem, importTaskResource(0).Memory)
}

func TestTaskResource_ConfigOverride(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(Params.DataCoordCfg.TaskResourceVectorIndexCPU.Key, "4")
	pt.Save(Params.DataCoordCfg.TaskResourceIndexMemoryFactor.Key, "3")
	defer pt.Reset(Params.DataCoordCfg.TaskResourceVectorIndexCPU.Key)
	defer pt.Reset(Params.DataCoordCfg.TaskResourceIndexMemoryFactor.Key)

	assert.Equal(t, taskcommon.Resource{CPU: 4, Memory: 3 * testGiB}, indexTaskResource(testGiB, true))
}

func TestEstimateSegmentSize(t *testing.T) {
	paramtable.Init()
	schema := testResourceSchema()

	// Stats present (every storage version persists it): use it verbatim.
	withStats := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 1000, StorageVersion: 3,
		Stats: &datapb.Statistics{InsertBinlogSize: 700, StatsBinlogSize: 200, DeltaBinlogSize: 100}}}
	assert.Equal(t, int64(1000), estimateSegmentSize(withStats, schema))

	// V1 without Stats but with binlogs: EnsureStats rebuilds from the arrays.
	fromBinlogs := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 1000,
		Binlogs: []*datapb.FieldBinlog{{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: 512000, EntriesNum: 1000}}}}}}
	assert.Equal(t, int64(512000), estimateSegmentSize(fromBinlogs, schema))

	// External-collection shape: no Stats, no binlogs, rows known -> rows x per-record estimate.
	external := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 1000, ManifestPath: "m"}}
	perRecord, err := typeutilEstimateSizePerRecord(schema)
	assert.NoError(t, err)
	assert.Equal(t, int64(1000)*perRecord, estimateSegmentSize(external, schema))

	// Nothing to go on at all.
	assert.Equal(t, int64(0), estimateSegmentSize(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 4}}, schema))
	assert.Equal(t, int64(0), estimateSegmentSize(external, nil))
	assert.Equal(t, int64(0), estimateSegmentSize(nil, schema))
}

func TestEstimateFieldSize(t *testing.T) {
	paramtable.Init()
	schema := testResourceSchema()

	// Binlog bytes for the field exist: use them.
	v1 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 1, NumOfRows: 1000,
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 101, Binlogs: []*datapb.Binlog{{MemorySize: 512000}}},
			{FieldID: 102, Binlogs: []*datapb.Binlog{{MemorySize: 64000}}},
		}}}
	assert.Equal(t, int64(512000), estimateFieldSize(v1, schema, 101))
	assert.Equal(t, int64(64000), estimateFieldSize(v1, schema, 102))

	// V3 after a DataCoord restart: Binlogs empty, Stats present. Vector field is
	// closed-form rows x dim x 4; scalar field is its share of the segment size.
	// Expected scalar bytes come from the same estimator the code apportions with,
	// so the test does not hard-code the varchar length policy.
	perRecord, err := typeutilEstimateSizePerRecord(schema)
	assert.NoError(t, err)
	strBytes := fieldBytesPerRow(typeutil.GetFieldByID(schema, 102))
	assert.Greater(t, strBytes, int64(0))

	total := int64(1000) * perRecord
	v3 := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 2, NumOfRows: 1000, StorageVersion: 3, ManifestPath: "m",
		Stats: &datapb.Statistics{InsertBinlogSize: total}}}
	assert.Equal(t, int64(1000*128*4), estimateFieldSize(v3, schema, 101))
	assert.Equal(t, total*strBytes/perRecord, estimateFieldSize(v3, schema, 102))

	// External collection: no Stats either -> rows x per-field bytes.
	external := &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 3, NumOfRows: 1000, ManifestPath: "m"}}
	assert.Equal(t, int64(1000*128*4), estimateFieldSize(external, schema, 101))
	assert.Equal(t, int64(1000)*strBytes, estimateFieldSize(external, schema, 102))

	// Unknown field / nil schema: fall back to the whole segment size (conservative).
	assert.Equal(t, total, estimateFieldSize(v3, schema, 999))
	assert.Equal(t, int64(0), estimateFieldSize(external, nil, 101))
}

func TestResourceCache(t *testing.T) {
	var c resourceCache
	calls := 0
	compute := func(ok bool) func() (taskcommon.Resource, bool) {
		return func() (taskcommon.Resource, bool) {
			calls++
			return taskcommon.Resource{CPU: int64(calls), Memory: 1}, ok
		}
	}
	// Not ok: value is returned but not cached, so the next call recomputes.
	assert.Equal(t, int64(1), c.get(compute(false)).CPU)
	assert.Equal(t, int64(2), c.get(compute(false)).CPU)
	// Ok: cached; subsequent calls do not recompute.
	assert.Equal(t, int64(3), c.get(compute(true)).CPU)
	assert.Equal(t, int64(3), c.get(compute(true)).CPU)
	assert.Equal(t, 3, calls)
}
```
(`typeutilEstimateSizePerRecord` is a tiny test-file alias — add at the bottom of the test file: `func typeutilEstimateSizePerRecord(s *schemapb.CollectionSchema) (int64, error) { n, err := typeutil.EstimateSizePerRecord(s); return int64(n), err }` and import `github.com/milvus-io/milvus/pkg/v3/util/typeutil`.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOTEST ./internal/datacoord/ -run 'TestTaskResource|TestEstimateSegmentSize|TestEstimateFieldSize|TestResourceCache'`
Expected: compile failure, `undefined: indexTaskResource` etc.

- [ ] **Step 3: Write `internal/datacoord/task_resource.go`**

```go
// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"sync/atomic"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// This file is the only place DataCoord prices a task in CPU and memory. Every
// task family calls one of the formulas below from GetTaskResource(); the
// worker never recomputes, it books whatever the request carries.
//
// CPU is a request, not a reservation: it only ranks candidate workers.
// Memory is what a worker can refuse a task for. Both are floored so that a
// task whose inputs could not be resolved is still placed as costing
// something, never as free.

func defaultCPU() int64 {
	return max(Params.DataCoordCfg.TaskResourceDefaultCPU.GetAsInt64(), 1)
}

// clampTaskMemory applies the configured floor.
func clampTaskMemory(memory int64) int64 {
	return max(memory, Params.DataCoordCfg.TaskResourceMinTaskMemory.GetAsSize())
}

func scaled(size int64, factor float64) int64 {
	return int64(float64(size) * factor)
}

// defaultTaskResource is the answer when a task cannot resolve its inputs
// (segment dropped between enqueue and dispatch, schema not cached yet).
func defaultTaskResource() taskcommon.Resource {
	return taskcommon.Resource{CPU: defaultCPU(), Memory: clampTaskMemory(0)}
}

func indexTaskResource(fieldSize int64, isVectorIndex bool) taskcommon.Resource {
	cpu := defaultCPU()
	if isVectorIndex {
		cpu = max(Params.DataCoordCfg.TaskResourceVectorIndexCPU.GetAsInt64(), 1)
	}
	return taskcommon.Resource{
		CPU:    cpu,
		Memory: clampTaskMemory(scaled(fieldSize, Params.DataCoordCfg.TaskResourceIndexMemoryFactor.GetAsFloat())),
	}
}

// statsTaskResource prices text-match / bm25 / json-key stats and sort
// compaction alike: all of them read the whole segment.
func statsTaskResource(segmentSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    defaultCPU(),
		Memory: clampTaskMemory(scaled(segmentSize, Params.DataCoordCfg.TaskResourceStatsMemoryFactor.GetAsFloat())),
	}
}

// mixCompactionTaskResource is bounded by the output: a mix (or schema bump)
// compaction writes at most one segment of segment.maxSize.
func mixCompactionTaskResource() taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    defaultCPU(),
		Memory: clampTaskMemory(Params.DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024),
	}
}

func l0CompactionTaskResource(deltaSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    defaultCPU(),
		Memory: clampTaskMemory(scaled(deltaSize, Params.DataCoordCfg.TaskResourceL0CompactionMemoryFactor.GetAsFloat())),
	}
}

func clusteringCompactionTaskResource() taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    max(Params.DataCoordCfg.TaskResourceClusteringCompactionCPU.GetAsInt64(), 1),
		Memory: clampTaskMemory(Params.DataCoordCfg.TaskResourceClusteringCompactionMemory.GetAsSize()),
	}
}

func analyzeTaskResource(rawDataSize int64) taskcommon.Resource {
	return taskcommon.Resource{
		CPU:    max(Params.DataCoordCfg.TaskResourceAnalyzeCPU.GetAsInt64(), 1),
		Memory: clampTaskMemory(scaled(rawDataSize, Params.DataCoordCfg.TaskResourceAnalyzeMemoryFactor.GetAsFloat())),
	}
}

// importTaskResource prices an import by its write buffer, which is what the
// worker actually holds in memory (see CalculateTaskBufferSize).
func importTaskResource(bufferSize int64) taskcommon.Resource {
	return taskcommon.Resource{CPU: defaultCPU(), Memory: clampTaskMemory(bufferSize)}
}

// lightweightTaskResource prices copy-segment and external-refresh tasks,
// which stream data and hold little of it.
func lightweightTaskResource() taskcommon.Resource {
	return defaultTaskResource()
}

// estimateSegmentSize is getSegmentSize with a fallback for segments whose
// Stats were never persisted (external-collection segments): rows times the
// schema's per-record estimate.
func estimateSegmentSize(segment *SegmentInfo, schema *schemapb.CollectionSchema) int64 {
	if segment == nil || segment.SegmentInfo == nil {
		return 0
	}
	if size := segment.getSegmentSize(); size > 0 {
		return size
	}
	if schema == nil || segment.GetNumOfRows() <= 0 {
		return 0
	}
	perRecord, err := typeutil.EstimateSizePerRecord(schema)
	if err != nil {
		mlog.Warn(context.TODO(), "estimate segment size from schema failed",
			mlog.FieldSegmentID(segment.GetID()), mlog.Err(err))
		return 0
	}
	size := segment.GetNumOfRows() * int64(perRecord)
	mlog.Warn(context.TODO(), "segment has no size statistics, estimated from schema",
		mlog.FieldSegmentID(segment.GetID()), mlog.Int64("rows", segment.GetNumOfRows()), mlog.Int64("estimatedSize", size))
	return size
}

// estimateFieldSize returns the bytes of one field in a segment.
//
// The per-field binlog arrays are authoritative when present, but V3 segments
// do not persist them (kv_catalog: paths live in the LOON manifest), so after
// a DataCoord restart they are empty. Then: a vector field is rows x dim x
// element size, exact on every storage version; a scalar field is its share
// of the segment size, apportioned by the schema's per-record estimate.
func estimateFieldSize(segment *SegmentInfo, schema *schemapb.CollectionSchema, fieldID int64) int64 {
	if segment == nil || segment.SegmentInfo == nil {
		return 0
	}
	if size := rawFieldBinlogSize(segment, fieldID); size > 0 {
		return size
	}
	field := typeutil.GetFieldByID(schema, fieldID)
	if field == nil {
		// Unknown field: be conservative and charge the whole segment.
		return estimateSegmentSize(segment, schema)
	}
	rows := segment.GetNumOfRows()
	if typeutil.IsVectorType(field.GetDataType()) {
		if size := vectorFieldBytes(field, rows); size > 0 {
			mlog.Warn(context.TODO(), "vector field has no binlog size, estimated from dim and rows",
				mlog.FieldSegmentID(segment.GetID()), mlog.FieldFieldID(fieldID), mlog.Int64("estimatedSize", size))
			return size
		}
	}
	fieldBytes := fieldBytesPerRow(field)
	perRecord, err := typeutil.EstimateSizePerRecord(schema)
	if err != nil || perRecord <= 0 || fieldBytes <= 0 {
		return estimateSegmentSize(segment, schema)
	}
	var size int64
	if segmentSize := segment.getSegmentSize(); segmentSize > 0 {
		size = segmentSize * fieldBytes / int64(perRecord)
	} else {
		size = rows * fieldBytes
	}
	mlog.Warn(context.TODO(), "field has no binlog size, estimated from schema",
		mlog.FieldSegmentID(segment.GetID()), mlog.FieldFieldID(fieldID), mlog.Int64("estimatedSize", size))
	return size
}

// rawFieldBinlogSize is getFieldBinlogSize WITHOUT its whole-segment fallback,
// so the caller can tell "no binlog bytes" from "small field".
func rawFieldBinlogSize(segment *SegmentInfo, fieldID int64) int64 {
	var size int64
	for _, binlogs := range segment.GetBinlogs() {
		match := binlogs.GetFieldID() == fieldID
		if !match {
			for _, child := range binlogs.GetChildFields() {
				if child == fieldID {
					match = true
					break
				}
			}
		}
		if !match {
			continue
		}
		for _, l := range binlogs.GetBinlogs() {
			size += l.GetMemorySize()
		}
	}
	return size
}

func vectorFieldBytes(field *schemapb.FieldSchema, rows int64) int64 {
	dim, err := typeutil.GetDim(field)
	if err != nil || dim <= 0 {
		return 0
	}
	return int64(float64(rows) * float64(dim) * typeutil.VectorTypeSize(field.GetDataType()))
}

// fieldBytesPerRow reuses EstimateSizePerRecord on a one-field schema so the
// scalar apportioning uses exactly the estimator the rest of DataCoord uses.
func fieldBytesPerRow(field *schemapb.FieldSchema) int64 {
	n, err := typeutil.EstimateSizePerRecord(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}})
	if err != nil {
		return 0
	}
	return int64(n)
}

// resourceCache memoizes a task's requirement so what the scheduler placed and
// what the request ships are the same number, and so the meta walk runs once
// per task rather than once per scheduling round. A computation that could
// not resolve its inputs returns ok=false and is NOT cached, so the next round
// retries instead of freezing a placeholder.
type resourceCache struct {
	value atomic.Pointer[taskcommon.Resource]
}

func (c *resourceCache) get(compute func() (taskcommon.Resource, bool)) taskcommon.Resource {
	if v := c.value.Load(); v != nil {
		return *v
	}
	res, ok := compute()
	if ok {
		c.value.Store(&res)
	}
	return res
}
```
Check `typeutil.GetDim` exists with signature `func GetDim(field *schemapb.FieldSchema) (int64, error)` (`grep -n 'func GetDim(' pkg/util/typeutil/schema.go`); if it does not, use `storage.GetDimFromParams(field.GetTypeParams())` (returns `(int, error)`) from `internal/storage`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOTEST ./internal/datacoord/ -run 'TestTaskResource|TestEstimateSegmentSize|TestEstimateFieldSize|TestResourceCache' -v 2>&1 | tail -20`
Expected: PASS. If `TestEstimateFieldSize` scalar apportioning differs by rounding, adjust the expected value to the integer division the code performs, not the other way round.

- [ ] **Step 5: Extend the `Task` interface and regenerate the mock**

`internal/datacoord/task/task.go` — after `GetTaskSlot() int64` add:
```go
	// GetTaskResource is the coordinator-side cpu/memory estimate the scheduler
	// places on and the request ships. Never zero: a family that cannot price
	// itself returns the configured floor.
	GetTaskResource() taskcommon.Resource
```
Regenerate: `unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy; /home/zc/work/milvus/bin/mockery --config internal/datacoord/.mockery.yaml`
Verify: `git status --short internal/ | grep mock_` lists only `internal/datacoord/task/mock_task.go` (other mocks unchanged). If other mocks changed, `git checkout` them — only `mock_task.go` belongs to this task.

- [ ] **Step 6: Make the existing scheduler tests expect the new call**

In `internal/datacoord/task/global_scheduler_test.go`, after each of the four `task.EXPECT().GetTaskSlot()...` lines (L285, L370, L485, L521) add:
```go
		task.EXPECT().GetTaskResource().Return(taskcommon.Resource{}).Maybe()
```
(the scheduler will call it from Task 6 onward; adding it now keeps the mock strict and the tests stable).

- [ ] **Step 7: Build everything that implements `task.Task`**

Run: `go build -tags dynamic,test ./internal/datacoord/... 2>&1 | head`
Expected: errors of the form `*indexBuildTask does not implement task.Task (missing method GetTaskResource)` for the 11 families. That is the to-do list for Tasks 4–5. Do not commit yet.

---

### Task 4: compaction families implement and ship the estimate

**Files:**
- Modify: `internal/datacoord/compaction_task_mix.go` (struct L28-39, `GetTaskSlot` L53, `BuildCompactionRequest` L346-370), `compaction_task_l0.go` (struct near L40-50, `BuildCompactionRequest` L341-357), `compaction_task_clustering.go` (struct, `BuildCompactionRequest` L349-370), `compaction_task_bump_schema_version.go` (struct L42-48, `BuildCompactionRequest` L102-115)
- Test: `internal/datacoord/compaction_task_resource_test.go` (new)

**Interfaces:**
- Consumes: Task 3 helpers, `CompactionMeta.GetHealthySegment(ctx, segID) *SegmentInfo`.
- Produces: `GetTaskResource()` on the four compaction task types; `CompactionPlan.Cpu/Memory` set in every `BuildCompactionRequest`.

- [ ] **Step 1: Write the failing tests**

`internal/datacoord/compaction_task_resource_test.go`:
```go
package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestCompactionTaskResource_Mix(t *testing.T) {
	paramtable.Init()
	task := newMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_MixCompaction, InputSegments: []int64{10}},
		nil, NewMockCompactionMeta(t), newMockVersionManager())
	assert.Equal(t, mixCompactionTaskResource(), task.GetTaskResource())
}

func TestCompactionTaskResource_Sort(t *testing.T) {
	paramtable.Init()
	meta := NewMockCompactionMeta(t)
	calls := 0
	meta.EXPECT().GetHealthySegment(mock.Anything, int64(10)).RunAndReturn(func(ctx context.Context, id int64) *SegmentInfo {
		calls++
		return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 10, NumOfRows: 100,
			Stats: &datapb.Statistics{InsertBinlogSize: 3 * testGiB}}}
	})
	task := newMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_SortCompaction, InputSegments: []int64{10}},
		nil, meta, newMockVersionManager())
	assert.Equal(t, statsTaskResource(3*testGiB), task.GetTaskResource())
	// Cached: the second call does not walk meta again.
	assert.Equal(t, statsTaskResource(3*testGiB), task.GetTaskResource())
	assert.Equal(t, 1, calls)
}

func TestCompactionTaskResource_SortSegmentMissing(t *testing.T) {
	paramtable.Init()
	meta := NewMockCompactionMeta(t)
	meta.EXPECT().GetHealthySegment(mock.Anything, int64(10)).Return(nil).Twice()
	task := newMixCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_SortCompaction, InputSegments: []int64{10}},
		nil, meta, newMockVersionManager())
	// Not resolvable: floor, and NOT cached so the next round retries.
	assert.Equal(t, defaultTaskResource(), task.GetTaskResource())
	assert.Equal(t, defaultTaskResource(), task.GetTaskResource())
}

func TestCompactionTaskResource_L0(t *testing.T) {
	paramtable.Init()
	meta := NewMockCompactionMeta(t)
	meta.EXPECT().GetHealthySegment(mock.Anything, int64(10)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 10,
		Stats: &datapb.Statistics{DeltaBinlogSize: 300 * testMiB}}}).Once()
	meta.EXPECT().GetHealthySegment(mock.Anything, int64(11)).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 11,
		Stats: &datapb.Statistics{DeltaBinlogSize: 200 * testMiB}}}).Once()
	task := newL0CompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_Level0DeleteCompaction, InputSegments: []int64{10, 11}}, nil, meta)
	assert.Equal(t, l0CompactionTaskResource(500*testMiB), task.GetTaskResource())
	assert.Equal(t, l0CompactionTaskResource(500*testMiB), task.GetTaskResource()) // cached
}

func TestCompactionTaskResource_L0SegmentMissing(t *testing.T) {
	paramtable.Init()
	meta := NewMockCompactionMeta(t)
	meta.EXPECT().GetHealthySegment(mock.Anything, int64(10)).Return(nil)
	task := newL0CompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_Level0DeleteCompaction, InputSegments: []int64{10}}, nil, meta)
	assert.Equal(t, defaultTaskResource(), task.GetTaskResource())
}

func TestCompactionTaskResource_ClusteringAndBump(t *testing.T) {
	paramtable.Init()
	clustering := newClusteringCompactionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_ClusteringCompaction},
		nil, NewMockCompactionMeta(t), nil, nil, newMockVersionManager())
	assert.Equal(t, clusteringCompactionTaskResource(), clustering.GetTaskResource())

	bump := newBumpSchemaVersionTask(&datapb.CompactionTask{PlanID: 1, Type: datapb.CompactionType_BumpSchemaVersionCompaction},
		nil, NewMockCompactionMeta(t), newMockVersionManager())
	assert.Equal(t, mixCompactionTaskResource(), bump.GetTaskResource())
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 1024 * testMiB}, bump.GetTaskResource())
}
```
Note: `newClusteringCompactionTask` / `newMixCompactionTask` may dereference `ievm` or `meta` in the constructor — if the constructor panics with nil, pass `NewMockCompactionMeta(t)` / `newMockVersionManager()` where shown; if `newMockVersionManager` does not exist in the test package, `grep -rn 'func newMockVersionManager' internal/datacoord/*_test.go` and use the helper that is there.

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOTEST ./internal/datacoord/ -run 'TestCompactionTaskResource' -v 2>&1 | tail -5`
Expected: compile error (`GetTaskResource` undefined) — package still does not build from Task 3 Step 7.

- [ ] **Step 3: Implement mix / sort**

`compaction_task_mix.go` — add to the struct after `slotUsage atomic.Int64`:
```go
	resource resourceCache
```
Add after `GetTaskSlot`:
```go
// GetTaskResource prices a sort compaction by the segment it sorts (it reads
// and rewrites all of it) and a mix compaction by its output bound.
func (t *mixCompactionTask) GetTaskResource() taskcommon.Resource {
	return t.resource.get(func() (taskcommon.Resource, bool) {
		taskProto := t.GetTaskProto()
		if taskProto.GetType() != datapb.CompactionType_SortCompaction {
			return mixCompactionTaskResource(), true
		}
		inputs := taskProto.GetInputSegments()
		if len(inputs) == 0 {
			return defaultTaskResource(), false
		}
		segment := t.meta.GetHealthySegment(context.TODO(), inputs[0])
		if segment == nil {
			return defaultTaskResource(), false
		}
		size := estimateSegmentSize(segment, taskProto.GetSchema())
		if size <= 0 {
			return defaultTaskResource(), false
		}
		return statsTaskResource(size), true
	})
}
```
In `BuildCompactionRequest`, after `SlotUsage: t.GetSlotUsage(),` insert:
```go
		Cpu:                       t.GetTaskResource().CPU,
		Memory:                    t.GetTaskResource().Memory,
```
(run `gofmt -w` afterwards; alignment is cosmetic.)

- [ ] **Step 4: Implement L0**

`compaction_task_l0.go` — add `resource resourceCache` to the `l0CompactionTask` struct. After `GetTaskSlot`:
```go
// GetTaskResource prices an L0 compaction by the delete records it must hold
// in memory: the delta logs of every input L0 segment.
func (t *l0CompactionTask) GetTaskResource() taskcommon.Resource {
	return t.resource.get(func() (taskcommon.Resource, bool) {
		var deltaSize int64
		for _, segID := range t.GetTaskProto().GetInputSegments() {
			segment := t.meta.GetHealthySegment(context.TODO(), segID)
			if segment == nil {
				return defaultTaskResource(), false
			}
			deltaSize += segment.EnsureStats().GetDeltaBinlogSize()
		}
		return l0CompactionTaskResource(deltaSize), true
	})
}
```
In `BuildCompactionRequest`, after `SlotUsage:     t.GetSlotUsage(),` insert `Cpu: t.GetTaskResource().CPU,` and `Memory: t.GetTaskResource().Memory,`.

- [ ] **Step 5: Implement clustering and schema bump**

`compaction_task_clustering.go` — after `GetTaskSlot`:
```go
func (t *clusteringCompactionTask) GetTaskResource() taskcommon.Resource {
	return clusteringCompactionTaskResource()
}
```
In its `BuildCompactionRequest` after `SlotUsage: t.GetSlotUsage(),` insert the `Cpu`/`Memory` pair.

`compaction_task_bump_schema_version.go` — after `GetTaskSlot`:
```go
// GetTaskResource: a schema bump rewrites one segment, so it is bounded like a mix compaction.
func (t *bumpSchemaVersionTask) GetTaskResource() taskcommon.Resource {
	return mixCompactionTaskResource()
}
```
In its `BuildCompactionRequest` after `SlotUsage: t.GetSlotUsage(),` insert the `Cpu`/`Memory` pair.

- [ ] **Step 6: Build the package (still expected to fail on non-compaction families) and run the compaction tests**

Run: `go build -tags dynamic,test ./internal/datacoord/ 2>&1 | grep -c 'missing method GetTaskResource'`
Expected: 7 (index, stats, analyze, import, preimport, copy, refresh). Tests cannot run until Task 5 completes; continue.

---

### Task 5: index / stats / analyze / import / copy / refresh implement and ship

**Files:**
- Modify: `internal/datacoord/task_index.go` (struct L44-55, after `GetTaskSlot` L106, request L577), `task_stats.go` (struct L40-50, after `GetTaskSlot` L91, request L493), `task_analyze.go` (struct L40-48, after `GetTaskSlot` L84, request L233), `import_util.go` (L836 `CalculateTaskSlot`, L298 and L370 request assembly), `import_task_import.go` (after `GetTaskSlot` L128), `import_task_preimport.go` (after `GetTaskSlot` L91), `copy_segment_task.go` (after `GetTaskSlot` L299, request L811), `task_refresh_external_collection.go` (after `GetTaskSlot` L91)
- Test: `internal/datacoord/task_resource_families_test.go` (new), extend `import_util_test.go`

**Interfaces:**
- Consumes: Task 3 helpers; `meta.GetHealthySegment`, `meta.GetCollection(id) *collectionInfo` (`.Schema`), `meta.indexMeta.GetIndexParams(collID, indexID)`, `meta.indexMeta.GetFieldIDByIndexID(collID, indexID)`, `vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType)`.
- Produces: `func CalculateTaskBufferSize(task ImportTask, job ImportJob) int64` (extracted from `CalculateTaskSlot`, which now calls it); `GetTaskResource()` on the seven families.

- [ ] **Step 1: Write the failing tests**

`internal/datacoord/task_resource_families_test.go`:
```go
package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// familyMeta builds a meta with one collection (schema from testResourceSchema),
// one segment (V3 shape: no binlogs, Stats present) and one HNSW index on the
// vector field, which is the shape PR #52561's review found unpriced.
func familyMeta(t *testing.T) *meta {
	const collID, partID, segID, indexID, fieldID, buildID = int64(1), int64(2), int64(3), int64(4), int64(101), int64(5)
	catalog := mocks.NewDataCoordCatalog(t)
	catalog.EXPECT().AlterSegmentIndexes(mock.Anything, mock.Anything).Return(nil).Maybe()
	im := createIndexMetaWithSegment(catalog, collID, partID, segID, indexID, fieldID, buildID)
	mt := &meta{
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		segments: &SegmentsInfo{segments: map[int64]*SegmentInfo{
			segID: {SegmentInfo: &datapb.SegmentInfo{ID: segID, CollectionID: collID, PartitionID: partID,
				NumOfRows: 1000, State: commonpb.SegmentState_Flushed, StorageVersion: 3, ManifestPath: "m",
				Stats: &datapb.Statistics{InsertBinlogSize: 1000 * (8 + 512 + 64)}}},
		}},
		indexMeta: im,
	}
	mt.collections.Insert(collID, &collectionInfo{ID: collID, Schema: testResourceSchema()})
	return mt
}

func TestTaskResource_Index(t *testing.T) {
	paramtable.Init()
	mt := familyMeta(t)
	segIndex := &model.SegmentIndex{CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 4, BuildID: 5, NumRows: 1000}
	it := newIndexBuildTask(segIndex, 1, mt, nil, nil, nil)
	// HNSW on a 128-dim float vector over 1000 rows with no binlogs: closed form.
	assert.Equal(t, indexTaskResource(1000*128*4, true), it.GetTaskResource())

	// Segment gone: floor, not cached.
	orphan := newIndexBuildTask(&model.SegmentIndex{CollectionID: 1, SegmentID: 999, IndexID: 4, BuildID: 6}, 1, mt, nil, nil, nil)
	assert.Equal(t, defaultTaskResource(), orphan.GetTaskResource())
}

func TestTaskResource_Stats(t *testing.T) {
	paramtable.Init()
	mt := familyMeta(t)
	st := newStatsTask(&indexpb.StatsTask{CollectionID: 1, SegmentID: 3, TaskID: 7, SubJobType: indexpb.StatsSubJob_TextIndexJob}, 1, mt, nil, nil, nil)
	assert.Equal(t, statsTaskResource(1000*(8+512+64)), st.GetTaskResource())

	orphan := newStatsTask(&indexpb.StatsTask{CollectionID: 1, SegmentID: 999, TaskID: 8}, 1, mt, nil, nil, nil)
	assert.Equal(t, defaultTaskResource(), orphan.GetTaskResource())
}

func TestTaskResource_Analyze(t *testing.T) {
	paramtable.Init()
	mt := familyMeta(t)
	at := newAnalyzeTask(&indexpb.AnalyzeTask{CollectionID: 1, TaskID: 9, FieldID: 101, FieldType: schemapb.DataType_FloatVector, SegmentIDs: []int64{3}}, mt)
	assert.Equal(t, analyzeTaskResource(1000*128*4), at.GetTaskResource())

	missing := newAnalyzeTask(&indexpb.AnalyzeTask{CollectionID: 1, TaskID: 10, FieldID: 101, FieldType: schemapb.DataType_FloatVector, SegmentIDs: []int64{999}}, mt)
	assert.Equal(t, defaultTaskResource(), missing.GetTaskResource())
}
```
Append to `internal/datacoord/import_util_test.go`:
```go
func TestCalculateTaskBufferSize(t *testing.T) {
	paramtable.Init()
	base := paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, Vchannels: []string{"a", "b"}, PartitionIDs: []int64{1, 2, 3}}}

	pre := &preImportTask{PreImportTask: &datapb.PreImportTask{JobID: 1, TaskID: 2}}
	assert.Equal(t, base, CalculateTaskBufferSize(pre, job))

	imp := &importTask{ImportTaskV2: &datapb.ImportTaskV2{JobID: 1, TaskID: 3}}
	assert.Equal(t, base*2*3, CalculateTaskBufferSize(imp, job))

	l0Job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1, Vchannels: []string{"a"}, PartitionIDs: []int64{1},
		Options: []*commonpb.KeyValuePair{{Key: "l0_import", Value: "true"}}}}
	assert.Equal(t, paramtable.Get().DataNodeCfg.ImportDeleteBufferSize.GetAsInt64(), CalculateTaskBufferSize(imp, l0Job))

	assert.Equal(t, importTaskResource(base*2*3), taskcommon.Resource{CPU: 1, Memory: max(base*2*3, 64<<20)})
}
```
(Check the exact struct names `preImportTask`/`importTask` and their embedded proto types with `grep -n '^type preImportTask struct\|^type importTask struct' -A4 internal/datacoord/import_task*.go`, and the L0 option key with `grep -rn 'IsL0Import' internal/util/importutilv2/option.go`. Adjust the fixture to whatever those are; the assertions stay.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOTEST ./internal/datacoord/ -run 'TestTaskResource_|TestCalculateTaskBufferSize' -v 2>&1 | tail -5`
Expected: compile error.

- [ ] **Step 3: Implement index**

`task_index.go` — add `resource resourceCache` to `indexBuildTask` struct. After `GetTaskSlot`:
```go
// GetTaskResource prices the build by the bytes of the indexed field. It walks
// meta once and caches; a segment or schema that is not there yet is priced at
// the floor and retried next round.
func (it *indexBuildTask) GetTaskResource() taskcommon.Resource {
	return it.resource.get(func() (taskcommon.Resource, bool) {
		segment := it.meta.GetHealthySegment(context.TODO(), it.SegmentID)
		if segment == nil {
			return defaultTaskResource(), false
		}
		var schema *schemapb.CollectionSchema
		if coll := it.meta.GetCollection(it.CollectionID); coll != nil {
			schema = coll.Schema
		}
		indexType := GetIndexType(it.meta.indexMeta.GetIndexParams(it.CollectionID, it.IndexID))
		isVectorIndex := vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType)
		fieldID := it.meta.indexMeta.GetFieldIDByIndexID(it.CollectionID, it.IndexID)
		fieldSize := estimateFieldSize(segment, schema, fieldID)
		if fieldSize <= 0 {
			return defaultTaskResource(), false
		}
		return indexTaskResource(fieldSize, isVectorIndex), true
	})
}
```
Add imports `github.com/milvus-io/milvus/internal/util/vecindexmgr` and `schemapb` if missing. In `CreateTaskOnWorker`'s `workerpb.CreateJobRequest{...}` after `TaskSlot: it.taskSlot,` add `Cpu: it.GetTaskResource().CPU,` and `Memory: it.GetTaskResource().Memory,`.

- [ ] **Step 4: Implement stats**

`task_stats.go` — add `resource resourceCache` to `statsTask`. After `GetTaskSlot`:
```go
func (st *statsTask) GetTaskResource() taskcommon.Resource {
	return st.resource.get(func() (taskcommon.Resource, bool) {
		segment := st.meta.GetHealthySegment(context.TODO(), st.GetSegmentID())
		if segment == nil {
			return defaultTaskResource(), false
		}
		var schema *schemapb.CollectionSchema
		if coll := st.meta.GetCollection(segment.GetCollectionID()); coll != nil {
			schema = coll.Schema
		}
		size := estimateSegmentSize(segment, schema)
		if size <= 0 {
			return defaultTaskResource(), false
		}
		return statsTaskResource(size), true
	})
}
```
In the `workerpb.CreateStatsRequest{...}` after `TaskSlot: st.taskSlot,` add the `Cpu`/`Memory` pair.

- [ ] **Step 5: Implement analyze**

`task_analyze.go` — add `resource resourceCache` to `analyzeTask`. After `GetTaskSlot`:
```go
// GetTaskResource prices the analyze by the raw vectors it trains on:
// rows x dim x element size across every input segment.
func (at *analyzeTask) GetTaskResource() taskcommon.Resource {
	return at.resource.get(func() (taskcommon.Resource, bool) {
		field := typeutil.GetFieldByID(at.schema, at.GetFieldID())
		if field == nil {
			return defaultTaskResource(), false
		}
		var rows int64
		for _, segID := range at.GetSegmentIDs() {
			segment := at.meta.GetHealthySegment(context.TODO(), segID)
			if segment == nil {
				return defaultTaskResource(), false
			}
			rows += segment.GetNumOfRows()
		}
		raw := vectorFieldBytes(field, rows)
		if raw <= 0 {
			return defaultTaskResource(), false
		}
		return analyzeTaskResource(raw), true
	})
}
```
In `CreateTaskOnWorker`, next to `req.TaskSlot = ...` add:
```go
	req.Cpu = at.GetTaskResource().CPU
	req.Memory = at.GetTaskResource().Memory
```

- [ ] **Step 6: Implement import / preimport**

`import_util.go` — replace the body of `CalculateTaskSlot` so the buffer arithmetic lives in one place:
```go
// CalculateTaskBufferSize is the write buffer an import task holds in memory:
// the base buffer per (vchannel, partition) pair for a full import, one base
// buffer for a pre-import, and the delete buffer for an L0 import.
func CalculateTaskBufferSize(task ImportTask, job ImportJob) int64 {
	baseBufferSize := paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	var taskBufferSize int64
	if task.GetType() == ImportTaskType {
		taskBufferSize = baseBufferSize * int64(len(job.GetVchannels())) * int64(len(job.GetPartitionIDs()))
	} else {
		taskBufferSize = baseBufferSize
	}
	if importutilv2.IsL0Import(job.GetOptions()) {
		taskBufferSize = paramtable.Get().DataNodeCfg.ImportDeleteBufferSize.GetAsInt64()
	}
	return taskBufferSize
}

// CalculateTaskSlot ... (keep the existing doc comment)
func CalculateTaskSlot(task ImportTask, importMeta ImportMeta) int {
	job := importMeta.GetJob(context.TODO(), task.GetJobID())

	fileNumPerSlot := paramtable.Get().DataCoordCfg.ImportFileNumPerSlot.GetAsInt()
	cpuBasedSlots := len(task.GetFileStats()) / fileNumPerSlot
	if cpuBasedSlots < 1 {
		cpuBasedSlots = 1
	}

	memoryLimitPerSlot := paramtable.Get().DataCoordCfg.ImportMemoryLimitPerSlot.GetAsInt()
	memoryBasedSlots := int(CalculateTaskBufferSize(task, job)) / memoryLimitPerSlot

	if cpuBasedSlots > memoryBasedSlots {
		return cpuBasedSlots
	}
	return memoryBasedSlots
}
```
`import_task_import.go` after `GetTaskSlot`:
```go
func (t *importTask) GetTaskResource() taskcommon.Resource {
	job := t.importMeta.GetJob(context.TODO(), t.GetJobID())
	if job == nil {
		return defaultTaskResource()
	}
	return importTaskResource(CalculateTaskBufferSize(t, job))
}
```
`import_task_preimport.go` after `GetTaskSlot`: identical body on `(p *preImportTask)` using `p.importMeta` / `p.GetJobID()` / `CalculateTaskBufferSize(p, job)`.
(Import is deliberately not cached: its job's file stats fill in as pre-import completes.)
`import_util.go` — in `AssemblePreImportRequest`'s `&datapb.PreImportRequest{...}` after `TaskSlot: task.GetTaskSlot(),` add `Cpu: task.GetTaskResource().CPU,` / `Memory: task.GetTaskResource().Memory,`; same in `AssembleImportRequest`'s `&datapb.ImportRequest{...}`.

- [ ] **Step 7: Implement copy segment and refresh external**

`copy_segment_task.go` after `GetTaskSlot`:
```go
// GetTaskResource: copy tasks stream objects and hold little of them.
func (t *copySegmentTask) GetTaskResource() taskcommon.Resource {
	return lightweightTaskResource()
}
```
In the `&datapb.CopySegmentRequest{...}` (L811) after `TaskSlot: task.GetTaskSlot(),` add the `Cpu`/`Memory` pair using `task.GetTaskResource()`.

`task_refresh_external_collection.go` after `GetTaskSlot`:
```go
func (t *refreshExternalCollectionTask) GetTaskResource() taskcommon.Resource {
	return lightweightTaskResource()
}
```
(No request field: the DataNode does not book this task today.)

- [ ] **Step 8: Build and run the whole datacoord package**

Run: `go build -tags dynamic,test ./internal/datacoord/... && GOTEST ./internal/datacoord/ ./internal/datacoord/task/ 2>&1 | tail -5`
Expected: build clean; both packages `ok`. Then coverage of the new files:
`GOTEST ./internal/datacoord/ -run 'TestTaskResource|TestEstimate|TestResourceCache|TestCompactionTaskResource|TestCalculateTaskBufferSize' -coverprofile=/tmp/claude-1000/-home-zc-work-milvus/a8e23eec-da58-49e1-9340-a33b42be6487/scratchpad/cov.out && go tool cover -func=/tmp/claude-1000/-home-zc-work-milvus/a8e23eec-da58-49e1-9340-a33b42be6487/scratchpad/cov.out | grep -E 'task_resource.go|GetTaskResource|CalculateTaskBufferSize'`
Expected: every listed function ≥ 90%. Add cases for any branch under that.

- [ ] **Step 9: Commit (Tasks 3–5 together — one behavior: every task is priced before dispatch)**

```bash
git add internal/datacoord/task_resource.go internal/datacoord/task_resource_test.go internal/datacoord/compaction_task_resource_test.go internal/datacoord/task_resource_families_test.go internal/datacoord/task/task.go internal/datacoord/task/mock_task.go internal/datacoord/task/global_scheduler_test.go internal/datacoord/compaction_task_mix.go internal/datacoord/compaction_task_l0.go internal/datacoord/compaction_task_clustering.go internal/datacoord/compaction_task_bump_schema_version.go internal/datacoord/task_index.go internal/datacoord/task_stats.go internal/datacoord/task_analyze.go internal/datacoord/import_util.go internal/datacoord/import_util_test.go internal/datacoord/import_task_import.go internal/datacoord/import_task_preimport.go internal/datacoord/copy_segment_task.go internal/datacoord/task_refresh_external_collection.go
git commit -s -m "enhance: estimate cpu/memory for every DataCoord task type

Every task family implements GetTaskResource() from the formulas in
task_resource.go and ships the same cached value on its request. Field and
segment sizes fall back to closed-form (vector) or schema-apportioned
(scalar) estimates when the per-field binlogs are absent, which is the V3
shape after a DataCoord restart. The scalar GetTaskSlot path is untouched.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01V39kgspQN9aawcNuoRgkER"
```

---

### Task 6: DataNode books what it accepts and reports it

**Files:**
- Modify: `internal/datanode/compactor/compactor.go` (interface L26-36), `executor.go` (struct L53-67, `Enqueue` L105-130, `Slots` L133-137, `completeTask` L140-170), `mix_compactor.go` L628, `sort_compaction.go` L547, `l0_compactor.go` L552, `clustering_compactor.go` L1029, `bump_schema_version_compactor.go` L1417; `internal/datanode/index/task.go` L38-50, `scheduler.go` L55-62 + L138-165, `task_index.go` L156, `task_stats.go` L142, `task_analyze.go` L77; `internal/datanode/importv2/task.go` L204, `scheduler.go` L32-36 + L109-115, `task_import.go` L107, `task_preimport.go` L107, `task_l0_import.go` L101, `task_l0_preimport.go` L98, `task_copy_segment.go` L45 + L117 + L174 + L208; `internal/datanode/services.go` L735-772; `pkg/metrics/datanode_metrics.go` L356-363 + L472
- Regenerate: `internal/datanode/compactor/mock_compactor.go`, `internal/datanode/importv2/mock_task.go`
- Tests: `compactor/executor_test.go`, `index/scheduler_test.go` (fakeTask L113), `importv2/scheduler_test.go`, `datanode/services_test.go` L514

**Interfaces:**
- Produces: `compactor.Compactor.GetResource() taskcommon.Resource`; `compactor.Executor.Resource() taskcommon.Resource`; `index.Task.GetResource()`; `(*IndexTaskQueue).GetUsingResource() taskcommon.Resource`; `importv2.Task.GetResource()`; `importv2.Scheduler.Resource()`; `metrics.DataNodeTaskResource` gauge `{nodeID, type ∈ cpu|memory, state ∈ total|available}`.
- `QuerySlotResponse.TotalCpu/AvailableCpu/TotalMemory/AvailableMemory` populated.

- [ ] **Step 1: Write the failing tests**

Append to `internal/datanode/compactor/executor_test.go` inside `TestCompactionExecutor`:
```go
	t.Run("Test_Resource_Bookkeeping", func(t *testing.T) {
		ex := NewExecutor()
		mockC := NewMockCompactor(t)
		mockC.EXPECT().GetPlanID().Return(int64(1))
		mockC.EXPECT().GetSlotUsage().Return(int64(8))
		mockC.EXPECT().GetResource().Return(taskcommon.Resource{CPU: 2, Memory: 1 << 30}).Once()
		mockC.EXPECT().Complete().Return()
		mockC.EXPECT().GetStorageConfig().Return(nil).Maybe()

		_, err := ex.Enqueue(mockC)
		assert.NoError(t, err)
		assert.Equal(t, taskcommon.Resource{CPU: 2, Memory: 1 << 30}, ex.Resource())

		// Completion releases exactly what enqueue booked, without asking the task again.
		ex.completeTask(1, &datapb.CompactionPlanResult{PlanID: 1})
		assert.Equal(t, taskcommon.Resource{}, ex.Resource())
	})
```
Every existing subtest in that file that calls `ex.Enqueue(mockC)` needs `mockC.EXPECT().GetResource().Return(taskcommon.Resource{}).Maybe()` next to its `GetSlotUsage` expectation; do that with:
`sed -i 's/^\(\s*\)\(mock[A-Za-z0-9_]*\)\.EXPECT()\.GetSlotUsage()\(.*\)$/&\n\1\2.EXPECT().GetResource().Return(taskcommon.Resource{}).Maybe()/' internal/datanode/compactor/executor_test.go`
then add the `taskcommon` import. Also check other test files in the package that construct `NewMockCompactor` and call `Enqueue` (`grep -ln 'Enqueue(' internal/datanode/compactor/*_test.go`) and apply the same sed to them.

`internal/datanode/index/scheduler_test.go` — add to `fakeTask`:
```go
func (t *fakeTask) GetResource() taskcommon.Resource {
	return taskcommon.Resource{CPU: 1, Memory: 100}
}
```
and a new test:
```go
func TestIndexTaskQueue_Resource(t *testing.T) {
	queue := NewIndexBuildTaskQueue(&TaskScheduler{ctx: context.Background()})
	task := &fakeTask{id: 1, ctx: &stagectx{ch: make(chan struct{})}}
	assert.NoError(t, queue.Enqueue(task))
	assert.Equal(t, int64(1), queue.GetUsingSlot())
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: 100}, queue.GetUsingResource())

	queue.AddActiveTask(task)
	queue.PopActiveTask(task.Name())
	assert.Equal(t, int64(0), queue.GetUsingSlot())
	assert.Equal(t, taskcommon.Resource{}, queue.GetUsingResource())
}
```
(`fakeTask` construction: mirror how `TestIndexTaskScheduler` builds one near L182; `OnEnqueue` touches `_taskwg` — if the fixture needs more fields set, copy the construction from that test verbatim.)

`internal/datanode/importv2/scheduler_test.go` — after `TestScheduler_Slots` (L128) add:
```go
func (s *SchedulerSuite) TestScheduler_Resource() {
	task := NewMockTask(s.T())
	task.EXPECT().GetTaskID().Return(int64(1)).Maybe()
	task.EXPECT().GetJobID().Return(int64(1)).Maybe()
	task.EXPECT().GetState().Return(datapb.ImportTaskStateV2_InProgress).Maybe()
	task.EXPECT().GetResource().Return(taskcommon.Resource{CPU: 1, Memory: 256 << 20})
	task.EXPECT().Clone().Return(task).Maybe()
	s.manager.Add(task)

	s.Equal(taskcommon.Resource{CPU: 1, Memory: 256 << 20}, s.scheduler.Resource())
}
```
(Look at `TestScheduler_Slots` L128-145 for the exact mock expectations `manager.Add` / `GetBy` need and copy them; the assertion is the point.)

`internal/datanode/services_test.go` — extend `TestQuerySlot`'s "normal case":
```go
	s.Run("normal case", func() {
		s.SetupTest()
		cpuMock := mockey.Mock(hardware.GetCPUNum).Return(16).Build()
		defer cpuMock.UnPatch()
		memMock := mockey.Mock(hardware.GetMemoryCount).Return(uint64(64) << 30).Build()
		defer memMock.UnPatch()

		ctx := context.Background()
		resp, err := s.node.QuerySlot(ctx, nil)
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		s.Equal(int64(16), resp.GetTotalCpu())
		s.Equal(int64(64)<<30, resp.GetTotalMemory())
		s.Equal(resp.GetTotalCpu(), resp.GetAvailableCpu())
		s.Equal(resp.GetTotalMemory(), resp.GetAvailableMemory())
	})

	s.Run("standalone discounts the totals", func() {
		s.SetupTest()
		cpuMock := mockey.Mock(hardware.GetCPUNum).Return(16).Build()
		defer cpuMock.UnPatch()
		memMock := mockey.Mock(hardware.GetMemoryCount).Return(uint64(64) << 30).Build()
		defer memMock.UnPatch()
		roleMock := mockey.Mock(paramtable.GetRole).Return(typeutil.StandaloneRole).Build()
		defer roleMock.UnPatch()

		resp, err := s.node.QuerySlot(context.Background(), nil)
		s.NoError(err)
		s.True(merr.Ok(resp.GetStatus()))
		s.Equal(int64(4), resp.GetTotalCpu())
		s.Equal(int64(16)<<30, resp.GetTotalMemory())
	})
```
Add imports `github.com/bytedance/mockey`, `github.com/milvus-io/milvus/pkg/v3/util/hardware`, `paramtable`, `typeutil` as needed (check which already exist in the file).

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOTEST ./internal/datanode/compactor/ ./internal/datanode/index/ ./internal/datanode/importv2/ ./internal/datanode/ -run 'Test_Resource_Bookkeeping|TestIndexTaskQueue_Resource|TestScheduler_Resource|TestQuerySlot' 2>&1 | tail -8`
Expected: compile errors (`GetResource` undefined, `GetTotalCpu` fine but `Resource()` undefined).

- [ ] **Step 3: Compaction executor**

`compactor.go` — add to the `Compactor` interface after `GetSlotUsage() int64`:
```go
	// GetResource is the cpu/memory DataCoord estimated for this plan. The
	// worker never computes it; it books it on enqueue and releases it on
	// completion. Zero when the coordinator predates the field.
	GetResource() taskcommon.Resource
```
Each compactor (mix L628, sort L547, l0 L552, clustering L1029, bump L1417) — right after its `GetSlotUsage`:
```go
func (t *mixCompactionTask) GetResource() taskcommon.Resource {
	return taskcommon.Resource{CPU: t.plan.GetCpu(), Memory: t.plan.GetMemory()}
}
```
(receiver types: `*sortCompactionTask`, `*LevelZeroCompactionTask`, `*clusteringCompactionTask`, `*bumpSchemaVersionCompactionTask`.) Check with `grep -rn 'GetSlotUsage() int64 {' internal/datanode/compactor/*.go | grep -v mock` that no sixth compactor exists (e.g. a namespace compactor) — if one does, add the method there too.

`executor.go`:
- `Executor` interface: add `Resource() taskcommon.Resource` after `Slots() int64`.
- `taskState`: add `resource taskcommon.Resource // booked at enqueue, released at completion`.
- `executor` struct: add `usingResource taskcommon.Resource` after `usingSlots int64`.
- `Enqueue`: after `e.usingSlots += getTaskSlotUsage(task)`:
```go
	resource := task.GetResource()
	e.usingResource = e.usingResource.Add(resource)
	e.tasks[planID] = &taskState{
		compactor: task,
		state:     datapb.CompactionTaskState_executing,
		result:    nil,
		resource:  resource,
	}
```
(replace the existing `e.tasks[planID] = &taskState{...}` literal).
- After `Slots()`:
```go
// Resource returns the cpu/memory booked by accepted, unfinished compactions.
func (e *executor) Resource() taskcommon.Resource {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.usingResource
}
```
- `completeTask`: after the `usingSlots` clamp add `e.usingResource = e.usingResource.Sub(task.resource)`.

Regenerate the mock: `/home/zc/work/milvus/bin/mockery --config internal/datanode/.mockery.yaml` and keep only `internal/datanode/compactor/mock_compactor.go` and `internal/datanode/importv2/mock_task.go` from the diff (revert any other regenerated file).

- [ ] **Step 4: Index queue**

`index/task.go` — add `GetResource() taskcommon.Resource` after `GetSlot() int64`.
`index/task_index.go` L156, `task_stats.go` L142, `task_analyze.go` L77 — after each `GetSlot`:
```go
func (it *indexBuildTask) GetResource() taskcommon.Resource {
	return taskcommon.Resource{CPU: it.req.GetCpu(), Memory: it.req.GetMemory()}
}
```
(receivers `st *statsTask`, `at *analyzeTask` with `st.req` / `at.req`.)
`index/scheduler.go`:
- struct: after `usingSlot atomic.Int64` add `usingCPU atomic.Int64` and `usingMemory atomic.Int64`.
- after `GetUsingSlot`:
```go
// GetUsingResource returns the cpu/memory booked by enqueued and active tasks.
func (queue *IndexTaskQueue) GetUsingResource() taskcommon.Resource {
	return taskcommon.Resource{CPU: queue.usingCPU.Load(), Memory: queue.usingMemory.Load()}
}
```
- `PopActiveTask`: next to `queue.usingSlot.Sub(t.GetSlot())`:
```go
		res := t.GetResource()
		queue.usingCPU.Add(-res.CPU)
		queue.usingMemory.Add(-res.Memory)
```
- `Enqueue`: next to `queue.usingSlot.Add(t.GetSlot())`:
```go
	res := t.GetResource()
	queue.usingCPU.Add(res.CPU)
	queue.usingMemory.Add(res.Memory)
```
Every other `Task` implementation in the package (`grep -rn 'GetSlot() int64' internal/datanode/index/ | grep -v _test`) gets the same one-liner.

- [ ] **Step 5: Import scheduler**

`importv2/task.go` — add `GetResource() taskcommon.Resource` after `GetSlots() int64`.
`task_import.go`, `task_preimport.go`, `task_l0_import.go`, `task_l0_preimport.go` — after each `GetSlots`:
```go
func (t *ImportTask) GetResource() taskcommon.Resource {
	return taskcommon.Resource{CPU: t.req.GetCpu(), Memory: t.req.GetMemory()}
}
```
`task_copy_segment.go` — struct L45: add `resource taskcommon.Resource // cpu/memory estimated by DataCoord`; at L117 next to `slots: req.GetTaskSlot(),` add `resource: taskcommon.Resource{CPU: req.GetCpu(), Memory: req.GetMemory()},`; after `GetSlots` (L174) add `func (t *CopySegmentTask) GetResource() taskcommon.Resource { return t.resource }`; in `Clone` (L208) copy `resource: t.resource,`.
`scheduler.go` — interface: add `Resource() taskcommon.Resource` after `Slots() int64`; after `Slots()`:
```go
// Resource returns the cpu/memory booked by pending and in-progress import tasks.
func (s *scheduler) Resource() taskcommon.Resource {
	tasks := s.manager.GetBy(WithStates(datapb.ImportTaskStateV2_Pending, datapb.ImportTaskStateV2_InProgress))
	var used taskcommon.Resource
	for _, t := range tasks {
		used = used.Add(t.GetResource())
	}
	return used
}
```
Regenerate `importv2/mock_task.go` (same mockery command as Step 3 if not already done).

- [ ] **Step 6: QuerySlot and the gauge**

`pkg/metrics/datanode_metrics.go` — after `DataNodeSlot` (L363) add:
```go
	DataNodeTaskResource = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: milvusNamespace,
			Subsystem: typeutil.DataNodeRole,
			Name:      "task_resource",
			Help:      "total and available cpu (cores) and memory (bytes) for coordinator-dispatched tasks",
		}, []string{nodeIDLabelName, "type", "state"})
```
and `registry.MustRegister(DataNodeTaskResource)` next to L472.

`internal/datanode/services.go` — replace the body of `QuerySlot` after the health check with:
```go
	var (
		totalSlots     = index.CalculateNodeSlots()
		indexStatsUsed = node.taskScheduler.TaskQueue.GetUsingSlot()
		compactionUsed = node.compactionExecutor.Slots()
		importUsed     = node.importScheduler.Slots()
	)

	availableSlots := totalSlots - indexStatsUsed - compactionUsed - importUsed
	if availableSlots < 0 {
		availableSlots = 0
	}

	total := nodeTaskCapacity()
	used := node.taskScheduler.TaskQueue.GetUsingResource().
		Add(node.compactionExecutor.Resource()).
		Add(node.importScheduler.Resource())
	available := total.Sub(used)

	mlog.Info(ctx, "query slots done",
		mlog.Int64("totalSlots", totalSlots),
		mlog.Int64("availableSlots", availableSlots),
		mlog.Int64("indexStatsUsed", indexStatsUsed),
		mlog.Int64("compactionUsed", compactionUsed),
		mlog.Int64("importUsed", importUsed),
		mlog.Stringer("totalResource", total),
		mlog.Stringer("usedResource", used),
		mlog.Stringer("availableResource", available),
	)

	nodeID := fmt.Sprint(node.GetNodeID())
	metrics.DataNodeSlot.WithLabelValues(nodeID, "available").Set(float64(availableSlots))
	metrics.DataNodeSlot.WithLabelValues(nodeID, "total").Set(float64(totalSlots))
	metrics.DataNodeSlot.WithLabelValues(nodeID, "indexStatsUsed").Set(float64(indexStatsUsed))
	metrics.DataNodeSlot.WithLabelValues(nodeID, "compactionUsed").Set(float64(compactionUsed))
	metrics.DataNodeSlot.WithLabelValues(nodeID, "importUsed").Set(float64(importUsed))
	metrics.DataNodeTaskResource.WithLabelValues(nodeID, "cpu", "total").Set(float64(total.CPU))
	metrics.DataNodeTaskResource.WithLabelValues(nodeID, "cpu", "available").Set(float64(available.CPU))
	metrics.DataNodeTaskResource.WithLabelValues(nodeID, "memory", "total").Set(float64(total.Memory))
	metrics.DataNodeTaskResource.WithLabelValues(nodeID, "memory", "available").Set(float64(available.Memory))

	return &datapb.QuerySlotResponse{
		Status:          merr.Success(),
		AvailableSlots:  availableSlots,
		TotalCpu:        total.CPU,
		AvailableCpu:    available.CPU,
		TotalMemory:     total.Memory,
		AvailableMemory: available.Memory,
	}, nil
}

// nodeTaskCapacity is what this node offers coordinator-dispatched tasks: the
// machine (cgroup-aware) in cluster mode, and the same fraction of it the
// scalar slots already use in standalone mode, where a QueryNode shares the
// process.
func nodeTaskCapacity() taskcommon.Resource {
	total := taskcommon.Resource{
		CPU:    int64(hardware.GetCPUNum()),
		Memory: int64(hardware.GetMemoryCount()),
	}
	if paramtable.GetRole() == typeutil.StandaloneRole {
		ratio := paramtable.Get().DataNodeCfg.StandaloneSlotRatio.GetAsFloat()
		total.CPU = max(int64(float64(total.CPU)*ratio), 1)
		total.Memory = int64(float64(total.Memory) * ratio)
	}
	return total
}
```
(`mlog.Stringer` — check it exists with `grep -n 'func Stringer' pkg/mlog/*.go`; if not, use `mlog.String("totalResource", total.String())`.) Add imports `hardware`, `taskcommon`, `typeutil` as needed.

- [ ] **Step 7: Run the DataNode packages**

Run: `GOTEST ./internal/datanode/compactor/ ./internal/datanode/index/ ./internal/datanode/importv2/ ./internal/datanode/ 2>&1 | tail -8`
Expected: all `ok`. Any `mock: I don't know what to return because the method call was unexpected` → that test constructs a mock that now reaches `GetResource`; add the `.Maybe()` expectation there.

- [ ] **Step 8: Commit**

```bash
git add pkg/metrics/datanode_metrics.go internal/datanode/
git commit -s -m "enhance: DataNode reports cpu/memory ledger in QuerySlot

Each executor books the cpu/memory its requests carry at the same point it
books the scalar slot and releases it on completion. QuerySlot reports the
machine totals (standalone-discounted) and total minus booked, next to the
unchanged available_slots. A request from an older coordinator books zero.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01V39kgspQN9aawcNuoRgkER"
```

---

### Task 7: two-tier placement in the scheduler

**Files:**
- Modify: `internal/datacoord/session/cluster.go` (`WorkerSlots` L39-42, `QuerySlot` L200-205), `internal/datacoord/task/global_scheduler.go` (`pickNode` L236-262, `schedule` L264-290)
- Create: `internal/datacoord/task/node_picker.go`, `internal/datacoord/task/node_picker_test.go`

**Interfaces:**
- Consumes: `WorkerSlots` gains `TotalCPU, AvailableCPU, TotalMemory, AvailableMemory int64`; `Task.GetTaskResource()`.
- Produces: `func newNodePicker(workerSlots map[int64]*session.WorkerSlots) *nodePicker`; `func (p *nodePicker) Pick(taskSlot int64, req taskcommon.Resource) int64`; `func pickNodeFromHeap(slotHeap typeutil.Heap[*nodeSlotEntry], taskSlot int64) int64` (the existing `pickNode` body, now package-level; the method delegates to it so the existing tests keep passing).

- [ ] **Step 1: Write the failing tests**

`internal/datacoord/task/node_picker_test.go`:
```go
package task

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
)

const (
	pickGiB = int64(1) << 30
)

func resourceWorker(id int64, slots, cpu, mem int64) *session.WorkerSlots {
	return &session.WorkerSlots{NodeID: id, AvailableSlots: slots,
		TotalCPU: cpu, AvailableCPU: cpu, TotalMemory: mem, AvailableMemory: mem}
}

func TestNodePicker_MemoryIsAHardFilter(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 8, 4*pickGiB),
		2: resourceWorker(2, 10, 8, 16*pickGiB),
	})
	// 6GiB only fits node 2, even though node 1 has the same cpu and slots.
	assert.Equal(t, int64(2), p.Pick(1, taskcommon.Resource{CPU: 1, Memory: 6 * pickGiB}))
}

func TestNodePicker_ScoresLeastLoaded(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 8, 16*pickGiB),
		2: resourceWorker(2, 10, 8, 16*pickGiB),
	})
	// Pre-load node 1: it now has less of both left, so node 2 wins.
	for _, n := range p.nodes {
		if n.nodeID == 1 {
			n.availableMemory -= 8 * pickGiB
			n.availableCPU -= 4
		}
	}
	assert.Equal(t, int64(2), p.Pick(1, taskcommon.Resource{CPU: 1, Memory: pickGiB}))
}

func TestNodePicker_ChargesWithinRound(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 8, 16*pickGiB),
		2: resourceWorker(2, 10, 8, 16*pickGiB),
	})
	req := taskcommon.Resource{CPU: 1, Memory: 6 * pickGiB}
	first := p.Pick(1, req)
	second := p.Pick(1, req)
	// Water-filling: two identical workers get one task each.
	assert.NotEqual(t, first, second)
	// Each has 10GiB left; a third 6GiB task fits on either, a fourth on neither.
	assert.NotEqual(t, int64(NullNodeID), p.Pick(1, req))
	assert.NotEqual(t, int64(NullNodeID), p.Pick(1, req))
	assert.Equal(t, int64(NullNodeID), p.Pick(1, req))
}

func TestNodePicker_CPUOnlyRanks(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 2, 64*pickGiB),
	})
	// 8-core request on a 2-core worker is still placed: cpu never refuses.
	assert.Equal(t, int64(1), p.Pick(1, taskcommon.Resource{CPU: 8, Memory: pickGiB}))
	assert.Equal(t, int64(1), p.Pick(1, taskcommon.Resource{CPU: 8, Memory: pickGiB}))
}

func TestNodePicker_SlotsExhaustedSkipsWorker(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 0, 8, 64*pickGiB),
		2: resourceWorker(2, 10, 8, 8*pickGiB),
	})
	// Node 1 has the memory but its queue is full (scalar 0): node 2 is picked.
	assert.Equal(t, int64(2), p.Pick(1, taskcommon.Resource{CPU: 1, Memory: pickGiB}))
}

func TestNodePicker_OversizedGoesToEmptiest(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 8, 8*pickGiB),
		2: resourceWorker(2, 10, 8, 16*pickGiB),
	})
	p.nodes[0].availableMemory, p.nodes[1].availableMemory = 6*pickGiB, 5*pickGiB
	emptiest := p.nodes[0].nodeID
	// 32GiB fits nowhere even empty: dispatch to whoever has most free memory now.
	assert.Equal(t, emptiest, p.Pick(1, taskcommon.Resource{CPU: 8, Memory: 32 * pickGiB}))
}

func TestNodePicker_BusyWaits(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 8, 16*pickGiB),
	})
	p.nodes[0].availableMemory = 2 * pickGiB
	// 8GiB fits an empty node 1 but not now: wait for the next round.
	assert.Equal(t, int64(NullNodeID), p.Pick(1, taskcommon.Resource{CPU: 1, Memory: 8 * pickGiB}))
}

func TestNodePicker_ScalarWorkersUseTheHeap(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: {NodeID: 1, AvailableSlots: 20},
		2: {NodeID: 2, AvailableSlots: 80},
	})
	assert.Empty(t, p.nodes)
	assert.Equal(t, int64(2), p.Pick(10, taskcommon.Resource{CPU: 1, Memory: pickGiB}))
}

func TestNodePicker_MixedClusterFallsThrough(t *testing.T) {
	p := newNodePicker(map[int64]*session.WorkerSlots{
		1: resourceWorker(1, 10, 8, 4*pickGiB),
		2: {NodeID: 2, AvailableSlots: 80},
	})
	// Too big for the only resource-reporting worker: the scalar worker takes it.
	assert.Equal(t, int64(2), p.Pick(10, taskcommon.Resource{CPU: 1, Memory: 6 * pickGiB}))
	// Fits: the resource-reporting worker is preferred.
	assert.Equal(t, int64(1), p.Pick(10, taskcommon.Resource{CPU: 1, Memory: pickGiB}))
}

func TestNodePicker_ZeroRequirement(t *testing.T) {
	onlyResource := newNodePicker(map[int64]*session.WorkerSlots{1: resourceWorker(1, 10, 8, 16*pickGiB)})
	// A task that did not price itself is still placed rather than starved.
	assert.Equal(t, int64(1), onlyResource.Pick(1, taskcommon.Resource{}))

	empty := newNodePicker(nil)
	assert.Equal(t, int64(NullNodeID), empty.Pick(1, taskcommon.Resource{CPU: 1, Memory: pickGiB}))
}

func TestNodePicker_Score(t *testing.T) {
	n := &resourceNode{totalCPU: 8, availableCPU: 8, totalMemory: 16 * pickGiB, availableMemory: 16 * pickGiB}
	empty := n.score(taskcommon.Resource{})
	half := n.score(taskcommon.Resource{CPU: 4, Memory: 8 * pickGiB})
	assert.InDelta(t, 1.0, empty, 1e-9)
	assert.InDelta(t, 0.6*0.5+0.25*0.5+0.15*1.0, half, 1e-9)
	// Lopsided (all cpu gone, memory untouched) scores below balanced.
	lopsided := n.score(taskcommon.Resource{CPU: 8})
	assert.Less(t, lopsided, empty)
	// Over-subscribed cpu clamps at 0 instead of going negative.
	assert.InDelta(t, 0.6*1.0+0.25*0+0.15*0, n.score(taskcommon.Resource{CPU: 100}), 1e-9)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOTEST ./internal/datacoord/task/ -run TestNodePicker 2>&1 | tail -3`
Expected: compile error `undefined: newNodePicker`.

- [ ] **Step 3: Carry the report through `WorkerSlots`**

`session/cluster.go`:
```go
// WorkerSlots represents the slot information for a worker node
type WorkerSlots struct {
	NodeID         int64
	AvailableSlots int64
	// Two-dimensional report. TotalMemory == 0 means the worker predates it
	// and must be placed on AvailableSlots alone.
	TotalCPU        int64
	AvailableCPU    int64
	TotalMemory     int64
	AvailableMemory int64
}
```
and in `QuerySlot`:
```go
			availableNodeSlots[nodeID] = &WorkerSlots{
				NodeID:          nodeID,
				AvailableSlots:  resp.GetAvailableSlots(),
				TotalCPU:        resp.GetTotalCpu(),
				AvailableCPU:    resp.GetAvailableCpu(),
				TotalMemory:     resp.GetTotalMemory(),
				AvailableMemory: resp.GetAvailableMemory(),
			}
```

- [ ] **Step 4: Write `node_picker.go`**

```go
// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package task

import (
	"math"

	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Score weights. Memory dominates because it is the dimension that refuses
// work; CPU spreads compute-heavy tasks; balance penalizes leaving a worker
// with lots of one dimension and none of the other (Kubernetes'
// BalancedAllocation idea).
const (
	scoreMemoryWeight  = 0.6
	scoreCPUWeight     = 0.25
	scoreBalanceWeight = 0.15
)

// nodePicker places one task per call and charges what it picked, so later
// picks in the same round see the effect.
//
// It has two tiers because a rolling upgrade has both kinds of worker at once
// and there is no honest exchange rate between a slot and a byte:
//   - workers that report cpu/memory are placed on those: memory is a hard
//     filter, cpu only ranks;
//   - workers that do not are placed by the pre-existing slot max-heap.
//
// Nothing gets worse for a task: one that finds no home in the first tier
// falls through to the tier that existed before.
type nodePicker struct {
	nodes []*resourceNode
	heap  typeutil.Heap[*nodeSlotEntry]
}

type resourceNode struct {
	nodeID          int64
	availableSlots  int64
	totalCPU        int64
	availableCPU    int64
	totalMemory     int64
	availableMemory int64
}

func newNodePicker(workerSlots map[int64]*session.WorkerSlots) *nodePicker {
	p := &nodePicker{}
	scalar := make(map[int64]*session.WorkerSlots, len(workerSlots))
	for nodeID, ws := range workerSlots {
		if ws.TotalMemory <= 0 {
			scalar[nodeID] = ws
			continue
		}
		p.nodes = append(p.nodes, &resourceNode{
			nodeID:          nodeID,
			availableSlots:  ws.AvailableSlots,
			totalCPU:        ws.TotalCPU,
			availableCPU:    ws.AvailableCPU,
			totalMemory:     ws.TotalMemory,
			availableMemory: ws.AvailableMemory,
		})
	}
	p.heap = newNodeSlotHeap(scalar)
	return p
}

// Pick returns the node for a task needing taskSlot slots and req resources,
// or NullNodeID when it should wait for the next round.
func (p *nodePicker) Pick(taskSlot int64, req taskcommon.Resource) int64 {
	if nodeID, ok := p.pickByResource(taskSlot, req); ok {
		return nodeID
	}
	return pickNodeFromHeap(p.heap, taskSlot)
}

// pickByResource returns ok=false when no resource-reporting worker should
// take the task now, so the caller falls through to the scalar tier.
func (p *nodePicker) pickByResource(taskSlot int64, req taskcommon.Resource) (int64, bool) {
	if len(p.nodes) == 0 {
		return NullNodeID, false
	}
	var (
		best      *resourceNode
		bestScore = math.Inf(-1)
		largest   int64
	)
	for _, n := range p.nodes {
		largest = max(largest, n.totalMemory)
		if n.availableSlots <= 0 {
			// The worker's queue is full however much memory its ledger has
			// free; the scalar still carries what is merely queued there.
			continue
		}
		if n.availableMemory < req.Memory {
			// Memory gates. It is the only dimension a task is refused a
			// worker for, because exceeding it kills the process rather
			// than slowing it down.
			continue
		}
		if s := n.score(req); s > bestScore {
			best, bestScore = n, s
		}
	}
	if best != nil {
		best.charge(taskSlot, req)
		return best.nodeID, true
	}
	if req.Memory > largest {
		// Larger than the largest worker even empty: waiting never helps.
		// Start it where it has the most room and let the worker's own
		// limits pace it.
		var emptiest *resourceNode
		for _, n := range p.nodes {
			if n.availableSlots <= 0 {
				continue
			}
			if emptiest == nil || n.availableMemory > emptiest.availableMemory {
				emptiest = n
			}
		}
		if emptiest != nil {
			emptiest.charge(taskSlot, req)
			return emptiest.nodeID, true
		}
	}
	// Merely busy: wait rather than force it onto a worker that cannot hold it.
	return NullNodeID, false
}

func (n *resourceNode) charge(taskSlot int64, req taskcommon.Resource) {
	n.availableCPU -= req.CPU
	n.availableMemory -= req.Memory
	if taskSlot > 0 {
		n.availableSlots = max(n.availableSlots-taskSlot, 0)
	}
}

// score ranks a worker that already fits: how much memory and cpu would be
// left after the task, as fractions of the worker, plus how balanced the
// remainder is. Higher is better; the range is [0, 1].
func (n *resourceNode) score(req taskcommon.Resource) float64 {
	memFrac := remainingFraction(n.availableMemory-req.Memory, n.totalMemory)
	cpuFrac := remainingFraction(n.availableCPU-req.CPU, n.totalCPU)
	balance := 1.0 - math.Abs(memFrac-cpuFrac)
	return scoreMemoryWeight*memFrac + scoreCPUWeight*cpuFrac + scoreBalanceWeight*balance
}

func remainingFraction(remaining, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return math.Min(math.Max(float64(remaining)/float64(total), 0), 1)
}
```

- [ ] **Step 5: Wire it into the scheduler**

`global_scheduler.go`:
- Rename the body of `pickNode` into a package function and keep the method as a delegate:
```go
// pickNode keeps the historical method for callers and tests; the placement
// itself lives in pickNodeFromHeap so nodePicker can share it.
func (s *globalTaskScheduler) pickNode(slotHeap typeutil.Heap[*nodeSlotEntry], taskSlot int64) int64 {
	return pickNodeFromHeap(slotHeap, taskSlot)
}

func pickNodeFromHeap(slotHeap typeutil.Heap[*nodeSlotEntry], taskSlot int64) int64 {
	... (existing body, unchanged) ...
}
```
- In `schedule()`, replace
```go
	slotHeap := newNodeSlotHeap(nodeSlots)
```
with
```go
	picker := newNodePicker(nodeSlots)
```
and
```go
		taskSlot := task.GetTaskSlot()
		nodeID := s.pickNode(slotHeap, taskSlot)
```
with
```go
		taskSlot := task.GetTaskSlot()
		nodeID := picker.Pick(taskSlot, task.GetTaskResource())
```
- Extend the existing `"processing task..."` log call in `schedule()` with `mlog.Stringer("resource", task.GetTaskResource())` (or `mlog.String("resource", task.GetTaskResource().String())`) so a placement can be read back from the log.

- [ ] **Step 6: Run the scheduler package and the picker tests**

Run: `GOTEST ./internal/datacoord/task/ ./internal/datacoord/session/ -v 2>&1 | grep -E '^(=== RUN|--- FAIL|FAIL|ok|PASS)' | grep -v '=== RUN' `
Expected: `ok` for both; no `--- FAIL`. Coverage:
`GOTEST ./internal/datacoord/task/ -run 'TestNodePicker|TestGlobalScheduler' -coverprofile=/tmp/claude-1000/-home-zc-work-milvus/a8e23eec-da58-49e1-9340-a33b42be6487/scratchpad/cov.out && go tool cover -func=/tmp/claude-1000/-home-zc-work-milvus/a8e23eec-da58-49e1-9340-a33b42be6487/scratchpad/cov.out | grep node_picker`
Expected: every function ≥ 90%.

- [ ] **Step 7: Commit**

```bash
git add internal/datacoord/session/cluster.go internal/datacoord/task/node_picker.go internal/datacoord/task/node_picker_test.go internal/datacoord/task/global_scheduler.go
git commit -s -m "enhance: place tasks on cpu/memory when workers report them

Workers that report total/available cpu and memory are placed on those:
memory is a hard filter, cpu only ranks, and the score prefers the worker
left most balanced. A task larger than any worker goes to the one with the
most room; a merely busy cluster makes it wait a round. Workers without the
report keep the scalar max-heap, so a mixed cluster never regresses.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01V39kgspQN9aawcNuoRgkER"
```

---

### Task 8: whole-tree verification and self-review

**Files:** none new.

- [ ] **Step 1: Full builds**

Run: `cd pkg && go build ./... && go vet ./taskcommon/ ./util/paramtable/ && cd .. && go build -tags dynamic,test ./internal/... ./cmd/...`
Expected: clean.

- [ ] **Step 2: Full test runs of every touched package**

Run: `GOTEST ./pkg/taskcommon/ ./internal/datacoord/... ./internal/datanode/... 2>&1 | grep -v '^ok' ; echo EXIT=$?`
(`./pkg/taskcommon/` must be run from `pkg/` — do `cd pkg && go test ./taskcommon/` separately.) Expected: no FAIL lines; only the `ok` lines were filtered.
Note from memory: `TestServer_getSystemInfoMetrics` in `internal/datacoord` may fail only when the whole package runs, on a clean 3.0 too — confirm on `upstream/3.0` with `git stash` before blaming this branch.

- [ ] **Step 3: Lint the diff**

Run: `unset HTTP_PROXY HTTPS_PROXY; gofumpt -l $(git diff --name-only upstream/3.0 -- '*.go' | grep -v pb.go | grep -v mock_) ; gci list --skip-generated -s standard -s default -s 'prefix(github.com/milvus-io)' $(git diff --name-only upstream/3.0 -- '*.go' | grep -v pb.go | grep -v mock_)`
Expected: no files listed. If `gofumpt`/`gci` are missing, `ls /home/zc/work/milvus/bin/` and use the ones there.

- [ ] **Step 4: Adversarial pass (G1–G4 from CLAUDE.md)**

Answer each in writing in the final report:
1. G1 — every construction site of the estimate: `grep -rn 'GetTaskResource()' internal/datacoord --include='*.go' | grep -v _test` lists 11 implementations and every request builder; confirm none ships a value different from what `Pick` saw (all go through `resourceCache` or a pure function of stable inputs).
2. G2 — trace: (a) V3 segment after restart → `estimateFieldSize` closed form (test exists); (b) external segment with no Stats → `estimateSegmentSize` schema path (test exists); (c) old DataNode → `TotalMemory == 0` → heap (test exists); (d) old DataCoord → DataNode books zero (`GetCpu()==0`) → `available == total` (assert in `TestQuerySlot` normal case).
3. G3 — the commit messages claim only what the tests above show.
4. G4 — list anything not traced (e.g. namespace compactor, if one exists) in the report.

- [ ] **Step 5: Update the spec if the implementation deviated**

Known deviation to record in `docs/superpowers/specs/2026-08-27-datanode-cpu-memory-slots-design.md` "DataNode ledger and report": there is no separate `internal/datanode/resource` package; each executor keeps its `Resource` counter next to its `usingSlots` (same lifecycle, less code). Commit as `doc: record ledger placement in the cpu/memory design`.
