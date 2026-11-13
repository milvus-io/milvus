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

package bulkimport

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/taskresource"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestImportFileBufferSize(t *testing.T) {
	paramtable.Init()
	base := Params.DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	job := &Job{ImportJob: &datapb.ImportJob{JobID: 1, Vchannels: []string{"a", "b"}, PartitionIDs: []int64{1, 2, 3}}}
	task := &Task{}
	task.Task.Store(&datapb.ImportTaskV2{JobID: 1, TaskID: 3})

	// Base buffer per (vchannel, partition) pair, exactly as the worker sizes it,
	// with no largest-file cap because the worker never applies one.
	assert.Equal(t, base*2*3, importFileBufferSize(task, job))

	// A snapshot-source task imports into its single target partition, which is
	// all the request carries, so the worker sizes one partition's buffer.
	snapshot := &Task{}
	snapshot.Task.Store(&datapb.ImportTaskV2{JobID: 1, TaskID: 4, FileStats: []*datapb.ImportFileStats{{
		ImportFile: &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{TargetPartitionId: 2}},
	}}})
	assert.Equal(t, base*2, importFileBufferSize(snapshot, job))

	// L0 import: the worker uses the base buffer regardless of shards.
	l0Job := &Job{ImportJob: &datapb.ImportJob{
		JobID: 1, Vchannels: []string{"a", "b"}, PartitionIDs: []int64{1, 2, 3},
		Options: []*commonpb.KeyValuePair{{Key: importutilv2.L0Import, Value: "true"}},
	}}
	assert.Equal(t, base, importFileBufferSize(task, l0Job))
}

// TestImportTaskResource_ManyFilesManyPartitions is the shape the adversarial
// review called out: a partition-key collection (16 partitions) with many small
// files. The per-file buffer is 16 x 2 x 16 = 512MiB, but each file only holds
// its own bytes, so the task is priced at their sum rather than at 100 buffers.
func TestImportTaskResource_ManyFilesManyPartitions(t *testing.T) {
	paramtable.Init()
	const mib = int64(1) << 20

	partitions := make([]int64, 16)
	for i := range partitions {
		partitions[i] = int64(i + 1)
	}
	job := &Job{ImportJob: &datapb.ImportJob{JobID: 1, Vchannels: []string{"a", "b"}, PartitionIDs: partitions}}
	importMeta := NewMockImportMeta(t)
	importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job)

	fileStats := make([]*datapb.ImportFileStats, 0, 100)
	for i := 0; i < 100; i++ {
		fileStats = append(fileStats, &datapb.ImportFileStats{TotalMemorySize: mib})
	}
	task := &Task{ImportMeta: importMeta}
	task.Task.Store(&datapb.ImportTaskV2{JobID: 1, TaskID: 3, FileStats: fileStats})

	assert.Equal(t, taskresource.Import(100*mib, 0), taskresource.Price(task.GetTaskResource()))
	// Far below what "one buffer per file" would have priced.
	assert.Less(t, taskresource.Price(task.GetTaskResource()).Memory, taskresource.Import(100*importFileBufferSize(task, job), 0).Memory/100)
}

func TestImportTaskResource(t *testing.T) {
	paramtable.Init()
	base := paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	job := &Job{ImportJob: &datapb.ImportJob{JobID: 1, Vchannels: []string{"a", "b"}, PartitionIDs: []int64{1, 2, 3}}}

	importMeta := NewMockImportMeta(t)
	importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job)
	importMeta.EXPECT().GetJob(mock.Anything, int64(2)).Return(nil)

	// Three files, each larger than the (base x 2 x 3) buffer it is read with,
	// so each is charged a whole buffer.
	imp := &Task{ImportMeta: importMeta}
	imp.Task.Store(&datapb.ImportTaskV2{JobID: 1, TaskID: 3, FileStats: []*datapb.ImportFileStats{
		{TotalMemorySize: base * 100}, {TotalMemorySize: base * 100}, {TotalMemorySize: base * 100},
	}})
	assert.Equal(t, taskresource.Import(3*base*2*3, 0), taskresource.Price(imp.GetTaskResource()))
	assert.Equal(t, taskcommon.Resource{CPU: 1, Memory: max(3*base*2*3*2, 64<<20)}, taskresource.Price(imp.GetTaskResource()))

	// Files smaller than the buffer are charged their own bytes: a buffer never
	// fills beyond the file it reads.
	smallFiles := &Task{ImportMeta: importMeta}
	smallFiles.Task.Store(&datapb.ImportTaskV2{JobID: 1, TaskID: 7, FileStats: []*datapb.ImportFileStats{
		{TotalMemorySize: base}, {TotalMemorySize: base / 2},
	}})
	assert.Equal(t, taskresource.Import(base+base/2, 0), taskresource.Price(smallFiles.GetTaskResource()))

	// Pre-import: one base buffer per file, no job lookup needed.
	pre := &PreImportTask{ImportMeta: importMeta}
	pre.Task.Store(&datapb.PreImportTask{JobID: 1, TaskID: 4, FileStats: []*datapb.ImportFileStats{{}, {}}})
	assert.Equal(t, taskresource.PreImport(2*base), taskresource.Price(pre.GetTaskResource()))

	// Job dropped between enqueue and dispatch: the floor, never zero.
	orphanImport := &Task{ImportMeta: importMeta}
	orphanImport.Task.Store(&datapb.ImportTaskV2{JobID: 2, TaskID: 5})
	assert.Equal(t, taskresource.Default(), taskresource.Price(orphanImport.GetTaskResource()))
	assert.False(t, taskresource.Resolved(orphanImport.GetTaskResource()), "a dropped job cannot be priced")

	// A pre-import prices itself from its own file list, so a dropped job does
	// not change its answer.
	orphanPre := &PreImportTask{ImportMeta: importMeta}
	orphanPre.Task.Store(&datapb.PreImportTask{JobID: 2, TaskID: 6, FileStats: []*datapb.ImportFileStats{{}}})
	assert.Equal(t, taskresource.PreImport(base), taskresource.Price(orphanPre.GetTaskResource()))
}

// TestSnapshotImportTaskResource: a snapshot-source reader reserves its row
// buffer plus a delete budget, and a shared-L0 task one more delete batch, so
// the price must cover those bytes -- the same bytes its slot is sized on --
// not only the read buffers.
func TestSnapshotImportTaskResource(t *testing.T) {
	paramtable.Init()
	base := paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()
	deleteBudget := paramtable.Get().DataNodeCfg.ImportDeleteBufferSize.GetAsInt64()
	job := &Job{ImportJob: &datapb.ImportJob{JobID: 1, Vchannels: []string{"a", "b"}, PartitionIDs: []int64{1, 2, 3}}}
	l0Job := &Job{ImportJob: &datapb.ImportJob{
		JobID: 2, Vchannels: []string{"a", "b"}, PartitionIDs: []int64{1, 2, 3},
		Options: []*commonpb.KeyValuePair{{Key: importutilv2.L0Import, Value: "true"}},
	}}
	importMeta := NewMockImportMeta(t)
	importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job)
	importMeta.EXPECT().GetJob(mock.Anything, int64(2)).Return(l0Job)
	importMeta.EXPECT().GetJob(mock.Anything, int64(3)).Return(nil)

	snapshotFiles := func(version uint32) []*datapb.ImportFileStats {
		files := make([]*datapb.ImportFileStats, 0, 3)
		for i := 0; i < 3; i++ {
			files = append(files, &datapb.ImportFileStats{
				// Small files: the ordinary formula would price them at 1 byte each.
				TotalMemorySize: 1,
				ImportFile: &internalpb.ImportFile{SnapshotSource: &internalpb.SnapshotImportSource{
					TargetPartitionId: 2, Version: version,
				}},
			})
		}
		return files
	}

	// One target partition: per-reader buffer = base x 2 vchannels.
	perReader := base * 2
	private := &Task{ImportMeta: importMeta}
	private.Task.Store(&datapb.ImportTaskV2{JobID: 1, TaskID: 10, FileStats: snapshotFiles(4)})
	// The memory factor applies to the row buffers only; delete budgets are
	// fixed-size reservations and are added once.
	assert.Equal(t, taskresource.Import(3*perReader, 3*deleteBudget), taskresource.Price(private.GetTaskResource()))
	factor := paramtable.Get().DataCoordCfg.TaskResourceImportMemoryFactor.GetAsFloat()
	assert.Equal(t, int64(float64(3*perReader)*factor)+3*deleteBudget, taskresource.Price(private.GetTaskResource()).Memory)

	shared := &Task{ImportMeta: importMeta}
	shared.Task.Store(&datapb.ImportTaskV2{JobID: 1, TaskID: 11, FileStats: snapshotFiles(5)})
	assert.True(t, importutilv2.SnapshotSourceUsesSharedL0(shared.GetFileStats()[0].GetImportFile().GetSnapshotSource()))
	assert.Equal(t, taskresource.Import(3*perReader, 3*deleteBudget+deleteBudget), taskresource.Price(shared.GetTaskResource()))

	// The slot comes from the same bytes.
	rowBytes, deleteBytes, ok := snapshotImportBufferBytes(shared, job, perReader)
	assert.True(t, ok)
	bytes := rowBytes + deleteBytes
	assert.Equal(t, 3*(perReader+deleteBudget)+deleteBudget, bytes)
	memoryLimitPerSlot := paramtable.Get().DataCoordCfg.ImportMemoryLimitPerSlot.GetAsInt64()
	cpuBasedSlots := max(int64(1), 3/paramtable.Get().DataCoordCfg.ImportFileNumPerSlot.GetAsInt64())
	assert.Equal(t, max(cpuBasedSlots, (bytes-1)/memoryLimitPerSlot+1), shared.GetTaskSlot())

	// Pre-import of snapshot files: one base buffer plus a delete budget per reader.
	pre := &PreImportTask{ImportMeta: importMeta}
	pre.Task.Store(&datapb.PreImportTask{JobID: 1, TaskID: 12, FileStats: snapshotFiles(4)})
	assert.Equal(t, taskresource.PreImport(3*(base+deleteBudget)), taskresource.Price(pre.GetTaskResource()))

	// L0 import is not a snapshot reservation, whatever its files say.
	l0 := &Task{ImportMeta: importMeta}
	l0.Task.Store(&datapb.ImportTaskV2{JobID: 2, TaskID: 13, FileStats: snapshotFiles(4)})
	_, _, ok = snapshotImportBufferBytes(l0, l0Job, base)
	assert.False(t, ok)
	assert.Equal(t, taskresource.Import(3, 0), taskresource.Price(l0.GetTaskResource()))

	// A pre-import whose job is gone keeps the ordinary per-file price.
	orphanPre := &PreImportTask{ImportMeta: importMeta}
	orphanPre.Task.Store(&datapb.PreImportTask{JobID: 3, TaskID: 14, FileStats: snapshotFiles(4)})
	assert.Equal(t, taskresource.PreImport(3), taskresource.Price(orphanPre.GetTaskResource()))
}

// TestImportNodeMemoryShare: an import whose readers all reserve from the
// worker's import allocator is bounded at placement by the allocator's share
// of the node; an L0 import and a plain pre-import are not.
func TestImportNodeMemoryShare(t *testing.T) {
	paramtable.Init()
	share := taskresource.ImportNodeMemoryShare()
	assert.Greater(t, share, 0.0)
	job := &Job{ImportJob: &datapb.ImportJob{JobID: 1, Vchannels: []string{"a"}, PartitionIDs: []int64{1}}}
	l0Job := &Job{ImportJob: &datapb.ImportJob{
		JobID: 2, Vchannels: []string{"a"}, PartitionIDs: []int64{1},
		Options: []*commonpb.KeyValuePair{{Key: importutilv2.L0Import, Value: "true"}},
	}}
	importMeta := NewMockImportMeta(t)
	importMeta.EXPECT().GetJob(mock.Anything, int64(1)).Return(job)
	importMeta.EXPECT().GetJob(mock.Anything, int64(2)).Return(l0Job)
	importMeta.EXPECT().GetJob(mock.Anything, int64(3)).Return(nil)
	snapshotFile := []*datapb.ImportFileStats{{ImportFile: &internalpb.ImportFile{
		SnapshotSource: &internalpb.SnapshotImportSource{TargetPartitionId: 1, Version: 4},
	}}}
	plainFile := []*datapb.ImportFileStats{{ImportFile: &internalpb.ImportFile{Paths: []string{"a.parquet"}}}}

	newImport := func(jobID int64) *Task {
		task := &Task{ImportMeta: importMeta}
		task.Task.Store(&datapb.ImportTaskV2{JobID: jobID, TaskID: 20, FileStats: plainFile})
		return task
	}
	newPre := func(jobID int64, files []*datapb.ImportFileStats) *PreImportTask {
		task := &PreImportTask{ImportMeta: importMeta}
		task.Task.Store(&datapb.PreImportTask{JobID: jobID, TaskID: 21, FileStats: files})
		return task
	}

	assert.Equal(t, taskresource.ImportTaskNodeMemoryShare(), newImport(1).NodeMemoryShare(), "an import also holds batches waiting for sync")
	assert.Greater(t, newImport(1).NodeMemoryShare(), share)
	assert.Zero(t, newImport(2).NodeMemoryShare(), "L0 import")
	assert.Zero(t, newImport(3).NodeMemoryShare(), "job gone")
	assert.Equal(t, share, newPre(1, snapshotFile).NodeMemoryShare())
	assert.Zero(t, newPre(1, plainFile).NodeMemoryShare(), "plain pre-import")
	assert.Zero(t, newPre(3, snapshotFile).NodeMemoryShare(), "job gone")
}

// The count-only V3 preimport is priced at one base read buffer per file, from
// the task's own file count: the worker counts every file at once, and a JSON
// or CSV file is scanned through one buffer. The files are not resolved, so a
// parquet file is priced the same.
func TestPreImportV3TaskResource(t *testing.T) {
	paramtable.Init()
	files := func(n int) *preImportV3Task {
		stats := make([]*datapb.ImportV3FileStats, n)
		for i := range stats {
			stats[i] = &datapb.ImportV3FileStats{FileId: int64(i + 1)}
		}
		return newPreImportTaskV3(&datapb.PreImportTaskV3{JobId: 1, TaskId: 2, FileStats: stats}, nil)
	}
	price := func(task *preImportV3Task) taskcommon.Resource {
		res, ok := task.GetTaskResource()
		assert.True(t, ok)
		return res
	}
	base := paramtable.Get().DataNodeCfg.ImportBaseBufferSize.GetAsInt64()

	assert.Equal(t, taskresource.PreImport(2*base), price(files(2)))
	// With the default 16MB buffer a few files stay at the floor.
	assert.Equal(t, taskresource.Default(), price(files(2)))
	assert.Equal(t, taskresource.Default(), price(files(0)))
	assert.Equal(t, int64(1), files(2).GetTaskSlot())

	// A larger read buffer raises the price with it.
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.ImportBaseBufferSize.Key, "128")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.ImportBaseBufferSize.Key)
	assert.Equal(t, taskcommon.Resource{CPU: taskresource.DefaultCPU(), Memory: 2 * 128 << 20}, price(files(2)))
	assert.Equal(t, taskcommon.Resource{CPU: taskresource.DefaultCPU(), Memory: 5 * 128 << 20}, price(files(5)))
}
