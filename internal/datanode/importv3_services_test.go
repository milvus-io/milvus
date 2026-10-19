// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package datanode

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/importv3"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// importV3FakeTask is a scripted import V3 worker task that also reports
// progress, so the RPC query path can be driven without any storage.
type importV3FakeTask struct {
	taskID   int64
	runID    int64
	slot     int64
	kind     string
	progress importv3.TaskProgress
	result   any
}

func (t *importV3FakeTask) TaskID() int64                   { return t.taskID }
func (t *importV3FakeTask) RunID() int64                    { return t.runID }
func (t *importV3FakeTask) Kind() string                    { return t.kind }
func (t *importV3FakeTask) Slot() int64                     { return t.slot }
func (t *importV3FakeTask) Progress() importv3.TaskProgress { return t.progress }
func (t *importV3FakeTask) Execute(context.Context) (any, error) {
	return t.result, nil
}

// importV3StorageFactory is a no-op storage factory that returns a nil chunk
// manager; the valid-path request tests only build the worker task and never
// execute it.
type importV3StorageFactory struct{}

func (importV3StorageFactory) NewChunkManager(context.Context, *indexpb.StorageConfig) (storage.ChunkManager, error) {
	return nil, nil
}

func newImportV3ServicesNode(t *testing.T, started bool) (*DataNode, *importv3.Scheduler) {
	t.Helper()
	paramtable.Init()
	scheduler := importv3.NewScheduler(context.Background(), func() int64 { return 100 }, importv3.NewMetrics(1))
	if started {
		done := make(chan struct{})
		go func() {
			scheduler.Start()
			close(done)
		}()
		t.Cleanup(func() {
			scheduler.Close()
			<-done
		})
	} else {
		t.Cleanup(scheduler.Close)
	}
	node := &DataNode{
		importV3Scheduler: scheduler,
		importV3Metrics:   importv3.NewMetrics(2),
		storageFactory:    importV3StorageFactory{},
	}
	return node, scheduler
}

func waitImportV3State(t *testing.T, s *importv3.Scheduler, taskID, runID int64, state datapb.ImportTaskStateV2) importv3.Snapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if snapshot, ok := s.Query(taskID, runID); ok && snapshot.State == state {
			return snapshot
		}
		time.Sleep(time.Millisecond)
	}
	snapshot, _ := s.Query(taskID, runID)
	t.Fatalf("task %d/%d did not reach %s: %+v", taskID, runID, state, snapshot)
	return importv3.Snapshot{}
}

func validReshardRequest() *datapb.ReshardTaskRequest {
	return &datapb.ReshardTaskRequest{
		JobId: 1, TaskId: 2, RunId: 3, Slot: 1,
		StorageConfig: &indexpb.StorageConfig{RootPath: "root", BucketName: "bucket"},
		Plan:          &importv3pb.ReshardTaskPlan{CollectionSchema: &schemapb.CollectionSchema{}},
	}
}

func validImportV3Request() *datapb.ImportTaskV3Request {
	return &datapb.ImportTaskV3Request{
		JobId: 1, TaskId: 2, RunId: 3, SegmentId: 4, Slot: 1,
		StorageConfig: &indexpb.StorageConfig{RootPath: "root", BucketName: "bucket"},
		Plan:          &importv3pb.ImportTaskPlan{CollectionSchema: &schemapb.CollectionSchema{}},
	}
}

// TestSubmitReshardTaskValidatesRequest pins the reshard submit boundary's
// rejection branches, including a canceled context.
func TestSubmitReshardTaskValidatesRequest(t *testing.T) {
	node, _ := newImportV3ServicesNode(t, false)
	ctx := context.Background()

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	status, err := node.submitReshardTask(canceled, validReshardRequest())
	require.NoError(t, err)
	require.False(t, merr.Ok(status))

	cases := map[string]*datapb.ReshardTaskRequest{
		"nil request":        nil,
		"zero run id":        {RunId: 0, Slot: 1, StorageConfig: &indexpb.StorageConfig{}, Plan: &importv3pb.ReshardTaskPlan{}},
		"nil storage config": {RunId: 1, Slot: 1, Plan: &importv3pb.ReshardTaskPlan{}},
		"non-positive slot":  {RunId: 1, Slot: 0, StorageConfig: &indexpb.StorageConfig{}, Plan: &importv3pb.ReshardTaskPlan{}},
		"nil plan":           {RunId: 1, Slot: 1, StorageConfig: &indexpb.StorageConfig{}},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			status, err := node.submitReshardTask(ctx, req)
			require.NoError(t, err)
			require.False(t, merr.Ok(status))
			require.Contains(t, status.GetReason(), "invalid or incomplete ReshardTask request")
		})
	}
}

// TestSubmitReshardTaskQueuesValidRequest pins the valid path: a well-formed
// request is handed to the scheduler, where it waits as Pending.
func TestSubmitReshardTaskQueuesValidRequest(t *testing.T) {
	node, scheduler := newImportV3ServicesNode(t, false)
	status, err := node.submitReshardTask(context.Background(), validReshardRequest())
	require.NoError(t, err)
	require.True(t, merr.Ok(status))
	snapshot, ok := scheduler.Query(2, 3)
	require.True(t, ok)
	require.Equal(t, datapb.ImportTaskStateV2_Pending, snapshot.State)
}

// TestSubmitImportTaskV3ValidatesRequest pins the import V3 submit boundary
// validation, mirroring the reshard path, and that a valid request is queued.
func TestSubmitImportTaskV3ValidatesRequest(t *testing.T) {
	node, _ := newImportV3ServicesNode(t, false)
	ctx := context.Background()

	cases := map[string]*datapb.ImportTaskV3Request{
		"nil request":        nil,
		"zero run id":        {RunId: 0, Slot: 1, StorageConfig: &indexpb.StorageConfig{}, Plan: &importv3pb.ImportTaskPlan{}},
		"nil storage config": {RunId: 1, Slot: 1, Plan: &importv3pb.ImportTaskPlan{}},
		"non-positive slot":  {RunId: 1, Slot: 0, StorageConfig: &indexpb.StorageConfig{}, Plan: &importv3pb.ImportTaskPlan{}},
		"nil plan":           {RunId: 1, Slot: 1, StorageConfig: &indexpb.StorageConfig{}},
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			status, err := node.submitImportTaskV3(ctx, req)
			require.NoError(t, err)
			require.False(t, merr.Ok(status))
			require.Contains(t, status.GetReason(), "invalid or incomplete ImportTaskV3 request")
		})
	}

	status, err := node.submitImportTaskV3(ctx, validImportV3Request())
	require.NoError(t, err)
	require.True(t, merr.Ok(status))
}

// TestSubmitImportV3WorkerTaskNotReady pins the not-initialized guard of the
// shared submit helper.
func TestSubmitImportV3WorkerTaskNotReady(t *testing.T) {
	node := &DataNode{}
	status, err := node.submitImportV3WorkerTask(context.Background(),
		&importV3FakeTask{taskID: 1, runID: 1, slot: 1, kind: "import"})
	require.NoError(t, err)
	require.False(t, merr.Ok(status))
	require.Contains(t, status.GetReason(), "not initialized")
}

// TestQueryImportV3WorkerTaskReshardProgress pins the Reshard query path: the
// scheduler snapshot's *ReshardProgress is converted into SourceProgresses and
// surfaced on the wire response.
func TestQueryImportV3WorkerTaskReshardProgress(t *testing.T) {
	node, scheduler := newImportV3ServicesNode(t, true)
	progress := importv3.NewReshardProgress()
	progress.AddHashed(7, 5)
	progress.AddHashed(8, 3)
	require.NoError(t, scheduler.Submit(&importV3FakeTask{taskID: 41, runID: 1, slot: 1, kind: "reshard", progress: progress}))
	waitImportV3State(t, scheduler, 41, 1, datapb.ImportTaskStateV2_Completed)

	resp, err := node.queryImportV3WorkerTask(context.Background(), 41, 1, taskcommon.Reshard)
	require.NoError(t, err)
	require.True(t, merr.Ok(resp.GetStatus()))
	payload := &datapb.QueryReshardTaskResponse{}
	require.NoError(t, proto.Unmarshal(resp.GetPayload(), payload))
	require.Equal(t, datapb.ImportTaskStateV2_Completed, payload.GetState())
	require.ElementsMatch(t, []*datapb.ReshardSourceProgress{
		{FileId: 7, HashedRows: 5},
		{FileId: 8, HashedRows: 3},
	}, payload.GetSourceProgresses())
}

// TestQueryImportV3WorkerTaskImportSegments pins the ImportV3 query path: the
// task's []*ImportTaskV3Result is surfaced as the response segments.
func TestQueryImportV3WorkerTaskImportSegments(t *testing.T) {
	node, scheduler := newImportV3ServicesNode(t, true)
	segments := []*datapb.ImportTaskV3Result{{Rows: 5, ManifestPath: "manifest/seg.pb"}}
	require.NoError(t, scheduler.Submit(&importV3FakeTask{taskID: 42, runID: 1, slot: 1, kind: "import", result: segments}))
	waitImportV3State(t, scheduler, 42, 1, datapb.ImportTaskStateV2_Completed)

	resp, err := node.queryImportV3WorkerTask(context.Background(), 42, 1, taskcommon.ImportV3)
	require.NoError(t, err)
	require.True(t, merr.Ok(resp.GetStatus()))
	payload := &datapb.QueryImportTaskV3Response{}
	require.NoError(t, proto.Unmarshal(resp.GetPayload(), payload))
	require.Len(t, payload.GetSegments(), 1)
	require.Equal(t, int64(5), payload.GetSegments()[0].GetRows())
	require.Equal(t, "manifest/seg.pb", payload.GetSegments()[0].GetManifestPath())
}

// TestQueryImportV3WorkerTaskRejectsUnexpectedPayload pins the defensive guard:
// a completed import V3 run whose result is not the expected segment slice is
// surfaced as an internal error rather than crashing the query path.
func TestQueryImportV3WorkerTaskRejectsUnexpectedPayload(t *testing.T) {
	node, scheduler := newImportV3ServicesNode(t, true)
	require.NoError(t, scheduler.Submit(&importV3FakeTask{taskID: 43, runID: 1, slot: 1, kind: "import", result: "unexpected"}))
	waitImportV3State(t, scheduler, 43, 1, datapb.ImportTaskStateV2_Completed)

	resp, err := node.queryImportV3WorkerTask(context.Background(), 43, 1, taskcommon.ImportV3)
	require.NoError(t, err)
	require.False(t, merr.Ok(resp.GetStatus()))
	require.Contains(t, resp.GetStatus().GetReason(), "unexpected result payload")
}

// TestQueryImportV3WorkerTaskGRPCSizeWarning pins the size-warning branch: with
// a tiny configured gRPC limit, a completed import result trips the
// near-limit warning and the response is still returned intact.
func TestQueryImportV3WorkerTaskGRPCSizeWarning(t *testing.T) {
	key := paramtable.Get().DataNodeGrpcServerCfg.ServerMaxSendSize.Key
	paramtable.Get().Save(key, "1")
	t.Cleanup(func() { paramtable.Get().Reset(key) })

	node, scheduler := newImportV3ServicesNode(t, true)
	segments := []*datapb.ImportTaskV3Result{{Rows: 5, ManifestPath: "manifest/seg.pb"}}
	require.NoError(t, scheduler.Submit(&importV3FakeTask{taskID: 44, runID: 1, slot: 1, kind: "import", result: segments}))
	waitImportV3State(t, scheduler, 44, 1, datapb.ImportTaskStateV2_Completed)

	resp, err := node.queryImportV3WorkerTask(context.Background(), 44, 1, taskcommon.ImportV3)
	require.NoError(t, err)
	require.True(t, merr.Ok(resp.GetStatus()))
	payload := &datapb.QueryImportTaskV3Response{}
	require.NoError(t, proto.Unmarshal(resp.GetPayload(), payload))
	require.Len(t, payload.GetSegments(), 1)
}

// TestQueryImportV3WorkerTaskErrors pins the query-path error branches: an
// uninitialized scheduler, an absent run, and an unknown task type.
func TestQueryImportV3WorkerTaskErrors(t *testing.T) {
	t.Run("scheduler not initialized", func(t *testing.T) {
		node := &DataNode{}
		resp, err := node.queryImportV3WorkerTask(context.Background(), 1, 1, taskcommon.Reshard)
		require.NoError(t, err)
		require.False(t, merr.Ok(resp.GetStatus()))
		require.Contains(t, resp.GetStatus().GetReason(), "not initialized")
	})

	t.Run("absent run", func(t *testing.T) {
		node, _ := newImportV3ServicesNode(t, false)
		resp, err := node.queryImportV3WorkerTask(context.Background(), 999, 1, taskcommon.Reshard)
		require.NoError(t, err)
		require.False(t, merr.Ok(resp.GetStatus()))
		require.Contains(t, resp.GetStatus().GetReason(), "cannot find current import V3 task run")
	})

	t.Run("invalid task type", func(t *testing.T) {
		node, scheduler := newImportV3ServicesNode(t, true)
		require.NoError(t, scheduler.Submit(&importV3FakeTask{taskID: 45, runID: 1, slot: 1, kind: "reshard"}))
		waitImportV3State(t, scheduler, 45, 1, datapb.ImportTaskStateV2_Completed)
		resp, err := node.queryImportV3WorkerTask(context.Background(), 45, 1, taskcommon.Type("bogus"))
		require.NoError(t, err)
		require.False(t, merr.Ok(resp.GetStatus()))
		require.Contains(t, resp.GetStatus().GetReason(), "invalid V3 task type")
	})
}
