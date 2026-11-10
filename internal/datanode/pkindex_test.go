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

package datanode

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datanode/importv2"
	pkcompaction "github.com/milvus-io/milvus/internal/datanode/pkindex/compaction"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/sst"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// failStore fails every download with a permanent error once gate is closed
// or, when gate is nil, at once.
type failStore struct{ gate chan struct{} }

func (s failStore) UploadFile(context.Context, string, string) error { return nil }

func (s failStore) DownloadFile(ctx context.Context, _, _ string) error {
	if s.gate != nil {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.Mark(errors.New("absent"), pkindex.ErrCorrupted)
}

func newPKIndexTestNode(t *testing.T, store pkcompaction.Store) *DataNode {
	paramtable.Init()
	ctx := context.Background()
	node := NewDataNode(ctx)
	node.importScheduler = importv2.NewScheduler(importv2.NewTaskManager())
	exec, err := pkcompaction.New(ctx, pkcompaction.Config{
		LocalRoot:       filepath.Join(t.TempDir(), "pkc"),
		TerminalTaskTTL: time.Hour,
		TaskSlot:        4,
		Parallelism:     1,
	}, store)
	require.NoError(t, err)
	exec.Start()
	node.pkindexCompaction = exec
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	t.Cleanup(func() {
		exec.Close()
		node.cancel()
	})
	return node
}

func pkindexProps(taskID int64) map[string]string {
	p := taskcommon.NewProperties(nil)
	p.AppendClusterID("by-dev")
	p.AppendTaskID(taskID)
	p.AppendType(taskcommon.PKIndexCompaction)
	return p
}

func pkindexCreate(t *testing.T, node *DataNode, req *pkindexpb.CompactionRequest) *commonpb.Status {
	payload, err := proto.Marshal(req)
	require.NoError(t, err)
	status, err := node.CreateTask(context.Background(), &workerpb.CreateTaskRequest{
		Properties: pkindexProps(req.GetTaskId()),
		Payload:    payload,
	})
	require.NoError(t, err)
	return status
}

func pkindexQuery(t *testing.T, node *DataNode, taskID int64) (taskcommon.State, string, *pkindexpb.CompactionResult, *commonpb.Status) {
	resp, err := node.QueryTask(context.Background(), &workerpb.QueryTaskRequest{Properties: pkindexProps(taskID)})
	require.NoError(t, err)
	if !merr.Ok(resp.GetStatus()) {
		return taskcommon.None, "", nil, resp.GetStatus()
	}
	props := taskcommon.NewProperties(resp.GetProperties())
	state, err := props.GetTaskState()
	require.NoError(t, err)
	reason := props.GetTaskReason()
	result := &pkindexpb.CompactionResult{}
	require.NoError(t, proto.Unmarshal(resp.GetPayload(), result))
	return state, reason, result, resp.GetStatus()
}

func validPKIndexRequest(taskID int64) *pkindexpb.CompactionRequest {
	return &pkindexpb.CompactionRequest{
		TaskId:         taskID,
		SstBasePath:    "b",
		Inputs:         []*pkindexpb.SSTRef{{Id: 1, Size: 1}},
		OutputIdBegin:  10,
		OutputIdEnd:    12,
		TargetFileSize: 1,
	}
}

func TestPKIndexCompactionTasks(t *testing.T) {
	node := newPKIndexTestNode(t, failStore{})

	assert.True(t, merr.Ok(pkindexCreate(t, node, validPKIndexRequest(1))))

	assert.Eventually(t, func() bool {
		state, _, _, _ := pkindexQuery(t, node, 1)
		return state == taskcommon.Failed
	}, 10*time.Second, 10*time.Millisecond)
	state, reason, result, status := pkindexQuery(t, node, 1)
	assert.Equal(t, taskcommon.Failed, state)
	assert.NotEmpty(t, reason)
	assert.False(t, result.GetRetriable())
	assert.True(t, merr.Ok(status))

	state, _, _, status = pkindexQuery(t, node, 2)
	assert.Equal(t, taskcommon.None, state)
	assert.True(t, merr.Ok(status))

	dropStatus, err := node.DropTask(context.Background(), &workerpb.DropTaskRequest{Properties: pkindexProps(1)})
	require.NoError(t, err)
	assert.True(t, merr.Ok(dropStatus))
	state, _, _, _ = pkindexQuery(t, node, 1)
	assert.Equal(t, taskcommon.None, state)
}

func TestPKIndexCompactionMalformedRequest(t *testing.T) {
	node := newPKIndexTestNode(t, failStore{})
	status := pkindexCreate(t, node, &pkindexpb.CompactionRequest{TaskId: 3, SstBasePath: "b"})
	assert.False(t, merr.Ok(status))
	assert.ErrorIs(t, merr.Error(status), merr.ErrServiceInternal)
}

func TestPKIndexCompactionCorruptPayload(t *testing.T) {
	node := newPKIndexTestNode(t, failStore{})
	status, err := node.CreateTask(context.Background(), &workerpb.CreateTaskRequest{
		Properties: pkindexProps(4),
		Payload:    []byte{0xff},
	})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(status), merr.ErrServiceInternal)
}

func TestPKIndexCompactionDisabledWithoutStorageAddress(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "minio")
	pt.Save(pt.MinioCfg.Address.Key, "")
	defer pt.Reset(pt.CommonCfg.StorageType.Key)
	defer pt.Reset(pt.MinioCfg.Address.Key)

	node := NewDataNode(context.Background())
	defer node.cancel()
	require.NoError(t, node.initPKIndexCompaction())
	assert.Nil(t, node.pkindexCompaction)
	assert.Equal(t, int64(0), node.pkindexCompactionSlots())
}

func TestPKIndexCompactionNotInitialized(t *testing.T) {
	node := newPKIndexTestNode(t, failStore{})
	node.pkindexCompaction = nil
	assert.Equal(t, int64(0), node.pkindexCompactionSlots())
	assert.False(t, merr.Ok(pkindexCreate(t, node, validPKIndexRequest(1))))
	_, _, _, status := pkindexQuery(t, node, 1)
	assert.False(t, merr.Ok(status))
	dropStatus, err := node.DropTask(context.Background(), &workerpb.DropTaskRequest{Properties: pkindexProps(1)})
	require.NoError(t, err)
	assert.False(t, merr.Ok(dropStatus))
}

func TestPKIndexCompactionQuerySlot(t *testing.T) {
	gate := make(chan struct{})
	node := newPKIndexTestNode(t, failStore{gate: gate})
	ctx := context.Background()

	idle, err := node.QuerySlot(ctx, &datapb.QuerySlotRequest{})
	require.NoError(t, err)
	require.True(t, merr.Ok(idle.GetStatus()))
	require.GreaterOrEqual(t, idle.GetAvailableSlots(), int64(4))

	require.True(t, merr.Ok(pkindexCreate(t, node, validPKIndexRequest(1))))
	busy, err := node.QuerySlot(ctx, &datapb.QuerySlotRequest{})
	require.NoError(t, err)
	assert.Equal(t, idle.GetAvailableSlots()-4, busy.GetAvailableSlots())

	close(gate)
	assert.Eventually(t, func() bool {
		resp, err := node.QuerySlot(ctx, &datapb.QuerySlotRequest{})
		return err == nil && resp.GetAvailableSlots() == idle.GetAvailableSlots()
	}, 10*time.Second, 10*time.Millisecond)
}

type fakeStorageFactory struct {
	calls int
	fn    func() (storage.ChunkManager, error)
}

func (f *fakeStorageFactory) NewChunkManager(context.Context, *indexpb.StorageConfig) (storage.ChunkManager, error) {
	f.calls++
	return f.fn()
}

func newLazyStoreNode(t *testing.T, factory StorageFactory) *DataNode {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.LocalStorageCfg.Path.Key, t.TempDir())
	t.Cleanup(func() { pt.Reset(pt.LocalStorageCfg.Path.Key) })

	node := NewDataNode(context.Background())
	node.storageFactory = factory
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	require.NoError(t, node.initPKIndexCompaction())
	require.NotNil(t, node.pkindexCompaction)
	node.pkindexCompaction.Start()
	t.Cleanup(func() {
		node.pkindexCompaction.Close()
		node.cancel()
	})
	return node
}

func TestPKIndexCompactionLazyStoreCreationFails(t *testing.T) {
	factory := &fakeStorageFactory{fn: func() (storage.ChunkManager, error) {
		return nil, errors.New("storage unreachable")
	}}
	node := newLazyStoreNode(t, factory)
	require.Zero(t, factory.calls, "Init must not create the chunk manager")

	require.True(t, merr.Ok(pkindexCreate(t, node, validPKIndexRequest(1))))
	require.Eventually(t, func() bool {
		state, _, _, _ := pkindexQuery(t, node, 1)
		return state == taskcommon.Failed
	}, 10*time.Second, 10*time.Millisecond)
	_, reason, result, status := pkindexQuery(t, node, 1)
	assert.NotEmpty(t, reason)
	assert.True(t, result.GetRetriable())
	assert.True(t, merr.Ok(status))
}

func TestPKIndexCompactionLazyStoreCompacts(t *testing.T) {
	root := t.TempDir()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	factory := &fakeStorageFactory{fn: func() (storage.ChunkManager, error) { return cm, nil }}
	node := newLazyStoreNode(t, factory)
	require.Zero(t, factory.calls)

	ctx := context.Background()
	// The local chunk manager takes the key as the path.
	base := root + "/sst/1/v"
	var refs []*pkindexpb.SSTRef
	for id, kv := range map[int64][]string{1: {"a", "old", "c", "c"}, 2: {"a", "new", "b", "b"}} {
		dir := t.TempDir()
		w, err := sst.NewWriter(dir, sst.ID(id))
		require.NoError(t, err)
		for i := 0; i+1 < len(kv); i += 2 {
			require.NoError(t, w.Add([]byte(kv[i]), []byte(kv[i+1])))
		}
		info, err := w.Close()
		require.NoError(t, err)
		content, err := os.ReadFile(dir + "/" + sst.FileName(sst.ID(id)))
		require.NoError(t, err)
		require.NoError(t, cm.Write(ctx, pkindex.SSTKey(base, id), content))
		refs = append(refs, &pkindexpb.SSTRef{Id: id, Size: info.Size, MinKey: info.MinKey, MaxKey: info.MaxKey, NumEntries: info.NumEntries, TableFormat: uint32(info.TableFormat)})
	}
	// Newer table first.
	if refs[0].GetId() == 1 {
		refs[0], refs[1] = refs[1], refs[0]
	}

	require.True(t, merr.Ok(pkindexCreate(t, node, &pkindexpb.CompactionRequest{
		TaskId: 7, CollectionId: 1, Vchannel: "v", SstBasePath: base, Inputs: refs,
		RunId: 9, OutputIdBegin: 100, OutputIdEnd: 110, TargetFileSize: 1 << 20,
	})))
	require.Eventually(t, func() bool {
		state, _, _, _ := pkindexQuery(t, node, 7)
		return state == taskcommon.Finished
	}, 10*time.Second, 10*time.Millisecond)
	_, _, result, _ := pkindexQuery(t, node, 7)
	require.Len(t, result.GetOutputs(), 1)
	assert.EqualValues(t, 3, result.GetOutputs()[0].GetNumEntries())
	assert.Equal(t, 1, factory.calls)
}
