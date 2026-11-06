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
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/compaction"
	pkcompaction "github.com/milvus-io/milvus/internal/datanode/pkindex/compaction"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/objstore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// lazyStore is the object store of the primary key index compaction executor.
// It creates the chunk manager on the first task, so that a node whose object
// storage is not reachable yet still starts. A failed creation is marked
// pkindex.ErrUnavailable and is not kept: the next task tries again.
type lazyStore struct {
	newChunkManager func(ctx context.Context) (storage.ChunkManager, error)
	ctx             context.Context

	mu    sync.Mutex
	store *objstore.Store
}

func (s *lazyStore) get() (*objstore.Store, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.store != nil {
		return s.store, nil
	}
	cm, err := s.newChunkManager(s.ctx)
	if err != nil {
		return nil, errors.Mark(errors.Wrap(err, "create the chunk manager of the primary key index compaction"), pkindex.ErrUnavailable)
	}
	s.store = objstore.New(cm)
	return s.store, nil
}

func (s *lazyStore) UploadFile(ctx context.Context, key, path string) error {
	store, err := s.get()
	if err != nil {
		return err
	}
	return store.UploadFile(ctx, key, path)
}

func (s *lazyStore) DownloadFile(ctx context.Context, key, path string) error {
	store, err := s.get()
	if err != nil {
		return err
	}
	return store.DownloadFile(ctx, key, path)
}

// initPKIndexCompaction builds the primary key index compaction executor. When
// the storage is not local and has no address, it leaves the executor nil: the
// task RPCs then answer not-ready. Otherwise it returns the error of the local
// directory. The chunk manager is created on the first task, not here.
func (node *DataNode) initPKIndexCompaction() error {
	storageConfig := compaction.CreateStorageConfig()
	if storageConfig.GetStorageType() != "local" && storageConfig.GetAddress() == "" {
		mlog.Info(node.ctx, "No storage address configured, primary key index compaction is disabled")
		return nil
	}
	store := &lazyStore{
		ctx: node.ctx,
		newChunkManager: func(ctx context.Context) (storage.ChunkManager, error) {
			return node.storageFactory.NewChunkManager(ctx, compaction.CreateStorageConfig())
		},
	}
	cfg := pkcompaction.ConfigFromParams()
	exec, err := pkcompaction.New(node.ctx, cfg, store)
	if err != nil {
		return err
	}
	exec.SetNodeID(node.GetNodeID())
	node.pkindexCompaction = exec
	return nil
}

// pkindexCompactionSlots returns the slots that the running primary key index
// compaction tasks use. It returns 0 before the executor exists.
func (node *DataNode) pkindexCompactionSlots() int64 {
	if node.pkindexCompaction == nil {
		return 0
	}
	return node.pkindexCompaction.Slots()
}

func (node *DataNode) pkindexCompactionNotReady() error {
	if node.pkindexCompaction == nil {
		return merr.WrapErrServiceNotReady(paramtable.GetRole(), node.GetNodeID(), "pkindex compaction not initialized")
	}
	return nil
}

func (node *DataNode) createPKIndexCompaction(ctx context.Context, req *pkindexpb.CompactionRequest) (*commonpb.Status, error) {
	if err := node.pkindexCompactionNotReady(); err != nil {
		return merr.Status(err), nil
	}
	if err := node.pkindexCompaction.Create(ctx, req); err != nil {
		mlog.Warn(ctx, "pkindex compaction task rejected", mlog.FieldTaskID(req.GetTaskId()), mlog.Err(err))
		return merr.Status(pkindex.ToMerr(err)), nil
	}
	return merr.Success(), nil
}

// queryPKIndexCompaction answers a QueryTask. A task this node does not know
// is reported as None: the coordinator dispatches it again.
func (node *DataNode) queryPKIndexCompaction(_ context.Context, taskID int64) (*workerpb.QueryTaskResponse, error) {
	if err := node.pkindexCompactionNotReady(); err != nil {
		return &workerpb.QueryTaskResponse{Status: merr.Status(err)}, nil
	}
	state, reason, result, ok := node.pkindexCompaction.Query(taskID)
	if !ok || result == nil {
		result = &pkindexpb.CompactionResult{}
	}
	if !ok {
		state = taskcommon.None
	}
	result.Status = merr.Success()
	properties := taskcommon.NewProperties(nil)
	properties.AppendTaskState(state)
	properties.AppendReason(reason)
	return wrapQueryTaskResult(result, properties)
}

func (node *DataNode) dropPKIndexCompaction(_ context.Context, taskID int64) (*commonpb.Status, error) {
	if err := node.pkindexCompactionNotReady(); err != nil {
		return merr.Status(err), nil
	}
	node.pkindexCompaction.Drop(taskID)
	return merr.Success(), nil
}
