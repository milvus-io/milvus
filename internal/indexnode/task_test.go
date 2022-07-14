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

package indexnode

import (
	"context"
	"errors"
	"math/rand"
	"path"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus/internal/kv"

	"github.com/golang/protobuf/proto"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/stretchr/testify/assert"
)

func TestIndexBuildTask_saveIndexMeta(t *testing.T) {
	Params.Init()
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.NoError(t, err)
	assert.NotNil(t, etcdCli)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	assert.NotNil(t, etcdKV)
	indexBuildID := rand.Int63()
	indexMeta := &indexpb.IndexMeta{
		IndexBuildID: indexBuildID,
		State:        commonpb.IndexState_InProgress,
		NodeID:       1,
		IndexVersion: 1,
	}
	metaPath := path.Join("indexes", strconv.FormatInt(indexMeta.IndexBuildID, 10))
	metaValue, err := proto.Marshal(indexMeta)
	assert.NoError(t, err)
	err = etcdKV.Save(metaPath, string(metaValue))
	assert.NoError(t, err)
	indexBuildTask := &IndexBuildTask{
		BaseTask: BaseTask{
			internalErr: errors.New("internal err"),
		},
		etcdKV: etcdKV,
		req: &indexpb.CreateIndexRequest{
			IndexBuildID: indexBuildID,
			Version:      1,
			MetaPath:     metaPath,
		},
		tr: &timerecord.TimeRecorder{},
	}
	err = indexBuildTask.saveIndexMeta(context.Background())
	assert.NoError(t, err)

	indexMeta2, _, err := indexBuildTask.loadIndexMeta(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, indexMeta2)
	assert.Equal(t, commonpb.IndexState_Unissued, indexMeta2.State)

	err = etcdKV.Remove(metaPath)
	assert.NoError(t, err)
}

type mockChunkManager struct {
	storage.ChunkManager

	read func(key string) ([]byte, error)
}

func (mcm *mockChunkManager) Read(key string) ([]byte, error) {
	return mcm.read(key)
}

func TestIndexBuildTask_Execute(t *testing.T) {
	t.Run("task retry", func(t *testing.T) {
		indexTask := &IndexBuildTask{
			cm: &mockChunkManager{
				read: func(key string) ([]byte, error) {
					return nil, errors.New("error occurred")
				},
			},
			req: &indexpb.CreateIndexRequest{
				IndexBuildID: 1,
				DataPaths:    []string{"path1", "path2"},
			},
		}

		err := indexTask.Execute(context.Background())
		assert.Error(t, err)
		assert.Equal(t, TaskStateRetry, indexTask.state)
	})

	t.Run("task failed", func(t *testing.T) {
		indexTask := &IndexBuildTask{
			cm: &mockChunkManager{
				read: func(key string) ([]byte, error) {
					return nil, ErrNoSuchKey
				},
			},
			req: &indexpb.CreateIndexRequest{
				IndexBuildID: 1,
				DataPaths:    []string{"path1", "path2"},
			},
		}

		err := indexTask.Execute(context.Background())
		assert.ErrorIs(t, err, ErrNoSuchKey)
		assert.Equal(t, TaskStateFailed, indexTask.state)

	})
}

type mockETCDKV struct {
	kv.MetaKv

	loadWithPrefix2 func(key string) ([]string, []string, []int64, error)
}

func (mk *mockETCDKV) LoadWithPrefix2(key string) ([]string, []string, []int64, error) {
	return mk.loadWithPrefix2(key)
}

func TestIndexBuildTask_loadIndexMeta(t *testing.T) {
	t.Run("load empty meta", func(t *testing.T) {
		indexTask := &IndexBuildTask{
			etcdKV: &mockETCDKV{
				loadWithPrefix2: func(key string) ([]string, []string, []int64, error) {
					return []string{}, []string{}, []int64{}, nil
				},
			},
			req: &indexpb.CreateIndexRequest{
				IndexBuildID: 1,
				DataPaths:    []string{"path1", "path2"},
			},
		}

		indexMeta, revision, err := indexTask.loadIndexMeta(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, int64(0), revision)
		assert.Equal(t, TaskStateAbandon, indexTask.GetState())

		indexTask.updateTaskState(indexMeta, nil)
		assert.Equal(t, TaskStateAbandon, indexTask.GetState())
	})
}

func TestIndexBuildTask_saveIndex(t *testing.T) {
	t.Run("save index failed", func(t *testing.T) {
		indexTask := &IndexBuildTask{
			etcdKV: &mockETCDKV{
				loadWithPrefix2: func(key string) ([]string, []string, []int64, error) {
					return []string{}, []string{}, []int64{}, errors.New("error")
				},
			},
			partitionID: 1,
			segmentID:   1,
			req: &indexpb.CreateIndexRequest{
				IndexBuildID: 1,
				DataPaths:    []string{"path1", "path2"},
				Version:      1,
			},
		}

		blobs := []*storage.Blob{
			{
				Key:   "key1",
				Value: []byte("value1"),
			},
			{
				Key:   "key2",
				Value: []byte("value2"),
			},
		}

		err := indexTask.saveIndex(context.Background(), blobs)
		assert.Error(t, err)
	})
}
