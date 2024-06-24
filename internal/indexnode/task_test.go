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
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	milvus_storage "github.com/milvus-io/milvus-storage/go/storage"
	"github.com/milvus-io/milvus-storage/go/storage/options"
	"github.com/milvus-io/milvus-storage/go/storage/schema"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
)

// import (
// 	"context"
// 	"github.com/cockroachdb/errors"
// 	"math/rand"
// 	"path"
// 	"strconv"
// 	"testing"

// 	"github.com/milvus-io/milvus/internal/kv"

// 	"github.com/golang/protobuf/proto"
// 	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
// 	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
// 	"github.com/milvus-io/milvus/internal/proto/indexpb"
// 	"github.com/milvus-io/milvus/internal/storage"
// 	"github.com/milvus-io/milvus/pkg/util/etcd"
// 	"github.com/milvus-io/milvus/pkg/util/timerecord"
// 	"github.com/stretchr/testify/assert"
// )

// func TestIndexBuildTask_saveIndexMeta(t *testing.T) {
// 	Params.Init()
// 	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
// 	assert.NoError(t, err)
// 	assert.NotNil(t, etcdCli)
// 	etcdKV := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
// 	assert.NotNil(t, etcdKV)
// 	indexBuildID := rand.Int63()
// 	indexMeta := &indexpb.IndexMeta{
// 		IndexBuildID: indexBuildID,
// 		State:        commonpb.IndexState_InProgress,
// 		NodeID:       1,
// 		IndexVersion: 1,
// 	}
// 	metaPath := path.Join("indexes", strconv.FormatInt(indexMeta.IndexBuildID, 10))
// 	metaValue, err := proto.Marshal(indexMeta)
// 	assert.NoError(t, err)
// 	err = etcdKV.Save(metaPath, string(metaValue))
// 	assert.NoError(t, err)
// 	indexBuildTask := &IndexBuildTask{
// 		BaseTask: BaseTask{
// 			internalErr: errors.New("internal err"),
// 		},
// 		etcdKV: etcdKV,
// 		req: &indexpb.CreateIndexRequest{
// 			IndexBuildID: indexBuildID,
// 			Version:      1,
// 			MetaPath:     metaPath,
// 		},
// 		tr: &timerecord.TimeRecorder{},
// 	}
// 	err = indexBuildTask.saveIndexMeta(context.Background())
// 	assert.NoError(t, err)

// 	indexMeta2, _, err := indexBuildTask.loadIndexMeta(context.Background())
// 	assert.NoError(t, err)
// 	assert.NotNil(t, indexMeta2)
// 	assert.Equal(t, commonpb.IndexState_Unissued, indexMeta2.State)

// 	err = etcdKV.Remove(metaPath)
// 	assert.NoError(t, err)
// }

// type mockChunkManager struct {
// 	storage.ChunkManager

// 	read func(key string) ([]byte, error)
// }

// func (mcm *mockChunkManager) Read(key string) ([]byte, error) {
// 	return mcm.read(key)
// }

// func TestIndexBuildTask_Execute(t *testing.T) {
// 	t.Run("task retry", func(t *testing.T) {
// 		indexTask := &IndexBuildTask{
// 			cm: &mockChunkManager{
// 				read: func(key string) ([]byte, error) {
// 					return nil, errors.New("error occurred")
// 				},
// 			},
// 			req: &indexpb.CreateIndexRequest{
// 				IndexBuildID: 1,
// 				DataPaths:    []string{"path1", "path2"},
// 			},
// 		}

// 		err := indexTask.Execute(context.Background())
// 		assert.Error(t, err)
// 		assert.Equal(t, TaskStateRetry, indexTask.state)
// 	})

// 	t.Run("task failed", func(t *testing.T) {
// 		indexTask := &IndexBuildTask{
// 			cm: &mockChunkManager{
// 				read: func(key string) ([]byte, error) {
// 					return nil, ErrNoSuchKey
// 				},
// 			},
// 			req: &indexpb.CreateIndexRequest{
// 				IndexBuildID: 1,
// 				DataPaths:    []string{"path1", "path2"},
// 			},
// 		}

// 		err := indexTask.Execute(context.Background())
// 		assert.ErrorIs(t, err, ErrNoSuchKey)
// 		assert.Equal(t, TaskStateFailed, indexTask.state)

// 	})
// }

// type mockETCDKV struct {
// 	kv.MetaKv

// 	loadWithPrefix2 func(key string) ([]string, []string, []int64, error)
// }

// func TestIndexBuildTask_loadIndexMeta(t *testing.T) {
// 	t.Run("load empty meta", func(t *testing.T) {
// 		indexTask := &IndexBuildTask{
// 			etcdKV: &mockETCDKV{
// 				loadWithPrefix2: func(key string) ([]string, []string, []int64, error) {
// 					return []string{}, []string{}, []int64{}, nil
// 				},
// 			},
// 			req: &indexpb.CreateIndexRequest{
// 				IndexBuildID: 1,
// 				DataPaths:    []string{"path1", "path2"},
// 			},
// 		}

// 		indexMeta, revision, err := indexTask.loadIndexMeta(context.Background())
// 		assert.NoError(t, err)
// 		assert.Equal(t, int64(0), revision)
// 		assert.Equal(t, TaskStateAbandon, indexTask.GetState())

// 		indexTask.updateTaskState(indexMeta, nil)
// 		assert.Equal(t, TaskStateAbandon, indexTask.GetState())
// 	})
// }

// func TestIndexBuildTask_saveIndex(t *testing.T) {
// 	t.Run("save index failed", func(t *testing.T) {
// 		indexTask := &IndexBuildTask{
// 			etcdKV: &mockETCDKV{
// 				loadWithPrefix2: func(key string) ([]string, []string, []int64, error) {
// 					return []string{}, []string{}, []int64{}, errors.New("error")
// 				},
// 			},
// 			partitionID: 1,
// 			segmentID:   1,
// 			req: &indexpb.CreateIndexRequest{
// 				IndexBuildID: 1,
// 				DataPaths:    []string{"path1", "path2"},
// 				Version:      1,
// 			},
// 		}

// 		blobs := []*storage.Blob{
// 			{
// 				Key:   "key1",
// 				Value: []byte("value1"),
// 			},
// 			{
// 				Key:   "key2",
// 				Value: []byte("value2"),
// 			},
// 		}

// 		err := indexTask.saveIndex(context.Background(), blobs)
// 		assert.Error(t, err)
// 	})
// }

type IndexBuildTaskV2Suite struct {
	suite.Suite
	schema      *schemapb.CollectionSchema
	arrowSchema *arrow.Schema
	space       *milvus_storage.Space
}

func (suite *IndexBuildTaskV2Suite) SetupSuite() {
	paramtable.Init()
}

func (suite *IndexBuildTaskV2Suite) SetupTest() {
	suite.schema = &schemapb.CollectionSchema{
		Name:        "test",
		Description: "test",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 2, Name: "ts", DataType: schemapb.DataType_Int64},
			{FieldID: 3, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "1"}}},
		},
	}

	var err error
	suite.arrowSchema, err = typeutil.ConvertToArrowSchema(suite.schema.Fields)
	suite.NoError(err)

	tmpDir := suite.T().TempDir()
	opt := options.NewSpaceOptionBuilder().
		SetSchema(schema.NewSchema(
			suite.arrowSchema,
			&schema.SchemaOptions{
				PrimaryColumn: "pk",
				VectorColumn:  "vec",
				VersionColumn: "ts",
			})).
		Build()
	suite.space, err = milvus_storage.Open("file://"+tmpDir, opt)
	suite.NoError(err)

	b := array.NewRecordBuilder(memory.DefaultAllocator, suite.arrowSchema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	fb := b.Field(2).(*array.FixedSizeBinaryBuilder)
	fb.Reserve(1)
	fb.Append([]byte{1, 2, 3, 4})

	rec := b.NewRecord()
	defer rec.Release()
	reader, err := array.NewRecordReader(suite.arrowSchema, []arrow.Record{rec})
	suite.NoError(err)
	err = suite.space.Write(reader, &options.DefaultWriteOptions)
	suite.NoError(err)
}

func (suite *IndexBuildTaskV2Suite) TestBuildIndex() {
	req := &indexpb.CreateJobRequest{
		BuildID:      1,
		IndexVersion: 1,
		IndexID:      0,
		IndexName:    "",
		IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}, {Key: common.MetricTypeKey, Value: metric.L2}, {Key: common.DimKey, Value: "1"}},
		TypeParams:   []*commonpb.KeyValuePair{{Key: "dim", Value: "1"}},
		NumRows:      10,
		StorageConfig: &indexpb.StorageConfig{
			RootPath:    "/tmp/milvus/data",
			StorageType: "local",
		},
		CollectionID: 1,
		PartitionID:  1,
		SegmentID:    1,
		Field: &schemapb.FieldSchema{
			FieldID:  3,
			Name:     "vec",
			DataType: schemapb.DataType_FloatVector,
		},
		StorePath:      "file://" + suite.space.Path(),
		StoreVersion:   suite.space.GetCurrentVersion(),
		IndexStorePath: "file://" + suite.space.Path(),
		Dim:            4,
		OptionalScalarFields: []*indexpb.OptionalFieldInfo{
			{FieldID: 1, FieldName: "pk", FieldType: 5, DataIds: []int64{0}},
		},
	}

	task := &indexBuildTaskV2{
		indexBuildTask: &indexBuildTask{
			ident:     "test",
			ctx:       context.Background(),
			BuildID:   req.GetBuildID(),
			ClusterID: req.GetClusterID(),
			req:       req,
			tr:        timerecord.NewTimeRecorder("test"),
			node:      NewIndexNode(context.Background(), dependency.NewDefaultFactory(true)),
		},
	}

	var err error
	err = task.Prepare(context.Background())
	suite.NoError(err)
	err = task.BuildIndex(context.Background())
	suite.NoError(err)
	err = task.SaveIndexFiles(context.Background())
	suite.NoError(err)
}

func TestIndexBuildTaskV2Suite(t *testing.T) {
	suite.Run(t, new(IndexBuildTaskV2Suite))
}
