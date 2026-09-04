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

package index

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/analyzecgowrapper"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/timerecord"
)

type IndexBuildTaskSuite struct {
	suite.Suite
	schema       *schemapb.CollectionSchema
	collectionID int64
	partitionID  int64
	segmentID    int64
	dataPath     string
	rootPath     string

	numRows int
	dim     int
}

type emptyIndex struct {
	deleted bool
}

func (*emptyIndex) Build(*indexcgowrapper.Dataset) error                        { return nil }
func (*emptyIndex) Serialize() ([]*indexcgowrapper.Blob, error)                 { return nil, nil }
func (*emptyIndex) GetIndexFileInfo() ([]*indexcgowrapper.IndexFileInfo, error) { return nil, nil }
func (*emptyIndex) Load([]*indexcgowrapper.Blob) error                          { return nil }
func (index *emptyIndex) Delete() error {
	index.deleted = true
	return nil
}
func (*emptyIndex) CleanLocalData() error { return nil }
func (*emptyIndex) UpLoad() (*cgopb.IndexStats, error) {
	return &cgopb.IndexStats{}, nil
}

func (suite *IndexBuildTaskSuite) SetupSuite() {
	paramtable.Init()
	suite.collectionID = 1000
	suite.partitionID = 1001
	suite.segmentID = 1002
	suite.rootPath = suite.T().TempDir() + "/data"
	suite.dataPath = suite.rootPath + "/1000/1001/1002/3/1"
	suite.numRows = 100
	suite.dim = 128
}

func (suite *IndexBuildTaskSuite) SetupTest() {
	suite.schema = &schemapb.CollectionSchema{
		Name:        "test",
		Description: "test",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "ts", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
		},
	}
}

func (suite *IndexBuildTaskSuite) serializeData() ([]*storage.Blob, error) {
	insertCodec := storage.NewInsertCodecWithSchema(&etcdpb.CollectionMeta{
		Schema: suite.schema,
	})
	return insertCodec.Serialize(suite.partitionID, suite.segmentID, &storage.InsertData{
		Data: map[storage.FieldID]storage.FieldData{
			0:   &storage.Int64FieldData{Data: generateLongs(suite.numRows)},
			1:   &storage.Int64FieldData{Data: generateLongs(suite.numRows)},
			100: &storage.Int64FieldData{Data: generateLongs(suite.numRows)},
			101: &storage.Int64FieldData{Data: generateLongs(suite.numRows)},
			102: &storage.FloatVectorFieldData{Data: generateFloats(suite.numRows * suite.dim), Dim: suite.dim},
		},
		Infos: []storage.BlobInfo{{Length: suite.numRows}},
	})
}

func (suite *IndexBuildTaskSuite) TestPreExecuteFiltersRuntimeOnlyIndexParams() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req := &workerpb.CreateJobRequest{
		BuildID:      1,
		CollectionID: suite.collectionID,
		PartitionID:  suite.partitionID,
		SegmentID:    suite.segmentID,
		DataPaths:    []string{suite.dataPath},
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "FLAT"},
			{Key: common.MetricTypeKey, Value: metric.L2},
			{Key: common.MmapEnabledKey, Value: "true"},
			{Key: common.IndexOffsetCacheEnabledKey, Value: "true"},
			{Key: common.WarmupKey, Value: common.WarmupDisable},
			{Key: common.EvictableKey, Value: "false"},
		},
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
			{Key: common.MmapEnabledKey, Value: "true"},
			{Key: common.IndexOffsetCacheEnabledKey, Value: "true"},
			{Key: common.WarmupKey, Value: common.WarmupDisable},
			{Key: common.EvictableKey, Value: "false"},
		},
		Field: &schemapb.FieldSchema{
			FieldID:  102,
			Name:     "vec",
			DataType: schemapb.DataType_FloatVector,
		},
	}
	task := NewIndexBuildTask(ctx, cancel, req, nil, NewTaskManager(ctx), nil)

	suite.NoError(task.PreExecute(ctx))
	suite.Equal("FLAT", task.newIndexParams[common.IndexTypeKey])
	suite.Equal(metric.L2, task.newIndexParams[common.MetricTypeKey])
	suite.Equal("128", task.newTypeParams[common.DimKey])
	suite.Equal("128", task.newIndexParams[common.DimKey])
	for _, key := range []string{
		common.MmapEnabledKey,
		common.IndexOffsetCacheEnabledKey,
		common.WarmupKey,
		common.EvictableKey,
	} {
		suite.NotContains(task.newTypeParams, key)
		suite.NotContains(task.newIndexParams, key)
	}
}

func (suite *IndexBuildTaskSuite) TestBuildMemoryIndex() {
	ctx, cancel := context.WithCancel(context.Background())
	req := &workerpb.CreateJobRequest{
		BuildID:      1,
		IndexVersion: 1,
		DataPaths:    []string{suite.dataPath},
		IndexID:      0,
		IndexName:    "",
		IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}, {Key: common.MetricTypeKey, Value: metric.L2}},
		TypeParams:   []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}},
		NumRows:      int64(suite.numRows),
		StorageConfig: &indexpb.StorageConfig{
			RootPath:    suite.rootPath,
			StorageType: "local",
		},
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		FieldID:      102,
		FieldName:    "vec",
		FieldType:    schemapb.DataType_FloatVector,
	}

	cm, err := dependency.NewDefaultFactory(true).NewPersistentStorageChunkManager(ctx)
	suite.NoError(err)
	blobs, err := suite.serializeData()
	suite.NoError(err)
	err = cm.Write(ctx, suite.dataPath, blobs[0].Value)
	suite.NoError(err)

	t := NewIndexBuildTask(ctx, cancel, req, cm, NewTaskManager(context.Background()), nil)

	err = t.PreExecute(context.Background())
	suite.NoError(err)
	err = t.Execute(context.Background())
	suite.NoError(err)
	err = t.PostExecute(context.Background())
	suite.NoError(err)
}

func (suite *IndexBuildTaskSuite) TestExecuteDoesNotLogStorageCredentials() {
	logs := captureStatsTaskLogs(suite.T())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	accessKey := statsLogSentinel("INDEX", "ACCESS", "KEY", "SENTINEL")
	secretKey := statsLogSentinel("INDEX", "SECRET", "KEY", "SENTINEL")
	caCert := statsLogSentinel("INDEX", "CA", "CERT", "SENTINEL")
	gcpCredential := statsLogSentinel("INDEX", "GCP", "CREDENTIAL", "SENTINEL")
	externalSpecSecret := statsLogSentinel("INDEX", "EXTERNAL", "SPEC", "SENTINEL")
	externalSpec := `{"format":"parquet","extfs":{"future_secret":"` + externalSpecSecret + `"}}`

	createIndexPatch := mockey.Mock(indexcgowrapper.CreateIndex).To(func(_ context.Context, params *indexcgopb.BuildIndexInfo) (indexcgowrapper.CodecIndex, error) {
		suite.Equal(accessKey, params.GetStorageConfig().GetAccessKeyID())
		suite.Equal(secretKey, params.GetStorageConfig().GetSecretAccessKey())
		suite.Equal(caCert, params.GetStorageConfig().GetSslCACert())
		suite.Contains(params.GetStorageConfig().GetGcpCredentialJSON(), gcpCredential)
		suite.Equal(externalSpec, params.GetExternalSpec())
		return nil, nil
	}).Build()
	defer createIndexPatch.UnPatch()

	req := &workerpb.CreateJobRequest{
		ClusterID:    "test-cluster",
		BuildID:      1,
		CollectionID: suite.collectionID,
		PartitionID:  suite.partitionID,
		SegmentID:    suite.segmentID,
		NumRows:      int64(suite.numRows),
		Dim:          int64(suite.dim),
		Field: &schemapb.FieldSchema{
			FieldID:  102,
			Name:     "vec",
			DataType: schemapb.DataType_FloatVector,
		},
		StorageConfig: &indexpb.StorageConfig{
			Address:           "storage.example.test",
			StorageType:       "s3",
			BucketName:        "index-bucket",
			RootPath:          "index/root",
			AccessKeyID:       accessKey,
			SecretAccessKey:   secretKey,
			SslCACert:         caCert,
			GcpCredentialJSON: statsLogCredentialJSON(gcpCredential),
		},
		StorageVersion: storage.StorageV2,
		ExternalSource: "s3://index-bucket/data",
		ExternalSpec:   externalSpec,
	}

	task := NewIndexBuildTask(ctx, cancel, req, nil, NewTaskManager(ctx), nil)
	task.newIndexParams = map[string]string{
		common.IndexTypeKey:  "FLAT",
		common.MetricTypeKey: metric.L2,
	}
	task.newTypeParams = map[string]string{"dim": "128"}

	err := task.Execute(ctx)
	suite.NoError(err)
	output := logs.String()
	suite.NotContains(output, accessKey)
	suite.NotContains(output, secretKey)
	suite.NotContains(output, caCert)
	suite.NotContains(output, gcpCredential)
	suite.NotContains(output, externalSpecSecret)
	suite.Contains(output, "storage.example.test")
	suite.Contains(output, "index-bucket")
	suite.Contains(output, "index/root")
	suite.Contains(output, "s3")
	suite.Contains(output, "<redacted>")
}

func (suite *IndexBuildTaskSuite) TestPostExecuteAcceptsEmptyIndexStats() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const (
		clusterID                 = "empty-nested-index"
		buildID                   = int64(1)
		currentIndexVersion       = int32(2)
		currentScalarIndexVersion = int32(3)
	)
	manager := NewTaskManager(ctx)
	manager.LoadOrStoreIndexTask(clusterID, buildID, &IndexTaskInfo{})

	req := &workerpb.CreateJobRequest{
		ClusterID:                 clusterID,
		BuildID:                   buildID,
		CurrentIndexVersion:       currentIndexVersion,
		CurrentScalarIndexVersion: currentScalarIndexVersion,
	}
	task := NewIndexBuildTask(ctx, cancel, req, nil, manager, nil)
	index := &emptyIndex{}
	task.index = index

	suite.NoError(task.PostExecute(ctx))
	suite.True(index.deleted)

	info := manager.GetIndexTaskInfo(clusterID, buildID)
	suite.Require().NotNil(info)
	suite.Empty(info.FileKeys)
	suite.Zero(info.SerializedSize)
	suite.Zero(info.MemSize)
	suite.Equal(currentIndexVersion, info.CurrentIndexVersion)
	suite.Equal(currentScalarIndexVersion, info.CurrentScalarIndexVersion)
}

func (suite *IndexBuildTaskSuite) TestMaxConnectionsReachesCreateIndex() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var captured *indexcgopb.BuildIndexInfo
	patch := mockey.Mock(indexcgowrapper.CreateIndex).To(
		func(_ context.Context, info *indexcgopb.BuildIndexInfo) (indexcgowrapper.CodecIndex, error) {
			captured = info
			return nil, nil
		}).Build()
	defer patch.UnPatch()

	req := &workerpb.CreateJobRequest{
		BuildID:     1,
		NumRows:     int64(suite.numRows),
		Dim:         int64(suite.dim),
		IndexParams: []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}},
		Field: &schemapb.FieldSchema{
			FieldID:  102,
			Name:     "vec",
			DataType: schemapb.DataType_FloatVector,
		},
		StorageConfig: &indexpb.StorageConfig{
			StorageType:    "minio",
			MaxConnections: 237,
		},
	}
	task := NewIndexBuildTask(ctx, cancel, req, nil, NewTaskManager(context.Background()), nil)
	task.newIndexParams = map[string]string{common.IndexTypeKey: "FLAT"}
	task.newTypeParams = map[string]string{"dim": "128"}
	task.tr = timerecord.NewTimeRecorder("test-max-connections")

	err := task.Execute(ctx)
	suite.NoError(err)
	suite.Require().NotNil(captured)
	suite.Equal(uint32(237), captured.GetStorageConfig().GetMaxConnections())
}

func TestIndexBuildTask(t *testing.T) {
	suite.Run(t, new(IndexBuildTaskSuite))
}

type AnalyzeTaskSuite struct {
	suite.Suite
	schema       *schemapb.CollectionSchema
	collectionID int64
	partitionID  int64
	segmentID    int64
	fieldID      int64
	taskID       int64
}

func (suite *AnalyzeTaskSuite) SetupSuite() {
	paramtable.Init()
	suite.collectionID = 1000
	suite.partitionID = 1001
	suite.segmentID = 1002
	suite.fieldID = 102
	suite.taskID = 1004
}

func (suite *AnalyzeTaskSuite) SetupTest() {
	suite.schema = &schemapb.CollectionSchema{
		Name:        "test",
		Description: "test",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "ts", DataType: schemapb.DataType_Int64},
			{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "1"}}},
		},
	}
}

func (suite *AnalyzeTaskSuite) serializeData() ([]*storage.Blob, error) {
	insertCodec := storage.NewInsertCodecWithSchema(&etcdpb.CollectionMeta{
		Schema: suite.schema,
	})
	return insertCodec.Serialize(suite.partitionID, suite.segmentID, &storage.InsertData{
		Data: map[storage.FieldID]storage.FieldData{
			0:   &storage.Int64FieldData{Data: []int64{0, 1, 2}},
			1:   &storage.Int64FieldData{Data: []int64{1, 2, 3}},
			100: &storage.Int64FieldData{Data: []int64{0, 1, 2}},
			101: &storage.Int64FieldData{Data: []int64{0, 1, 2}},
			102: &storage.FloatVectorFieldData{Data: []float32{1, 2, 3}, Dim: 1},
		},
		Infos: []storage.BlobInfo{{Length: 3}},
	})
}

func (suite *AnalyzeTaskSuite) TestAnalyze() {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel is deferred below
	defer cancel()
	req := &workerpb.AnalyzeRequest{
		ClusterID:    "test",
		TaskID:       1,
		CollectionID: suite.collectionID,
		PartitionID:  suite.partitionID,
		FieldID:      suite.fieldID,
		FieldName:    "vec",
		FieldType:    schemapb.DataType_FloatVector,
		SegmentStats: map[int64]*indexpb.SegmentStats{
			suite.segmentID: {
				ID:      suite.segmentID,
				NumRows: 1024,
				LogIDs:  []int64{1},
			},
		},
		Version: 1,
		StorageConfig: &indexpb.StorageConfig{
			RootPath:    suite.T().TempDir() + "/data",
			StorageType: "local",
		},
		Dim: 1,
	}

	cm, err := dependency.NewDefaultFactory(true).NewPersistentStorageChunkManager(ctx)
	suite.NoError(err)
	blobs, err := suite.serializeData()
	suite.NoError(err)
	dataPath := metautil.BuildInsertLogPath(cm.RootPath(), suite.collectionID, suite.partitionID, suite.segmentID,
		suite.fieldID, 1)

	err = cm.Write(ctx, dataPath, blobs[0].Value)
	suite.NoError(err)

	t := &analyzeTask{
		ident:    "",
		cancel:   cancel,
		ctx:      ctx,
		req:      req,
		tr:       timerecord.NewTimeRecorder("test-indexBuildTask"),
		queueDur: 0,
		manager:  NewTaskManager(context.Background()),
	}

	err = t.PreExecute(context.Background())
	suite.NoError(err)
}

func (suite *AnalyzeTaskSuite) TestMaxConnectionsReachesAnalyze() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var capturedMaxConnections uint32
	patch := mockey.Mock(analyzecgowrapper.Analyze).To(
		func(_ context.Context, info *clusteringpb.AnalyzeInfo, _ *indexcgopb.StoragePluginContext) (analyzecgowrapper.CodecAnalyze, error) {
			capturedMaxConnections = info.GetStorageConfig().GetMaxConnections()
			return nil, nil
		}).Build()
	defer patch.UnPatch()

	req := &workerpb.AnalyzeRequest{
		TaskID:       suite.taskID,
		CollectionID: suite.collectionID,
		PartitionID:  suite.partitionID,
		FieldID:      suite.fieldID,
		FieldName:    "vec",
		FieldType:    schemapb.DataType_FloatVector,
		Dim:          128,
		StorageConfig: &indexpb.StorageConfig{
			StorageType:    "minio",
			RootPath:       "files",
			MaxConnections: 237,
		},
	}
	task := &analyzeTask{
		ctx:    ctx,
		cancel: cancel,
		req:    req,
		tr:     timerecord.NewTimeRecorder("test-analyze-max-connections"),
	}

	err := task.Execute(ctx)
	suite.NoError(err)
	suite.Equal(uint32(237), capturedMaxConnections)
}

func TestAnalyzeTaskSuite(t *testing.T) {
	suite.Run(t, new(AnalyzeTaskSuite))
}
