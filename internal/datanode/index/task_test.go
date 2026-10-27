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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/analyzecgowrapper"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
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

func (*emptyIndex) LoadData() error                { return nil }
func (*emptyIndex) TryReserveBuild() (bool, error) { return true, nil }
func (*emptyIndex) Build() error                   { return nil }
func (index *emptyIndex) Delete() error {
	index.deleted = true
	return nil
}

func (*emptyIndex) UpLoad() (*cgopb.IndexStats, error) {
	return &cgopb.IndexStats{}, nil
}

func (suite *IndexBuildTaskSuite) SetupSuite() {
	paramtable.Init()
	suite.collectionID = 1000
	suite.partitionID = 1001
	suite.segmentID = 1002
	suite.rootPath = suite.T().TempDir() + "/data"
	suite.Require().NoError(initcore.InitLocalChunkManager(suite.rootPath))
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
	var vectorBlob *storage.Blob
	for _, blob := range blobs {
		if blob.Key == "102" {
			vectorBlob = blob
			break
		}
	}
	suite.Require().NotNil(vectorBlob)
	err = cm.Write(ctx, suite.dataPath, vectorBlob.Value)
	suite.NoError(err)

	t := NewIndexBuildTask(ctx, cancel, req, cm, NewTaskManager(context.Background()), nil)

	err = t.PreExecute(context.Background())
	suite.NoError(err)
	err = t.LoadData(context.Background())
	suite.NoError(err)
	err = t.Execute(context.Background())
	suite.NoError(err)
	err = t.PostExecute(context.Background())
	suite.NoError(err)
}

func (suite *IndexBuildTaskSuite) TestBuildReturnsNativeErrorAfterCancellation() {
	for _, when := range []string{"before native call", "while handling native error"} {
		suite.Run(when, func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			builder, err := indexcgowrapper.CreateIndexBuilder(ctx, &indexcgopb.BuildIndexInfo{
				CollectionID: 1, PartitionID: 2, SegmentID: 3, BuildID: 4,
				NumRows: 1, Dim: 4,
				FieldSchema: &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_FloatVector},
				IndexParams: []*commonpb.KeyValuePair{
					{Key: common.IndexTypeKey, Value: "FLAT"},
					{Key: common.MetricTypeKey, Value: "L2"},
				},
				TypeParams:    []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
				StorageConfig: &indexcgopb.StorageConfig{StorageType: "local", RootPath: suite.rootPath},
			})
			suite.Require().NoError(err)
			defer builder.Delete()
			if when == "before native call" {
				cancel()
			} else {
				// Cancel after C++ has returned its failure but before Build returns.
				log := mockey.Mock(mlog.Warn).To(func(_ context.Context, message string, _ ...mlog.Field) {
					if strings.Contains(message, "failed to build index") {
						cancel()
					}
				}).Build()
				defer log.UnPatch()
			}
			// A real native call without LoadData reports its invalid state.
			err = builder.Build()
			suite.Require().Error(err)
			suite.ErrorIs(ctx.Err(), context.Canceled)
			suite.NotErrorIs(err, context.Canceled)
			suite.Contains(err.Error(), "requires prepared input")
		})
	}
}

func (suite *IndexBuildTaskSuite) TestBuildLatencyExcludesAdmissionWait() {
	ctx := context.Background()
	task := NewIndexBuildTask(ctx, func() {}, &workerpb.CreateJobRequest{
		BuildID: 901, CollectionID: 1, PartitionID: 2, SegmentID: 3, Dim: 4,
		Field:         &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_FloatVector},
		IndexParams:   []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}},
		TypeParams:    []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
		StorageConfig: &indexpb.StorageConfig{StorageType: "local", RootPath: suite.rootPath},
	}, nil, NewTaskManager(ctx), nil)
	observer := metrics.DataNodeKnowhereBuildIndexLatency.WithLabelValues(paramtable.GetStringNodeID())
	readMetric := func() *dto.Histogram {
		value := &dto.Metric{}
		suite.Require().NoError(observer.(prometheus.Metric).Write(value))
		return value.GetHistogram()
	}
	before := readMetric()
	var elapsed atomic.Int64
	clock := mockey.Mock(time.Now).To(func() time.Time {
		return time.Unix(1000, elapsed.Load())
	}).Build()
	defer clock.UnPatch()
	prepare := mockey.Mock(getCurrentIndexVersion).To(func(version int32) int32 {
		elapsed.Add(int64(2 * time.Second))
		return version
	}).Build()
	defer prepare.UnPatch()
	create := mockey.Mock(indexcgowrapper.CreateIndexBuilder).To(func(context.Context, *indexcgopb.BuildIndexInfo) (indexcgowrapper.PreparedIndex, error) {
		elapsed.Add(int64(time.Second))
		return &emptyIndex{}, nil
	}).Build()
	defer create.UnPatch()
	load := mockey.Mock((*emptyIndex).LoadData).To(func(*emptyIndex) error {
		elapsed.Add(int64(3 * time.Second))
		return nil
	}).Build()
	defer load.UnPatch()
	build := mockey.Mock((*emptyIndex).Build).To(func(*emptyIndex) error {
		elapsed.Add(int64(5 * time.Second))
		return nil
	}).Build()
	defer build.UnPatch()
	task.prepareDur = time.Hour
	suite.Require().NoError(task.OnEnqueue(ctx))
	elapsed.Add(int64(30 * time.Second)) // Initial queueing.
	suite.Require().NoError(task.PreExecute(ctx))
	elapsed.Add(int64(20 * time.Second)) // Dispatch between preparation and loading.
	suite.Require().NoError(task.LoadData(ctx))
	elapsed.Add(int64(100 * time.Second)) // Waiting for build admission.
	suite.Require().NoError(task.Execute(ctx))
	after := readMetric()
	suite.Equal(before.GetSampleCount()+1, after.GetSampleCount())
	suite.InDelta(11.0, after.GetSampleSum()-before.GetSampleSum(), 0.000001)
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

	createIndexPatch := mockey.Mock(indexcgowrapper.CreateIndexBuilder).To(func(_ context.Context, params *indexcgopb.BuildIndexInfo) (indexcgowrapper.PreparedIndex, error) {
		suite.Equal(accessKey, params.GetStorageConfig().GetAccessKeyID())
		suite.Equal(secretKey, params.GetStorageConfig().GetSecretAccessKey())
		suite.Equal(caCert, params.GetStorageConfig().GetSslCACert())
		suite.Contains(params.GetStorageConfig().GetGcpCredentialJSON(), gcpCredential)
		suite.Equal(externalSpec, params.GetExternalSpec())
		return &emptyIndex{}, nil
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

	err := task.LoadData(ctx)
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
	patch := mockey.Mock(indexcgowrapper.CreateIndexBuilder).To(
		func(_ context.Context, info *indexcgopb.BuildIndexInfo) (indexcgowrapper.PreparedIndex, error) {
			captured = info
			return &emptyIndex{}, nil
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
			StorageType:                 "minio",
			MaxConnections:              237,
			TalonMode:                   2,
			TalonSmallReadThreshold:     65536,
			TalonCoordinator:            "talon:7000",
			TalonBlockSize:              8388608,
			TalonMaxIdlePerAddr:         32,
			TalonEnableForExternalTable: true,
		},
	}
	task := NewIndexBuildTask(ctx, cancel, req, nil, NewTaskManager(context.Background()), nil)
	task.newIndexParams = map[string]string{common.IndexTypeKey: "FLAT"}
	task.newTypeParams = map[string]string{"dim": "128"}
	task.tr = timerecord.NewTimeRecorder("test-max-connections")

	err := task.LoadData(ctx)
	suite.NoError(err)
	suite.Require().NotNil(captured)
	suite.Equal(uint32(237), captured.GetStorageConfig().GetMaxConnections())
	suite.Equal(uint32(2), captured.GetStorageConfig().GetTalonMode())
	suite.Equal(uint32(65536), captured.GetStorageConfig().GetTalonSmallReadThreshold())
	suite.Equal("talon:7000", captured.GetStorageConfig().GetTalonCoordinator())
	suite.Equal(uint32(8388608), captured.GetStorageConfig().GetTalonBlockSize())
	suite.Equal(uint32(32), captured.GetStorageConfig().GetTalonMaxIdlePerAddr())
	suite.True(captured.GetStorageConfig().GetTalonEnableForExternalTable())
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

	var captured *clusteringpb.StorageConfig
	patch := mockey.Mock(analyzecgowrapper.Analyze).To(
		func(_ context.Context, info *clusteringpb.AnalyzeInfo, _ *indexcgopb.StoragePluginContext) (analyzecgowrapper.CodecAnalyze, error) {
			captured = info.GetStorageConfig()
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
			StorageType:                 "minio",
			RootPath:                    "files",
			MaxConnections:              237,
			TalonMode:                   2,
			TalonSmallReadThreshold:     65536,
			TalonCoordinator:            "talon:7000",
			TalonBlockSize:              8388608,
			TalonMaxIdlePerAddr:         32,
			TalonEnableForExternalTable: true,
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
	suite.Require().NotNil(captured)
	suite.Equal(uint32(237), captured.GetMaxConnections())
	suite.Equal(uint32(2), captured.GetTalonMode())
	suite.Equal(uint32(65536), captured.GetTalonSmallReadThreshold())
	suite.Equal("talon:7000", captured.GetTalonCoordinator())
	suite.Equal(uint32(8388608), captured.GetTalonBlockSize())
	suite.Equal(uint32(32), captured.GetTalonMaxIdlePerAddr())
	suite.True(captured.GetTalonEnableForExternalTable())
}

func TestAnalyzeTaskSuite(t *testing.T) {
	suite.Run(t, new(AnalyzeTaskSuite))
}
