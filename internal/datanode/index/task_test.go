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

	"github.com/shirou/gopsutil/v3/disk"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v2/util/metautil"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/timerecord"
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

func (suite *IndexBuildTaskSuite) TestEstimateIndexDiskCost() {
	testCases := []struct {
		fieldDataSize uint64
		expected      int64
	}{
		{0, 0},
		{1024 * 1024, 2 * 1024 * 1024}, // 1MB -> 2MB
		{1024 * 1024 * 1024, 2 * 1024 * 1024 * 1024}, // 1GB -> 2GB
	}

	for _, tc := range testCases {
		actual := estimateIndexDiskCost(tc.fieldDataSize)
		suite.Equal(tc.expected, actual, "disk cost should be 2x field data size for input %d", tc.fieldDataSize)
	}
}

func (suite *IndexBuildTaskSuite) TestCheckDiskCapacityForBuild() {
	ctx := context.Background()

	// Reset committed disk space before test
	committedDiskSpaceLock.Lock()
	committedDiskSpace = 0
	committedDiskSpaceLock.Unlock()

	// Test with small data size - should pass and reserve space
	reserved, err := checkDiskCapacityForBuild(ctx, "DISKANN", 1024*1024) // 1MB
	suite.Nil(err, "disk capacity check should succeed for small data size")
	suite.Greater(reserved, uint64(0), "should reserve space when check passes")

	// Verify space was reserved
	committed := getCommittedDiskSpace()
	suite.Equal(reserved, committed, "committed space should equal reserved space")

	// Release the reserved space
	subCommittedDiskSpace(reserved)

	// Test with zero data size - should pass
	reserved, err = checkDiskCapacityForBuild(ctx, "DISKANN", 0)
	suite.Nil(err, "disk capacity check should succeed for zero data size")
	suite.Equal(uint64(0), reserved, "should reserve 0 for zero data size")
}

func (suite *IndexBuildTaskSuite) TestCommittedDiskSpace() {
	// Reset committed disk space before test
	committedDiskSpaceLock.Lock()
	committedDiskSpace = 0
	committedDiskSpaceLock.Unlock()

	// Test add
	addCommittedDiskSpace(1000)
	suite.Equal(uint64(1000), getCommittedDiskSpace())

	addCommittedDiskSpace(500)
	suite.Equal(uint64(1500), getCommittedDiskSpace())

	// Test sub
	subCommittedDiskSpace(500)
	suite.Equal(uint64(1000), getCommittedDiskSpace())

	// Test sub with underflow protection
	subCommittedDiskSpace(2000)
	suite.Equal(uint64(0), getCommittedDiskSpace(), "should not go negative")
}

func (suite *IndexBuildTaskSuite) TestConcurrentDiskReservation() {
	ctx := context.Background()

	// Reset committed disk space before test
	committedDiskSpaceLock.Lock()
	committedDiskSpace = 0
	committedDiskSpaceLock.Unlock()

	// Get disk usage to calculate a size that would fail if not considering committed space
	localStoragePath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	diskUsage, err := disk.Usage(localStoragePath)
	suite.NoError(err)

	maxUsageRatio := paramtable.Get().DataNodeCfg.IndexMaxDiskUsagePercentage.GetAsFloat()
	maxAllowedUsage := uint64(float64(diskUsage.Total) * maxUsageRatio)
	availableSpace := maxAllowedUsage - diskUsage.Used

	// If available space is less than 100MB, skip this test
	if availableSpace < 100*1024*1024 {
		suite.T().Skip("not enough disk space for concurrent reservation test")
	}

	// Request half of available space
	halfAvailable := availableSpace / 2

	// First reservation should succeed
	reserved1, err := checkDiskCapacityForBuild(ctx, "DISKANN", halfAvailable/3) // /3 because requiredSize = fieldDataSize * 3
	suite.Nil(err, "first reservation should succeed")
	suite.Greater(reserved1, uint64(0))

	// Second reservation with same size should also succeed (still within limits)
	reserved2, err := checkDiskCapacityForBuild(ctx, "DISKANN", halfAvailable/3)
	suite.Nil(err, "second reservation should succeed")
	suite.Greater(reserved2, uint64(0))

	// Verify total committed space
	totalCommitted := getCommittedDiskSpace()
	suite.Equal(reserved1+reserved2, totalCommitted)

	// Cleanup
	subCommittedDiskSpace(reserved1)
	subCommittedDiskSpace(reserved2)
	suite.Equal(uint64(0), getCommittedDiskSpace())
}

func (suite *IndexBuildTaskSuite) TestDiskUsage() {
	localStoragePath := paramtable.Get().LocalStorageCfg.Path.GetValue()

	// Test disk.Usage returns valid data
	diskUsage, err := disk.Usage(localStoragePath)
	suite.NoError(err, "disk.Usage should succeed")
	suite.NotNil(diskUsage)
	suite.Greater(diskUsage.Total, uint64(0), "disk total should be positive")
	suite.LessOrEqual(diskUsage.Used, diskUsage.Total, "disk used should not exceed total")
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
	ctx, cancel := context.WithCancel(context.Background())
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

func TestAnalyzeTaskSuite(t *testing.T) {
	suite.Run(t, new(AnalyzeTaskSuite))
}
