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

package segments

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type SegmentLoaderSuite struct {
	suite.Suite
	loader Loader

	// Dependencies
	manager      *Manager
	rootPath     string
	chunkManager storage.ChunkManager

	// Data
	collectionID int64
	partitionID  int64
	segmentID    int64
	schema       *schemapb.CollectionSchema
	segmentNum   int
}

type eofRecordReader struct{}

func (eofRecordReader) Next() (storage.Record, error) {
	return nil, io.EOF
}

func (eofRecordReader) Close() error {
	return nil
}

type deltaLoadTestSegment struct {
	Segment

	id           int64
	collectionID int64
	deltaData    *storage.DeltaData
}

func (s *deltaLoadTestSegment) ID() int64 {
	return s.id
}

func (s *deltaLoadTestSegment) Collection() int64 {
	return s.collectionID
}

func (s *deltaLoadTestSegment) LastDeltaTimestamp() uint64 {
	return 0
}

func (s *deltaLoadTestSegment) LoadDeltaData(ctx context.Context, deltaData *storage.DeltaData) error {
	s.deltaData = deltaData
	return nil
}

func TestLoadDeltalogsExternalRealPKManifestReadsSourceDeltas(t *testing.T) {
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	ctx := context.Background()

	collectionID := int64(1000)
	schema := milvusTableCollectionSchema(false)
	schema.ExternalSource = "s3://source-bucket/snapshots/100/metadata/200.json"

	manager := NewManager()
	err := manager.Collection.PutOrRef(collectionID, schema, nil, &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: collectionID,
	})
	if !assert.NoError(t, err) {
		return
	}
	defer manager.Collection.Unref(collectionID, 1)

	loader := &segmentLoader{manager: manager}
	segment := &deltaLoadTestSegment{id: 3000, collectionID: collectionID}

	sourceDeltaPath := "s3://source-bucket/files/insert_log/1/_delta/100"
	manifestPath := packed.MarshalManifestPath("files/insert_log/100/200/300", 1)

	patchSourceDeltalogs := mockey.Mock(packed.GetDeltaLogsFromManifestWithExtfs).
		Return([]*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{
				LogPath:    sourceDeltaPath,
				EntriesNum: 1,
			}},
		}}, nil).
		Build()
	defer patchSourceDeltalogs.UnPatch()

	readerCalled := atomic.NewInt32(0)
	var gotPathSets [][]string
	patchReader := mockey.Mock(storage.NewDeltalogReader).To(
		func(pkType schemapb.DataType, paths []string, option ...storage.RwOption) (storage.RecordReader, error) {
			readerCalled.Inc()
			gotPathSets = append(gotPathSets, append([]string(nil), paths...))
			return eofRecordReader{}, nil
		},
	).Build()
	defer patchReader.UnPatch()

	err = loader.loadDeltalogs(ctx, segment, &querypb.SegmentLoadInfo{
		SegmentID:    segment.id,
		CollectionID: collectionID,
		ManifestPath: manifestPath,
	})

	assert.NoError(t, err)
	assert.NotNil(t, segment.deltaData)
	assert.EqualValues(t, 1, readerCalled.Load())
	assert.Equal(t, [][]string{{sourceDeltaPath}}, gotPathSets)
}

func TestLoadDeltalogsExternalRealPKManifestRejectsTargetDeltas(t *testing.T) {
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	ctx := context.Background()

	collectionID := int64(1000)
	schema := milvusTableCollectionSchema(false)
	schema.ExternalSource = "s3://source-bucket/snapshots/100/metadata/200.json"

	manager := NewManager()
	err := manager.Collection.PutOrRef(collectionID, schema, nil, &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: collectionID,
	})
	if !assert.NoError(t, err) {
		return
	}
	defer manager.Collection.Unref(collectionID, 1)

	loader := &segmentLoader{manager: manager}
	segment := &deltaLoadTestSegment{id: 3000, collectionID: collectionID}

	targetDeltaPath := "files/insert_log/100/200/300/_delta/88"
	manifestPath := packed.MarshalManifestPath("files/insert_log/100/200/300", 1)

	patchSourceDeltalogs := mockey.Mock(packed.GetDeltaLogsFromManifestWithExtfs).
		Return([]*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{
				LogPath:    targetDeltaPath,
				EntriesNum: 1,
			}},
		}}, nil).
		Build()
	defer patchSourceDeltalogs.UnPatch()

	readerCalled := atomic.NewInt32(0)
	patchReader := mockey.Mock(storage.NewDeltalogReader).To(
		func(pkType schemapb.DataType, paths []string, option ...storage.RwOption) (storage.RecordReader, error) {
			readerCalled.Inc()
			return eofRecordReader{}, nil
		},
	).Build()
	defer patchReader.UnPatch()

	err = loader.loadDeltalogs(ctx, segment, &querypb.SegmentLoadInfo{
		SegmentID:    segment.id,
		CollectionID: collectionID,
		ManifestPath: manifestPath,
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "real-PK manifest must not contain target-owned deltalog")
	assert.EqualValues(t, 0, readerCalled.Load())
}

func (suite *SegmentLoaderSuite) SetupSuite() {
	paramtable.Init()
	suite.rootPath = suite.T().Name()
	suite.collectionID = rand.Int63()
	suite.partitionID = rand.Int63()
	suite.segmentID = rand.Int63()
	suite.segmentNum = 5
	initcore.InitExecExpressionFunctionFactory()
}

func (suite *SegmentLoaderSuite) SetupTest() {
	ctx := context.Background()

	// TODO:: cpp chunk manager not support local chunk manager
	// suite.chunkManager = storage.NewLocalChunkManager(storage.RootPath(
	//	fmt.Sprintf("/tmp/milvus-ut/%d", rand.Int63())))
	chunkManagerFactory := storage.NewTestChunkManagerFactory(paramtable.Get(), suite.rootPath)
	suite.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(ctx)

	// Dependencies
	suite.manager = NewManager()
	suite.loader = NewLoader(ctx, suite.manager, suite.chunkManager)
	initcore.InitRemoteChunkManager(paramtable.Get())
	initcore.InitLocalChunkManager(suite.rootPath)
	initcore.InitMmapManager(paramtable.Get(), 1)
	initcore.InitTieredStorage(paramtable.Get())
	initcore.InitLocalArrowFileSystem(suite.rootPath)
	initcore.InitRemoteArrowFileSystem(paramtable.Get())

	// Data
	suite.schema = mock_segcore.GenTestCollectionSchema("test", schemapb.DataType_Int64, false)
	indexMeta := mock_segcore.GenTestIndexMeta(suite.collectionID, suite.schema)
	loadMeta := &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: suite.collectionID,
		PartitionIDs: []int64{suite.partitionID},
	}
	suite.manager.Collection.PutOrRef(suite.collectionID, suite.schema, indexMeta, loadMeta)
}

func (suite *SegmentLoaderSuite) TearDownTest() {
	ctx := context.Background()
	for i := 0; i < suite.segmentNum; i++ {
		suite.manager.Segment.Remove(context.Background(), suite.segmentID+int64(i), querypb.DataScope_All)
	}
	suite.chunkManager.RemoveWithPrefix(ctx, suite.rootPath)
}

func (suite *SegmentLoaderSuite) TestLoad() {
	ctx := context.Background()

	msgLength := 4

	// Load sealed
	binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.NoError(err)

	_, err = suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		BinlogPaths:   binlogs,
		Statslogs:     statsLogs,
		NumOfRows:     int64(msgLength),
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	})
	suite.NoError(err)

	// Load growing
	binlogs, statsLogs, err = mock_segcore.SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID+1,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.NoError(err)

	_, err = suite.loader.Load(ctx, suite.collectionID, SegmentTypeGrowing, 0, &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID + 1,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		BinlogPaths:   binlogs,
		Statslogs:     statsLogs,
		NumOfRows:     int64(msgLength),
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	})
	suite.NoError(err)
}

func (suite *SegmentLoaderSuite) TestLoadFail() {
	ctx := context.Background()

	msgLength := 4

	// Load sealed
	binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.NoError(err)

	// make file & binlog mismatch
	for _, binlog := range binlogs {
		for _, log := range binlog.GetBinlogs() {
			log.LogPath = log.LogPath + "-suffix"
		}
	}

	_, err = suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		BinlogPaths:   binlogs,
		Statslogs:     statsLogs,
		NumOfRows:     int64(msgLength),
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	})
	suite.Error(err)
}

func (suite *SegmentLoaderSuite) TestLoadMultipleSegments() {
	ctx := context.Background()
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, suite.segmentNum)

	msgLength := 100
	// Load sealed
	for i := 0; i < suite.segmentNum; i++ {
		segmentID := suite.segmentID + int64(i)
		binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			msgLength,
			suite.schema,
			suite.chunkManager,
		)
		suite.NoError(err)
		loadInfos = append(loadInfos, &querypb.SegmentLoadInfo{
			SegmentID:     segmentID,
			PartitionID:   suite.partitionID,
			CollectionID:  suite.collectionID,
			BinlogPaths:   binlogs,
			Statslogs:     statsLogs,
			NumOfRows:     int64(msgLength),
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		})
	}

	segments, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, loadInfos...)
	suite.NoError(err)

	// Will load bloom filter with sealed segments
	for _, segment := range segments {
		for pk := 0; pk < 100; pk++ {
			lc := storage.NewLocationsCache(storage.NewInt64PrimaryKey(int64(pk)))
			pkReady := segment.PkCandidateExist()
			suite.Require().True(pkReady)
			exist := segment.MayPkExist(lc)
			suite.Require().True(exist)
		}
	}

	// Load growing
	loadInfos = loadInfos[:0]
	for i := 0; i < suite.segmentNum; i++ {
		segmentID := suite.segmentID + int64(suite.segmentNum) + int64(i)
		binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			msgLength,
			suite.schema,
			suite.chunkManager,
		)
		suite.NoError(err)
		loadInfos = append(loadInfos, &querypb.SegmentLoadInfo{
			SegmentID:     segmentID,
			PartitionID:   suite.partitionID,
			CollectionID:  suite.collectionID,
			BinlogPaths:   binlogs,
			Statslogs:     statsLogs,
			NumOfRows:     int64(msgLength),
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		})
	}

	segments, err = suite.loader.Load(ctx, suite.collectionID, SegmentTypeGrowing, 0, loadInfos...)
	suite.NoError(err)
	// Should load bloom filter with growing segments
	for _, segment := range segments {
		for pk := 0; pk < 100; pk++ {
			lc := storage.NewLocationsCache(storage.NewInt64PrimaryKey(int64(pk)))
			pkReady := segment.PkCandidateExist()
			suite.True(pkReady)
			exist := segment.MayPkExist(lc)
			suite.True(exist)
		}
	}
}

func (suite *SegmentLoaderSuite) TestLoadWithIndex() {
	ctx := context.Background()
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, suite.segmentNum)

	msgLength := 100
	// Load sealed
	for i := 0; i < suite.segmentNum; i++ {
		segmentID := suite.segmentID + int64(i)
		binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			msgLength,
			suite.schema,
			suite.chunkManager,
		)
		suite.NoError(err)

		vecFields := funcutil.GetVecFieldIDs(suite.schema)
		indexInfo, err := mock_segcore.GenAndSaveIndex(
			suite.collectionID,
			suite.partitionID,
			segmentID,
			vecFields[0],
			msgLength,
			mock_segcore.IndexFaissIVFFlat,
			metric.L2,
			suite.chunkManager,
		)
		suite.NoError(err)
		loadInfos = append(loadInfos, &querypb.SegmentLoadInfo{
			SegmentID:     segmentID,
			PartitionID:   suite.partitionID,
			CollectionID:  suite.collectionID,
			BinlogPaths:   binlogs,
			Statslogs:     statsLogs,
			IndexInfos:    []*querypb.FieldIndexInfo{indexInfo},
			NumOfRows:     int64(msgLength),
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		})
	}

	segments, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, loadInfos...)
	suite.NoError(err)

	vecFields := funcutil.GetVecFieldIDs(suite.schema)
	for _, segment := range segments {
		suite.True(segment.ExistIndex(vecFields[0]))
	}
}

func (suite *SegmentLoaderSuite) TestLoadWithIndexPreferFieldDataWhenIndexHasRawData() {
	ctx := context.Background()
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, suite.segmentNum)

	msgLength := 100
	oldPreferFieldData := paramtable.Get().QueryNodeCfg.PreferFieldDataWhenIndexHasRawData.SwapTempValue("true")
	initcore.SyncPreferFieldDataWhenIndexHasRawData(ctx, paramtable.Get())
	defer func() {
		paramtable.Get().QueryNodeCfg.PreferFieldDataWhenIndexHasRawData.SwapTempValue(oldPreferFieldData)
		initcore.SyncPreferFieldDataWhenIndexHasRawData(ctx, paramtable.Get())
	}()

	for i := 0; i < suite.segmentNum; i++ {
		segmentID := suite.segmentID + int64(i)
		binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			msgLength,
			suite.schema,
			suite.chunkManager,
		)
		suite.NoError(err)

		vecFields := funcutil.GetVecFieldIDs(suite.schema)
		indexInfo, err := mock_segcore.GenAndSaveIndex(
			suite.collectionID,
			suite.partitionID,
			segmentID,
			vecFields[0],
			msgLength,
			mock_segcore.IndexFaissIVFFlat,
			metric.L2,
			suite.chunkManager,
		)
		suite.NoError(err)
		loadInfos = append(loadInfos, &querypb.SegmentLoadInfo{
			SegmentID:     segmentID,
			PartitionID:   suite.partitionID,
			CollectionID:  suite.collectionID,
			BinlogPaths:   binlogs,
			Statslogs:     statsLogs,
			IndexInfos:    []*querypb.FieldIndexInfo{indexInfo},
			NumOfRows:     int64(msgLength),
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		})
	}

	segments, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, loadInfos...)
	suite.NoError(err)

	vecFields := funcutil.GetVecFieldIDs(suite.schema)
	for _, segment := range segments {
		suite.True(segment.ExistIndex(vecFields[0]))
		suite.True(segment.HasRawData(vecFields[0]))

		localSegment, ok := segment.(*LocalSegment)
		suite.True(ok)
		suite.True(localSegment.HasFieldData(vecFields[0]))
	}
}

// Negative counterpart: with the flag off (default) and the index already
// holding raw data, the loader must skip loading the field data — otherwise
// the memory-saving behavior relied on by existing deployments is broken.
func (suite *SegmentLoaderSuite) TestLoadWithIndexSkipsFieldDataByDefault() {
	ctx := context.Background()
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, suite.segmentNum)

	msgLength := 100
	oldPreferFieldData := paramtable.Get().QueryNodeCfg.PreferFieldDataWhenIndexHasRawData.SwapTempValue("false")
	initcore.SyncPreferFieldDataWhenIndexHasRawData(ctx, paramtable.Get())
	defer func() {
		paramtable.Get().QueryNodeCfg.PreferFieldDataWhenIndexHasRawData.SwapTempValue(oldPreferFieldData)
		initcore.SyncPreferFieldDataWhenIndexHasRawData(ctx, paramtable.Get())
	}()

	for i := 0; i < suite.segmentNum; i++ {
		segmentID := suite.segmentID + int64(i)
		binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			msgLength,
			suite.schema,
			suite.chunkManager,
		)
		suite.NoError(err)

		vecFields := funcutil.GetVecFieldIDs(suite.schema)
		indexInfo, err := mock_segcore.GenAndSaveIndex(
			suite.collectionID,
			suite.partitionID,
			segmentID,
			vecFields[0],
			msgLength,
			mock_segcore.IndexFaissIVFFlat,
			metric.L2,
			suite.chunkManager,
		)
		suite.NoError(err)
		loadInfos = append(loadInfos, &querypb.SegmentLoadInfo{
			SegmentID:     segmentID,
			PartitionID:   suite.partitionID,
			CollectionID:  suite.collectionID,
			BinlogPaths:   binlogs,
			Statslogs:     statsLogs,
			IndexInfos:    []*querypb.FieldIndexInfo{indexInfo},
			NumOfRows:     int64(msgLength),
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		})
	}

	segments, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, loadInfos...)
	suite.NoError(err)

	vecFields := funcutil.GetVecFieldIDs(suite.schema)
	for _, segment := range segments {
		suite.True(segment.ExistIndex(vecFields[0]))
		suite.True(segment.HasRawData(vecFields[0]))

		localSegment, ok := segment.(*LocalSegment)
		suite.True(ok)
		suite.False(localSegment.HasFieldData(vecFields[0]))
	}
}

func (suite *SegmentLoaderSuite) TestLoadBloomFilter() {
	ctx := context.Background()
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, suite.segmentNum)

	msgLength := 100
	// Load sealed
	for i := 0; i < suite.segmentNum; i++ {
		segmentID := suite.segmentID + int64(i)
		binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			msgLength,
			suite.schema,
			suite.chunkManager,
		)
		suite.NoError(err)

		loadInfos = append(loadInfos, &querypb.SegmentLoadInfo{
			SegmentID:     segmentID,
			PartitionID:   suite.partitionID,
			CollectionID:  suite.collectionID,
			BinlogPaths:   binlogs,
			Statslogs:     statsLogs,
			NumOfRows:     int64(msgLength),
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		})
	}

	bfs, err := suite.loader.LoadBloomFilterSet(ctx, suite.collectionID, loadInfos...)
	suite.NoError(err)

	for _, bf := range bfs {
		for pk := 0; pk < 100; pk++ {
			lc := storage.NewLocationsCache(storage.NewInt64PrimaryKey(int64(pk)))
			exist := bf.MayPkExist(lc)
			suite.Require().True(exist)
		}
	}
}

func (suite *SegmentLoaderSuite) TestLoadWithoutBloomFilter() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.BloomFilterEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.BloomFilterEnabled.Key)

	ctx := context.Background()
	msgLength := 100

	binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.NoError(err)

	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		BinlogPaths:   binlogs,
		Statslogs:     statsLogs,
		NumOfRows:     int64(msgLength),
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	}

	segments, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, loadInfo)
	suite.NoError(err)
	suite.Len(segments, 1)
	suite.False(segments[0].PkCandidateExist())

	bfs, err := suite.loader.LoadBloomFilterSet(ctx, suite.collectionID, loadInfo)
	suite.NoError(err)
	suite.NotNil(bfs)
	suite.Len(bfs, 1)
	suite.False(bfs[0].PkCandidateExist()) // metadata-only stub when BF disabled
}

func (suite *SegmentLoaderSuite) TestLoadDeltaLogs() {
	ctx := context.Background()
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, suite.segmentNum)

	msgLength := 100
	// Load sealed
	for i := 0; i < suite.segmentNum; i++ {
		segmentID := suite.segmentID + int64(i)
		binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			msgLength,
			suite.schema,
			suite.chunkManager,
		)
		suite.NoError(err)

		// Delete PKs 1, 2
		deltaLogs, err := mock_segcore.SaveDeltaLog(suite.collectionID,
			suite.partitionID,
			segmentID,
			suite.chunkManager,
		)
		suite.NoError(err)

		loadInfos = append(loadInfos, &querypb.SegmentLoadInfo{
			SegmentID:     segmentID,
			PartitionID:   suite.partitionID,
			CollectionID:  suite.collectionID,
			BinlogPaths:   binlogs,
			Statslogs:     statsLogs,
			Deltalogs:     deltaLogs,
			NumOfRows:     int64(msgLength),
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		})
	}

	segments, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeGrowing, 0, loadInfos...)
	suite.NoError(err)

	for _, segment := range segments {
		suite.Equal(int64(100-2), segment.RowNum())
		for pk := 0; pk < 100; pk++ {
			if pk == 1 || pk == 2 {
				continue
			}
			lc := storage.NewLocationsCache(storage.NewInt64PrimaryKey(int64(pk)))
			exist := segment.MayPkExist(lc)
			suite.Require().True(exist)
		}
	}
}

func (suite *SegmentLoaderSuite) TestLoadVirtualPKExternalCollectionLoadsDeltaLogs() {
	ctx := context.Background()
	suite.schema.ExternalSource = "s3://bucket/source"
	suite.schema.ExternalSpec = `{"format":"milvus-table"}`
	pkField := GetPkField(suite.schema)
	suite.Require().NotNil(pkField)
	pkField.Name = common.VirtualPKFieldName
	suite.Require().NoError(suite.manager.Collection.UpdateSchema(suite.collectionID, suite.schema, 1))

	msgLength := 100
	binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.Require().NoError(err)

	deltaLogs, err := mock_segcore.SaveDeltaLog(suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		suite.chunkManager,
	)
	suite.Require().NoError(err)

	segments, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		BinlogPaths:   binlogs,
		Statslogs:     statsLogs,
		Deltalogs:     deltaLogs,
		NumOfRows:     int64(msgLength),
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	})
	suite.Require().NoError(err)
	suite.Require().Len(segments, 1)
	suite.Equal(int64(msgLength-2), segments[0].RowNum())
}

func (suite *SegmentLoaderSuite) TestLoadDupDeltaLogs() {
	ctx := context.Background()
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, suite.segmentNum)

	msgLength := 100
	// Load sealed
	for i := 0; i < suite.segmentNum; i++ {
		segmentID := suite.segmentID + int64(i)
		binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			msgLength,
			suite.schema,
			suite.chunkManager,
		)
		suite.NoError(err)

		// Delete PKs 1, 2
		deltaLogs, err := mock_segcore.SaveDeltaLog(suite.collectionID,
			suite.partitionID,
			segmentID,
			suite.chunkManager,
		)
		suite.NoError(err)

		loadInfos = append(loadInfos, &querypb.SegmentLoadInfo{
			SegmentID:     segmentID,
			PartitionID:   suite.partitionID,
			CollectionID:  suite.collectionID,
			BinlogPaths:   binlogs,
			Statslogs:     statsLogs,
			Deltalogs:     deltaLogs,
			NumOfRows:     int64(msgLength),
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		})
	}

	segments, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeGrowing, 0, loadInfos...)
	suite.NoError(err)

	for i, segment := range segments {
		suite.Equal(int64(100-2), segment.RowNum())
		for pk := 0; pk < 100; pk++ {
			if pk == 1 || pk == 2 {
				continue
			}
			lc := storage.NewLocationsCache(storage.NewInt64PrimaryKey(int64(pk)))
			exist := segment.MayPkExist(lc)
			suite.Require().True(exist)
		}

		seg := segment.(*LocalSegment)
		// nothing would happen as the delta logs have been all applied,
		// so the released segment won't cause error
		seg.Release(ctx)
		loadInfos[i].Deltalogs[0].Binlogs[0].TimestampTo--
		err := suite.loader.LoadDeltaLogs(ctx, seg, loadInfos[i])
		suite.NoError(err)
	}
}

// TestLoadDeltaLogsV3PlaceholderSkipsPathRead verifies that for V3 (manifest)
// segments, the pathless Deltalogs summary placeholder in SegmentLoadInfo is
// treated as metadata-only and does NOT trigger the V1 path-based delta
// download loop. The real delta data for V3 segments is loaded via
// NewDeltalogReaderFromManifest instead.
func (suite *SegmentLoaderSuite) TestLoadDeltaLogsV3PlaceholderSkipsPathRead() {
	ctx := context.Background()

	// Load a base segment so we have a concrete LocalSegment to run loadDeltalogs against.
	msgLength := 4
	binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.Require().NoError(err)

	segs, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		BinlogPaths:   binlogs,
		Statslogs:     statsLogs,
		NumOfRows:     int64(msgLength),
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	})
	suite.Require().NoError(err)
	suite.Require().Len(segs, 1)
	segment := segs[0]

	readerCalled := atomic.NewInt32(0)
	manifestCalled := atomic.NewInt32(0)

	patchManifest := mockey.Mock(packed.GetDeltaLogPathsFromManifest).To(
		func(manifestPath string, storageConfig *indexpb.StorageConfig) ([]string, error) {
			manifestCalled.Inc()
			// Return empty paths — no delta data in manifest.
			return nil, nil
		},
	).Build()
	defer patchManifest.UnPatch()

	patchReader := mockey.Mock(storage.NewDeltalogReader).To(
		func(pkType schemapb.DataType, paths []string, option ...storage.RwOption) (storage.RecordReader, error) {
			readerCalled.Inc()
			return nil, errors.New("should not be called when manifest has no delta paths")
		},
	).Build()
	defer patchReader.UnPatch()

	v3LoadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    suite.segmentID,
		PartitionID:  suite.partitionID,
		CollectionID: suite.collectionID,
		ManifestPath: "/tmp/fake/manifest.json?version=0",
		Deltalogs: []*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{
				LogID:      1234,
				EntriesNum: 10,
				MemorySize: 1024,
				// LogPath intentionally empty — this is the summary placeholder.
			}},
		}},
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	}

	loader := suite.loader.(*segmentLoader)
	err = loader.loadDeltalogs(ctx, segment, v3LoadInfo)
	suite.NoError(err)
	suite.EqualValues(1, manifestCalled.Load(),
		"GetDeltaLogPathsFromManifest must be called for V3 segments")
	suite.EqualValues(0, readerCalled.Load(),
		"NewDeltalogReader must not be called when manifest returns no paths")
}

func (suite *SegmentLoaderSuite) TestLoadDeltaLogsExternalRealPKManifestStorageV3DeltasUseFFIReader() {
	ctx := context.Background()

	msgLength := 4
	binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.Require().NoError(err)

	segs, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		BinlogPaths:   binlogs,
		Statslogs:     statsLogs,
		NumOfRows:     int64(msgLength),
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	})
	suite.Require().NoError(err)
	suite.Require().Len(segs, 1)
	segment := segs[0]

	suite.schema.ExternalSource = "s3://source-bucket/snapshots/100/metadata/200.json"
	suite.schema.ExternalSpec = `{"format":"milvus-table"}`
	pkField := GetPkField(suite.schema)
	suite.Require().NotNil(pkField)
	pkField.ExternalField = pkField.GetName()
	suite.Require().NoError(suite.manager.Collection.UpdateSchema(suite.collectionID, suite.schema, 1))

	sourceDeltaPath := "s3://source-bucket/files/insert_log/1/_delta/100"
	manifestPath := packed.MarshalManifestPath("files/insert_log/100/200/300", 1)

	sourceDeltalogsCalled := atomic.NewInt32(0)
	patchSourceDeltalogs := mockey.Mock(packed.GetDeltaLogsFromManifestWithExtfs).To(
		func(gotManifestPath string, storageConfig *indexpb.StorageConfig, extfs packed.ExternalSpecContext) ([]*datapb.FieldBinlog, error) {
			sourceDeltalogsCalled.Inc()
			suite.Equal(manifestPath, gotManifestPath)
			suite.Equal(suite.collectionID, extfs.CollectionID)
			suite.Equal(suite.schema.GetExternalSource(), extfs.Source)
			suite.Equal(suite.schema.GetExternalSpec(), extfs.Spec)
			return []*datapb.FieldBinlog{{
				Binlogs: []*datapb.Binlog{{
					LogPath:    sourceDeltaPath,
					EntriesNum: 1,
				}},
			}}, nil
		},
	).Build()
	defer patchSourceDeltalogs.UnPatch()

	readerCalled := atomic.NewInt32(0)
	patchReader := mockey.Mock(storage.NewDeltalogReader).To(
		func(pkType schemapb.DataType, paths []string, option ...storage.RwOption) (storage.RecordReader, error) {
			readerCalled.Inc()
			suite.Equal(schemapb.DataType_Int64, pkType)
			suite.Equal([]string{sourceDeltaPath}, paths)
			return eofRecordReader{}, nil
		},
	).Build()
	defer patchReader.UnPatch()

	err = suite.loader.(*segmentLoader).loadDeltalogs(ctx, segment, &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		ManifestPath:  manifestPath,
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	})

	suite.NoError(err)
	suite.EqualValues(1, sourceDeltalogsCalled.Load())
	suite.EqualValues(1, readerCalled.Load())
}

func (suite *SegmentLoaderSuite) TestLoadDeltaLogsExternalRealPKManifestLegacyL0UsesLegacyReader() {
	ctx := context.Background()

	msgLength := 4
	binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.Require().NoError(err)

	segs, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		BinlogPaths:   binlogs,
		Statslogs:     statsLogs,
		NumOfRows:     int64(msgLength),
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	})
	suite.Require().NoError(err)
	suite.Require().Len(segs, 1)
	segment := segs[0]

	suite.schema.ExternalSource = "s3://source-bucket/snapshots/100/metadata/200.json"
	suite.schema.ExternalSpec = `{"format":"milvus-table"}`
	pkField := GetPkField(suite.schema)
	suite.Require().NotNil(pkField)
	pkField.ExternalField = pkField.GetName()
	suite.Require().NoError(suite.manager.Collection.UpdateSchema(suite.collectionID, suite.schema, 1))

	sourceDeltaPath := "s3://source-bucket/files/delta_log/1/2/3/100"
	manifestPath := packed.MarshalManifestPath("files/insert_log/100/200/300", 1)

	patchSourceDeltalogs := mockey.Mock(packed.GetDeltaLogsFromManifestWithExtfs).
		Return([]*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{
				LogPath:    sourceDeltaPath,
				EntriesNum: 1,
			}},
		}}, nil).
		Build()
	defer patchSourceDeltalogs.UnPatch()

	legacyReaderCalled := atomic.NewInt32(0)
	patchReader := mockey.Mock(storage.NewDeltalogReader).
		To(func(pkType schemapb.DataType, paths []string, option ...storage.RwOption) (storage.RecordReader, error) {
			legacyReaderCalled.Inc()
			suite.Equal(schemapb.DataType_Int64, pkType)
			suite.Equal([]string{sourceDeltaPath}, paths)
			return eofRecordReader{}, nil
		}).Build()
	defer patchReader.UnPatch()

	err = suite.loader.(*segmentLoader).loadDeltalogs(ctx, segment, &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		ManifestPath:  manifestPath,
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	})

	suite.NoError(err)
	suite.EqualValues(1, legacyReaderCalled.Load())
}

func TestReadExternalFiles(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{StorageType: "local"}
	extfs := packed.ExternalSpecContext{
		CollectionID: 100,
		Source:       "s3://bucket/source",
		Spec:         `{"format":"milvus-table"}`,
	}
	paths := []string{
		"s3://bucket/source/_delta/1",
		"files/insert_log/100/10/20/_delta/2",
	}

	var gotPaths []string
	var gotExtfs []packed.ExternalSpecContext
	patchRead := mockey.Mock(packed.ReadFileWithExternalSpec).
		To(func(sc *indexpb.StorageConfig, filePath string, ctx packed.ExternalSpecContext) ([]byte, error) {
			assert.Same(t, storageConfig, sc)
			gotPaths = append(gotPaths, filePath)
			gotExtfs = append(gotExtfs, ctx)
			return []byte("delta:" + filePath), nil
		}).Build()
	defer patchRead.UnPatch()

	data, err := readExternalFiles(context.Background(), storageConfig, extfs, paths)

	assert.NoError(t, err)
	assert.Equal(t, paths, gotPaths)
	assert.Equal(t, []packed.ExternalSpecContext{extfs, extfs}, gotExtfs)
	assert.Equal(t, [][]byte{
		[]byte("delta:s3://bucket/source/_delta/1"),
		[]byte("delta:files/insert_log/100/10/20/_delta/2"),
	}, data)
}

func (suite *SegmentLoaderSuite) makeExternalRealPKBFLoadInfo(ctx context.Context) (*querypb.SegmentLoadInfo, []string, []byte) {
	suite.schema.ExternalSource = "s3://bucket/source"
	suite.schema.ExternalSpec = `{"format":"milvus-table"}`
	pkField := GetPkField(suite.schema)

	_, statsLogs, err := mock_segcore.SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		10,
		suite.schema,
		suite.chunkManager,
	)
	suite.Require().NoError(err)
	suite.Require().Len(statsLogs, 1)
	suite.Require().Len(statsLogs[0].GetBinlogs(), 1)

	localStatsPath := statsLogs[0].GetBinlogs()[0].GetLogPath()
	payload, err := suite.chunkManager.Read(ctx, localStatsPath)
	suite.Require().NoError(err)

	externalStatsPaths := []string{
		fmt.Sprintf("s3://bucket/source/_stats/bloom_filter.%d/1001", pkField.GetFieldID()),
		fmt.Sprintf("s3://bucket/source/_stats/bloom_filter.%d/1002", pkField.GetFieldID()),
	}
	statsLogs[0].FieldID = pkField.GetFieldID()
	statsLogs[0].GetBinlogs()[0].LogPath = externalStatsPaths[0]
	statsLogs[0].GetBinlogs()[0].MemorySize = int64(len(payload))
	statsLogs[0].Binlogs = append(statsLogs[0].GetBinlogs(), &datapb.Binlog{
		LogID:      1002,
		LogPath:    externalStatsPaths[1],
		MemorySize: int64(len(payload)),
	})

	return &querypb.SegmentLoadInfo{
		SegmentID:      suite.segmentID,
		PartitionID:    suite.partitionID,
		CollectionID:   suite.collectionID,
		Statslogs:      statsLogs,
		StorageVersion: storage.StorageV3,
		InsertChannel:  fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	}, externalStatsPaths, payload
}

func (suite *SegmentLoaderSuite) TestLoadSingleBloomFilterSetExternalRealPKUsesExternalStats() {
	ctx := context.Background()
	loadInfo, externalStatsPaths, payload := suite.makeExternalRealPKBFLoadInfo(ctx)

	readCalls := atomic.NewInt32(0)
	var gotPaths []string
	patchRead := mockey.Mock(packed.ReadFileWithExternalSpec).
		To(func(sc *indexpb.StorageConfig, filePath string, extfs packed.ExternalSpecContext) ([]byte, error) {
			readCalls.Inc()
			gotPaths = append(gotPaths, filePath)
			suite.Equal(suite.collectionID, extfs.CollectionID)
			suite.Equal(suite.schema.GetExternalSource(), extfs.Source)
			suite.Equal(suite.schema.GetExternalSpec(), extfs.Spec)
			return payload, nil
		}).Build()
	defer patchRead.UnPatch()

	bfs, err := suite.loader.(*segmentLoader).loadSingleBloomFilterSet(ctx, suite.collectionID, loadInfo, SegmentTypeSealed)

	suite.NoError(err)
	suite.EqualValues(2, readCalls.Load())
	suite.Equal(externalStatsPaths, gotPaths)
	suite.True(bfs.PkCandidateExist())
	suite.True(bfs.MayPkExist(storage.NewLocationsCache(storage.NewInt64PrimaryKey(1))))
}

func (suite *SegmentLoaderSuite) TestLoadSingleBloomFilterSetExternalRealPKIgnoresBloomFilterDisable() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.BloomFilterEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.BloomFilterEnabled.Key)

	ctx := context.Background()
	loadInfo, externalStatsPaths, payload := suite.makeExternalRealPKBFLoadInfo(ctx)

	readCalls := atomic.NewInt32(0)
	var gotPaths []string
	patchRead := mockey.Mock(packed.ReadFileWithExternalSpec).
		To(func(sc *indexpb.StorageConfig, filePath string, extfs packed.ExternalSpecContext) ([]byte, error) {
			readCalls.Inc()
			gotPaths = append(gotPaths, filePath)
			return payload, nil
		}).Build()
	defer patchRead.UnPatch()

	bfs, err := suite.loader.(*segmentLoader).loadSingleBloomFilterSet(ctx, suite.collectionID, loadInfo, SegmentTypeSealed)

	suite.NoError(err)
	suite.EqualValues(2, readCalls.Load())
	suite.Equal(externalStatsPaths, gotPaths)
	suite.True(bfs.PkCandidateExist())
	suite.True(bfs.MayPkExist(storage.NewLocationsCache(storage.NewInt64PrimaryKey(1))))
}

func (suite *SegmentLoaderSuite) TestLoadBloomFilterSetExternalRealPKUsesExternalStats() {
	ctx := context.Background()
	loadInfo, externalStatsPaths, payload := suite.makeExternalRealPKBFLoadInfo(ctx)

	readCalls := atomic.NewInt32(0)
	var gotPaths []string
	patchRead := mockey.Mock(packed.ReadFileWithExternalSpec).
		To(func(sc *indexpb.StorageConfig, filePath string, extfs packed.ExternalSpecContext) ([]byte, error) {
			readCalls.Inc()
			gotPaths = append(gotPaths, filePath)
			suite.Equal(suite.collectionID, extfs.CollectionID)
			suite.Equal(suite.schema.GetExternalSource(), extfs.Source)
			suite.Equal(suite.schema.GetExternalSpec(), extfs.Spec)
			return payload, nil
		}).Build()
	defer patchRead.UnPatch()

	bfs, err := suite.loader.LoadBloomFilterSet(ctx, suite.collectionID, loadInfo)

	suite.NoError(err)
	suite.Len(bfs, 1)
	suite.EqualValues(2, readCalls.Load())
	suite.Equal(externalStatsPaths, gotPaths)
	suite.True(bfs[0].PkCandidateExist())
	suite.True(bfs[0].MayPkExist(storage.NewLocationsCache(storage.NewInt64PrimaryKey(1))))
}

func (suite *SegmentLoaderSuite) TestLoadBloomFilterSetExternalRealPKIgnoresBloomFilterDisable() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.BloomFilterEnabled.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.BloomFilterEnabled.Key)

	ctx := context.Background()
	loadInfo, externalStatsPaths, payload := suite.makeExternalRealPKBFLoadInfo(ctx)

	readCalls := atomic.NewInt32(0)
	var gotPaths []string
	patchRead := mockey.Mock(packed.ReadFileWithExternalSpec).
		To(func(sc *indexpb.StorageConfig, filePath string, extfs packed.ExternalSpecContext) ([]byte, error) {
			readCalls.Inc()
			gotPaths = append(gotPaths, filePath)
			return payload, nil
		}).Build()
	defer patchRead.UnPatch()

	bfs, err := suite.loader.LoadBloomFilterSet(ctx, suite.collectionID, loadInfo)

	suite.NoError(err)
	suite.Len(bfs, 1)
	suite.EqualValues(2, readCalls.Load())
	suite.Equal(externalStatsPaths, gotPaths)
	suite.True(bfs[0].PkCandidateExist())
	suite.True(bfs[0].MayPkExist(storage.NewLocationsCache(storage.NewInt64PrimaryKey(1))))
}

// TestLoadDeltaLogsV1StillUsesPathRead ensures the skip logic does not break
// the legacy V1 path: when ManifestPath is empty, the path-based delta reader
// is still exercised.
func (suite *SegmentLoaderSuite) TestLoadDeltaLogsV1StillUsesPathRead() {
	ctx := context.Background()

	msgLength := 4
	binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.Require().NoError(err)

	deltaLogs, err := mock_segcore.SaveDeltaLog(suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		suite.chunkManager,
	)
	suite.Require().NoError(err)

	segs, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		BinlogPaths:   binlogs,
		Statslogs:     statsLogs,
		NumOfRows:     int64(msgLength),
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	})
	suite.Require().NoError(err)
	suite.Require().Len(segs, 1)
	segment := segs[0]

	readerCalled := atomic.NewInt32(0)
	manifestCalled := atomic.NewInt32(0)

	patchManifest := mockey.Mock(packed.GetDeltaLogPathsFromManifest).To(
		func(manifestPath string, storageConfig *indexpb.StorageConfig) ([]string, error) {
			manifestCalled.Inc()
			return nil, nil
		},
	).Build()
	defer patchManifest.UnPatch()

	patchReader := mockey.Mock(storage.NewDeltalogReader).To(
		func(pkType schemapb.DataType, paths []string, option ...storage.RwOption) (storage.RecordReader, error) {
			readerCalled.Inc()
			return nil, io.EOF
		},
	).Build()
	defer patchReader.UnPatch()

	v1LoadInfo := &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		Deltalogs:     deltaLogs,
		NumOfRows:     int64(msgLength),
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		// ManifestPath left empty → V1 path.
	}

	loader := suite.loader.(*segmentLoader)
	_ = loader.loadDeltalogs(ctx, segment, v1LoadInfo)
	suite.Greater(readerCalled.Load(), int32(0),
		"NewDeltalogReader must be invoked for V1 segments")
	suite.EqualValues(0, manifestCalled.Load(),
		"GetDeltaLogPathsFromManifest must not be called when ManifestPath is empty")
}

func (suite *SegmentLoaderSuite) TestLoadIndex() {
	ctx := context.Background()
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    1,
		PartitionID:  suite.partitionID,
		CollectionID: suite.collectionID,
		IndexInfos: []*querypb.FieldIndexInfo{
			{
				IndexFilePaths: []string{},
			},
		},
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	}
	segment := &LocalSegment{
		baseSegment:     baseSegment{loadInfo: atomic.NewPointer[querypb.SegmentLoadInfo](loadInfo)},
		bm25StatsHolder: newBM25StatsHolder(),
	}

	err := suite.loader.LoadIndex(ctx, segment, loadInfo, 0)
	suite.ErrorIs(err, merr.ErrIndexNotFound)
}

func (suite *SegmentLoaderSuite) TestLoadIndexWithLimitedResource() {
	ctx := context.Background()
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    1,
		PartitionID:  suite.partitionID,
		CollectionID: suite.collectionID,
		IndexInfos: []*querypb.FieldIndexInfo{
			{
				FieldID:        1,
				IndexFilePaths: []string{},
				IndexParams: []*commonpb.KeyValuePair{
					{
						Key:   common.IndexTypeKey,
						Value: indexparamcheck.IndexINVERTED,
					},
				},
			},
		},
		BinlogPaths: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						LogSize:    1000000000,
						MemorySize: 1000000000,
					},
				},
			},
		},
	}

	segment := &LocalSegment{
		baseSegment:     baseSegment{loadInfo: atomic.NewPointer[querypb.SegmentLoadInfo](loadInfo)},
		bm25StatsHolder: newBM25StatsHolder(),
	}
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.Key, "100000")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.Key)
	err := suite.loader.LoadIndex(ctx, segment, loadInfo, 0)
	suite.Error(err)
}

func (suite *SegmentLoaderSuite) TestLoadWithMmap() {
	key := paramtable.Get().QueryNodeCfg.MmapDirPath.Key
	paramtable.Get().Save(key, "/tmp/mmap-test")
	defer paramtable.Get().Reset(key)
	ctx := context.Background()

	collection := suite.manager.Collection.Get(suite.collectionID)
	for _, field := range collection.Schema().GetFields() {
		field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
			Key:   common.MmapEnabledKey,
			Value: "true",
		})
	}

	msgLength := 100
	// Load sealed
	binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.NoError(err)

	_, err = suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		BinlogPaths:   binlogs,
		Statslogs:     statsLogs,
		NumOfRows:     int64(msgLength),
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	})
	suite.NoError(err)
}

func (suite *SegmentLoaderSuite) TestRunOutMemory() {
	ctx := context.Background()
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.Key, "0")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.Key)

	msgLength := 4

	// Load sealed
	binlogs, statsLogs, err := mock_segcore.SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.NoError(err)

	_, err = suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		BinlogPaths:   binlogs,
		Statslogs:     statsLogs,
		NumOfRows:     int64(msgLength),
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	})
	// TODO: this case should be fixed!
	// currently the binlog size is all zero, this expected error is triggered by interim index usage calculation instead of binlog size
	if !paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool() {
		suite.Error(err)
	}

	// Load growing
	binlogs, statsLogs, err = mock_segcore.SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID+1,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.NoError(err)

	_, err = suite.loader.Load(ctx, suite.collectionID, SegmentTypeGrowing, 0, &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID + 1,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		BinlogPaths:   binlogs,
		Statslogs:     statsLogs,
		NumOfRows:     int64(msgLength),
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	})
	if !paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool() {
		suite.Error(err)
	}

	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapDirPath.Key, "./mmap")
	_, err = suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		BinlogPaths:   binlogs,
		Statslogs:     statsLogs,
		NumOfRows:     int64(msgLength),
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	})
	if !paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool() {
		suite.Error(err)
	}
	_, err = suite.loader.Load(ctx, suite.collectionID, SegmentTypeGrowing, 0, &querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID + 1,
		PartitionID:   suite.partitionID,
		CollectionID:  suite.collectionID,
		BinlogPaths:   binlogs,
		Statslogs:     statsLogs,
		NumOfRows:     int64(msgLength),
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	})
	if !paramtable.Get().QueryNodeCfg.TieredEvictionEnabled.GetAsBool() {
		suite.Error(err)
	}
}

type SegmentLoaderDetailSuite struct {
	suite.Suite

	loader  *segmentLoader
	manager *Manager

	rootPath     string
	chunkManager storage.ChunkManager

	// Data
	collectionID int64
	partitionID  int64
	segmentID    int64
	schema       *schemapb.CollectionSchema
	segmentNum   int
}

func (suite *SegmentLoaderDetailSuite) SetupSuite() {
	paramtable.Init()
	suite.rootPath = suite.T().Name()
	suite.collectionID = rand.Int63()
	suite.partitionID = rand.Int63()
	suite.segmentID = rand.Int63()
	suite.segmentNum = 5
	suite.schema = mock_segcore.GenTestCollectionSchema("test", schemapb.DataType_Int64, false)
}

func (suite *SegmentLoaderDetailSuite) SetupTest() {
	// Dependencies
	suite.manager = NewManager()

	ctx := context.Background()
	chunkManagerFactory := storage.NewTestChunkManagerFactory(paramtable.Get(), suite.rootPath)
	suite.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	suite.loader = NewLoader(ctx, suite.manager, suite.chunkManager)
	initcore.InitRemoteChunkManager(paramtable.Get())

	// Data
	schema := mock_segcore.GenTestCollectionSchema("test", schemapb.DataType_Int64, false)

	indexMeta := mock_segcore.GenTestIndexMeta(suite.collectionID, schema)
	loadMeta := &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: suite.collectionID,
		PartitionIDs: []int64{suite.partitionID},
	}

	suite.Require().NoError(suite.manager.Collection.PutOrRef(suite.collectionID, schema, indexMeta, loadMeta))
}

func (suite *SegmentLoaderDetailSuite) TestWaitSegmentLoadDone() {
	suite.Run("wait_success", func() {
		infos := suite.loader.prepare(context.Background(), SegmentTypeSealed, &querypb.SegmentLoadInfo{
			SegmentID:     suite.segmentID,
			PartitionID:   suite.partitionID,
			CollectionID:  suite.collectionID,
			NumOfRows:     100,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		})
		go func() {
			<-time.After(time.Second)
			suite.loader.notifyLoadFinish(infos...)
		}()

		err := suite.loader.waitSegmentLoadDone(context.Background(), SegmentTypeSealed, []int64{suite.segmentID}, 0)
		suite.NoError(err)
	})

	suite.Run("wait_failure", func() {
		suite.SetupTest()

		infos := suite.loader.prepare(context.Background(), SegmentTypeSealed, &querypb.SegmentLoadInfo{
			SegmentID:     suite.segmentID,
			PartitionID:   suite.partitionID,
			CollectionID:  suite.collectionID,
			NumOfRows:     100,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		})
		go func() {
			<-time.After(time.Second)
			suite.loader.unregister(infos...)
		}()

		err := suite.loader.waitSegmentLoadDone(context.Background(), SegmentTypeSealed, []int64{suite.segmentID}, 0)
		suite.Error(err)
	})

	suite.Run("wait_timeout", func() {
		suite.SetupTest()

		suite.loader.prepare(context.Background(), SegmentTypeSealed, &querypb.SegmentLoadInfo{
			SegmentID:     suite.segmentID,
			PartitionID:   suite.partitionID,
			CollectionID:  suite.collectionID,
			NumOfRows:     100,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		})

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := suite.loader.waitSegmentLoadDone(ctx, SegmentTypeSealed, []int64{suite.segmentID}, 0)
		suite.Error(err)
		suite.True(merr.IsCanceledOrTimeout(err))
	})
}

func TestSegmentLoaderPrepareLoadsWhenSegmentIsNotActive(t *testing.T) {
	segmentID := rand.Int63()
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:     segmentID,
		PartitionID:   rand.Int63(),
		CollectionID:  rand.Int63(),
		NumOfRows:     100,
		InsertChannel: "by-dev-rootcoord-dml_0_1v0",
	}
	segMgr := &segmentManager{}
	getWithTypeCalled := atomic.NewInt32(0)
	patchGetWithType := mockey.Mock(mockey.GetMethod(segMgr, "GetWithType")).
		To(func(segmentID typeutil.UniqueID, typ SegmentType) Segment {
			getWithTypeCalled.Inc()
			assert.EqualValues(t, loadInfo.GetSegmentID(), segmentID)
			assert.Equal(t, SegmentTypeGrowing, typ)
			return nil
		}).
		Build()
	defer patchGetWithType.UnPatch()

	loader := &segmentLoader{
		manager: &Manager{
			Segment: segMgr,
		},
		loadingSegments: typeutil.NewConcurrentMap[int64, *loadResult](),
	}

	infos := loader.prepare(context.Background(), SegmentTypeGrowing, loadInfo)

	assert.Len(t, infos, 1)
	assert.Equal(t, segmentID, infos[0].GetSegmentID())
	assert.True(t, loader.loadingSegments.Contain(segmentID))
	assert.EqualValues(t, 1, getWithTypeCalled.Load())
}

func TestConfigureUseTakeForOutput(t *testing.T) {
	paramtable.Init()
	internalKey := paramtable.Get().QueryNodeCfg.InternalCollectionUseTakeForOutput.Key
	externalKey := paramtable.Get().QueryNodeCfg.ExternalCollectionUseTakeForOutput.Key
	paramtable.Get().Reset(internalKey)
	paramtable.Get().Reset(externalKey)
	defer paramtable.Get().Reset(internalKey)
	defer paramtable.Get().Reset(externalKey)

	t.Run("nil load info", func(t *testing.T) {
		assert.NotPanics(t, func() {
			configureUseTakeForOutput(nil, &schemapb.CollectionSchema{})
		})
	})

	t.Run("internal collection disabled by default", func(t *testing.T) {
		loadInfo := &querypb.SegmentLoadInfo{}
		configureUseTakeForOutput(loadInfo, &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64}},
		})
		assert.False(t, loadInfo.GetUseTakeForOutput())
	})

	t.Run("internal collection uses internal switch", func(t *testing.T) {
		paramtable.Get().Save(internalKey, "true")
		defer paramtable.Get().Reset(internalKey)

		loadInfo := &querypb.SegmentLoadInfo{}
		configureUseTakeForOutput(loadInfo, &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64}},
		})
		assert.True(t, loadInfo.GetUseTakeForOutput())
	})

	t.Run("external collection uses external switch", func(t *testing.T) {
		paramtable.Get().Save(externalKey, "false")
		defer paramtable.Get().Reset(externalKey)

		loadInfo := &querypb.SegmentLoadInfo{}
		configureUseTakeForOutput(loadInfo, &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, ExternalField: "id"}},
		})
		assert.False(t, loadInfo.GetUseTakeForOutput())
	})
}

func (suite *SegmentLoaderDetailSuite) TestRequestResource() {
	suite.Run("out_of_memory_zero_info", func() {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.Key, "0")
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.Key)

		_, err := suite.loader.requestResource(context.Background())
		suite.NoError(err)
	})

	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    100,
		CollectionID: suite.collectionID,
		Level:        datapb.SegmentLevel_L0,
		Deltalogs: []*datapb.FieldBinlog{
			{
				Binlogs: []*datapb.Binlog{
					{LogSize: 10000, MemorySize: 10000},
					{LogSize: 12000, MemorySize: 12000},
				},
			},
		},
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
	}

	suite.Run("l0_segment_deltalog", func() {
		paramtable.Get().Save(paramtable.Get().QueryNodeCfg.DeltaDataExpansionRate.Key, "50")
		defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.DeltaDataExpansionRate.Key)

		resource, err := suite.loader.requestResource(context.Background(), loadInfo)

		suite.NoError(err)
		suite.EqualValues(1100000, resource.Resource.MemorySize)
	})
}

func (suite *SegmentLoaderDetailSuite) TestCheckSegmentSizeWithDiskLimit() {
	ctx := context.Background()

	// Save original value and restore after test
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.Key, "1") // 1MB
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.Key)

	// Set disk usage threshold
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.Key, "0.8") // 80% threshold
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.Key)

	// set mmap, trigger dist cost
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	// Create a test segment that would exceed the disk limit
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    suite.segmentID,
		PartitionID:  suite.partitionID,
		CollectionID: suite.collectionID,
		NumOfRows:    1000,
		BinlogPaths: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						LogPath:    "test_path",
						LogSize:    1024 * 1024 * 1024 * 2, // 2GB
						MemorySize: 1024 * 1024 * 1024 * 4,
					},
				},
			},
			{
				FieldID: 105,
				Binlogs: []*datapb.Binlog{
					{
						LogPath:    "test_path",
						LogSize:    1024 * 1024 * 1024 * 2, // 2GB
						MemorySize: 1024 * 1024 * 1024 * 4,
					},
				},
			},
		},
	}

	memUsage := uint64(100 * 1024 * 1024)  // 100MB
	totalMem := uint64(1024 * 1024 * 1024) // 1GB
	localDiskUsage := int64(100 * 1024)    // 100KB

	_, _, err := suite.loader.checkSegmentSize(ctx, []*querypb.SegmentLoadInfo{loadInfo}, memUsage, totalMem, localDiskUsage)
	suite.Error(err)
	suite.True(errors.Is(err, merr.ErrSegmentRequestResourceFailed))
}

func (suite *SegmentLoaderDetailSuite) TestCheckSegmentSizeWithMemoryLimit() {
	ctx := context.Background()

	// Create a test segment that would exceed the memory limit
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    suite.segmentID,
		PartitionID:  suite.partitionID,
		CollectionID: suite.collectionID,
		NumOfRows:    1000,
		BinlogPaths: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						LogPath:    "test_path",
						LogSize:    1024 * 1024,       // 1MB
						MemorySize: 1024 * 1024 * 900, // 900MB
					},
				},
			},
		},
	}

	memUsage := uint64(100 * 1024 * 1024)  // 100MB
	totalMem := uint64(1024 * 1024 * 1024) // 1GB
	localDiskUsage := int64(100 * 1024)    // 100KB

	// Set memory threshold to 80%
	paramtable.Get().Save("queryNode.overloadedMemoryThresholdPercentage", "0.8")

	_, _, err := suite.loader.checkSegmentSize(ctx, []*querypb.SegmentLoadInfo{loadInfo}, memUsage, totalMem, localDiskUsage)
	suite.Error(err)
	suite.True(errors.Is(err, merr.ErrSegmentRequestResourceFailed))
}

// SegmentLoaderTextIndexEstimateSuite tests resource estimation for text index (TextStatsLogs).
// These tests directly call estimateLoadingResourceUsageOfSegment and
// estimateLogicalResourceUsageOfSegment to verify that TextStatsLogs are correctly
// accounted for in both physical loading and logical resource estimates.
type SegmentLoaderTextIndexEstimateSuite struct {
	suite.Suite
	schema *schemapb.CollectionSchema
}

func (suite *SegmentLoaderTextIndexEstimateSuite) SetupSuite() {
	paramtable.Init()
	// Minimal schema: just a PK field + one VarChar field; no need for full schema
	// since text stats estimation only iterates loadInfo.GetTextStatsLogs() directly.
	suite.schema = &schemapb.CollectionSchema{
		Name: "test_text_estimate",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "text2", DataType: schemapb.DataType_VarChar},
		},
	}
}

// baseLoadInfo returns a minimal SegmentLoadInfo with the given TextStatsLogs.
func (suite *SegmentLoaderTextIndexEstimateSuite) baseLoadInfo(textStats map[int64]*datapb.TextIndexStats) *querypb.SegmentLoadInfo {
	return &querypb.SegmentLoadInfo{
		SegmentID:     1,
		PartitionID:   2,
		CollectionID:  3,
		NumOfRows:     100,
		TextStatsLogs: textStats,
	}
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLoadingEstimate_NonMmap_NoTieredEviction() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	const textIndexSize = int64(50 * 1024 * 1024) // 50 MB
	loadInfo := suite.baseLoadInfo(map[int64]*datapb.TextIndexStats{
		101: {FieldID: 101, MemorySize: textIndexSize},
	})

	factor := resourceEstimateFactor{
		TieredEvictionEnabled:       false,
		textIndexExpansionFactor:    1.0,
		deltaDataExpansionFactor:    2.0,
		jsonKeyStatsExpansionFactor: 1.0,
	}
	usage, err := estimateLoadingResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	suite.EqualValues(textIndexSize, usage.MemorySize, "non-mmap text index must be counted in memory")
	suite.EqualValues(0, usage.DiskSize, "non-mmap text index must not be counted in disk")
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLoadingEstimate_Mmap_NoTieredEviction() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	const textIndexSize = int64(50 * 1024 * 1024) // 50 MB
	loadInfo := suite.baseLoadInfo(map[int64]*datapb.TextIndexStats{
		101: {FieldID: 101, MemorySize: textIndexSize},
	})

	factor := resourceEstimateFactor{
		TieredEvictionEnabled:       false,
		textIndexExpansionFactor:    1.0,
		deltaDataExpansionFactor:    2.0,
		jsonKeyStatsExpansionFactor: 1.0,
	}
	usage, err := estimateLoadingResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	suite.EqualValues(0, usage.MemorySize, "mmap text index must not be counted in memory")
	suite.EqualValues(textIndexSize, usage.DiskSize, "mmap text index must be counted in disk")
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLoadingEstimate_TieredEvictionEnabled_TextIndexSkipped() {
	// When tiered eviction is enabled the caching layer manages text indexes,
	// so they must NOT be added to the physical loading estimate.
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	const textIndexSize = int64(50 * 1024 * 1024)
	loadInfo := suite.baseLoadInfo(map[int64]*datapb.TextIndexStats{
		101: {FieldID: 101, MemorySize: textIndexSize},
	})

	factor := resourceEstimateFactor{
		TieredEvictionEnabled:    true,
		textIndexExpansionFactor: 1.0,
		deltaDataExpansionFactor: 2.0,
	}
	usage, err := estimateLoadingResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	suite.EqualValues(0, usage.MemorySize, "text index must be skipped when tiered eviction is enabled")
	suite.EqualValues(0, usage.DiskSize)
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLoadingEstimate_MultipleTextFields() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	const size1 = int64(30 * 1024 * 1024) // 30 MB
	const size2 = int64(20 * 1024 * 1024) // 20 MB
	loadInfo := suite.baseLoadInfo(map[int64]*datapb.TextIndexStats{
		101: {FieldID: 101, MemorySize: size1},
		102: {FieldID: 102, MemorySize: size2},
	})

	factor := resourceEstimateFactor{
		TieredEvictionEnabled:    false,
		textIndexExpansionFactor: 1.0,
		deltaDataExpansionFactor: 2.0,
	}
	usage, err := estimateLoadingResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	suite.EqualValues(size1+size2, usage.MemorySize, "all text index sizes must be summed")
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLoadingEstimate_ExpansionFactor() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	const textIndexSize = int64(40 * 1024 * 1024) // 40 MB
	const expansionFactor = 1.5
	loadInfo := suite.baseLoadInfo(map[int64]*datapb.TextIndexStats{
		101: {FieldID: 101, MemorySize: textIndexSize},
	})

	factor := resourceEstimateFactor{
		TieredEvictionEnabled:    false,
		textIndexExpansionFactor: expansionFactor,
		deltaDataExpansionFactor: 2.0,
	}
	usage, err := estimateLoadingResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	expected := uint64(float64(textIndexSize) * expansionFactor)
	suite.EqualValues(expected, usage.MemorySize, "expansion factor must be applied to text index size")
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLoadingEstimate_EmptyTextStats_NoContribution() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	loadInfo := suite.baseLoadInfo(nil) // no TextStatsLogs
	factor := resourceEstimateFactor{
		TieredEvictionEnabled:    false,
		textIndexExpansionFactor: 1.0,
		deltaDataExpansionFactor: 2.0,
	}
	usage, err := estimateLoadingResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	suite.EqualValues(0, usage.MemorySize)
	suite.EqualValues(0, usage.DiskSize)
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLoadingEstimate_Mmap_TieredEvictionEnabled_TextIndexSkipped() {
	// When tiered eviction is enabled, text index is managed by caching layer
	// regardless of mmap setting — must NOT appear in physical loading estimate.
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	const textIndexSize = int64(50 * 1024 * 1024)
	loadInfo := suite.baseLoadInfo(map[int64]*datapb.TextIndexStats{
		101: {FieldID: 101, MemorySize: textIndexSize},
	})

	factor := resourceEstimateFactor{
		TieredEvictionEnabled:    true,
		textIndexExpansionFactor: 1.0,
		deltaDataExpansionFactor: 2.0,
	}
	usage, err := estimateLoadingResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	suite.EqualValues(0, usage.MemorySize, "text index must be skipped when tiered eviction is enabled (mmap)")
	suite.EqualValues(0, usage.DiskSize, "text index must be skipped when tiered eviction is enabled (mmap)")
}

// --- estimateLogicalResourceUsageOfSegment tests ---

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLogicalEstimate_NonMmap_EvictableMemory() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	const textIndexSize = int64(60 * 1024 * 1024) // 60 MB
	loadInfo := suite.baseLoadInfo(map[int64]*datapb.TextIndexStats{
		101: {FieldID: 101, MemorySize: textIndexSize},
	})

	factor := resourceEstimateFactor{
		TieredEvictionEnabled:           true,
		textIndexExpansionFactor:        1.0,
		deltaDataExpansionFactor:        2.0,
		TieredEvictableMemoryCacheRatio: 1.0, // 100% of evictable memory is counted
		TieredEvictableDiskCacheRatio:   1.0,
	}
	usage, err := estimateLogicalResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	suite.EqualValues(textIndexSize, usage.MemorySize, "non-mmap text index must be in evictable memory")
	suite.EqualValues(0, usage.DiskSize)
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLogicalEstimate_Mmap_EvictableDisk() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	const textIndexSize = int64(60 * 1024 * 1024) // 60 MB
	loadInfo := suite.baseLoadInfo(map[int64]*datapb.TextIndexStats{
		101: {FieldID: 101, MemorySize: textIndexSize},
	})

	factor := resourceEstimateFactor{
		TieredEvictionEnabled:           true,
		textIndexExpansionFactor:        1.0,
		deltaDataExpansionFactor:        2.0,
		TieredEvictableMemoryCacheRatio: 1.0,
		TieredEvictableDiskCacheRatio:   1.0,
	}
	usage, err := estimateLogicalResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	suite.EqualValues(0, usage.MemorySize, "mmap text index must not be in memory")
	suite.EqualValues(textIndexSize, usage.DiskSize, "mmap text index must be in evictable disk")
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLogicalEstimate_CacheRatioApplied() {
	// Verify TieredEvictableMemoryCacheRatio is applied to the evictable text index size.
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	const textIndexSize = int64(100 * 1024 * 1024) // 100 MB
	loadInfo := suite.baseLoadInfo(map[int64]*datapb.TextIndexStats{
		101: {FieldID: 101, MemorySize: textIndexSize},
	})

	const cacheRatio = 0.5
	factor := resourceEstimateFactor{
		TieredEvictionEnabled:           true,
		textIndexExpansionFactor:        1.0,
		deltaDataExpansionFactor:        2.0,
		TieredEvictableMemoryCacheRatio: cacheRatio,
		TieredEvictableDiskCacheRatio:   1.0,
	}
	usage, err := estimateLogicalResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	expected := uint64(float64(textIndexSize) * cacheRatio)
	suite.EqualValues(expected, usage.MemorySize, "cache ratio must be applied to evictable text index memory")
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLogicalEstimate_EmptyTextStats_NoContribution() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	loadInfo := suite.baseLoadInfo(nil) // no TextStatsLogs
	factor := resourceEstimateFactor{
		TieredEvictionEnabled:           true,
		textIndexExpansionFactor:        1.0,
		deltaDataExpansionFactor:        2.0,
		TieredEvictableMemoryCacheRatio: 1.0,
		TieredEvictableDiskCacheRatio:   1.0,
	}
	usage, err := estimateLogicalResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	suite.EqualValues(0, usage.MemorySize)
	suite.EqualValues(0, usage.DiskSize)
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLogicalEstimate_MultipleTextFields() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	const size1 = int64(30 * 1024 * 1024)
	const size2 = int64(20 * 1024 * 1024)
	loadInfo := suite.baseLoadInfo(map[int64]*datapb.TextIndexStats{
		101: {FieldID: 101, MemorySize: size1},
		102: {FieldID: 102, MemorySize: size2},
	})

	factor := resourceEstimateFactor{
		TieredEvictionEnabled:           true,
		textIndexExpansionFactor:        1.0,
		deltaDataExpansionFactor:        2.0,
		TieredEvictableMemoryCacheRatio: 1.0,
		TieredEvictableDiskCacheRatio:   1.0,
	}
	usage, err := estimateLogicalResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	suite.EqualValues(size1+size2, usage.MemorySize, "all text index sizes must be summed in logical estimate")
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLogicalEstimate_DiskCacheRatioApplied() {
	// Verify TieredEvictableDiskCacheRatio is applied to mmap text index size.
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	const textIndexSize = int64(100 * 1024 * 1024)
	loadInfo := suite.baseLoadInfo(map[int64]*datapb.TextIndexStats{
		101: {FieldID: 101, MemorySize: textIndexSize},
	})

	const diskCacheRatio = 0.3
	factor := resourceEstimateFactor{
		TieredEvictionEnabled:           true,
		textIndexExpansionFactor:        1.0,
		deltaDataExpansionFactor:        2.0,
		TieredEvictableMemoryCacheRatio: 1.0,
		TieredEvictableDiskCacheRatio:   diskCacheRatio,
	}
	usage, err := estimateLogicalResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	suite.EqualValues(0, usage.MemorySize, "mmap text index must not be in memory")
	expected := uint64(float64(textIndexSize) * diskCacheRatio)
	suite.EqualValues(expected, usage.DiskSize, "disk cache ratio must be applied to mmap text index")
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLogicalEstimate_ExpansionFactor() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	const textIndexSize = int64(40 * 1024 * 1024)
	const expansionFactor = 2.0
	loadInfo := suite.baseLoadInfo(map[int64]*datapb.TextIndexStats{
		101: {FieldID: 101, MemorySize: textIndexSize},
	})

	factor := resourceEstimateFactor{
		TieredEvictionEnabled:           true,
		textIndexExpansionFactor:        expansionFactor,
		deltaDataExpansionFactor:        2.0,
		TieredEvictableMemoryCacheRatio: 1.0,
		TieredEvictableDiskCacheRatio:   1.0,
	}
	usage, err := estimateLogicalResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	expected := uint64(float64(textIndexSize) * expansionFactor)
	suite.EqualValues(expected, usage.MemorySize)
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLogicalEstimate_NonEvictableTextStatsNotCacheRatioed() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	schema := &schemapb.CollectionSchema{
		Name: "test_text_estimate_nonevictable",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.EvictableEnabledKey, Value: "false"},
				},
			},
		},
	}

	const textIndexSize = int64(100 * 1024 * 1024)
	loadInfo := suite.baseLoadInfo(map[int64]*datapb.TextIndexStats{
		101: {FieldID: 101, MemorySize: textIndexSize},
	})

	factor := resourceEstimateFactor{
		TieredEvictionEnabled:           true,
		textIndexExpansionFactor:        1.0,
		deltaDataExpansionFactor:        2.0,
		TieredEvictableMemoryCacheRatio: 0.1,
		TieredEvictableDiskCacheRatio:   0.1,
	}
	usage, err := estimateLogicalResourceUsageOfSegment(schema, loadInfo, factor)
	suite.NoError(err)
	suite.EqualValues(textIndexSize, usage.MemorySize, "non-evictable text stats must not be multiplied by cache ratio")
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLogicalEstimate_NonEvictableRawFieldNotCacheRatioed() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	schema := &schemapb.CollectionSchema{
		Name: "test_raw_estimate_nonevictable",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID:  101,
				Name:     "scalar",
				DataType: schemapb.DataType_Int64,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.EvictableEnabledKey, Value: "false"},
				},
			},
		},
	}
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    1,
		PartitionID:  2,
		CollectionID: 3,
		NumOfRows:    100,
		BinlogPaths: []*datapb.FieldBinlog{
			{
				FieldID: 101,
				Binlogs: []*datapb.Binlog{
					{MemorySize: 1000, LogSize: 1000},
				},
			},
		},
	}
	factor := resourceEstimateFactor{
		TieredEvictionEnabled:           true,
		TieredEvictableMemoryCacheRatio: 0.1,
		TieredEvictableDiskCacheRatio:   0.1,
	}
	usage, err := estimateLogicalResourceUsageOfSegment(schema, loadInfo, factor)
	suite.NoError(err)
	suite.EqualValues(1000, usage.MemorySize, "non-evictable raw field must not be multiplied by cache ratio")
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLogicalEstimate_StorageV2GroupAnyChildFalseIsNonEvictable() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	schema := &schemapb.CollectionSchema{
		Name: "test_group_estimate_nonevictable",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID:  101,
				Name:     "scalar_false",
				DataType: schemapb.DataType_Int64,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.EvictableEnabledKey, Value: "false"},
				},
			},
			{
				FieldID:  102,
				Name:     "scalar_true",
				DataType: schemapb.DataType_Int64,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.EvictableEnabledKey, Value: "true"},
				},
			},
		},
	}
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:      1,
		PartitionID:    2,
		CollectionID:   3,
		NumOfRows:      100,
		StorageVersion: storage.StorageV2,
		BinlogPaths: []*datapb.FieldBinlog{
			{
				FieldID:     0,
				ChildFields: []int64{101, 102},
				Binlogs: []*datapb.Binlog{
					{MemorySize: 1000, LogSize: 1000},
				},
			},
		},
	}
	factor := resourceEstimateFactor{
		TieredEvictionEnabled:           true,
		TieredEvictableMemoryCacheRatio: 0.1,
		TieredEvictableDiskCacheRatio:   0.1,
	}
	usage, err := estimateLogicalResourceUsageOfSegment(schema, loadInfo, factor)
	suite.NoError(err)
	suite.EqualValues(1000, usage.MemorySize, "group with any non-evictable child must not be multiplied by cache ratio")
}

func (suite *SegmentLoaderTextIndexEstimateSuite) TestLoadingEstimate_TieredEvictionEnabled_NonEvictableRawFieldCounted() {
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.MmapScalarField.Key, "false")
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.MmapScalarField.Key)

	schema := &schemapb.CollectionSchema{
		Name: "test_loading_raw_nonevictable",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID:  101,
				Name:     "scalar",
				DataType: schemapb.DataType_Int64,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.EvictableEnabledKey, Value: "false"},
				},
			},
		},
	}
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    1,
		PartitionID:  2,
		CollectionID: 3,
		NumOfRows:    100,
		BinlogPaths: []*datapb.FieldBinlog{
			{
				FieldID: 101,
				Binlogs: []*datapb.Binlog{
					{MemorySize: 1000, LogSize: 1000},
				},
			},
		},
	}
	factor := resourceEstimateFactor{TieredEvictionEnabled: true}
	usage, err := estimateLoadingResourceUsageOfSegment(schema, loadInfo, factor)
	suite.NoError(err)
	suite.EqualValues(1000, usage.MemorySize, "non-evictable raw field must reserve loading memory under tiered eviction")
}

func TestSeparateLoadInfoV2_ExternalFieldIndexNotSkipped(t *testing.T) {
	// Verifies that indexes on external fields are NOT skipped in separateLoadInfoV2.
	// Previously, external field indexes were filtered out, preventing index loading for external tables.
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "__virtual_pk__", FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: "id", FieldID: 101, DataType: schemapb.DataType_Int64, ExternalField: "id"},
			{Name: "embedding", FieldID: 102, DataType: schemapb.DataType_FloatVector, ExternalField: "embedding"},
		},
	}

	loadInfo := &querypb.SegmentLoadInfo{
		StorageVersion: storage.StorageV3,
		IndexInfos: []*querypb.FieldIndexInfo{
			{
				FieldID:        101,
				IndexID:        1001,
				IndexFilePaths: []string{"index/101/file1", "index/101/file2"},
			},
			{
				FieldID:        102,
				IndexID:        1002,
				IndexFilePaths: []string{"index/102/file1"},
			},
		},
		// External segments have no BinlogPaths — data is loaded from manifest
		BinlogPaths: []*datapb.FieldBinlog{},
	}

	indexedFieldInfos, fieldBinlogs, _, _, _, _, _ := separateLoadInfoV2(loadInfo, schema)

	// External field indexes should be included (not skipped)
	assert.Len(t, indexedFieldInfos, 0, "no binlog paths means no indexed field infos via binlog matching")
	assert.Len(t, fieldBinlogs, 0, "no binlog paths for external segments")

	// The key point: fieldID2IndexInfo should contain external field indexes.
	// Even though indexedFieldInfos is empty (no binlog to match), the indexes are
	// still available in loadInfo.IndexInfos for the C++ managed loading path.
	// This test ensures the code doesn't filter them out from IndexInfos.
	assert.Len(t, loadInfo.IndexInfos, 2, "IndexInfos should still contain all external field indexes")
}

func TestSeparateLoadInfoV2_MixedExternalAndNormalFields(t *testing.T) {
	// Verify that normal field indexes are matched while external field binlogs are skipped.
	// Index info for both external and normal fields should remain in IndexInfos.
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{Name: "__virtual_pk__", FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{Name: "id", FieldID: 101, DataType: schemapb.DataType_Int64, ExternalField: "id"},
			{Name: "normal_field", FieldID: 103, DataType: schemapb.DataType_Int64},
		},
	}

	loadInfo := &querypb.SegmentLoadInfo{
		StorageVersion: storage.StorageV3,
		IndexInfos: []*querypb.FieldIndexInfo{
			{
				FieldID:        101,
				IndexID:        1001,
				IndexFilePaths: []string{"index/101/file1"},
			},
			{
				FieldID:        103,
				IndexID:        1003,
				IndexFilePaths: []string{"index/103/file1"},
			},
		},
		BinlogPaths: []*datapb.FieldBinlog{
			// Normal field has binlog; external field 101 does not
			{FieldID: 103, Binlogs: []*datapb.Binlog{{LogPath: "binlog/103"}}},
		},
	}

	indexedFieldInfos, fieldBinlogs, _, _, _, _, _ := separateLoadInfoV2(loadInfo, schema)

	// Normal field 103 has binlog + index → indexed
	assert.Len(t, indexedFieldInfos, 1)
	assert.Contains(t, indexedFieldInfos, int64(1003))
	assert.Len(t, fieldBinlogs, 0)
	// IndexInfos still contains both external and normal field indexes
	assert.Len(t, loadInfo.IndexInfos, 2)
}

// ExternalSegmentEstimateSuite tests resource estimation for external table segments
// with fake binlogs (pre-computed MemorySize from DataNode Take sampling).
type ExternalSegmentEstimateSuite struct {
	suite.Suite
	schema *schemapb.CollectionSchema
}

func (suite *ExternalSegmentEstimateSuite) SetupSuite() {
	paramtable.Init()
	suite.schema = &schemapb.CollectionSchema{
		Name:           "test_external_estimate",
		ExternalSource: "s3://bucket/data/",
		ExternalSpec:   `{"format":"parquet"}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "__virtual_pk__", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col"},
			{
				FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, ExternalField: "vec_col",
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "128"}},
			},
		},
	}
}

func (suite *ExternalSegmentEstimateSuite) externalLoadInfo(numRows int64, memorySize int64) *querypb.SegmentLoadInfo {
	return &querypb.SegmentLoadInfo{
		SegmentID:      1,
		CollectionID:   100,
		NumOfRows:      numRows,
		StorageVersion: storage.StorageV3,
		ManifestPath:   `{"ver":1,"base_path":"external/100/segments/1"}`,
		BinlogPaths: []*datapb.FieldBinlog{
			{
				FieldID:     0, // DefaultShortColumnGroupID
				ChildFields: []int64{100, 101, 102},
				Binlogs: []*datapb.Binlog{
					{LogID: 1, EntriesNum: numRows, MemorySize: memorySize, LogSize: memorySize},
				},
			},
		},
	}
}

func (suite *ExternalSegmentEstimateSuite) TestEstimatedBytesPerRow() {
	loadInfo := suite.externalLoadInfo(1000, 576000) // 576 bytes/row
	factor := resourceEstimateFactor{externalRawDataFactor: 1.0}

	usage, err := estimateLoadingResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	suite.NotNil(usage)
	suite.Equal(int64(576), loadInfo.EstimatedBytesPerRow)
}

func (suite *ExternalSegmentEstimateSuite) TestExternalRawDataFactor() {
	loadInfo := suite.externalLoadInfo(1000, 100000)
	factor := resourceEstimateFactor{externalRawDataFactor: 1.5}

	usage, err := estimateLoadingResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	// PART 2 adds binlogSize (100000) to memory
	// PART 2.5 adds extra = 100000 * (1.5 - 1.0) = 50000
	suite.True(usage.MemorySize >= 150000, "should include raw data factor: got %d", usage.MemorySize)
}

func (suite *ExternalSegmentEstimateSuite) TestExternalRawDataFactor_NoExtraWhenFactorLe1() {
	loadInfo := suite.externalLoadInfo(1000, 100000)
	factor := resourceEstimateFactor{externalRawDataFactor: 0.8}

	usage, err := estimateLoadingResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)
	// factor <= 1.0, no extra memory added by PART 2.5
	suite.True(usage.MemorySize >= 100000, "should include base binlog size")
}

func (suite *ExternalSegmentEstimateSuite) TestLazyLoadSubtractsRawData() {
	// Build a separate schema with warmup=disable on all external fields
	lazySchema := &schemapb.CollectionSchema{
		Name:           "test_external_lazy",
		ExternalSource: "s3://bucket/data/",
		ExternalSpec:   `{"format":"parquet"}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "__virtual_pk__", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar, ExternalField: "text_col",
				TypeParams: []*commonpb.KeyValuePair{{Key: common.WarmupKey, Value: common.WarmupDisable}},
			},
			{
				FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, ExternalField: "vec_col",
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
					{Key: common.WarmupKey, Value: common.WarmupDisable},
				},
			},
		},
	}

	// Override global warmup defaults to disable
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.TieredWarmupScalarField.Key, common.WarmupDisable)
	paramtable.Get().Save(paramtable.Get().QueryNodeCfg.TieredWarmupVectorField.Key, common.WarmupDisable)
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.TieredWarmupScalarField.Key)
	defer paramtable.Get().Reset(paramtable.Get().QueryNodeCfg.TieredWarmupVectorField.Key)

	suite.True(isExternalCollectionLazyLoad(lazySchema), "schema should be detected as lazy load")

	loadInfo := suite.externalLoadInfo(1000, 100000)
	factor := resourceEstimateFactor{externalRawDataFactor: 1.0}

	// With lazy load, memory estimate should be less than or equal to
	// the non-lazy-load case (which is at least binlogSize=100000).
	// The exact value depends on PART 2's mmap/tiered logic.
	nonLazyUsage, err := estimateLoadingResourceUsageOfSegment(suite.schema, loadInfo, factor)
	suite.NoError(err)

	lazyUsage, err := estimateLoadingResourceUsageOfSegment(lazySchema, loadInfo, factor)
	suite.NoError(err)

	suite.True(lazyUsage.MemorySize <= nonLazyUsage.MemorySize,
		"lazy load memory (%d) should be <= non-lazy (%d)", lazyUsage.MemorySize, nonLazyUsage.MemorySize)
}

func TestGpuIndexRequiresGpu(t *testing.T) {
	tests := []struct {
		name     string
		params   []*commonpb.KeyValuePair
		expected bool
	}{
		{
			name: "GPU_CAGRA adapt for CPU",
			params: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "GPU_CAGRA"},
				{Key: "adapt_for_cpu", Value: "true"},
			},
			expected: false,
		},
		{
			name: "GPU_CUVS_CAGRA adapt for CPU",
			params: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "GPU_CUVS_CAGRA"},
				{Key: "adapt_for_cpu", Value: "1"},
			},
			expected: false,
		},
		{
			name: "GPU_CAGRA without adapt for CPU",
			params: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "GPU_CAGRA"},
			},
			expected: true,
		},
		{
			name: "GPU_CAGRA invalid adapt for CPU",
			params: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "GPU_CAGRA"},
				{Key: "adapt_for_cpu", Value: "invalid"},
			},
			expected: true,
		},
		{
			name: "other GPU index ignores adapt for CPU",
			params: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "GPU_IVF_FLAT"},
				{Key: "adapt_for_cpu", Value: "true"},
			},
			expected: true,
		},
		{
			name: "CPU index",
			params: []*commonpb.KeyValuePair{
				{Key: common.IndexTypeKey, Value: "HNSW"},
			},
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expected, gpuIndexRequiresGpu(test.params))
		})
	}

	t.Run("GPU_CAGRA adapt for CPU from load config", func(t *testing.T) {
		params := paramtable.Get()
		oldEnable := params.KnowhereConfig.Enable.GetValue()
		adaptKey := params.KnowhereConfig.IndexParam.KeyPrefix + "GPU_CAGRA.load.adapt_for_cpu"
		oldAdaptValue := params.GetWithDefault(adaptKey, "")
		defer params.Save(params.KnowhereConfig.Enable.Key, oldEnable)
		defer func() {
			if oldAdaptValue == "" {
				params.Remove(adaptKey)
				return
			}
			params.Save(adaptKey, oldAdaptValue)
		}()

		params.Save(params.KnowhereConfig.Enable.Key, "true")
		params.Save(adaptKey, "true")

		assert.False(t, gpuIndexRequiresGpu([]*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "GPU_CAGRA"},
		}))
	})
}

func TestEstimateLoadingResourceUsage_DroppedFieldSkipped(t *testing.T) {
	paramtable.Init()

	// Schema only has fieldID=100 and 101; fieldID=999 is "dropped" (not in schema)
	schema := &schemapb.CollectionSchema{
		Name: "test_dropped_field",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
		},
	}

	t.Run("index on dropped field is skipped", func(t *testing.T) {
		loadInfo := &querypb.SegmentLoadInfo{
			SegmentID:    1,
			CollectionID: 10,
			NumOfRows:    100,
			IndexInfos: []*querypb.FieldIndexInfo{
				{
					FieldID:        999, // dropped field
					IndexID:        2001,
					IndexFilePaths: []string{"index/999/file1"},
				},
			},
		}
		factor := resourceEstimateFactor{}
		usage, err := estimateLoadingResourceUsageOfSegment(schema, loadInfo, factor)
		assert.NoError(t, err)
		assert.NotNil(t, usage)
	})

	t.Run("binlog with dropped field in child fields is skipped", func(t *testing.T) {
		loadInfo := &querypb.SegmentLoadInfo{
			SegmentID:    2,
			CollectionID: 10,
			NumOfRows:    100,
			BinlogPaths: []*datapb.FieldBinlog{
				{
					FieldID: 888, // column group containing a dropped field
					Binlogs: []*datapb.Binlog{
						{LogSize: 1024},
					},
					ChildFields: []int64{999}, // dropped field
				},
			},
		}
		factor := resourceEstimateFactor{}
		usage, err := estimateLoadingResourceUsageOfSegment(schema, loadInfo, factor)
		assert.NoError(t, err)
		assert.NotNil(t, usage)
	})
}

func TestSegmentLoader(t *testing.T) {
	suite.Run(t, &SegmentLoaderSuite{})
	suite.Run(t, &SegmentLoaderDetailSuite{})
	suite.Run(t, &SegmentLoaderTextIndexEstimateSuite{})
	suite.Run(t, &ExternalSegmentEstimateSuite{})
}
