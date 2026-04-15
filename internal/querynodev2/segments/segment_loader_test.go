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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
	defer paramtable.Get().QueryNodeCfg.PreferFieldDataWhenIndexHasRawData.SwapTempValue(oldPreferFieldData)

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
	defer paramtable.Get().QueryNodeCfg.PreferFieldDataWhenIndexHasRawData.SwapTempValue(oldPreferFieldData)

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
		baseSegment: baseSegment{
			loadInfo: atomic.NewPointer[querypb.SegmentLoadInfo](loadInfo),
		},
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
		baseSegment: baseSegment{
			loadInfo: atomic.NewPointer[querypb.SegmentLoadInfo](loadInfo),
		},
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

	loader            *segmentLoader
	manager           *Manager
	segmentManager    *MockSegmentManager
	collectionManager *MockCollectionManager

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
	suite.collectionManager = NewMockCollectionManager(suite.T())
	suite.segmentManager = NewMockSegmentManager(suite.T())
	suite.manager = &Manager{
		Segment:    suite.segmentManager,
		Collection: suite.collectionManager,
	}

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

	collection, err := NewCollection(suite.collectionID, schema, indexMeta, loadMeta)
	suite.Require().NoError(err)
	suite.collectionManager.EXPECT().Get(suite.collectionID).Return(collection).Maybe()
}

func (suite *SegmentLoaderDetailSuite) TestWaitSegmentLoadDone() {
	suite.Run("wait_success", func() {
		idx := 0

		var infos []*querypb.SegmentLoadInfo
		suite.segmentManager.EXPECT().Exist(mock.Anything, mock.Anything).Return(false)
		suite.segmentManager.EXPECT().GetWithType(suite.segmentID, SegmentTypeSealed).RunAndReturn(func(segmentID int64, segmentType commonpb.SegmentState) Segment {
			defer func() { idx++ }()
			if idx == 0 {
				go func() {
					<-time.After(time.Second)
					suite.loader.notifyLoadFinish(infos...)
				}()
			}
			return nil
		})
		suite.segmentManager.EXPECT().UpdateBy(mock.Anything, mock.Anything, mock.Anything).Return(0)
		infos = suite.loader.prepare(context.Background(), SegmentTypeSealed, &querypb.SegmentLoadInfo{
			SegmentID:     suite.segmentID,
			PartitionID:   suite.partitionID,
			CollectionID:  suite.collectionID,
			NumOfRows:     100,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		})

		err := suite.loader.waitSegmentLoadDone(context.Background(), SegmentTypeSealed, []int64{suite.segmentID}, 0)
		suite.NoError(err)
	})

	suite.Run("wait_failure", func() {
		suite.SetupTest()

		var idx int
		var infos []*querypb.SegmentLoadInfo
		suite.segmentManager.EXPECT().Exist(mock.Anything, mock.Anything).Return(false)
		suite.segmentManager.EXPECT().GetWithType(suite.segmentID, SegmentTypeSealed).RunAndReturn(func(segmentID int64, segmentType commonpb.SegmentState) Segment {
			defer func() { idx++ }()
			if idx == 0 {
				go func() {
					<-time.After(time.Second)
					suite.loader.unregister(infos...)
				}()
			}

			return nil
		})
		infos = suite.loader.prepare(context.Background(), SegmentTypeSealed, &querypb.SegmentLoadInfo{
			SegmentID:     suite.segmentID,
			PartitionID:   suite.partitionID,
			CollectionID:  suite.collectionID,
			NumOfRows:     100,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		})

		err := suite.loader.waitSegmentLoadDone(context.Background(), SegmentTypeSealed, []int64{suite.segmentID}, 0)
		suite.Error(err)
	})

	suite.Run("wait_timeout", func() {
		suite.SetupTest()

		suite.segmentManager.EXPECT().Exist(mock.Anything, mock.Anything).Return(false)
		suite.segmentManager.EXPECT().GetWithType(suite.segmentID, SegmentTypeSealed).RunAndReturn(func(segmentID int64, segmentType commonpb.SegmentState) Segment {
			return nil
		})
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

	// Mock collection manager to return a valid collection
	collection, err := NewCollection(suite.collectionID, suite.schema, nil, nil)
	suite.NoError(err)
	suite.collectionManager.EXPECT().Get(suite.collectionID).Return(collection)

	memUsage := uint64(100 * 1024 * 1024)  // 100MB
	totalMem := uint64(1024 * 1024 * 1024) // 1GB
	localDiskUsage := int64(100 * 1024)    // 100KB

	_, _, err = suite.loader.checkSegmentSize(ctx, []*querypb.SegmentLoadInfo{loadInfo}, memUsage, totalMem, localDiskUsage)
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

func TestSegmentLoader(t *testing.T) {
	suite.Run(t, &SegmentLoaderSuite{})
	suite.Run(t, &SegmentLoaderDetailSuite{})
	suite.Run(t, &SegmentLoaderTextIndexEstimateSuite{})
	suite.Run(t, &ExternalSegmentEstimateSuite{})
}
