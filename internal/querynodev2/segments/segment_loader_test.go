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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type SegmentLoaderSuite struct {
	suite.Suite
	loader Loader

	// Dependencies
	manager      *Manager
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
	suite.collectionID = rand.Int63()
	suite.partitionID = rand.Int63()
	suite.segmentID = rand.Int63()
	suite.segmentNum = 5
	suite.schema = GenTestCollectionSchema("test", schemapb.DataType_Int64)
}

func (suite *SegmentLoaderSuite) SetupTest() {
	// Dependencies
	suite.manager = NewManager()
	ctx := context.Background()
	// TODO:: cpp chunk manager not support local chunk manager
	//suite.chunkManager = storage.NewLocalChunkManager(storage.RootPath(
	//	fmt.Sprintf("/tmp/milvus-ut/%d", rand.Int63())))
	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(paramtable.Get())
	suite.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	suite.loader = NewLoader(suite.manager, suite.chunkManager)
	initcore.InitRemoteChunkManager(paramtable.Get())

	// Data
	schema := GenTestCollectionSchema("test", schemapb.DataType_Int64)
	indexMeta := GenTestIndexMeta(suite.collectionID, schema)
	loadMeta := &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: suite.collectionID,
		PartitionIDs: []int64{suite.partitionID},
	}
	suite.manager.Collection.Put(suite.collectionID, schema, indexMeta, loadMeta)
}

func (suite *SegmentLoaderSuite) TearDownTest() {
	ctx := context.Background()
	for i := 0; i < suite.segmentNum; i++ {
		suite.manager.Segment.Remove(suite.segmentID+int64(i), querypb.DataScope_All)
	}
	suite.chunkManager.RemoveWithPrefix(ctx, paramtable.Get().MinioCfg.RootPath.GetValue())
}

func (suite *SegmentLoaderSuite) TestLoad() {
	ctx := context.Background()

	msgLength := 4

	// Load sealed
	binlogs, statsLogs, err := SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.NoError(err)

	_, err = suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, &querypb.SegmentLoadInfo{
		SegmentID:    suite.segmentID,
		PartitionID:  suite.partitionID,
		CollectionID: suite.collectionID,
		BinlogPaths:  binlogs,
		Statslogs:    statsLogs,
		NumOfRows:    int64(msgLength),
	})
	suite.NoError(err)

	// Load growing
	binlogs, statsLogs, err = SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID+1,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.NoError(err)

	_, err = suite.loader.Load(ctx, suite.collectionID, SegmentTypeGrowing, 0, &querypb.SegmentLoadInfo{
		SegmentID:    suite.segmentID + 1,
		PartitionID:  suite.partitionID,
		CollectionID: suite.collectionID,
		BinlogPaths:  binlogs,
		Statslogs:    statsLogs,
		NumOfRows:    int64(msgLength),
	})
	suite.NoError(err)
}

func (suite *SegmentLoaderSuite) TestLoadMultipleSegments() {
	ctx := context.Background()
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, suite.segmentNum)

	msgLength := 100
	// Load sealed
	for i := 0; i < suite.segmentNum; i++ {
		segmentID := suite.segmentID + int64(i)
		binlogs, statsLogs, err := SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			msgLength,
			suite.schema,
			suite.chunkManager,
		)
		suite.NoError(err)
		loadInfos = append(loadInfos, &querypb.SegmentLoadInfo{
			SegmentID:    segmentID,
			PartitionID:  suite.partitionID,
			CollectionID: suite.collectionID,
			BinlogPaths:  binlogs,
			Statslogs:    statsLogs,
			NumOfRows:    int64(msgLength),
		})
	}

	segments, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, loadInfos...)
	suite.NoError(err)

	// Won't load bloom filter with sealed segments
	for _, segment := range segments {
		for pk := 0; pk < 100; pk++ {
			exist := segment.MayPkExist(storage.NewInt64PrimaryKey(int64(pk)))
			suite.Require().False(exist)
		}
	}

	// Load growing
	loadInfos = loadInfos[:0]
	for i := 0; i < suite.segmentNum; i++ {
		segmentID := suite.segmentID + int64(suite.segmentNum) + int64(i)
		binlogs, statsLogs, err := SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			msgLength,
			suite.schema,
			suite.chunkManager,
		)
		suite.NoError(err)
		loadInfos = append(loadInfos, &querypb.SegmentLoadInfo{
			SegmentID:    segmentID,
			PartitionID:  suite.partitionID,
			CollectionID: suite.collectionID,
			BinlogPaths:  binlogs,
			Statslogs:    statsLogs,
			NumOfRows:    int64(msgLength),
		})
	}

	segments, err = suite.loader.Load(ctx, suite.collectionID, SegmentTypeGrowing, 0, loadInfos...)
	suite.NoError(err)
	// Should load bloom filter with growing segments
	for _, segment := range segments {
		for pk := 0; pk < 100; pk++ {
			exist := segment.MayPkExist(storage.NewInt64PrimaryKey(int64(pk)))
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
		binlogs, statsLogs, err := SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			msgLength,
			suite.schema,
			suite.chunkManager,
		)
		suite.NoError(err)

		vecFields := funcutil.GetVecFieldIDs(suite.schema)
		indexInfo, err := GenAndSaveIndex(
			suite.collectionID,
			suite.partitionID,
			segmentID,
			vecFields[0],
			msgLength,
			IndexFaissIVFFlat,
			L2,
			suite.chunkManager,
		)
		suite.NoError(err)
		loadInfos = append(loadInfos, &querypb.SegmentLoadInfo{
			SegmentID:    segmentID,
			PartitionID:  suite.partitionID,
			CollectionID: suite.collectionID,
			BinlogPaths:  binlogs,
			Statslogs:    statsLogs,
			IndexInfos:   []*querypb.FieldIndexInfo{indexInfo},
			NumOfRows:    int64(msgLength),
		})
	}

	segments, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, loadInfos...)
	suite.NoError(err)

	vecFields := funcutil.GetVecFieldIDs(suite.schema)
	for _, segment := range segments {
		suite.True(segment.ExistIndex(vecFields[0]))
	}
}

func (suite *SegmentLoaderSuite) TestLoadBloomFilter() {
	ctx := context.Background()
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, suite.segmentNum)

	msgLength := 100
	// Load sealed
	for i := 0; i < suite.segmentNum; i++ {
		segmentID := suite.segmentID + int64(i)
		binlogs, statsLogs, err := SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			msgLength,
			suite.schema,
			suite.chunkManager,
		)
		suite.NoError(err)

		loadInfos = append(loadInfos, &querypb.SegmentLoadInfo{
			SegmentID:    segmentID,
			PartitionID:  suite.partitionID,
			CollectionID: suite.collectionID,
			BinlogPaths:  binlogs,
			Statslogs:    statsLogs,
			NumOfRows:    int64(msgLength),
		})
	}

	bfs, err := suite.loader.LoadBloomFilterSet(ctx, suite.collectionID, 0, loadInfos...)
	suite.NoError(err)

	for _, bf := range bfs {
		for pk := 0; pk < 100; pk++ {
			exist := bf.MayPkExist(storage.NewInt64PrimaryKey(int64(pk)))
			suite.Require().True(exist)
		}
	}
}

func (suite *SegmentLoaderSuite) TestLoadDeltaLogs() {
	ctx := context.Background()
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, suite.segmentNum)

	msgLength := 100
	// Load sealed
	for i := 0; i < suite.segmentNum; i++ {
		segmentID := suite.segmentID + int64(i)
		binlogs, statsLogs, err := SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			msgLength,
			suite.schema,
			suite.chunkManager,
		)
		suite.NoError(err)

		// Delete PKs 1, 2
		deltaLogs, err := SaveDeltaLog(suite.collectionID,
			suite.partitionID,
			segmentID,
			suite.chunkManager,
		)
		suite.NoError(err)

		loadInfos = append(loadInfos, &querypb.SegmentLoadInfo{
			SegmentID:    segmentID,
			PartitionID:  suite.partitionID,
			CollectionID: suite.collectionID,
			BinlogPaths:  binlogs,
			Statslogs:    statsLogs,
			Deltalogs:    deltaLogs,
			NumOfRows:    int64(msgLength),
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
			exist := segment.MayPkExist(storage.NewInt64PrimaryKey(int64(pk)))
			suite.Require().True(exist)
		}
	}
}

func (suite *SegmentLoaderSuite) TestLoadWithMmap() {
	key := paramtable.Get().QueryNodeCfg.MmapDirPath.Key
	paramtable.Get().Save(key, "/tmp/mmap-test")
	defer paramtable.Get().Reset(key)
	ctx := context.Background()

	msgLength := 100
	// Load sealed
	binlogs, statsLogs, err := SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.NoError(err)

	_, err = suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, &querypb.SegmentLoadInfo{
		SegmentID:    suite.segmentID,
		PartitionID:  suite.partitionID,
		CollectionID: suite.collectionID,
		BinlogPaths:  binlogs,
		Statslogs:    statsLogs,
		NumOfRows:    int64(msgLength),
	})
	suite.NoError(err)
}

func (suite *SegmentLoaderSuite) TestPatchEntryNum() {
	ctx := context.Background()

	msgLength := 100
	segmentID := suite.segmentID
	binlogs, statsLogs, err := SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		segmentID,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.NoError(err)

	vecFields := funcutil.GetVecFieldIDs(suite.schema)
	indexInfo, err := GenAndSaveIndex(
		suite.collectionID,
		suite.partitionID,
		segmentID,
		vecFields[0],
		msgLength,
		IndexFaissIVFFlat,
		L2,
		suite.chunkManager,
	)
	suite.NoError(err)
	loadInfo := &querypb.SegmentLoadInfo{
		SegmentID:    segmentID,
		PartitionID:  suite.partitionID,
		CollectionID: suite.collectionID,
		BinlogPaths:  binlogs,
		Statslogs:    statsLogs,
		IndexInfos:   []*querypb.FieldIndexInfo{indexInfo},
		NumOfRows:    int64(msgLength),
	}

	// mock legacy binlog entry num is zero case
	for _, fieldLog := range binlogs {
		for _, binlog := range fieldLog.GetBinlogs() {
			binlog.EntriesNum = 0
		}
	}

	segments, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, loadInfo)
	suite.Require().NoError(err)
	suite.Require().Equal(1, len(segments))

	segment := segments[0]
	info := segment.GetIndex(vecFields[0])
	suite.Require().NotNil(info)

	for _, binlog := range info.FieldBinlog.GetBinlogs() {
		suite.Greater(binlog.EntriesNum, int64(0))
	}
}

func TestSegmentLoader(t *testing.T) {
	suite.Run(t, &SegmentLoaderSuite{})
}
