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
	"math/rand"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/concurrency"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/suite"
)

type SegmentLoaderSuite struct {
	suite.Suite
	loader Loader

	// Dependencies
	collectionManager CollectionManager
	chunkManager      storage.ChunkManager
	pool              *concurrency.Pool

	// Data
	collectionID int64
	partitionID  int64
	segmentID    int64
	schema       *schemapb.CollectionSchema
}

func (suite *SegmentLoaderSuite) SetupSuite() {
	paramtable.Init()
	suite.collectionID = rand.Int63()
	suite.partitionID = rand.Int63()
	suite.segmentID = rand.Int63()
	suite.schema = GenTestCollectionSchema("test", schemapb.DataType_Int64)
}

func (suite *SegmentLoaderSuite) SetupTest() {
	var err error

	// Dependencies
	suite.collectionManager = NewCollectionManager()
	suite.chunkManager = storage.NewLocalChunkManager(storage.RootPath(
		fmt.Sprintf("/tmp/milvus-ut/%d", rand.Int63())))
	suite.pool, err = concurrency.NewPool(10)
	suite.NoError(err)

	suite.loader = NewLoader(suite.collectionManager, suite.chunkManager, suite.pool)

	// Data
	schema := GenTestCollectionSchema("test", schemapb.DataType_Int64)
	loadMeta := &querypb.LoadMetaInfo{
		LoadType:     querypb.LoadType_LoadCollection,
		CollectionID: suite.collectionID,
		PartitionIDs: []int64{suite.partitionID},
	}
	suite.collectionManager.Put(suite.collectionID, schema, loadMeta)
}

func (suite *SegmentLoaderSuite) TestLoad() {
	ctx := context.Background()

	// Load sealed
	binlogs, statsLogs, err := SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		100,
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
	})
	suite.NoError(err)

	// Load growing
	binlogs, statsLogs, err = SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID+1,
		100,
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
	})
	suite.NoError(err)
}

func (suite *SegmentLoaderSuite) TestLoadMultipleSegments() {
	ctx := context.Background()
	const SegmentNum = 5
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, SegmentNum)

	// Load sealed
	for i := 0; i < SegmentNum; i++ {
		segmentID := suite.segmentID + int64(i)
		binlogs, statsLogs, err := SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			100,
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
	for i := 0; i < SegmentNum; i++ {
		segmentID := suite.segmentID + SegmentNum + int64(i)
		binlogs, statsLogs, err := SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			100,
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
	const SegmentNum = 5
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, SegmentNum)

	// Load sealed
	for i := 0; i < SegmentNum; i++ {
		segmentID := suite.segmentID + int64(i)
		binlogs, statsLogs, err := SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			100,
			suite.schema,
			suite.chunkManager,
		)
		suite.NoError(err)

		indexInfo, err := GenAndSaveIndex(
			suite.collectionID,
			suite.partitionID,
			segmentID,
			100,
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
		})
	}

	segments, err := suite.loader.Load(ctx, suite.collectionID, SegmentTypeSealed, 0, loadInfos...)
	suite.NoError(err)

	for _, segment := range segments {
		suite.True(segment.ExistIndex(simpleFloatVecField.id))
	}
}

func (suite *SegmentLoaderSuite) TestLoadBloomFilter() {
	ctx := context.Background()
	const SegmentNum = 5
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, SegmentNum)

	// Load sealed
	for i := 0; i < SegmentNum; i++ {
		segmentID := suite.segmentID + int64(i)
		binlogs, statsLogs, err := SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			100,
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
	const SegmentNum = 5
	loadInfos := make([]*querypb.SegmentLoadInfo, 0, SegmentNum)

	// Load sealed
	for i := 0; i < SegmentNum; i++ {
		segmentID := suite.segmentID + int64(i)
		binlogs, statsLogs, err := SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segmentID,
			100,
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

func TestSegmentLoader(t *testing.T) {
	suite.Run(t, &SegmentLoaderSuite{})
}
