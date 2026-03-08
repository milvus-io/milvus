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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type SearchSuite struct {
	suite.Suite
	chunkManager storage.ChunkManager

	manager      *Manager
	collectionID int64
	partitionID  int64
	segmentID    int64
	schema       *schemapb.CollectionSchema
	collection   *Collection
	sealed       Segment
	growing      Segment
}

func (suite *SearchSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *SearchSuite) SetupTest() {
	var err error
	ctx := context.Background()
	msgLength := 100

	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(paramtable.Get())
	suite.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	initcore.InitRemoteChunkManager(paramtable.Get())
	initcore.InitLocalChunkManager(suite.T().Name())
	initcore.InitMmapManager(paramtable.Get(), 1)
	initcore.InitTieredStorage(paramtable.Get())

	suite.collectionID = 100
	suite.partitionID = 10
	suite.segmentID = 1

	suite.manager = NewManager()
	suite.schema = mock_segcore.GenTestCollectionSchema("test-reduce", schemapb.DataType_Int64, true)
	indexMeta := mock_segcore.GenTestIndexMeta(suite.collectionID, suite.schema)
	suite.manager.Collection.PutOrRef(suite.collectionID,
		suite.schema,
		indexMeta,
		&querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: suite.collectionID,
			PartitionIDs: []int64{suite.partitionID},
		},
	)
	suite.collection = suite.manager.Collection.Get(suite.collectionID)

	suite.sealed, err = NewSegment(ctx,
		suite.collection,
		suite.manager.Segment,
		SegmentTypeSealed,
		0,
		&querypb.SegmentLoadInfo{
			SegmentID:     suite.segmentID,
			CollectionID:  suite.collectionID,
			PartitionID:   suite.partitionID,
			NumOfRows:     int64(msgLength),
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
			Level:         datapb.SegmentLevel_Legacy,
		},
	)
	suite.Require().NoError(err)

	binlogs, _, err := mock_segcore.SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.Require().NoError(err)
	for _, binlog := range binlogs {
		err = suite.sealed.(*LocalSegment).LoadFieldData(ctx, binlog.FieldID, int64(msgLength), binlog)
		suite.Require().NoError(err)
	}

	suite.growing, err = NewSegment(ctx,
		suite.collection,
		suite.manager.Segment,
		SegmentTypeGrowing,
		0,
		&querypb.SegmentLoadInfo{
			SegmentID:     suite.segmentID + 1,
			CollectionID:  suite.collectionID,
			PartitionID:   suite.partitionID,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
			Level:         datapb.SegmentLevel_Legacy,
		},
	)
	suite.Require().NoError(err)

	insertMsg, err := mock_segcore.GenInsertMsg(suite.collection.GetCCollection(), suite.partitionID, suite.growing.ID(), msgLength)
	suite.Require().NoError(err)
	insertRecord, err := storage.TransferInsertMsgToInsertRecord(suite.collection.Schema(), insertMsg)
	suite.Require().NoError(err)
	suite.growing.Insert(ctx, insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)

	suite.manager.Segment.Put(context.Background(), SegmentTypeSealed, suite.sealed)
	suite.manager.Segment.Put(context.Background(), SegmentTypeGrowing, suite.growing)
}

func (suite *SearchSuite) TearDownTest() {
	suite.sealed.Release(context.Background())
	DeleteCollection(suite.collection)
	ctx := context.Background()
	suite.chunkManager.RemoveWithPrefix(ctx, paramtable.Get().MinioCfg.RootPath.GetValue())
}

func (suite *SearchSuite) TestSearchSealed() {
	nq := int64(10)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	searchReq, err := mock_segcore.GenSearchPlanAndRequests(suite.collection.GetCCollection(), []int64{suite.sealed.ID()}, mock_segcore.IndexFaissIDMap, nq)
	suite.NoError(err)

	_, segments, err := SearchHistorical(ctx, suite.manager, searchReq, suite.collectionID, nil, []int64{suite.sealed.ID()}, nil)
	suite.NoError(err)
	suite.manager.Segment.Unpin(segments)
}

func (suite *SearchSuite) TestSearchGrowing() {
	searchReq, err := mock_segcore.GenSearchPlanAndRequests(suite.collection.GetCCollection(), []int64{suite.growing.ID()}, mock_segcore.IndexFaissIDMap, 1)
	suite.NoError(err)

	res, segments, err := SearchStreaming(context.TODO(), suite.manager, searchReq,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.growing.ID()},
		nil,
	)
	suite.NoError(err)
	suite.Len(res, 1)
	suite.manager.Segment.Unpin(segments)
}

func (suite *SearchSuite) TestSearchWithFilter() {
	ctx := context.Background()

	// create more sealed segments with different pk ranges for testing
	// seg1: pk [0], seg2: pk [0,1], ..., seg10: pk [0..9]
	loader := NewLoader(ctx, suite.manager, suite.chunkManager)
	for i := range 10 {
		segID := int64(i + 1000)
		msgLen := i + 1
		binlogs, statslogs, err := mock_segcore.SaveBinLog(ctx,
			suite.collectionID,
			suite.partitionID,
			segID,
			msgLen,
			suite.schema,
			suite.chunkManager,
		)
		suite.Require().NoError(err)

		loadInfo := &querypb.SegmentLoadInfo{
			SegmentID:     segID,
			CollectionID:  suite.collectionID,
			PartitionID:   suite.partitionID,
			NumOfRows:     int64(msgLen),
			BinlogPaths:   binlogs,
			Statslogs:     statslogs,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
			Level:         datapb.SegmentLevel_Legacy,
		}

		seg, err := NewSegment(ctx,
			suite.collection,
			suite.manager.Segment,
			SegmentTypeSealed,
			0,
			loadInfo,
		)
		suite.Require().NoError(err)

		bfs, err := loader.loadSingleBloomFilterSet(ctx, suite.collectionID, loadInfo, SegmentTypeSealed)
		suite.Require().NoError(err)
		seg.SetBloomFilter(bfs)

		for _, binlog := range binlogs {
			err = seg.(*LocalSegment).LoadFieldData(ctx, binlog.FieldID, int64(msgLen), binlog)
			suite.Require().NoError(err)
		}

		suite.manager.Segment.Put(ctx, SegmentTypeSealed, seg)
	}

	segIDs := []int64{1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009}

	suite.Run("SearchWithSparseFilter", func() {
		searchReq, err := mock_segcore.GenSearchPlanAndRequests(suite.collection.GetCCollection(), segIDs, mock_segcore.IndexFaissIDMap, 1)
		suite.NoError(err)

		// create search plan with filter expression "int64Field == 5"
		// this should filter out segments that don't contain pk=5
		schemaHelper, _ := typeutil.CreateSchemaHelper(suite.schema)
		planNode, err := planparserv2.CreateSearchPlan(schemaHelper, "int64Field == 5", "floatVectorField", &planpb.QueryInfo{
			Topk:         10,
			MetricType:   "L2",
			SearchParams: `{"nprobe": 10}`,
			RoundDecimal: -1,
		}, nil, nil)
		suite.NoError(err)

		// search with filter - should only search segments containing pk=5 (seg6-seg10)
		res, segments, err := SearchHistorical(ctx, suite.manager, searchReq,
			suite.collectionID,
			[]int64{suite.partitionID},
			segIDs,
			planNode,
		)
		suite.NoError(err)
		// with sparse filter enabled, only 5 segments (seg6-seg10) should be searched
		suite.Len(segments, 5)
		suite.manager.Segment.Unpin(segments)
		DeleteSearchResults(res)
	})

	suite.Run("SearchWithoutFilter", func() {
		searchReq, err := mock_segcore.GenSearchPlanAndRequests(suite.collection.GetCCollection(), segIDs, mock_segcore.IndexFaissIDMap, 1)
		suite.NoError(err)

		// search without filter - should search all segments
		res, segments, err := SearchHistorical(ctx, suite.manager, searchReq,
			suite.collectionID,
			[]int64{suite.partitionID},
			segIDs,
			nil,
		)
		suite.NoError(err)
		suite.Len(segments, 10)
		suite.manager.Segment.Unpin(segments)
		DeleteSearchResults(res)
	})

	// cleanup
	for _, segID := range segIDs {
		suite.manager.Segment.Remove(ctx, segID, querypb.DataScope_Historical)
	}
}

func TestSearch(t *testing.T) {
	suite.Run(t, new(SearchSuite))
}
