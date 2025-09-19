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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type RetrieveSuite struct {
	suite.Suite

	// schema
	ctx    context.Context
	schema *schemapb.CollectionSchema

	// Dependencies
	rootPath     string
	chunkManager storage.ChunkManager

	// Data
	manager      *Manager
	collectionID int64
	partitionID  int64
	segmentID    int64
	collection   *Collection
	sealed       Segment
	growing      Segment
}

func (suite *RetrieveSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *RetrieveSuite) SetupTest() {
	var err error
	suite.ctx = context.Background()
	msgLength := 100

	suite.rootPath = suite.T().Name()
	chunkManagerFactory := storage.NewTestChunkManagerFactory(paramtable.Get(), suite.rootPath)
	suite.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(suite.ctx)
	initcore.InitRemoteChunkManager(paramtable.Get())
	initcore.InitLocalChunkManager(suite.rootPath)
	initcore.InitMmapManager(paramtable.Get(), 1)
	initcore.InitTieredStorage(paramtable.Get())

	suite.collectionID = 100
	suite.partitionID = 10
	suite.segmentID = 100

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
	loader := NewLoader(suite.ctx, suite.manager, suite.chunkManager)

	binlogs, statslogs, err := mock_segcore.SaveBinLog(suite.ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.Require().NoError(err)

	sealLoadInfo := querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID,
		CollectionID:  suite.collectionID,
		PartitionID:   suite.partitionID,
		NumOfRows:     int64(msgLength),
		BinlogPaths:   binlogs,
		Statslogs:     statslogs,
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		Level:         datapb.SegmentLevel_Legacy,
	}

	suite.sealed, err = NewSegment(suite.ctx,
		suite.collection,
		suite.manager.Segment,
		SegmentTypeSealed,
		0,
		&sealLoadInfo,
	)
	suite.Require().NoError(err)

	bfs, err := loader.loadSingleBloomFilterSet(suite.ctx, suite.collectionID, &sealLoadInfo, SegmentTypeSealed)
	suite.Require().NoError(err)
	suite.sealed.SetBloomFilter(bfs)

	for _, binlog := range binlogs {
		err = suite.sealed.(*LocalSegment).LoadFieldData(suite.ctx, binlog.FieldID, int64(msgLength), binlog)
		suite.Require().NoError(err)
	}

	binlogs, statlogs, err := mock_segcore.SaveBinLog(suite.ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID+1,
		msgLength,
		suite.schema,
		suite.chunkManager,
	)
	suite.Require().NoError(err)

	growingLoadInfo := querypb.SegmentLoadInfo{
		SegmentID:     suite.segmentID + 1,
		CollectionID:  suite.collectionID,
		PartitionID:   suite.partitionID,
		BinlogPaths:   binlogs,
		Statslogs:     statlogs,
		InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
		Level:         datapb.SegmentLevel_Legacy,
	}

	// allow growing segment use the bloom filter
	paramtable.Get().QueryNodeCfg.SkipGrowingSegmentBF.SwapTempValue("false")

	suite.growing, err = NewSegment(suite.ctx,
		suite.collection,
		suite.manager.Segment,
		SegmentTypeGrowing,
		0,
		&growingLoadInfo,
	)
	suite.Require().NoError(err)

	bfs, err = loader.loadSingleBloomFilterSet(suite.ctx, suite.collectionID, &growingLoadInfo, SegmentTypeGrowing)
	suite.Require().NoError(err)
	suite.growing.SetBloomFilter(bfs)

	insertMsg, err := mock_segcore.GenInsertMsg(suite.collection.GetCCollection(), suite.partitionID, suite.growing.ID(), msgLength)
	suite.Require().NoError(err)
	insertRecord, err := storage.TransferInsertMsgToInsertRecord(suite.collection.Schema(), insertMsg)
	suite.Require().NoError(err)
	err = suite.growing.Insert(suite.ctx, insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
	suite.Require().NoError(err)

	suite.manager.Segment.Put(context.Background(), SegmentTypeSealed, suite.sealed)
	suite.manager.Segment.Put(context.Background(), SegmentTypeGrowing, suite.growing)
}

func (suite *RetrieveSuite) TearDownTest() {
	suite.sealed.Release(context.Background())
	suite.growing.Release(context.Background())
	DeleteCollection(suite.collection)
	ctx := context.Background()
	suite.chunkManager.RemoveWithPrefix(ctx, suite.rootPath)
}

func (suite *RetrieveSuite) TestRetrieveSealed() {
	plan, err := mock_segcore.GenSimpleRetrievePlan(suite.collection.GetCCollection())
	suite.NoError(err)

	req := &querypb.QueryRequest{
		Req: &internalpb.RetrieveRequest{
			CollectionID: suite.collectionID,
			PartitionIDs: []int64{suite.partitionID},
		},
		SegmentIDs: []int64{suite.sealed.ID()},
		Scope:      querypb.DataScope_Historical,
	}

	res, segments, err := Retrieve(context.TODO(), suite.manager, plan, req, nil)
	suite.NoError(err)
	suite.Len(res[0].Result.Offset, 3)
	suite.manager.Segment.Unpin(segments)

	resultByOffsets, err := suite.sealed.RetrieveByOffsets(context.Background(), &segcore.RetrievePlanWithOffsets{
		RetrievePlan: plan,
		Offsets:      []int64{0, 1},
	})
	suite.NoError(err)
	suite.Len(resultByOffsets.Offset, 0)
}

func (suite *RetrieveSuite) TestRetrieveWithFilter() {
	plan, err := mock_segcore.GenSimpleRetrievePlan(suite.collection.GetCCollection())
	suite.NoError(err)

	suite.Run("SealSegmentFilter", func() {
		// no exist pk
		exprStr := "int64Field == 10000000"
		schemaHelper, _ := typeutil.CreateSchemaHelper(suite.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
		suite.NoError(err)

		req := &querypb.QueryRequest{
			Req: &internalpb.RetrieveRequest{
				CollectionID: suite.collectionID,
				PartitionIDs: []int64{suite.partitionID},
			},
			SegmentIDs: []int64{suite.sealed.ID()},
			Scope:      querypb.DataScope_Historical,
		}

		res, segments, err := Retrieve(context.TODO(), suite.manager, plan, req, planNode)
		suite.NoError(err)
		suite.Len(res, 0)
		suite.manager.Segment.Unpin(segments)
	})

	suite.Run("GrowingSegmentFilter", func() {
		exprStr := "int64Field == 10000000"
		schemaHelper, _ := typeutil.CreateSchemaHelper(suite.schema)
		planNode, err := planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
		suite.NoError(err)

		req := &querypb.QueryRequest{
			Req: &internalpb.RetrieveRequest{
				CollectionID: suite.collectionID,
				PartitionIDs: []int64{suite.partitionID},
			},
			SegmentIDs: []int64{suite.growing.ID()},
			Scope:      querypb.DataScope_Streaming,
		}

		res, segments, err := Retrieve(context.TODO(), suite.manager, plan, req, planNode)
		suite.NoError(err)
		suite.Len(res, 0)
		suite.manager.Segment.Unpin(segments)
	})

	suite.Run("SegmentFilterRules", func() {
		// create more 10 seal segments to test BF
		// The pk in seg range is {segN [0...N-1]}
		// ex.
		//  seg1 [0]
		//  seg2 [0, 1]
		//  ...
		//  seg10 [0, 1, 2, ..., 9]
		loader := NewLoader(suite.ctx, suite.manager, suite.chunkManager)
		for i := range 10 {
			segid := int64(i + 1)
			msgLen := i + 1
			bl, sl, err := mock_segcore.SaveBinLog(suite.ctx,
				suite.collectionID,
				suite.partitionID,
				segid,
				msgLen,
				suite.schema,
				suite.chunkManager)

			suite.Require().NoError(err)

			sealLoadInfo := querypb.SegmentLoadInfo{
				SegmentID:     segid,
				CollectionID:  suite.collectionID,
				PartitionID:   suite.partitionID,
				NumOfRows:     int64(msgLen),
				BinlogPaths:   bl,
				Statslogs:     sl,
				InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
				Level:         datapb.SegmentLevel_Legacy,
			}

			sealseg, err := NewSegment(suite.ctx,
				suite.collection,
				suite.manager.Segment,
				SegmentTypeSealed,
				0,
				&sealLoadInfo,
			)
			suite.Require().NoError(err)

			bfs, err := loader.loadSingleBloomFilterSet(suite.ctx, suite.collectionID,
				&sealLoadInfo, SegmentTypeSealed)
			suite.Require().NoError(err)
			sealseg.SetBloomFilter(bfs)

			suite.manager.Segment.Put(suite.ctx, SegmentTypeSealed, sealseg)
		}

		exprs := map[string]int{
			// empty plan
			"": 10,
			// filter half of seal segments
			"int64Field == 5": 5,
			"int64Field == 6": 4,
			// AND operator, int8Field have not stats but we still can use the int64Field(pk)
			"int64Field == 6 and int8Field == -10000": 4,
			// nesting expression
			"int64Field == 6 and (int64Field == 7 or int8Field == -10000)": 4,
			// OR operator
			// can't filter, OR operator need both side be filter
			"int64Field == 6 or int8Field == -10000": 10,
			// can filter
			"int64Field == 6 or (int64Field == 7 and int8Field == -10000)": 4,
			// IN operator
			"int64Field IN [7, 8, 9]": 3,
			// NOT IN operator should not be filter
			"int64Field NOT IN [7, 8, 9]":   10,
			"NOT (int64Field IN [7, 8, 9])": 10,
			// empty range
			"int64Field IN []": 10,
		}

		req := &querypb.QueryRequest{
			Req: &internalpb.RetrieveRequest{
				CollectionID: suite.collectionID,
				PartitionIDs: []int64{suite.partitionID},
			},
			SegmentIDs: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			Scope:      querypb.DataScope_Historical,
		}

		for exprStr, expect := range exprs {
			schemaHelper, _ := typeutil.CreateSchemaHelper(suite.schema)
			var planNode *planpb.PlanNode
			if exprStr == "" {
				planNode = nil
				err = nil
			} else {
				planNode, err = planparserv2.CreateRetrievePlan(schemaHelper, exprStr, nil)
			}
			suite.NoError(err)

			res, segments, err := Retrieve(context.TODO(), suite.manager, plan, req, planNode)
			suite.NoError(err)
			suite.Len(res, expect)
			suite.manager.Segment.Unpin(segments)
		}

		// remove the segs
		for i := range 10 {
			suite.manager.Segment.Remove(suite.ctx, int64(i+1) /*segmentID*/, querypb.DataScope_Historical)
		}
	})
}

func (suite *RetrieveSuite) TestRetrieveGrowing() {
	plan, err := mock_segcore.GenSimpleRetrievePlan(suite.collection.GetCCollection())
	suite.NoError(err)

	req := &querypb.QueryRequest{
		Req: &internalpb.RetrieveRequest{
			CollectionID: suite.collectionID,
			PartitionIDs: []int64{suite.partitionID},
		},
		SegmentIDs: []int64{suite.growing.ID()},
		Scope:      querypb.DataScope_Streaming,
	}

	res, segments, err := Retrieve(context.TODO(), suite.manager, plan, req, nil)
	suite.NoError(err)
	suite.Len(res[0].Result.Offset, 3)
	suite.manager.Segment.Unpin(segments)

	resultByOffsets, err := suite.growing.RetrieveByOffsets(context.Background(), &segcore.RetrievePlanWithOffsets{
		RetrievePlan: plan,
		Offsets:      []int64{0, 1},
	})
	suite.NoError(err)
	suite.Len(resultByOffsets.Offset, 0)
}

func (suite *RetrieveSuite) TestRetrieveStreamSealed() {
	plan, err := mock_segcore.GenSimpleRetrievePlan(suite.collection.GetCCollection())
	suite.NoError(err)

	req := &querypb.QueryRequest{
		Req: &internalpb.RetrieveRequest{
			CollectionID: suite.collectionID,
			PartitionIDs: []int64{suite.partitionID},
		},
		SegmentIDs: []int64{suite.sealed.ID()},
		Scope:      querypb.DataScope_Historical,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := streamrpc.NewLocalQueryClient(ctx)
	server := client.CreateServer()

	go func() {
		segments, err := RetrieveStream(ctx, suite.manager, plan, req, server)
		suite.NoError(err)
		suite.manager.Segment.Unpin(segments)
		server.FinishSend(err)
	}()

	sum := 0
	for {
		result, err := client.Recv()
		if err != nil {
			if err == io.EOF {
				suite.Equal(3, sum)
				break
			}
			suite.Fail("Retrieve stream fetch error")
		}

		err = merr.Error(result.GetStatus())
		suite.NoError(err)

		sum += len(result.Ids.GetIntId().GetData())
	}
}

func (suite *RetrieveSuite) TestRetrieveNonExistSegment() {
	plan, err := mock_segcore.GenSimpleRetrievePlan(suite.collection.GetCCollection())
	suite.NoError(err)

	req := &querypb.QueryRequest{
		Req: &internalpb.RetrieveRequest{
			CollectionID: suite.collectionID,
			PartitionIDs: []int64{suite.partitionID},
		},
		SegmentIDs: []int64{999},
		Scope:      querypb.DataScope_Streaming,
	}

	res, segments, err := Retrieve(context.TODO(), suite.manager, plan, req, nil)
	suite.Error(err)
	suite.Len(res, 0)
	suite.manager.Segment.Unpin(segments)
}

func (suite *RetrieveSuite) TestRetrieveNilSegment() {
	plan, err := mock_segcore.GenSimpleRetrievePlan(suite.collection.GetCCollection())
	suite.NoError(err)

	suite.sealed.Release(context.Background())
	req := &querypb.QueryRequest{
		Req: &internalpb.RetrieveRequest{
			CollectionID: suite.collectionID,
			PartitionIDs: []int64{suite.partitionID},
		},
		SegmentIDs: []int64{suite.sealed.ID()},
		Scope:      querypb.DataScope_Historical,
	}

	res, segments, err := Retrieve(context.TODO(), suite.manager, plan, req, nil)
	suite.ErrorIs(err, merr.ErrSegmentNotLoaded)
	suite.Len(res, 0)
	suite.manager.Segment.Unpin(segments)
}

func TestRetrieve(t *testing.T) {
	suite.Run(t, new(RetrieveSuite))
}
