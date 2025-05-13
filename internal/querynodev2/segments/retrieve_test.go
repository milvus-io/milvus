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
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type RetrieveSuite struct {
	suite.Suite

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
	ctx := context.Background()
	msgLength := 100

	suite.rootPath = suite.T().Name()
	chunkManagerFactory := storage.NewTestChunkManagerFactory(paramtable.Get(), suite.rootPath)
	suite.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	initcore.InitRemoteChunkManager(paramtable.Get())

	suite.collectionID = 100
	suite.partitionID = 10
	suite.segmentID = 1

	suite.manager = NewManager()
	schema := mock_segcore.GenTestCollectionSchema("test-reduce", schemapb.DataType_Int64, true)
	indexMeta := mock_segcore.GenTestIndexMeta(suite.collectionID, schema)
	suite.manager.Collection.PutOrRef(suite.collectionID,
		schema,
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
		schema,
		suite.chunkManager,
	)
	suite.Require().NoError(err)
	for _, binlog := range binlogs {
		err = suite.sealed.(*LocalSegment).LoadFieldData(ctx, binlog.FieldID, int64(msgLength), binlog)
		suite.Require().NoError(err)
	}

	suite.growing, err = NewSegment(ctx,
		suite.collection,
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
	err = suite.growing.Insert(ctx, insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
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

	res, segments, err := Retrieve(context.TODO(), suite.manager, plan, req)
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

	res, segments, err := Retrieve(context.TODO(), suite.manager, plan, req)
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

	res, segments, err := Retrieve(context.TODO(), suite.manager, plan, req)
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

	res, segments, err := Retrieve(context.TODO(), suite.manager, plan, req)
	suite.ErrorIs(err, merr.ErrSegmentNotLoaded)
	suite.Len(res, 0)
	suite.manager.Segment.Unpin(segments)
}

func TestRetrieve(t *testing.T) {
	suite.Run(t, new(RetrieveSuite))
}
