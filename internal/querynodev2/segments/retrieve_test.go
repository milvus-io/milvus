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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type RetrieveSuite struct {
	suite.Suite

	// Dependencies
	chunkManager storage.ChunkManager

	// Data
	manager      *Manager
	collectionID int64
	partitionID  int64
	segmentID    int64
	collection   *Collection
	sealed       *LocalSegment
	growing      *LocalSegment
}

func (suite *RetrieveSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *RetrieveSuite) SetupTest() {
	var err error
	ctx := context.Background()
	msgLength := 100

	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(paramtable.Get())
	suite.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	initcore.InitRemoteChunkManager(paramtable.Get())

	suite.collectionID = 100
	suite.partitionID = 10
	suite.segmentID = 1

	suite.manager = NewManager()
	schema := GenTestCollectionSchema("test-reduce", schemapb.DataType_Int64)
	indexMeta := GenTestIndexMeta(suite.collectionID, schema)
	suite.manager.Collection.Put(suite.collectionID,
		schema,
		indexMeta,
		&querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: suite.collectionID,
			PartitionIDs: []int64{suite.partitionID},
		},
	)
	suite.collection = suite.manager.Collection.Get(suite.collectionID)

	suite.sealed, err = NewSegment(suite.collection,
		suite.segmentID,
		suite.partitionID,
		suite.collectionID,
		"dml",
		SegmentTypeSealed,
		0,
		nil,
		nil,
	)
	suite.Require().NoError(err)

	binlogs, _, err := SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		schema,
		suite.chunkManager,
	)
	suite.Require().NoError(err)
	for _, binlog := range binlogs {
		err = suite.sealed.LoadFieldData(binlog.FieldID, int64(msgLength), binlog)
		suite.Require().NoError(err)
	}

	suite.growing, err = NewSegment(suite.collection,
		suite.segmentID+1,
		suite.partitionID,
		suite.collectionID,
		"dml",
		SegmentTypeGrowing,
		0,
		nil,
		nil,
	)
	suite.Require().NoError(err)

	insertMsg, err := genInsertMsg(suite.collection, suite.partitionID, suite.growing.segmentID, msgLength)
	suite.Require().NoError(err)
	insertRecord, err := storage.TransferInsertMsgToInsertRecord(suite.collection.Schema(), insertMsg)
	suite.Require().NoError(err)
	err = suite.growing.Insert(insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
	suite.Require().NoError(err)

	suite.manager.Segment.Put(SegmentTypeSealed, suite.sealed)
	suite.manager.Segment.Put(SegmentTypeGrowing, suite.growing)
}

func (suite *RetrieveSuite) TearDownTest() {
	DeleteSegment(suite.sealed)
	DeleteSegment(suite.growing)
	DeleteCollection(suite.collection)
	ctx := context.Background()
	suite.chunkManager.RemoveWithPrefix(ctx, paramtable.Get().MinioCfg.RootPath.GetValue())
}

func (suite *RetrieveSuite) TestRetrieveSealed() {
	plan, err := genSimpleRetrievePlan(suite.collection)
	suite.NoError(err)

	res, _, _, err := RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.sealed.ID()})
	suite.NoError(err)
	suite.Len(res[0].Offset, 3)
}

func (suite *RetrieveSuite) TestRetrieveGrowing() {
	plan, err := genSimpleRetrievePlan(suite.collection)
	suite.NoError(err)

	res, _, _, err := RetrieveStreaming(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.growing.ID()})
	suite.NoError(err)
	suite.Len(res[0].Offset, 3)
}

func (suite *RetrieveSuite) TestRetrieveNonExistSegment() {
	plan, err := genSimpleRetrievePlan(suite.collection)
	suite.NoError(err)

	res, _, _, err := RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{999})
	suite.NoError(err)
	suite.Len(res, 0)
}

func (suite *RetrieveSuite) TestRetrieveNilSegment() {
	plan, err := genSimpleRetrievePlan(suite.collection)
	suite.NoError(err)

	DeleteSegment(suite.sealed)
	res, _, _, err := RetrieveHistorical(context.TODO(), suite.manager, plan,
		suite.collectionID,
		[]int64{suite.partitionID},
		[]int64{suite.sealed.ID()})
	suite.ErrorIs(err, merr.ErrSegmentNotLoaded)
	suite.Len(res, 0)
}

func TestRetrieve(t *testing.T) {
	suite.Run(t, new(RetrieveSuite))
}
