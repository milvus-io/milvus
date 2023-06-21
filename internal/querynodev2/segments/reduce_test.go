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
	"log"
	"math"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type ReduceSuite struct {
	suite.Suite
	chunkManager storage.ChunkManager

	collectionID int64
	partitionID  int64
	segmentID    int64
	collection   *Collection
	segment      *LocalSegment
}

func (suite *ReduceSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *ReduceSuite) SetupTest() {
	var err error
	ctx := context.Background()
	msgLength := 100

	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(paramtable.Get())
	suite.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	initcore.InitRemoteChunkManager(paramtable.Get())

	suite.collectionID = 100
	suite.partitionID = 10
	suite.segmentID = 1
	schema := GenTestCollectionSchema("test-reduce", schemapb.DataType_Int64)
	suite.collection = NewCollection(suite.collectionID,
		schema,
		GenTestIndexMeta(suite.collectionID, schema),
		querypb.LoadType_LoadCollection,
	)
	suite.segment, err = NewSegment(suite.collection,
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
		err = suite.segment.LoadFieldData(binlog.FieldID, int64(msgLength), binlog)
		suite.Require().NoError(err)
	}
}

func (suite *ReduceSuite) TearDownTest() {
	DeleteSegment(suite.segment)
	DeleteCollection(suite.collection)
	ctx := context.Background()
	suite.chunkManager.RemoveWithPrefix(ctx, paramtable.Get().MinioCfg.RootPath.GetValue())
}

func (suite *ReduceSuite) TestReduceParseSliceInfo() {
	originNQs := []int64{2, 3, 2}
	originTopKs := []int64{10, 5, 20}
	nqPerSlice := int64(2)
	sInfo := ParseSliceInfo(originNQs, originTopKs, nqPerSlice)

	expectedSliceNQs := []int64{2, 2, 1, 2}
	expectedSliceTopKs := []int64{10, 5, 5, 20}
	suite.True(funcutil.SliceSetEqual(sInfo.SliceNQs, expectedSliceNQs))
	suite.True(funcutil.SliceSetEqual(sInfo.SliceTopKs, expectedSliceTopKs))
}

func (suite *ReduceSuite) TestReduceAllFunc() {
	nq := int64(10)

	// TODO: replace below by genPlaceholderGroup(nq)
	vec := generateFloatVectors(1, defaultDim)
	var searchRawData []byte
	for i, ele := range vec {
		buf := make([]byte, 4)
		common.Endian.PutUint32(buf, math.Float32bits(ele+float32(i*2)))
		searchRawData = append(searchRawData, buf...)
	}

	placeholderValue := commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   commonpb.PlaceholderType_FloatVector,
		Values: [][]byte{},
	}

	for i := 0; i < int(nq); i++ {
		placeholderValue.Values = append(placeholderValue.Values, searchRawData)
	}

	placeholderGroup := commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{&placeholderValue},
	}

	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		log.Print("marshal placeholderGroup failed")
	}

	dslString := "{\"bool\": { \n\"vector\": {\n \"floatVectorField\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\n \"topk\": 10 \n,\"round_decimal\": 6\n } \n } \n } \n }"

	plan, err := createSearchPlan(suite.collection, dslString)
	suite.NoError(err)
	searchReq, err := parseSearchRequest(plan, placeGroupByte)
	searchReq.timestamp = 0
	suite.NoError(err)
	defer searchReq.Delete()

	searchResult, err := suite.segment.Search(context.Background(), searchReq)
	suite.NoError(err)

	err = checkSearchResult(nq, plan, searchResult)
	suite.NoError(err)
}

func (suite *ReduceSuite) TestReduceInvalid() {
	plan := &SearchPlan{}
	_, err := ReduceSearchResultsAndFillData(plan, nil, 1, nil, nil)
	suite.Error(err)

	searchReq, err := genSearchPlanAndRequests(suite.collection, []int64{suite.segmentID}, IndexHNSW, 10)
	suite.NoError(err)
	searchResults := make([]*SearchResult, 0)
	searchResults = append(searchResults, nil)
	_, err = ReduceSearchResultsAndFillData(searchReq.plan, searchResults, 1, []int64{10}, []int64{10})
	suite.Error(err)
}

func TestReduce(t *testing.T) {
	suite.Run(t, new(ReduceSuite))
}
