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

package segcore_test

import (
	"context"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/testutils"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ReduceSuite struct {
	suite.Suite
	chunkManager storage.ChunkManager
	rootPath     string

	collectionID int64
	partitionID  int64
	segmentID    int64
	collection   *segcore.CCollection
	segment      segcore.CSegment
}

func (suite *ReduceSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *ReduceSuite) SetupTest() {
	localDataRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
	initcore.InitLocalChunkManager(localDataRootPath)
	err := initcore.InitMmapManager(paramtable.Get())
	suite.NoError(err)
	ctx := context.Background()
	msgLength := 100

	suite.rootPath = suite.T().Name()
	chunkManagerFactory := storage.NewTestChunkManagerFactory(paramtable.Get(), suite.rootPath)
	suite.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	initcore.InitRemoteChunkManager(paramtable.Get())

	suite.collectionID = 100
	suite.partitionID = 10
	suite.segmentID = 1
	schema := mock_segcore.GenTestCollectionSchema("test-reduce", schemapb.DataType_Int64, true)
	suite.collection, err = segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID: suite.collectionID,
		Schema:       schema,
		IndexMeta:    mock_segcore.GenTestIndexMeta(suite.collectionID, schema),
	})
	suite.NoError(err)
	suite.segment, err = segcore.CreateCSegment(&segcore.CreateCSegmentRequest{
		Collection:  suite.collection,
		SegmentID:   suite.segmentID,
		SegmentType: segcore.SegmentTypeSealed,
		IsSorted:    false,
	})
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
	req := &segcore.LoadFieldDataRequest{
		RowCount: int64(msgLength),
	}
	for _, binlog := range binlogs {
		req.Fields = append(req.Fields, segcore.LoadFieldDataInfo{Field: binlog})
	}
	_, err = suite.segment.LoadFieldData(ctx, req)
	suite.Require().NoError(err)
}

func (suite *ReduceSuite) TearDownTest() {
	suite.segment.Release()
	suite.collection.Release()
	ctx := context.Background()
	suite.chunkManager.RemoveWithPrefix(ctx, suite.rootPath)
}

func (suite *ReduceSuite) TestReduceParseSliceInfo() {
	originNQs := []int64{2, 3, 2}
	originTopKs := []int64{10, 5, 20}
	nqPerSlice := int64(2)
	sInfo := segcore.ParseSliceInfo(originNQs, originTopKs, nqPerSlice)

	expectedSliceNQs := []int64{2, 2, 1, 2}
	expectedSliceTopKs := []int64{10, 5, 5, 20}
	suite.True(funcutil.SliceSetEqual(sInfo.SliceNQs, expectedSliceNQs))
	suite.True(funcutil.SliceSetEqual(sInfo.SliceTopKs, expectedSliceTopKs))
}

func (suite *ReduceSuite) TestReduceAllFunc() {
	nq := int64(10)

	// TODO: replace below by genPlaceholderGroup(nq)
	vec := testutils.GenerateFloatVectors(1, mock_segcore.DefaultDim)
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

	planStr := `vector_anns: <
                 field_id: 107
                 query_info: <
                   topk: 10
                   round_decimal: 6
                   metric_type: "L2"
                   search_params: "{\"nprobe\": 10}"
                 >
                 placeholder_tag: "$0"
               >`
	var planNode planpb.PlanNode
	// proto.UnmarshalText(planStr, &planpb)
	prototext.Unmarshal([]byte(planStr), &planNode)
	serializedPlan, err := proto.Marshal(&planNode)
	suite.NoError(err)
	searchReq, err := segcore.NewSearchRequest(suite.collection, &querypb.SearchRequest{
		Req: &internalpb.SearchRequest{
			SerializedExprPlan: serializedPlan,
			MvccTimestamp:      typeutil.MaxTimestamp,
		},
	}, placeGroupByte)

	suite.NoError(err)
	defer searchReq.Delete()

	searchResult, err := suite.segment.Search(context.Background(), searchReq)
	suite.NoError(err)

	err = mock_segcore.CheckSearchResult(context.Background(), nq, searchReq.Plan(), searchResult)
	suite.NoError(err)

	// Test Illegal Query
	retrievePlan, err := segcore.NewRetrievePlan(
		suite.collection,
		[]byte(fmt.Sprintf("%d > 100", mock_segcore.RowIDField.ID)),
		typeutil.MaxTimestamp,
		0,
		0)
	suite.Error(err)
	suite.Nil(retrievePlan)

	plan := planpb.PlanNode{
		Node: &planpb.PlanNode_Query{
			Query: &planpb.QueryPlanNode{
				IsCount: true,
			},
		},
	}
	expr, err := proto.Marshal(&plan)
	suite.NoError(err)
	retrievePlan, err = segcore.NewRetrievePlan(
		suite.collection,
		expr,
		typeutil.MaxTimestamp,
		0,
		0)
	suite.NotNil(retrievePlan)
	suite.NoError(err)

	retrieveResult, err := suite.segment.Retrieve(context.Background(), retrievePlan)
	suite.NotNil(retrieveResult)
	suite.NoError(err)
	result, err := retrieveResult.GetResult()
	suite.NoError(err)
	suite.NotNil(result)
	suite.Equal(int64(100), result.AllRetrieveCount)
	retrieveResult.Release()
}

func (suite *ReduceSuite) TestReduceInvalid() {
	plan := &segcore.SearchPlan{}
	_, err := segcore.ReduceSearchResultsAndFillData(context.Background(), plan, nil, 1, nil, nil)
	suite.Error(err)

	searchReq, err := mock_segcore.GenSearchPlanAndRequests(suite.collection, []int64{suite.segmentID}, mock_segcore.IndexHNSW, 10)
	suite.NoError(err)
	searchResults := make([]*segcore.SearchResult, 0)
	searchResults = append(searchResults, nil)
	_, err = segcore.ReduceSearchResultsAndFillData(context.Background(), searchReq.Plan(), searchResults, 1, []int64{10}, []int64{10})
	suite.Error(err)
}

func TestReduce(t *testing.T) {
	suite.Run(t, new(ReduceSuite))
}
