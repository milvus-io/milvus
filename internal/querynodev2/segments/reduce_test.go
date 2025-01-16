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
	"log"
	"math"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
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
	collection   *Collection
	segment      Segment
}

func (suite *ReduceSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *ReduceSuite) SetupTest() {
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
	schema := GenTestCollectionSchema("test-reduce", schemapb.DataType_Int64, true)
	suite.collection, err = NewCollection(suite.collectionID,
		schema,
		GenTestIndexMeta(suite.collectionID, schema),
		&querypb.LoadMetaInfo{
			LoadType: querypb.LoadType_LoadCollection,
		})
	suite.Require().NoError(err)
	suite.segment, err = NewSegment(ctx,
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
		err = suite.segment.(*LocalSegment).LoadFieldData(ctx, binlog.FieldID, int64(msgLength), binlog)
		suite.Require().NoError(err)
	}
}

func (suite *ReduceSuite) TearDownTest() {
	suite.segment.Release(context.Background())
	DeleteCollection(suite.collection)
	ctx := context.Background()
	suite.chunkManager.RemoveWithPrefix(ctx, suite.rootPath)
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
	vec := testutils.GenerateFloatVectors(1, defaultDim)
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
	var planpb planpb.PlanNode
	// proto.UnmarshalText(planStr, &planpb)
	prototext.Unmarshal([]byte(planStr), &planpb)
	serializedPlan, err := proto.Marshal(&planpb)
	suite.NoError(err)
	plan, err := createSearchPlanByExpr(context.Background(), suite.collection, serializedPlan)
	suite.NoError(err)
	searchReq, err := parseSearchRequest(context.Background(), plan, placeGroupByte)
	searchReq.mvccTimestamp = typeutil.MaxTimestamp
	suite.NoError(err)
	defer searchReq.Delete()

	searchResult, err := suite.segment.Search(context.Background(), searchReq)
	suite.NoError(err)

	err = checkSearchResult(context.Background(), nq, plan, searchResult)
	suite.NoError(err)
}

func (suite *ReduceSuite) TestReduceInvalid() {
	plan := &SearchPlan{}
	_, err := ReduceSearchResultsAndFillData(context.Background(), plan, nil, 1, nil, nil)
	suite.Error(err)

	searchReq, err := genSearchPlanAndRequests(suite.collection, []int64{suite.segmentID}, IndexHNSW, 10)
	suite.NoError(err)
	searchResults := make([]*SearchResult, 0)
	searchResults = append(searchResults, nil)
	_, err = ReduceSearchResultsAndFillData(context.Background(), searchReq.plan, searchResults, 1, []int64{10}, []int64{10})
	suite.Error(err)
}

func TestReduce(t *testing.T) {
	suite.Run(t, new(ReduceSuite))
}
