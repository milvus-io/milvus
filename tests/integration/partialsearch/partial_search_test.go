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

package partialsearch

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

type PartialSearchSuite struct {
	integration.MiniClusterSuite
	dim               int
	numCollections    int
	rowsPerCollection int
	waitTimeInSec     time.Duration
	prefix            string
}

func (s *PartialSearchSuite) setupParam() {
	s.dim = 128
	s.numCollections = 1
	s.rowsPerCollection = 100
	s.waitTimeInSec = time.Second * 10
}

func (s *PartialSearchSuite) loadCollection(collectionName string, dim int, wg *sync.WaitGroup) {
	c := s.Cluster
	dbName := ""
	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(context.TODO(), &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(err)

	err = merr.Error(createCollectionStatus)
	s.NoError(err)

	showCollectionsResp, err := c.Proxy.ShowCollections(context.TODO(), &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.GetStatus()))

	batchSize := 500000
	for start := 0; start < s.rowsPerCollection; start += batchSize {
		rowNum := batchSize
		if start+batchSize > s.rowsPerCollection {
			rowNum = s.rowsPerCollection - start
		}
		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
		hashKeys := integration.GenerateHashKeys(rowNum)
		insertResult, err := c.Proxy.Insert(context.TODO(), &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{fVecColumn},
			HashKeys:       hashKeys,
			NumRows:        uint32(rowNum),
		})
		s.NoError(err)
		s.True(merr.Ok(insertResult.GetStatus()))
	}
	log.Info("=========================Data insertion finished=========================")

	// flush
	flushResp, err := c.Proxy.Flush(context.TODO(), &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	s.NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	ids := segmentIDs.GetData()
	s.Require().NotEmpty(segmentIDs)
	s.Require().True(has)
	flushTs, has := flushResp.GetCollFlushTs()[collectionName]
	s.True(has)

	segments, err := c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	s.WaitForFlush(context.TODO(), ids, flushTs, dbName, collectionName)
	log.Info("=========================Data flush finished=========================")

	// create index
	createIndexStatus, err := c.Proxy.CreateIndex(context.TODO(), &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.IP),
	})
	s.NoError(err)
	err = merr.Error(createIndexStatus)
	s.NoError(err)
	s.WaitForIndexBuilt(context.TODO(), collectionName, integration.FloatVecField)
	log.Info("=========================Index created=========================")

	// load
	loadStatus, err := c.Proxy.LoadCollection(context.TODO(), &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	err = merr.Error(loadStatus)
	s.NoError(err)
	s.WaitForLoad(context.TODO(), collectionName)
	log.Info("=========================Collection loaded=========================")
	wg.Done()
}

func (s *PartialSearchSuite) checkCollectionLoaded(collectionName string) bool {
	loadProgress, err := s.Cluster.Proxy.GetLoadingProgress(context.TODO(), &milvuspb.GetLoadingProgressRequest{
		DbName:         "",
		CollectionName: collectionName,
	})
	s.NoError(err)
	if loadProgress.GetProgress() != int64(100) {
		return false
	}
	return true
}

func (s *PartialSearchSuite) checkCollectionsLoaded(startCollectionID, endCollectionID int) bool {
	notLoaded := 0
	loaded := 0
	for idx := startCollectionID; idx < endCollectionID; idx++ {
		collectionName := s.prefix + "_" + strconv.Itoa(idx)
		if s.checkCollectionLoaded(collectionName) {
			notLoaded++
		} else {
			loaded++
		}
	}
	log.Info(fmt.Sprintf("loading status: %d/%d", loaded, endCollectionID-startCollectionID+1))
	return notLoaded == 0
}

func (s *PartialSearchSuite) checkAllCollectionsLoaded() bool {
	return s.checkCollectionsLoaded(0, s.numCollections)
}

func (s *PartialSearchSuite) search(collectionName string, dim int) {
	c := s.Cluster
	var err error
	// Query
	queryReq := &milvuspb.QueryRequest{
		Base:               nil,
		CollectionName:     collectionName,
		PartitionNames:     nil,
		Expr:               "",
		OutputFields:       []string{"count(*)"},
		TravelTimestamp:    0,
		GuaranteeTimestamp: 0,
	}
	queryResult, err := c.Proxy.Query(context.TODO(), queryReq)
	s.NoError(err)
	s.Equal(queryResult.Status.ErrorCode, commonpb.ErrorCode_Success)
	s.Equal(len(queryResult.FieldsData), 1)
	numEntities := queryResult.FieldsData[0].GetScalars().GetLongData().Data[0]
	s.Equal(numEntities, int64(s.rowsPerCollection))

	// Search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1
	radius := 10

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.IP)
	params["radius"] = radius
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.IP, params, nq, dim, topk, roundDecimal)

	searchResult, _ := c.Proxy.Search(context.TODO(), searchReq)

	err = merr.Error(searchResult.GetStatus())
	s.NoError(err)
}

func (s *PartialSearchSuite) FailOnSearch(collectionName string) {
	c := s.Cluster
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1
	radius := 10

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.IP)
	params["radius"] = radius
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.IP, params, nq, s.dim, topk, roundDecimal)

	searchResult, err := c.Proxy.Search(context.TODO(), searchReq)
	s.NoError(err)
	err = merr.Error(searchResult.GetStatus())
	s.Require().Error(err)
}

func (s *PartialSearchSuite) setupData() {
	// Add the second query node
	log.Info("=========================Start to inject data=========================")
	s.prefix = "TestPartialSearchUtil" + funcutil.GenRandomStr()
	searchName := s.prefix + "_0"
	wg := sync.WaitGroup{}
	for idx := 0; idx < s.numCollections; idx++ {
		wg.Add(1)
		go s.loadCollection(s.prefix+"_"+strconv.Itoa(idx), s.dim, &wg)
	}
	wg.Wait()
	log.Info("=========================Data injection finished=========================")
	s.checkAllCollectionsLoaded()
	log.Info(fmt.Sprintf("=========================start to search %s=========================", searchName))
	s.search(searchName, s.dim)
	log.Info("=========================Search finished=========================")
	time.Sleep(s.waitTimeInSec)
	s.checkAllCollectionsLoaded()
	log.Info(fmt.Sprintf("=========================start to search2 %s=========================", searchName))
	s.search(searchName, s.dim)
	log.Info("=========================Search2 finished=========================")
	s.checkAllCollectionsReady()
}

func (s *PartialSearchSuite) checkCollectionsReady(startCollectionID, endCollectionID int) {
	for i := startCollectionID; i < endCollectionID; i++ {
		collectionName := s.prefix + "_" + strconv.Itoa(i)
		s.search(collectionName, s.dim)
		queryReq := &milvuspb.QueryRequest{
			CollectionName: collectionName,
			Expr:           "",
			OutputFields:   []string{"count(*)"},
		}
		_, err := s.Cluster.Proxy.Query(context.TODO(), queryReq)
		s.NoError(err)
	}
}

func (s *PartialSearchSuite) checkAllCollectionsReady() {
	s.checkCollectionsReady(0, s.numCollections)
}

func (s *PartialSearchSuite) releaseSegmentsReq(collectionID, nodeID, segmentID typeutil.UniqueID, shard string) *querypb.ReleaseSegmentsRequest {
	req := &querypb.ReleaseSegmentsRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_ReleaseSegments),
			commonpbutil.WithMsgID(1<<30),
			commonpbutil.WithTargetID(nodeID),
		),

		NodeID:       nodeID,
		CollectionID: collectionID,
		SegmentIDs:   []int64{segmentID},
		Scope:        querypb.DataScope_Historical,
		Shard:        shard,
		NeedTransfer: false,
	}
	return req
}

func (s *PartialSearchSuite) describeCollection(name string) (int64, []string) {
	resp, err := s.Cluster.Proxy.DescribeCollection(context.TODO(), &milvuspb.DescribeCollectionRequest{
		DbName:         "default",
		CollectionName: name,
	})
	s.NoError(err)
	log.Info(fmt.Sprintf("describe collection: %v", resp))
	return resp.CollectionID, resp.VirtualChannelNames
}

func (s *PartialSearchSuite) getSegmentIDs(collectionName string) []int64 {
	resp, err := s.Cluster.Proxy.GetPersistentSegmentInfo(context.TODO(), &milvuspb.GetPersistentSegmentInfoRequest{
		DbName:         "default",
		CollectionName: collectionName,
	})
	s.NoError(err)
	var res []int64
	for _, seg := range resp.Infos {
		res = append(res, seg.SegmentID)
	}
	return res
}

func (s *PartialSearchSuite) TestPartialSearch() {
	s.setupParam()
	s.setupData()

	startCollectionID := 0
	endCollectionID := 0
	// Search should work in the beginning
	s.checkCollectionsReady(startCollectionID, endCollectionID)
	// Test case with one segment released
	// Partial search does not work yet.
	c := s.Cluster
	q1 := c.QueryNode
	c.MixCoord.StopCheckerForTestOnly()
	collectionName := s.prefix + "_0"
	nodeID := q1.GetServerIDForTestOnly()
	collectionID, channels := s.describeCollection(collectionName)
	segs := s.getSegmentIDs(collectionName)
	s.Require().Positive(len(segs))
	s.Require().Positive(len(channels))
	segmentID := segs[0]
	shard := channels[0]
	req := s.releaseSegmentsReq(collectionID, nodeID, segmentID, shard)
	q1.ReleaseSegments(context.TODO(), req)
	s.FailOnSearch(collectionName)
	c.MixCoord.StartCheckerForTestOnly()
}

func TestPartialSearchUtil(t *testing.T) {
	suite.Run(t, new(PartialSearchSuite))
}
