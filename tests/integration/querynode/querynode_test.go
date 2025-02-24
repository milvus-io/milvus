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

package querynode

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
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

type QueryNodeSuite struct {
	integration.MiniClusterSuite
	maxGoRoutineNum   int
	dim               int
	numCollections    int
	rowsPerCollection int
	waitTimeInSec     time.Duration
	prefix            string
}

func (s *QueryNodeSuite) setupParam() {
	s.maxGoRoutineNum = 100
	s.dim = 128
	s.numCollections = 2
	s.rowsPerCollection = 100
	s.waitTimeInSec = time.Second * 10
}

func (s *QueryNodeSuite) loadCollection(collectionName string, dim int) {
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
}

func (s *QueryNodeSuite) checkCollections() bool {
	req := &milvuspb.ShowCollectionsRequest{
		DbName:    "",
		TimeStamp: 0, // means now
	}
	resp, err := s.Cluster.Proxy.ShowCollections(context.TODO(), req)
	s.NoError(err)
	s.Equal(len(resp.CollectionIds), s.numCollections)
	notLoaded := 0
	loaded := 0
	for _, name := range resp.CollectionNames {
		loadProgress, err := s.Cluster.Proxy.GetLoadingProgress(context.TODO(), &milvuspb.GetLoadingProgressRequest{
			DbName:         "",
			CollectionName: name,
		})
		s.NoError(err)
		if loadProgress.GetProgress() != int64(100) {
			notLoaded++
		} else {
			loaded++
		}
	}
	log.Info(fmt.Sprintf("loading status: %d/%d", loaded, len(resp.GetCollectionNames())))
	return notLoaded == 0
}

func (s *QueryNodeSuite) search(collectionName string, dim int) {
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

func (s *QueryNodeSuite) insertBatchCollections(prefix string, collectionBatchSize, idxStart, dim int, wg *sync.WaitGroup) {
	for idx := 0; idx < collectionBatchSize; idx++ {
		collectionName := s.prefix + "_" + strconv.Itoa(idxStart+idx)
		s.loadCollection(collectionName, dim)
	}
	wg.Done()
}

func (s *QueryNodeSuite) setupData() {
	// Add the second query node
	s.Cluster.AddQueryNode()
	goRoutineNum := s.maxGoRoutineNum
	if goRoutineNum > s.numCollections {
		goRoutineNum = s.numCollections
	}
	collectionBatchSize := s.numCollections / goRoutineNum
	log.Info(fmt.Sprintf("=========================test with s.dim=%d, s.rowsPerCollection=%d, s.numCollections=%d, goRoutineNum=%d==================", s.dim, s.rowsPerCollection, s.numCollections, goRoutineNum))
	log.Info("=========================Start to inject data=========================")
	s.prefix = "TestQueryNodeUtil" + funcutil.GenRandomStr()
	searchName := s.prefix + "_0"
	wg := sync.WaitGroup{}
	for idx := 0; idx < goRoutineNum; idx++ {
		wg.Add(1)
		go s.insertBatchCollections(s.prefix, collectionBatchSize, idx*collectionBatchSize, s.dim, &wg)
	}
	wg.Wait()
	log.Info("=========================Data injection finished=========================")
	s.checkCollections()
	log.Info(fmt.Sprintf("=========================start to search %s=========================", searchName))
	s.search(searchName, s.dim)
	log.Info("=========================Search finished=========================")
	time.Sleep(s.waitTimeInSec)
	s.checkCollections()
	log.Info(fmt.Sprintf("=========================start to search2 %s=========================", searchName))
	s.search(searchName, s.dim)
	log.Info("=========================Search2 finished=========================")
	s.checkAllCollectionsReady()
}

func (s *QueryNodeSuite) checkAllCollectionsReady() {
	goRoutineNum := s.maxGoRoutineNum
	if goRoutineNum > s.numCollections {
		goRoutineNum = s.numCollections
	}
	collectionBatchSize := s.numCollections / goRoutineNum
	for i := 0; i < goRoutineNum; i++ {
		for idx := 0; idx < collectionBatchSize; idx++ {
			collectionName := s.prefix + "_" + strconv.Itoa(i*collectionBatchSize+idx)
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
}

func (s *QueryNodeSuite) checkQNRestarts() {
	// Stop all query nodes
	s.Cluster.StopAllQueryNodes()
	// Add new Query nodes.
	s.Cluster.AddQueryNode()
	s.Cluster.AddQueryNode()

	time.Sleep(s.waitTimeInSec)
	for i := 0; i < 1000; i++ {
		time.Sleep(s.waitTimeInSec)
		if s.checkCollections() {
			break
		}
	}
	s.checkAllCollectionsReady()
}

func (s *QueryNodeSuite) TestSwapQN() {
	s.setupParam()
	s.setupData()
	// Test case with one query node stopped
	s.Cluster.QueryNode.Stop()
	time.Sleep(s.waitTimeInSec)
	s.checkAllCollectionsReady()
	// Test case with new Query nodes added
	s.Cluster.AddQueryNode()
	s.Cluster.AddQueryNode()
	time.Sleep(s.waitTimeInSec)
	s.checkAllCollectionsReady()

	// Test case with all query nodes replaced
	for idx := 0; idx < 2; idx++ {
		s.checkQNRestarts()
	}
}

func TestQueryNodeUtil(t *testing.T) {
	suite.Run(t, new(QueryNodeSuite))
}
