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

package watchcompatibility

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	grpcdatacoord "github.com/milvus-io/milvus/internal/distributed/datacoord"
	grpcdatanode "github.com/milvus-io/milvus/internal/distributed/datanode"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type DataNodeCompatibility struct {
	integration.MiniClusterSuite
	maxGoRoutineNum   int
	dim               int
	numCollections    int
	rowsPerCollection int
	waitTimeInSec     time.Duration
	prefix            string
}

func (s *DataNodeCompatibility) setupParam() {
	s.maxGoRoutineNum = 100
	s.dim = 128
	s.numCollections = 1
	s.rowsPerCollection = 100
	s.waitTimeInSec = time.Second * 1
}

func (s *DataNodeCompatibility) flush(collectionName string) {
	c := s.Cluster
	flushResp, err := c.Proxy.Flush(context.TODO(), &milvuspb.FlushRequest{
		DbName:          "",
		CollectionNames: []string{collectionName},
	})
	s.NoError(err)
	segmentIDs, has := flushResp.GetCollSegIDs()[collectionName]
	s.Require().True(has)
	s.Require().NotEmpty(segmentIDs)
	ids := segmentIDs.GetData()
	flushTs, has := flushResp.GetCollFlushTs()[collectionName]
	s.True(has)

	segments, err := c.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	s.WaitForFlush(context.TODO(), ids, flushTs, "", collectionName)
}

func (s *DataNodeCompatibility) loadCollection(collectionName string) {
	c := s.Cluster
	dbName := ""
	schema := integration.ConstructSchema(collectionName, s.dim, true)
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
		fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, s.dim)
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
	s.flush(collectionName)

	// create index
	createIndexStatus, err := c.Proxy.CreateIndex(context.TODO(), &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(s.dim, integration.IndexFaissIvfFlat, metric.IP),
	})
	s.NoError(err)
	err = merr.Error(createIndexStatus)
	s.NoError(err)
	s.WaitForIndexBuilt(context.TODO(), collectionName, integration.FloatVecField)

	// load
	loadStatus, err := c.Proxy.LoadCollection(context.TODO(), &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	err = merr.Error(loadStatus)
	s.NoError(err)
	s.WaitForLoad(context.TODO(), collectionName)
}

func (s *DataNodeCompatibility) checkCollections() bool {
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
	return notLoaded == 0
}

func (s *DataNodeCompatibility) search(collectionName string, currentNumRows int) {
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
	s.Equal(len(queryResult.FieldsData), 1)
	numEntities := queryResult.FieldsData[0].GetScalars().GetLongData().Data[0]
	s.Equal(numEntities, int64(currentNumRows))

	// Search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1
	radius := 10

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.IP)
	params["radius"] = radius
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.IP, params, nq, s.dim, topk, roundDecimal)

	searchResult, _ := c.Proxy.Search(context.TODO(), searchReq)

	err = merr.Error(searchResult.GetStatus())
	s.NoError(err)
}

func (s *DataNodeCompatibility) insertBatchCollections(prefix string, collectionBatchSize, idxStart int, wg *sync.WaitGroup) {
	for idx := 0; idx < collectionBatchSize; idx++ {
		collectionName := prefix + "_" + strconv.Itoa(idxStart+idx)
		s.loadCollection(collectionName)
	}
	wg.Done()
}

func (s *DataNodeCompatibility) insert(collectionName string, rowNum int) {
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, s.dim)
	hashKeys := integration.GenerateHashKeys(rowNum)
	insertResult, err := s.Cluster.Proxy.Insert(context.TODO(), &milvuspb.InsertRequest{
		DbName:         "",
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
	s.NoError(err)
	s.True(merr.Ok(insertResult.GetStatus()))
	s.flush(collectionName)
}

func (s *DataNodeCompatibility) insertAndCheck(collectionName string, currentNumRows *int, testInsert bool) {
	s.search(collectionName, *currentNumRows)
	insertRows := 1000
	if testInsert {
		s.insert(collectionName, insertRows)
		*currentNumRows += insertRows
	}
	s.search(collectionName, *currentNumRows)
}

func (s *DataNodeCompatibility) setupData() {
	// Add the second data node
	s.Cluster.AddDataNode()
	goRoutineNum := s.maxGoRoutineNum
	if goRoutineNum > s.numCollections {
		goRoutineNum = s.numCollections
	}
	collectionBatchSize := s.numCollections / goRoutineNum
	log.Info(fmt.Sprintf("=========================test with dim=%d, s.rowsPerCollection=%d, s.numCollections=%d, goRoutineNum=%d==================", s.dim, s.rowsPerCollection, s.numCollections, goRoutineNum))
	log.Info("=========================Start to inject data=========================")
	s.prefix = "TestDataNodeUtil" + funcutil.GenRandomStr()
	searchName := s.prefix + "_0"
	wg := sync.WaitGroup{}
	for idx := 0; idx < goRoutineNum; idx++ {
		wg.Add(1)
		go s.insertBatchCollections(s.prefix, collectionBatchSize, idx*collectionBatchSize, &wg)
	}
	wg.Wait()
	log.Info("=========================Data injection finished=========================")
	s.checkCollections()
	log.Info(fmt.Sprintf("=========================start to search %s=========================", searchName))
	s.search(searchName, s.rowsPerCollection)
	log.Info("=========================Search finished=========================")
	time.Sleep(s.waitTimeInSec)
	s.checkCollections()
	log.Info(fmt.Sprintf("=========================start to search2 %s=========================", searchName))
	s.search(searchName, s.rowsPerCollection)
	log.Info("=========================Search2 finished=========================")
	s.checkAllCollectionsReady()
}

func (s *DataNodeCompatibility) checkAllCollectionsReady() {
	goRoutineNum := s.maxGoRoutineNum
	if goRoutineNum > s.numCollections {
		goRoutineNum = s.numCollections
	}
	collectionBatchSize := s.numCollections / goRoutineNum
	for i := 0; i < goRoutineNum; i++ {
		for idx := 0; idx < collectionBatchSize; idx++ {
			collectionName := s.prefix + "_" + strconv.Itoa(i*collectionBatchSize+idx)
			s.search(collectionName, s.rowsPerCollection)
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

func (s *DataNodeCompatibility) checkSingleDNRestarts(currentNumRows *int, numNodes, idx int, testInsert bool) {
	// Stop all data nodes
	s.Cluster.StopAllDataNodes()
	// Add new data nodes.
	var dn []*grpcdatanode.Server
	for i := 0; i < numNodes; i++ {
		dn = append(dn, s.Cluster.AddDataNode())
	}
	time.Sleep(s.waitTimeInSec)
	cn := fmt.Sprintf("%s_0", s.prefix)
	s.insertAndCheck(cn, currentNumRows, testInsert)
	dn[idx].Stop()
	time.Sleep(s.waitTimeInSec)
	s.insertAndCheck(cn, currentNumRows, testInsert)
}

func (s *DataNodeCompatibility) checkDNRestarts(currentNumRows *int, testInsert bool) {
	numDatanodes := 2 // configurable
	for idx := 0; idx < numDatanodes; idx++ {
		s.checkSingleDNRestarts(currentNumRows, numDatanodes, idx, testInsert)
	}
}

func (s *DataNodeCompatibility) restartDC() {
	c := s.Cluster
	c.DataCoord.Stop()
	c.DataCoord = grpcdatacoord.NewServer(context.TODO(), c.GetFactory())
	err := c.DataCoord.Run()
	s.NoError(err)
}

func (s *DataNodeCompatibility) TestCompatibility() {
	s.setupParam()
	s.setupData()
	rows := s.rowsPerCollection

	// new coord + new node
	s.checkDNRestarts(&rows, true)

	// new coord + old node
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableBalanceChannelWithRPC.Key, "false")
	s.checkDNRestarts(&rows, false)

	// old coord + old node
	s.restartDC()
	s.checkDNRestarts(&rows, true)

	// old coord + new node
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableBalanceChannelWithRPC.Key, "true")
	s.checkDNRestarts(&rows, false)

	// new coord + both old & new datanodes.
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableBalanceChannelWithRPC.Key, "false")
	s.restartDC()
	s.Cluster.StopAllDataNodes()
	d1 := s.Cluster.AddDataNode()
	d2 := s.Cluster.AddDataNode()
	cn := fmt.Sprintf("%s_0", s.prefix)
	s.insertAndCheck(cn, &rows, true)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableBalanceChannelWithRPC.Key, "true")
	s.restartDC()
	s.insertAndCheck(cn, &rows, false)
	s.Cluster.AddDataNode()
	d1.Stop()
	s.checkDNRestarts(&rows, true)
	s.Cluster.AddDataNode()
	d2.Stop()
	s.checkDNRestarts(&rows, true)
}

func TestDataNodeCompatibility(t *testing.T) {
	suite.Run(t, new(DataNodeCompatibility))
}
