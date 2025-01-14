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

package coorddownsearch

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
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type CoordDownSearch struct {
	integration.MiniClusterSuite
}

const (
	Dim                         = 128
	numCollections              = 1
	rowsPerCollection           = 1000
	maxGoRoutineNum             = 1
	maxAllowedInitTimeInSeconds = 60
)

var searchCollectionName = ""

func (s *CoordDownSearch) loadCollection(collectionName string, dim int) {
	c := s.Cluster
	dbName := ""
	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollectionStatus, err := c.Proxy.CreateCollection(context.TODO(), &milvuspb.CreateCollectionRequest{
		DbName:           dbName,
		CollectionName:   collectionName,
		Schema:           marshaledSchema,
		ShardsNum:        common.DefaultShardsNum,
		ConsistencyLevel: commonpb.ConsistencyLevel_Eventually,
	})
	s.NoError(err)

	err = merr.Error(createCollectionStatus)
	s.NoError(err)

	showCollectionsResp, err := c.Proxy.ShowCollections(context.TODO(), &milvuspb.ShowCollectionsRequest{})
	s.NoError(err)
	s.True(merr.Ok(showCollectionsResp.GetStatus()))

	batchSize := 5000
	for start := 0; start < rowsPerCollection; start += batchSize {
		rowNum := batchSize
		if start+batchSize > rowsPerCollection {
			rowNum = rowsPerCollection - start
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
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIDMap, metric.IP),
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

func (s *CoordDownSearch) checkCollections() bool {
	req := &milvuspb.ShowCollectionsRequest{
		DbName:    "",
		TimeStamp: 0, // means now
	}
	resp, err := s.Cluster.Proxy.ShowCollections(context.TODO(), req)
	s.NoError(err)
	s.Equal(len(resp.CollectionIds), numCollections)
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
			searchCollectionName = name
			loaded++
		}
	}
	log.Info(fmt.Sprintf("loading status: %d/%d", loaded, len(resp.GetCollectionNames())))
	return notLoaded == 0
}

func (s *CoordDownSearch) search(collectionName string, dim int, consistencyLevel commonpb.ConsistencyLevel) {
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
		ConsistencyLevel:   consistencyLevel,
	}
	queryResult, err := c.Proxy.Query(context.TODO(), queryReq)
	s.NoError(err)
	s.Equal(len(queryResult.FieldsData), 1)
	numEntities := queryResult.FieldsData[0].GetScalars().GetLongData().Data[0]
	s.Equal(numEntities, int64(rowsPerCollection))

	// Search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1
	radius := 10

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.IP)
	params["radius"] = radius
	searchReq := integration.ConstructSearchRequestWithConsistencyLevel("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.IP, params, nq, dim, topk,
		roundDecimal, false, consistencyLevel)

	searchResult, _ := c.Proxy.Search(context.TODO(), searchReq)

	err = merr.Error(searchResult.GetStatus())
	s.NoError(err)
}

func (s *CoordDownSearch) searchFailed(collectionName string, dim int, consistencyLevel commonpb.ConsistencyLevel) {
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
		ConsistencyLevel:   consistencyLevel,
	}
	queryResp, err := c.Proxy.Query(context.TODO(), queryReq)
	s.NoError(err)
	err = merr.Error(queryResp.GetStatus())
	s.Error(err)

	// Search
	expr := fmt.Sprintf("%s > 0", integration.Int64Field)
	nq := 10
	topk := 10
	roundDecimal := -1
	radius := 10

	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.IP)
	params["radius"] = radius
	searchReq := integration.ConstructSearchRequestWithConsistencyLevel("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.IP, params, nq, dim, topk,
		roundDecimal, false, consistencyLevel)

	searchResult, err := c.Proxy.Search(context.TODO(), searchReq)
	s.NoError(err)
	err = merr.Error(searchResult.GetStatus())
	s.Error(err)
}

func (s *CoordDownSearch) insertBatchCollections(prefix string, collectionBatchSize, idxStart, dim int, wg *sync.WaitGroup) {
	for idx := 0; idx < collectionBatchSize; idx++ {
		collectionName := prefix + "_" + strconv.Itoa(idxStart+idx)
		s.loadCollection(collectionName, dim)
	}
	wg.Done()
}

func (s *CoordDownSearch) setupData() {
	goRoutineNum := maxGoRoutineNum

	collectionBatchSize := numCollections / goRoutineNum
	log.Info(fmt.Sprintf("=========================test with Dim=%d, rowsPerCollection=%d, numCollections=%d, goRoutineNum=%d==================", Dim, rowsPerCollection, numCollections, goRoutineNum))
	log.Info("=========================Start to inject data=========================")
	prefix := "TestCoordSwitch" + funcutil.GenRandomStr()
	searchName := prefix + "_0"
	wg := sync.WaitGroup{}
	for idx := 0; idx < goRoutineNum; idx++ {
		wg.Add(1)
		go s.insertBatchCollections(prefix, collectionBatchSize, idx*collectionBatchSize, Dim, &wg)
	}
	wg.Wait()
	log.Info("=========================Data injection finished=========================")
	s.checkCollections()
	log.Info(fmt.Sprintf("=========================start to search %s=========================", searchName))
	s.search(searchName, Dim, commonpb.ConsistencyLevel_Eventually)
	log.Info("=========================Search finished=========================")
}

func (s *CoordDownSearch) searchAfterCoordDown() float64 {
	c := s.Cluster

	params := paramtable.Get()
	paramtable.Init()

	start := time.Now()
	log.Info("=========================Data Coordinators stopped=========================")
	c.StopDataCoord()
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Eventually)
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Bounded)
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Strong)

	log.Info("=========================Query Coordinators stopped=========================")
	c.StopQueryCoord()
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Eventually)
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Bounded)
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Strong)

	log.Info("=========================Root Coordinators stopped=========================")
	c.StopRootCoord()
	params.Save(params.CommonCfg.GracefulTime.Key, "60000")
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Bounded)
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Eventually)
	params.Reset(params.CommonCfg.GracefulTime.Key)
	failedStart := time.Now()
	s.searchFailed(searchCollectionName, Dim, commonpb.ConsistencyLevel_Strong)
	log.Info(fmt.Sprintf("=========================Failed search cost: %fs=========================", time.Since(failedStart).Seconds()))

	registry.ResetRegistration()

	log.Info("=========================restart Root Coordinators=========================")
	c.StartRootCoord()
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Eventually)
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Bounded)
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Strong)

	log.Info("=========================restart Data Coordinators=========================")
	c.StartDataCoord()
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Eventually)
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Bounded)
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Strong)

	log.Info("=========================restart Query Coordinators=========================")
	c.StartQueryCoord()
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Eventually)
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Bounded)
	s.search(searchCollectionName, Dim, commonpb.ConsistencyLevel_Strong)

	elapsed := time.Since(start).Seconds()
	return elapsed
}

func (s *CoordDownSearch) TestSearchAfterCoordDown() {
	s.setupData()

	elapsed := s.searchAfterCoordDown()
	log.Info(fmt.Sprintf("=========================Search After Coord Down Done in %f seconds=========================", elapsed))
	s.True(elapsed < float64(maxAllowedInitTimeInSeconds))
}

func TestCoordDownSearch(t *testing.T) {
	suite.Run(t, new(CoordDownSearch))
}
