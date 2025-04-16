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

package coordrecovery

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/tests/integration"
)

type CoordSwitchSuite struct {
	integration.MiniClusterSuite
}

const (
	Dim                         = 128
	numCollections              = 500
	rowsPerCollection           = 1000
	maxGoRoutineNum             = 100
	maxAllowedInitTimeInSeconds = 20
)

var searchName = ""

func (s *CoordSwitchSuite) loadCollection(collectionName string, dim int) {
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

func (s *CoordSwitchSuite) checkCollections() bool {
	req := &milvuspb.ShowCollectionsRequest{
		DbName:    "",
		TimeStamp: 0, // means now
	}
	resp, err := s.Cluster.Proxy.ShowCollections(context.TODO(), req)
	s.Require().NoError(merr.CheckRPCCall(resp, err))
	s.Require().Equal(len(resp.CollectionIds), numCollections)
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
			searchName = name
			loaded++
		}
	}
	log.Info(fmt.Sprintf("loading status: %d/%d", loaded, len(resp.GetCollectionNames())))
	return notLoaded == 0
}

func (s *CoordSwitchSuite) search(collectionName string, dim int) {
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
	s.Require().NoError(merr.CheckRPCCall(queryResult, err))
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
	searchReq := integration.ConstructSearchRequest("", collectionName, expr,
		integration.FloatVecField, schemapb.DataType_FloatVector, nil, metric.IP, params, nq, dim, topk, roundDecimal)

	searchResult, err := c.Proxy.Search(context.TODO(), searchReq)

	s.NoError(merr.CheckRPCCall(searchResult, err))
}

func (s *CoordSwitchSuite) insertBatchCollections(prefix string, collectionBatchSize, idxStart, dim int, wg *sync.WaitGroup) {
	for idx := 0; idx < collectionBatchSize; idx++ {
		collectionName := prefix + "_" + strconv.Itoa(idxStart+idx)
		s.loadCollection(collectionName, dim)
	}
	wg.Done()
}

func (s *CoordSwitchSuite) setupData() {
	goRoutineNum := maxGoRoutineNum
	if goRoutineNum > numCollections {
		goRoutineNum = numCollections
	}
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
	s.Require().True(s.checkCollections())
	log.Info(fmt.Sprintf("=========================start to search %s=========================", searchName))
	s.search(searchName, Dim)
	log.Info("=========================Search finished=========================")
}

func (s *CoordSwitchSuite) switchCoord() float64 {
	c := s.Cluster
	start := time.Now()
	log.Info("=========================Stopping Coordinators========================")
	c.StopMixCoord()
	log.Info("=========================Coordinators stopped=========================", zap.Duration("elapsed", time.Since(start)))
	start = time.Now()

	testutil.ResetEnvironment()

	c.StartMixCoord()
	log.Info("=========================RootCoord restarted=========================")

	for i := 0; i < 1000; i++ {
		time.Sleep(time.Second)
		if s.checkCollections() {
			break
		}
	}
	elapsed := time.Since(start).Seconds()

	log.Info(fmt.Sprintf("=========================CheckCollections Done in %f seconds=========================", elapsed))
	s.search(searchName, Dim)
	log.Info("=========================Search finished after reboot=========================")
	return elapsed
}

func (s *CoordSwitchSuite) TestCoordSwitch() {
	s.setupData()
	var totalElapsed, minTime, maxTime float64 = 0, -1, -1
	rounds := 10
	for idx := 0; idx < rounds; idx++ {
		t := s.switchCoord()
		totalElapsed += t
		if t < minTime || minTime < 0 {
			minTime = t
		}
		if t > maxTime || maxTime < 0 {
			maxTime = t
		}
	}
	log.Info(fmt.Sprintf("=========================Coordinators init time avg=%fs(%fs/%d), min=%fs, max=%fs=========================", totalElapsed/float64(rounds), totalElapsed, rounds, minTime, maxTime))
	s.True(totalElapsed < float64(maxAllowedInitTimeInSeconds*rounds))
}

func TestCoordSwitch(t *testing.T) {
	t.Skip("https://github.com/milvus-io/milvus/issues/33823")
	suite.Run(t, new(CoordSwitchSuite))
}
