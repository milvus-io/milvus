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

package retrysearch

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

type TestRetrySearchSuite struct {
	integration.MiniClusterSuite

	dbName string

	// test params
	nq         int
	topK       int
	indexType  string
	metricType string
	pkType     schemapb.DataType
	vecType    schemapb.DataType
}

func (s *TestRetrySearchSuite) run() {
	ctx, cancel := context.WithCancel(s.Cluster.GetContext())
	defer cancel()

	collection := fmt.Sprintf("TestRetrySearch_%d_%d_%s_%s_%s",
		s.nq, s.topK, s.indexType, s.metricType, funcutil.GenRandomStr())

	const (
		NB  = 2000
		dim = 128
	)

	paramtable.Get().Save(paramtable.Get().DataCoordCfg.EnableCompaction.Key, "false")

	if len(s.dbName) > 0 {
		createDataBaseStatus, err := s.Cluster.Proxy.CreateDatabase(ctx, &milvuspb.CreateDatabaseRequest{
			DbName: s.dbName,
		})
		s.Require().NoError(err)
		s.Require().True(merr.Ok(createDataBaseStatus))
	}

	pkFieldName := "pkField"
	vecFieldName := "vecField"
	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         pkFieldName,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     s.pkType,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.MaxLengthKey,
				Value: "100",
			},
		},
		IndexParams: nil,
		AutoID:      false,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         vecFieldName,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     s.vecType,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
		IndexParams: nil,
	}
	schema := integration.ConstructSchema(collection, dim, false, pk, fVec)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createCollectionStatus, err := s.Cluster.Proxy.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         s.dbName,
		CollectionName: collection,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(createCollectionStatus))
	health1, err := s.Cluster.DataCoord.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	s.NoError(err)
	log.Info("dataCoord health", zap.Any("health1", health1))
	for i := 0; i < 2; i++ {
		err = integration.GenerateNumpyFile(s.Cluster.ChunkManager.RootPath()+"/"+pkFieldName+".npy", NB*(i+1), schemapb.DataType_Int64, []*commonpb.KeyValuePair{})
		s.NoError(err)
		err = integration.GenerateNumpyFile(s.Cluster.ChunkManager.RootPath()+"/"+vecFieldName+".npy", NB*(i+1), schemapb.DataType_FloatVector, []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: strconv.Itoa(dim),
			},
		})
		s.NoError(err)

		bulkInsertFiles := []string{
			s.Cluster.ChunkManager.RootPath() + "/" + pkFieldName + ".npy",
			s.Cluster.ChunkManager.RootPath() + "/" + vecFieldName + ".npy",
		}
		importResp, err := s.Cluster.Proxy.Import(ctx, &milvuspb.ImportRequest{
			CollectionName: collection,
			Files:          bulkInsertFiles,
		})
		s.NoError(err)
		log.Info("Import result", zap.Any("importResp", importResp), zap.Int64s("tasks", importResp.GetTasks()))

		tasks := importResp.GetTasks()
		for _, task := range tasks {
		loop:
			for {
				importTaskState, err := s.Cluster.Proxy.GetImportState(ctx, &milvuspb.GetImportStateRequest{
					Task: task,
				})
				s.NoError(err)
				switch importTaskState.GetState() {
				case commonpb.ImportState_ImportCompleted:
					break loop
				case commonpb.ImportState_ImportFailed:
					break loop
				case commonpb.ImportState_ImportFailedAndCleaned:
					break loop
				default:
					log.Info("import task state", zap.Int64("id", task), zap.String("state", importTaskState.GetState().String()))
					time.Sleep(time.Second * time.Duration(3))
					continue
				}
			}
		}
	}

	health2, err := s.Cluster.DataCoord.CheckHealth(ctx, &milvuspb.CheckHealthRequest{})
	s.NoError(err)
	log.Info("dataCoord health", zap.Any("health2", health2))

	segments, err := s.Cluster.MetaWatcher.ShowSegments()
	s.NoError(err)
	s.NotEmpty(segments)
	for _, segment := range segments {
		log.Info("ShowSegments result", zap.String("segment", segment.String()))
	}

	// create index
	_, err = s.Cluster.Proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		DbName:         s.dbName,
		CollectionName: collection,
		FieldName:      vecFieldName,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, s.indexType, s.metricType),
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(createCollectionStatus))

	s.WaitForIndexBuiltWithDB(ctx, s.dbName, collection, vecFieldName)

	// load
	_, err = s.Cluster.Proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         s.dbName,
		CollectionName: collection,
	})
	s.Require().NoError(err)
	s.Require().True(merr.Ok(createCollectionStatus))
	s.WaitForLoadWithDB(ctx, s.dbName, collection)

	// search
	paramtable.Get().Save(paramtable.Get().AutoIndexConfig.Enable.Key, "true")
	nq := s.nq
	topk := s.topK

	outputFields := []string{vecFieldName}
	params := integration.GetSearchParams(s.indexType, s.metricType)

	// filter out one segment, and one segment remain few results, so retry search will be triggered
	searchReq := integration.ConstructSearchRequest(s.dbName, collection, pkFieldName+">"+strconv.Itoa(NB*2-100),
		vecFieldName, s.vecType, outputFields, s.metricType, params, nq, dim, topk, -1)

	searchResp, err := s.Cluster.Proxy.Search(ctx, searchReq)
	s.Require().NoError(err)
	s.Require().True(merr.Ok(searchResp.GetStatus()))

	result := searchResp.GetResults()
	log.Info("result num", zap.Int("length", len(result.GetIds().GetIntId().GetData())))
	if s.pkType == schemapb.DataType_Int64 {
		s.Require().Less(len(result.GetIds().GetIntId().GetData()), nq*topk)
	} else {
		s.Require().Less(len(result.GetIds().GetStrId().GetData()), nq*topk)
	}
	vecFieldIndex := -1
	for i, fieldData := range result.GetFieldsData() {
		if typeutil.IsVectorType(fieldData.GetType()) {
			vecFieldIndex = i
			break
		}
	}

	s.Require().EqualValues(nq, result.GetNumQueries())
	s.Require().Less(result.GetTopK(), int64(topk))

	// check output vectors
	if s.vecType == schemapb.DataType_FloatVector {
		s.Require().Less(len(result.GetFieldsData()[vecFieldIndex].GetVectors().GetFloatVector().GetData()), nq*topk*dim)
		resData := result.GetFieldsData()[vecFieldIndex].GetVectors().GetFloatVector().GetData()
		if s.pkType == schemapb.DataType_Int64 {
			for i, id := range result.GetIds().GetIntId().GetData() {
				actual := resData[i*dim : (i+1)*dim]
				expect := make([]float32, 0, dim)
				for range actual {
					expect = append(expect, float32(id))
				}
				s.Require().ElementsMatch(expect, actual)
			}
		} else {
			for i, idStr := range result.GetIds().GetStrId().GetData() {
				id, err := strconv.Atoi(idStr)
				s.Require().NoError(err)
				actual := resData[i*dim : (i+1)*dim]
				expect := make([]float32, 0, dim)
				for range actual {
					expect = append(expect, float32(id))
				}
				s.Require().ElementsMatch(expect, actual)
			}
		}
	}
}

func (s *TestRetrySearchSuite) TestRetrySearch_FLAT() {
	s.nq = 1
	s.topK = 200
	s.indexType = integration.IndexFaissIDMap
	s.metricType = metric.L2
	s.pkType = schemapb.DataType_Int64
	s.vecType = schemapb.DataType_FloatVector
	s.run()
}

func TestRetrySearch(t *testing.T) {
	suite.Run(t, new(TestRetrySearchSuite))
}
