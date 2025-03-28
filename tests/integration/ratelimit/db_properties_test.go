/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ratelimit

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

const dim = 768

type DBPropertiesSuite struct {
	integration.MiniClusterSuite
}

func (s *DBPropertiesSuite) prepareDatabase(ctx context.Context, dbName string, configKey string, configValue string) {
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFunc()

	resp, err := s.Cluster.MilvusClient.CreateDatabase(timeoutCtx, &milvuspb.CreateDatabaseRequest{
		DbName: dbName,
		Properties: []*commonpb.KeyValuePair{
			{
				Key:   configKey,
				Value: configValue,
			},
		},
	})
	s.NoError(merr.CheckRPCCall(resp, err))

	resp2, err2 := s.Cluster.MilvusClient.DescribeDatabase(timeoutCtx, &milvuspb.DescribeDatabaseRequest{DbName: dbName})
	s.NoError(merr.CheckRPCCall(resp2, err2))
}

func (s *DBPropertiesSuite) prepareCollection(ctx context.Context, dbName string, collectionName string) {
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, 15*time.Second)
	defer cancelFunc()

	schema := integration.ConstructSchemaOfVecDataType(collectionName, dim, true, schemapb.DataType_FloatVector)
	marshaledSchema, err := proto.Marshal(schema)
	s.NoError(err)

	createCollecctionResp, err := s.Cluster.MilvusClient.CreateCollection(timeoutCtx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      common.DefaultShardsNum,
	})
	s.NoError(merr.CheckRPCCall(createCollecctionResp, err))

	describeCollectionResp, err := s.Cluster.MilvusClient.DescribeCollection(timeoutCtx, &milvuspb.DescribeCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(merr.CheckRPCCall(describeCollectionResp, err))

	createIndexStatus, err := s.Cluster.MilvusClient.CreateIndex(timeoutCtx, &milvuspb.CreateIndexRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.IP),
	})
	s.NoError(err)
	err = merr.Error(createIndexStatus)
	if err != nil {
		log.Warn("createIndexStatus fail reason", zap.Error(err))
	}

	s.WaitForIndexBuiltWithDB(timeoutCtx, dbName, collectionName, integration.FloatVecField)
	log.Info("Create index done")

	// load
	loadStatus, err := s.Cluster.MilvusClient.LoadCollection(timeoutCtx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.NoError(err)
	err = merr.Error(loadStatus)
	if err != nil {
		log.Warn("LoadCollection fail reason", zap.Error(err))
	}
	s.WaitForLoadWithDB(ctx, dbName, collectionName)
	log.Info("Load collection done")
}

func (s *DBPropertiesSuite) insert(ctx context.Context, dbName string, collectionName string,
	rowNum int,
) (*milvuspb.MutationResult, error) {
	hashKeys := integration.GenerateHashKeys(rowNum)
	fVecColumn := integration.NewFloatVectorFieldData(integration.FloatVecField, rowNum, dim)
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFunc()
	return s.Cluster.MilvusClient.Insert(timeoutCtx, &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldsData:     []*schemapb.FieldData{fVecColumn},
		HashKeys:       hashKeys,
		NumRows:        uint32(rowNum),
	})
}

func (s *DBPropertiesSuite) search(ctx context.Context, dbName string, collectionName string) (*milvuspb.SearchResults, error) {
	params := integration.GetSearchParams(integration.IndexFaissIvfFlat, metric.IP)
	searchReq := integration.ConstructSearchRequest(dbName, collectionName, "", integration.FloatVecField,
		schemapb.DataType_FloatVector, nil, metric.IP, params, 10, dim, 10, -1)

	timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFunc()
	return s.Cluster.MilvusClient.Search(timeoutCtx, searchReq)
}

func (s *DBPropertiesSuite) TestLimitWithDBSize() {
	ctx := context.Background()
	dbName := "db3"
	s.prepareDatabase(ctx, dbName, common.DatabaseDiskQuotaKey, "1")

	collectionName := "Test" + funcutil.GenRandomStr()
	s.prepareCollection(ctx, dbName, collectionName)

	resp, err := s.insert(ctx, dbName, collectionName, 1000)
	s.NoError(err)
	s.True(merr.Ok(resp.GetStatus()))

	timeoutCtx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
	defer cancelFunc()
	flushResp, err := s.Cluster.MilvusClient.Flush(timeoutCtx, &milvuspb.FlushRequest{DbName: dbName, CollectionNames: []string{collectionName}})
	s.NoError(err)
	s.True(merr.Ok(flushResp.GetStatus()))

	s.waitForSuccessOrTimeout(func() (error, bool) {
		resp, err = s.insert(ctx, dbName, collectionName, 1000)
		if err != nil {
			return err, false
		}
		fmt.Println("TestLimitWithDBSize insert response:", resp)
		return nil, merr.ErrServiceQuotaExceeded.Is(merr.Error(resp.GetStatus()))
	}, "TestLimitWithDBSize-insert")
}

func (s *DBPropertiesSuite) TestDenyReadingDB() {
	ctx := context.Background()
	dbName := "db2"
	s.prepareDatabase(ctx, dbName, common.DatabaseForceDenyReadingKey, "true")

	collectionName := "Test" + funcutil.GenRandomStr()
	s.prepareCollection(ctx, dbName, collectionName)

	s.waitForSuccessOrTimeout(func() (error, bool) {
		resp, err := s.search(ctx, dbName, collectionName)
		if err != nil {
			return err, false
		}
		fmt.Println("TestDenyReadingDB search response:", resp)
		return nil, merr.ErrServiceQuotaExceeded.Is(merr.Error(resp.GetStatus()))
	}, "TestDenyReadingDB-search")
}

func (s *DBPropertiesSuite) TestDenyWringDB() {
	ctx := context.Background()
	dbName := "db1"
	s.prepareDatabase(ctx, dbName, common.DatabaseForceDenyWritingKey, "true")

	collectionName := "Test" + funcutil.GenRandomStr()
	s.prepareCollection(ctx, dbName, collectionName)

	s.waitForSuccessOrTimeout(func() (error, bool) {
		resp, err := s.insert(ctx, dbName, collectionName, 100)
		if err != nil {
			return err, false
		}
		fmt.Println("TestDenyWringDB insert response:", resp)
		return nil, merr.ErrServiceQuotaExceeded.Is(merr.Error(resp.GetStatus()))
	}, "TestDenyWringDB-insert")
}

func (s *DBPropertiesSuite) SetupSuite() {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().QuotaConfig.QuotaCenterCollectInterval.Key, "1")
	s.MiniClusterSuite.SetupSuite()
}

func (s *DBPropertiesSuite) TearDownSuite() {
	paramtable.Get().Reset(paramtable.Get().QuotaConfig.QuotaCenterCollectInterval.Key)
	s.MiniClusterSuite.TearDownSuite()
}

func TestLimitWithDBProperties(t *testing.T) {
	t.Skip("skip test")
	suite.Run(t, new(DBPropertiesSuite))
}

func (s *DBPropertiesSuite) waitForSuccessOrTimeout(fn func() (error, bool), desc string) {
	time.Sleep(3 * time.Second)
	for {
		select {
		case <-time.After(15 * time.Second):
			s.FailNow("waiting for " + desc + " timeout")
		default:
			err, ok := fn()
			if err != nil {
				s.Errorf(err, "failed to %s", desc)
			}
			if ok {
				return
			}
			time.Sleep(1 * time.Second)
		}
	}
}
