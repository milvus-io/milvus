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

package walswitch

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	management "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type WALSwitchSuite struct {
	integration.MiniClusterSuite
}

func (s *WALSwitchSuite) TestExistingQueryNodeContinuesAfterPulsarToWoodpeckerSwitch() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	const (
		dim       = 8
		initialPK = 1
		newPK     = 2
	)
	collectionName := "TestWALSwitch_" + funcutil.GenRandomStr()

	schema := integration.ConstructSchemaOfVecDataType(
		collectionName,
		dim,
		false,
		schemapb.DataType_FloatVector,
	)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName:   collectionName,
		Schema:           marshaledSchema,
		ShardsNum:        1,
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))

	s.insertRow(ctx, collectionName, initialPK, dim)
	s.flushCollection(ctx, collectionName)

	status, err = s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams: integration.ConstructIndexParam(
			dim,
			integration.IndexFaissIDMap,
			metric.L2,
		),
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForIndexBuilt(ctx, collectionName, integration.FloatVecField)

	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		CollectionName: collectionName,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, collectionName)
	s.Require().True(s.queryPrimaryKey(ctx, collectionName, initialPK))

	queryNodeID := s.Cluster.DefaultQueryNode().GetNodeID()
	// Keep the loaded channel idle so its existing consumer advances mainly via
	// TimeTick before the WAL backend changes.
	time.Sleep(3 * time.Second)

	s.alterWALToWoodpecker(ctx)
	s.waitForMQType(ctx, "woodpecker")

	s.Require().True(s.Cluster.DefaultQueryNode().IsWorking())
	s.Require().Equal(queryNodeID, s.Cluster.DefaultQueryNode().GetNodeID())

	// AlterWAL returns after the broadcast is accepted. Retry the same primary
	// key while StreamingNode finishes reopening the channel on Woodpecker.
	s.Require().Eventually(func() bool {
		return s.tryInsertRow(ctx, collectionName, newPK, dim)
	}, 3*time.Minute, 500*time.Millisecond)

	// Strong consistency requires the existing QueryNode subscription to cross
	// the historical WAL boundary and consume TimeTick from the new WAL.
	s.Require().Eventually(func() bool {
		return s.queryPrimaryKey(ctx, collectionName, newPK)
	}, 3*time.Minute, 500*time.Millisecond)

	s.Require().True(s.Cluster.DefaultQueryNode().IsWorking())
	s.Require().Equal(queryNodeID, s.Cluster.DefaultQueryNode().GetNodeID())

	status, err = s.Cluster.MilvusClient.DropCollection(ctx, &milvuspb.DropCollectionRequest{
		CollectionName: collectionName,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
}

func (s *WALSwitchSuite) insertRow(ctx context.Context, collectionName string, primaryKey int64, dim int) {
	s.Require().True(s.tryInsertRow(ctx, collectionName, primaryKey, dim))
}

func (s *WALSwitchSuite) tryInsertRow(ctx context.Context, collectionName string, primaryKey int64, dim int) bool {
	attemptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	insertResult, err := s.Cluster.MilvusClient.Insert(attemptCtx, &milvuspb.InsertRequest{
		CollectionName: collectionName,
		FieldsData: []*schemapb.FieldData{
			integration.NewInt64FieldDataWithStart(integration.Int64Field, 1, primaryKey),
			integration.NewFloatVectorFieldData(integration.FloatVecField, 1, dim),
		},
		NumRows: 1,
	})
	if err := merr.CheckRPCCall(insertResult, err); err != nil {
		s.T().Logf("insert while WAL is switching: %v", err)
		return false
	}
	return insertResult.GetInsertCnt() == 1
}

func (s *WALSwitchSuite) flushCollection(ctx context.Context, collectionName string) {
	flushResp, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		CollectionNames: []string{collectionName},
	})
	s.Require().NoError(merr.CheckRPCCall(flushResp, err))
	segmentIDs := flushResp.GetCollSegIDs()[collectionName]
	s.Require().NotNil(segmentIDs)
	flushTimeTick, ok := flushResp.GetCollFlushTs()[collectionName]
	s.Require().True(ok)
	s.WaitForFlush(ctx, segmentIDs.GetData(), flushTimeTick, "", collectionName)
}

func (s *WALSwitchSuite) queryPrimaryKey(ctx context.Context, collectionName string, primaryKey int64) bool {
	attemptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result, err := s.Cluster.MilvusClient.Query(attemptCtx, &milvuspb.QueryRequest{
		CollectionName:   collectionName,
		Expr:             fmt.Sprintf("%s == %d", integration.Int64Field, primaryKey),
		OutputFields:     []string{integration.Int64Field},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	if err := merr.CheckRPCCall(result, err); err != nil {
		s.T().Logf("query while WAL reader is switching: %v", err)
		return false
	}
	if len(result.GetFieldsData()) != 1 {
		return false
	}
	return len(result.GetFieldsData()[0].GetScalars().GetLongData().GetData()) > 0
}

func (s *WALSwitchSuite) alterWALToWoodpecker(ctx context.Context) {
	mixCoordManagementPort, err := s.Cluster.DefaultMixCoord().GetMetricsPort()
	s.Require().NoError(err)
	url := fmt.Sprintf("http://localhost:%d%s", mixCoordManagementPort, management.WALAlterPath)
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		url,
		bytes.NewBufferString(`{"target_wal_name":"woodpecker"}`),
	)
	s.Require().NoError(err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	s.Require().NoError(err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode, string(body))
}

func (s *WALSwitchSuite) waitForMQType(ctx context.Context, expected string) {
	configKey := path.Join(
		s.Cluster.RootPath(),
		"config",
		strings.NewReplacer("/", "", "_", "", ".", "").Replace(
			strings.ToLower(paramtable.Get().MQCfg.Type.Key),
		),
	)
	s.Require().Eventually(func() bool {
		resp, err := s.Cluster.EtcdCli.Get(ctx, configKey)
		if err != nil || len(resp.Kvs) != 1 {
			return false
		}
		return string(resp.Kvs[0].Value) == expected
	}, time.Minute, 200*time.Millisecond)
}

func TestWALSwitch(t *testing.T) {
	suite.Run(t, new(WALSwitchSuite))
}
