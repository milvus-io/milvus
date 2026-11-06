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

package upsertwal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	upsertfixture "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cluster"
)

func (s *UpsertWALSuite) TestGateOffAllowsDurableCommitAndLegacyProducer() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	begin, err := producer.Append(ctx, message.NewBeginTxnMessageBuilderV2().WithVChannel(c.vchannel).
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 60000}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable())
	s.Require().NoError(err)
	s.Require().NotNil(begin.TxnCtx)
	_, err = producer.Append(ctx, c.upsert(11, 2).WithTxnContext(*begin.TxnCtx))
	s.Require().NoError(err)
	pending := c.upsert(12, 4)
	restore := s.Cluster.MustModifyMilvusConfig(map[string]string{"streaming.wal.enableUpsertMessage": "false"})
	defer restore()
	s.waitForProxyUpsertFormat(ctx, "false")
	// A message generated before the update still appends after generation stops.
	_, err = producer.Append(ctx, pending)
	s.Require().NoError(err)
	s.assertRows(ctx, c, 12, 4, 1)
	_, err = producer.Append(ctx, message.NewCommitTxnMessageBuilderV2().WithVChannel(c.vchannel).
		WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(*begin.TxnCtx))
	s.Require().NoError(err)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	// API writes select legacy messages while previously generated Upsert remains supported.
	request := upsertfixture.Insert(message.MustAsMutableUpsertMessageV2(c.upsert(13, 5)).MustBody())

	for _, column := range request.GetFieldsData() {
		for _, field := range c.schema.GetFields() {
			if column.GetFieldId() == field.GetFieldID() {
				column.FieldName = field.GetName()
			}
		}
	}
	inserted, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{CollectionName: c.name, NumRows: 1, FieldsData: request.GetFieldsData()})
	s.Require().NoError(merr.CheckRPCCall(inserted, err))
	s.flush(ctx, c)
	s.assertRows(ctx, c, 13, 5, 1)
	request = upsertfixture.Insert(message.MustAsMutableUpsertMessageV2(c.upsert(13, 6)).MustBody())
	for _, column := range request.GetFieldsData() {
		for _, field := range c.schema.GetFields() {
			if column.GetFieldId() == field.GetFieldID() {
				column.FieldName = field.GetName()
			}
		}
	}
	result, err := s.Cluster.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{CollectionName: c.name, NumRows: 1, FieldsData: request.GetFieldsData()})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	s.assertRows(ctx, c, 13, 5, 0)
	s.assertRows(ctx, c, 13, 6, 1)

	request = upsertfixture.Insert(message.MustAsMutableUpsertMessageV2(c.upsert(13, 7)).MustBody())
	for _, column := range request.GetFieldsData() {
		for _, field := range c.schema.GetFields() {
			if column.GetFieldId() == field.GetFieldID() {
				column.FieldName = field.GetName()
			}
		}
	}
	result, err = s.Cluster.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{CollectionName: c.name, NumRows: 1, FieldsData: request.GetFieldsData(), PartialUpdate: true})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	s.assertRows(ctx, c, 13, 6, 0)
	s.assertRows(ctx, c, 13, 7, 1)
	// Gate-off also leaves retained Upsert history readable after reopening WAL.
	producer.Close()
	s.restartStreamingNode(c)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.assertRows(ctx, c, 13, 5, 0)
	s.assertRows(ctx, c, 13, 6, 0)
	s.assertRows(ctx, c, 13, 7, 1)
}

func (s *UpsertWALSuite) TestNamespaceRejectedBeforeUpsertAppend() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.createWithPK(ctx, schemapb.DataType_Int64, false, func(schema *schemapb.CollectionSchema) { schema.EnableNamespace = true })
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	defer producer.Close()
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().ErrorContains(err, "namespace")
	namespace := "tenant"
	insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{CollectionName: c.name, Namespace: &namespace, NumRows: 1})
	s.Require().ErrorContains(merr.CheckRPCCall(insert, err), "namespace")
	upsert, err := s.Cluster.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{CollectionName: c.name, Namespace: &namespace, NumRows: 1})
	s.Require().ErrorContains(merr.CheckRPCCall(upsert, err), "namespace")
	ordinary := s.create(ctx)
	ordinaryProducer := rawProducer(s.T(), ctx, s.Cluster, ordinary.vchannel)
	defer ordinaryProducer.Close()
	_, err = ordinaryProducer.Append(ctx, ordinary.upsert(11, 1))
	s.Require().NoError(err)
	s.flush(ctx, ordinary)
}

func (s *UpsertWALSuite) TestKeyedInsertRetryAcrossWALFormats() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for _, firstMode := range []string{"false", "true"} {
		s.Run(firstMode, func() {
			c := s.createWithPK(ctx, schemapb.DataType_Int64, true)
			producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
			defer producer.Close()
			setMode := func(mode string) {
				restore := s.Cluster.MustModifyMilvusConfig(map[string]string{"streaming.wal.enableUpsertMessage": mode})
				s.T().Cleanup(restore)
				s.waitForProxyUpsertFormat(ctx, mode)
			}
			setMode(firstMode)
			fields := upsertfixture.Insert(message.MustAsMutableUpsertMessageV2(c.upsert(11, 1)).MustBody()).GetFieldsData()
			for _, column := range fields {
				for _, field := range c.schema.GetFields() {
					if column.GetFieldId() == field.GetFieldID() {
						column.FieldName = field.GetName()
					}
				}
			}
			userFields := make([]*schemapb.FieldData, 0, len(fields)-1)
			for _, field := range fields {
				if field.GetFieldName() != "pk" {
					userFields = append(userFields, field)
				}
			}
			request := &milvuspb.InsertRequest{CollectionName: c.name, NumRows: 1, FieldsData: userFields}
			keyed := metadata.AppendToOutgoingContext(ctx, util.HeaderIdempotencyKey, fmt.Sprintf("format-%s-%d", firstMode, c.id))
			original, err := s.Cluster.MilvusClient.Insert(keyed, request)
			s.Require().NoError(merr.CheckRPCCall(original, err))
			otherMode := "true"
			if firstMode == "true" {
				otherMode = "false"
			}
			setMode(otherMode)
			checkRetry := func() {
				retried, err := s.Cluster.MilvusClient.Insert(keyed, request)
				s.Require().NoError(merr.CheckRPCCall(retried, err))
				s.Require().Equal(original.GetIDs().GetIntId().GetData(), retried.GetIDs().GetIntId().GetData())
				s.Require().Equal(original.GetTimestamp(), retried.GetTimestamp())
				s.assertRows(ctx, c, original.GetIDs().GetIntId().GetData()[0], 1, 1)
			}
			checkRetry()
			producer.Close()
			s.restartStreamingNode(c)
			checkRetry()
		})
	}
}

// Observe the producer config without writing probe rows or depending on append rejection.
func (s *upsertWALFixture) waitForProxyUpsertFormat(ctx context.Context, mode string) {
	s.Require().Eventually(func() bool {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+s.Cluster.DefaultProxy().GetMetricsAddr()+"/api/v1/_cluster/configs", nil)
		if err != nil {
			return false
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		configs := map[string]string{}
		if resp.StatusCode != http.StatusOK || json.NewDecoder(resp.Body).Decode(&configs) != nil {
			return false
		}
		for key, value := range configs {
			if strings.EqualFold(key, "streaming.wal.enableUpsertMessage") || strings.EqualFold(key, "streamingwalenableupsertmessage") {
				return value == mode
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "Proxy must observe the requested producer format")
}

// Keep StreamingNode's periodic config refresh behind the producer's refresh.
// This exercises a propagation window, not a mixed-version rollout.
type UpsertWALConfigWindowSuite struct{ upsertWALFixture }

func TestUpsertWALConfigWindow(t *testing.T) { suite.Run(t, new(UpsertWALConfigWindowSuite)) }

func (s *UpsertWALConfigWindowSuite) SetupSuite() {
	s.WithClusterOptions(cluster.WithMetricsPorts(), cluster.WithProcessEnv(func(role string, _ int64) map[string]string {
		if role == "streamingnode" {
			return map[string]string{paramtable.MilvusConfigRefreshIntervalEnvKey: "1h"}
		}
		return nil
	}))
	s.WithMilvusConfig("streaming.wal.enableUpsertMessage", "false")
	s.WithOptions(integration.WithDropAllCollectionsWhenTestTearDown())
	s.MiniClusterSuite.SetupSuite()
}

func (s *UpsertWALConfigWindowSuite) TestProducerEnablesBeforeStreamingNodeRefresh() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	s.waitForProxyUpsertFormat(ctx, "false")
	restore := s.Cluster.MustModifyMilvusConfig(map[string]string{"streaming.wal.enableUpsertMessage": "true"})
	defer restore()
	s.waitForProxyUpsertFormat(ctx, "true")
	fields := upsertfixture.Insert(message.MustAsMutableUpsertMessageV2(c.upsert(11, 1)).MustBody()).GetFieldsData()
	for _, field := range fields {
		for _, schemaField := range c.schema.GetFields() {
			if field.GetFieldId() == schemaField.GetFieldID() {
				field.FieldName = schemaField.GetName()
			}
		}
	}
	inserted, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{CollectionName: c.name, NumRows: 1, FieldsData: fields})
	s.Require().NoError(merr.CheckRPCCall(inserted, err))
	s.assertRows(ctx, c, 11, 1, 1)
}
