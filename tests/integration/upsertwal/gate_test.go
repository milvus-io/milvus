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
	"strings"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	upsertfixture "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (s *UpsertWALSuite) TestRawProduceRejectsForbiddenEnvelopeMarkers() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	for _, marker := range []string{"_ur", "_np", "_pcl", "_bh"} {
		msg := c.upsert(11, 2)
		msg.Properties().ToRawMap()[marker] = ""
		_, err := producer.Append(ctx, msg)
		s.Require().Error(err, "forbidden marker %s", marker)
		s.assertRows(ctx, c, 11, 1, 1)
		s.assertRows(ctx, c, 11, 2, 0)
	}
}

func (s *UpsertWALSuite) TestGateOffAllowsDurableCommitAndRejectsNewProducer() {
	for _, mode := range []string{"false", "auto"} {
		s.Run(mode, func() {
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
			restore := s.Cluster.MustModifyMilvusConfig(map[string]string{"streaming.wal.enableUpsertMessage": mode})
			defer restore()
			s.Require().Eventually(func() bool {
				_, err := producer.Append(ctx, c.upsert(12, 4))
				return err != nil && strings.Contains(err.Error(), "native Upsert WAL append is disabled")
			}, 10*time.Second, 100*time.Millisecond, "the generation gate must reject new Upsert appends")
			_, err = producer.Append(ctx, message.NewCommitTxnMessageBuilderV2().WithVChannel(c.vchannel).
				WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(*begin.TxnCtx))
			s.Require().NoError(err)
			s.assertRows(ctx, c, 11, 1, 0)
			s.assertRows(ctx, c, 11, 2, 1)
			// New API writes fail before sending any WAL message.
			request := upsertfixture.Insert(message.MustAsMutableUpsertMessageV2(c.upsert(13, 5)).MustBody())
			result, err := s.Cluster.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{CollectionName: c.name, NumRows: 1, FieldsData: request.GetFieldsData()})
			s.Require().Error(merr.CheckRPCCall(result, err))
			s.assertRows(ctx, c, 13, 5, 0)
			// Gate-off also leaves retained Upsert history readable after reopening WAL.
			producer.Close()
			s.restartStreamingNode(c)
			s.assertRows(ctx, c, 11, 1, 0)
			s.assertRows(ctx, c, 11, 2, 1)
			s.assertRows(ctx, c, 13, 5, 0)
		})
	}
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
