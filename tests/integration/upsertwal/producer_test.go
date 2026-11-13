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
	"fmt"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	upsertfixture "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (s *UpsertWALSuite) TestDurableTransactionAndNewProducerSurviveReopen() {
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
	// A prepared autocommit message can append while another transaction is open.
	_, err = producer.Append(ctx, pending)
	s.Require().NoError(err)
	s.assertRows(ctx, c, 12, 4, 1)
	_, err = producer.Append(ctx, message.NewCommitTxnMessageBuilderV2().WithVChannel(c.vchannel).
		WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(*begin.TxnCtx))
	s.Require().NoError(err)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	// Ordinary Insert, Upsert and partial update share the new row encoding.
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
	// Reopening preserves both the transaction and the subsequent API writes.
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
	fields := upsertfixture.Insert(message.MustAsMutableUpsertMessageV2(c.upsert(11, 1)).MustBody()).GetFieldsData()
	fields[len(fields)-1].FieldName = "pk"
	insert, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{CollectionName: c.name, Namespace: &namespace, NumRows: 1, FieldsData: fields})
	s.Require().ErrorContains(merr.CheckRPCCall(insert, err), "namespace")
	upsert, err := s.Cluster.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{CollectionName: c.name, Namespace: &namespace, NumRows: 1, FieldsData: fields})
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
	for _, firstFormat := range []string{"legacy", "v2"} {
		s.Run(firstFormat, func() {
			c := s.createWithPK(ctx, schemapb.DataType_Int64, true)
			producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
			defer producer.Close()
			fields := upsertfixture.Insert(message.MustAsMutableUpsertMessageV2(c.upsert(11, 1)).MustBody()).GetFieldsData()
			userFields := make([]*schemapb.FieldData, 0, len(fields)-1)
			for _, column := range fields {
				for _, field := range c.schema.GetFields() {
					if column.GetFieldId() == field.GetFieldID() {
						column.FieldName = field.GetName()
					}
				}
				if column.GetFieldName() != "pk" {
					userFields = append(userFields, column)
				}
			}
			request := &milvuspb.InsertRequest{CollectionName: c.name, NumRows: 1, FieldsData: userFields}
			key := fmt.Sprintf("format-%s-%d", firstFormat, c.id)
			keyed := metadata.AppendToOutgoingContext(ctx, util.HeaderIdempotencyKey, key)
			var original *milvuspb.MutationResult
			if firstFormat == "legacy" {
				legacy := message.MustAsMutableInsertMessageV1(c.legacy("Insert", 11, 1))
				header := legacy.Header()
				ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{11}}}}
				message.SetInsertHeaderIdempotentInsertResult(header, &messagespb.IdempotentInsertResult{Ids: ids, RowOffsets: []uint32{0}})
				originalMessage := message.NewInsertMessageBuilderV1().WithVChannel(c.vchannel).WithHeader(header).WithBody(legacy.MustBody()).WithIdempotencyKey(message.IdempotencyKey(key)).MustBuildMutable()
				result, err := producer.Append(ctx, originalMessage)
				s.Require().NoError(err)
				original = &milvuspb.MutationResult{IDs: ids, Timestamp: result.TimeTick}
			} else {
				var err error
				original, err = s.Cluster.MilvusClient.Insert(keyed, request)
				s.Require().NoError(merr.CheckRPCCall(original, err))
			}
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
