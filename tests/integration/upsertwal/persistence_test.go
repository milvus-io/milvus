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
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	upsertfixture "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (s *UpsertWALSuite) TestChunkedUpsertHasOneLogicalEffect() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	const rows = 32768
	msg := c.upsert(11, 2, "chunked")
	typed := message.MustAsMutableUpsertMessageV2(msg)
	body := typed.MustBody()
	keys, rowIDs, markers := make([]int64, rows), make([]int64, rows), make([]int64, rows)
	vectors := make([]float32, rows*2)
	offsets := make([]uint32, rows)
	for index := range rows {
		keys[index], rowIDs[index], markers[index] = int64(index+11), (1<<62)+rowSequence.Add(1), 2
		vectors[2*index], vectors[2*index+1] = 1, 2
		offsets[index] = uint32(index)
	}
	for _, field := range body.Columns {
		switch field.GetFieldName() {
		case "pk":
			field.GetScalars().GetLongData().Data = keys
		case "marker":
			field.GetScalars().GetLongData().Data = markers
		case "vector":
			field.GetVectors().GetFloatVector().Data = vectors
		}
	}
	body.PrimaryKeys.GetIntId().Data, body.RowIds = keys, rowIDs
	body.DeleteEffects[0].PrimaryKeys.GetIntId().Data = keys
	typed.OverwriteBody(body)
	header := typed.Header()
	body.RequestRowMapping.RowOffsets = offsets
	typed.OverwriteBody(body)
	header.DeleteRows, header.Partitions[0].Rows, header.Partitions[0].BinarySize = rows, rows, rows*32
	typed.OverwriteHeader(header)
	s.Require().Greater(msg.EstimateSize(), 524288, "input must exceed the configured physical message limit")
	result, err := producer.Append(ctx, msg)
	s.Require().NoError(err)
	s.Require().NotNil(result.MessageID)
	s.Require().Positive(result.TimeTick)
	// Check the persisted effects without depending on the query startup change.
	s.flush(ctx, c)
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, c.name)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.assertRows(ctx, c, rows+10, 2, 1)
}

func (s *UpsertWALSuite) TestCompactionPreservesReplacement() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	_, err = producer.Append(ctx, c.upsert(12, 9))
	s.Require().NoError(err)
	s.flush(ctx, c)
	_, err = producer.Append(ctx, c.upsert(11, 2))
	s.Require().NoError(err)
	s.flush(ctx, c)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.WaitForIndexBuilt(ctx, c.name, "vector")
	compaction, err := s.Cluster.MilvusClient.ManualCompaction(ctx, &milvuspb.ManualCompactionRequest{CollectionID: c.id, L0Compaction: true})
	s.Require().NoError(merr.CheckRPCCall(compaction, err))
	s.Require().Positive(compaction.GetCompactionID())
	s.Require().Eventually(func() bool {
		state, err := s.Cluster.MilvusClient.GetCompactionState(ctx, &milvuspb.GetCompactionStateRequest{CompactionID: compaction.GetCompactionID()})
		return merr.CheckRPCCall(state, err) == nil && state.GetState() == commonpb.CompactionState_Completed
	}, 2*time.Minute, 100*time.Millisecond)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.assertRows(ctx, c, 12, 9, 1)
}

func (s *UpsertWALSuite) TestReplacementSurvivesFlushRestartAndReload() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	s.flush(ctx, c)
	_, err = producer.Append(ctx, c.upsert(11, 2))
	s.Require().NoError(err)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.flush(ctx, c)
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	producer.Close()
	s.restartStreamingNode()
	queryNodes := s.Cluster.GetAllQueryNodes()
	s.Require().Len(queryNodes, 1)
	_ = queryNodes[0].Kill()
	s.Cluster.AddQueryNode()
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, c.name)
	s.waitForQueryRecovery(ctx, c)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
}

// The persisted path is independently testable before #52977 changes QueryNode
// startup. Live growing queries remain covered by the native runtime tests.
func (s *UpsertWALSuite) TestUpsertFormatPersistsThroughRealWAL() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	defer producer.Close()
	for _, marker := range []int64{1, 2} {
		_, err := producer.Append(ctx, c.upsert(11, marker))
		s.Require().NoError(err)
		s.flush(ctx, c)
	}
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, c.name)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.assertPhysicalRows(ctx, c, 2)
}

func (s *UpsertWALSuite) TestUpsertTransactionInterveningWriteAndKeyedReopen() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	begin, err := producer.Append(ctx, message.NewBeginTxnMessageBuilderV2().WithVChannel(c.vchannel).WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 600000}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable())
	s.Require().NoError(err)
	_, err = producer.Append(ctx, c.upsert(11, 2, "transaction-reopen").WithTxnContext(*begin.TxnCtx))
	s.Require().NoError(err)
	_, err = producer.Append(ctx, c.upsert(11, 3))
	s.Require().NoError(err)
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel(c.vchannel).WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(c.id, "transaction-reopen")).WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(*begin.TxnCtx)
	original, err := producer.Append(ctx, commit)
	s.Require().NoError(err)
	var result messagespb.IdempotentInsertResult
	s.Require().NoError(original.GetExtra(&result))
	s.Require().Equal([]int64{11}, result.GetIds().GetIntId().GetData())
	s.flush(ctx, c)
	producer.Close()
	s.restartStreamingNode(c)
	producer = rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	defer producer.Close()
	duplicate, err := producer.Append(ctx, commit)
	s.Require().NoError(err)
	s.Require().True(original.MessageID.EQ(duplicate.MessageID))
	s.Require().Equal(original.TimeTick, duplicate.TimeTick)
	s.Require().True(proto.Equal(original.Extra, duplicate.Extra))
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, c.name)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 3, 0)
	s.assertRows(ctx, c, 11, 2, 1)
}

// Check both public producers through WAL persistence. Live QueryView startup
// remains a separate deployment dependency.
func (s *UpsertWALSuite) TestPublicInsertAndUpsertPersist() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.create(ctx)
	fields := func(marker int64) []*schemapb.FieldData {
		body := message.MustAsMutableUpsertMessageV2(c.upsert(11, marker)).MustBody()
		request := upsertfixture.Insert(body)
		for _, column := range request.GetFieldsData() {
			for _, field := range c.schema.GetFields() {
				if column.GetFieldId() == field.GetFieldID() {
					column.FieldName = field.GetName()
				}
			}
		}
		return request.GetFieldsData()
	}
	inserted, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{CollectionName: c.name, NumRows: 1, FieldsData: fields(1)})
	s.Require().NoError(merr.CheckRPCCall(inserted, err))
	s.Require().Equal([]int64{11}, inserted.GetIDs().GetIntId().GetData())
	s.flush(ctx, c)
	updated, err := s.Cluster.MilvusClient.Upsert(ctx, &milvuspb.UpsertRequest{CollectionName: c.name, NumRows: 1, FieldsData: fields(2)})
	s.Require().NoError(merr.CheckRPCCall(updated, err))
	s.Require().Equal([]int64{11}, updated.GetIDs().GetIntId().GetData())
	s.flush(ctx, c)
	status, err := s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, c.name)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.assertPhysicalRows(ctx, c, 2)
}
