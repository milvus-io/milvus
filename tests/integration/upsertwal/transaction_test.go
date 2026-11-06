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
	"strconv"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	upsertfixture "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func (c collection) legacy(kind string, pk, marker int64) message.MutableMessage {
	upsert := message.MustAsMutableUpsertMessageV2(c.upsert(pk, marker))
	if kind == "Upsert" {
		return c.upsert(pk, marker)
	}
	if kind == "Insert" {
		body := upsertfixture.Insert(upsert.MustBody())
		body.CollectionID = c.id
		body.Base = &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert}
		body.Timestamps = make([]uint64, body.NumRows)
		return message.NewInsertMessageBuilderV1().WithVChannel(c.vchannel).WithHeader(&message.InsertMessageHeader{CollectionId: c.id, SchemaVersion: upsert.Header().SchemaVersion, Partitions: upsert.Header().Partitions}).WithBody(body).MustBuildMutable()
	}
	body := upsertfixture.Delete(upsert.MustBody())
	body.CollectionID = c.id
	body.Base = &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete}
	body.Timestamps = make([]uint64, body.NumRows)
	return message.NewDeleteMessageBuilderV1().WithVChannel(c.vchannel).WithHeader(&message.DeleteMessageHeader{CollectionId: c.id, Rows: uint64(body.NumRows)}).WithBody(body).MustBuildMutable()
}

func (s *UpsertWALSuite) TestProxyInsertKeepsExistingDuplicateBehavior() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	for _, marker := range []int64{1, 2} {
		request := upsertfixture.Insert(message.MustAsMutableUpsertMessageV2(c.upsert(11, marker)).MustBody())
		result, err := s.Cluster.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{CollectionName: c.name, NumRows: 1, FieldsData: request.GetFieldsData()})
		s.Require().NoError(merr.CheckRPCCall(result, err))
	}
	s.assertRows(ctx, c, 11, 1, 1)
	s.assertRows(ctx, c, 11, 2, 1)
}

func (s *UpsertWALSuite) TestMixedTransactionOverlapsAreAccepted() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	for _, kind := range []string{"Insert", "Delete", "Upsert"} {
		for _, upsertFirst := range []bool{true, false} {
			_, err = producer.Append(ctx, c.upsert(11, 1))
			s.Require().NoError(err)
			begin, err := producer.Append(ctx, message.NewBeginTxnMessageBuilderV2().WithVChannel(c.vchannel).
				WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 60000}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable())
			s.Require().NoError(err)
			s.Require().NotNil(begin.TxnCtx)
			bodies := []message.MutableMessage{c.upsert(11, 2), c.legacy(kind, 11, 3)}
			if !upsertFirst {
				bodies[0], bodies[1] = bodies[1], bodies[0]
			}
			for _, body := range bodies {
				_, err = producer.Append(ctx, body.WithTxnContext(*begin.TxnCtx))
				s.Require().NoError(err)
			}
			_, err = producer.Append(ctx, message.NewCommitTxnMessageBuilderV2().WithVChannel(c.vchannel).
				WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 2}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(*begin.TxnCtx))
			s.Require().NoError(err, "overlap is accepted for kind=%s upsertFirst=%t", kind, upsertFirst)
			s.assertRows(ctx, c, 11, 1, 0)
			// Equal-TimeTick bodies do not define last-write-wins. Confirm that
			// the consumer reached the commit without selecting an ordering policy.
			s.waitForQueryRecovery(ctx, c)
		}
	}
}

func (s *UpsertWALSuite) TestExpiredTransactionPreservesOldRows() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	begin, err := producer.Append(ctx, message.NewBeginTxnMessageBuilderV2().WithVChannel(c.vchannel).
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 1000}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable())
	s.Require().NoError(err)
	s.Require().NotNil(begin.TxnCtx)
	body, err := producer.Append(ctx, c.upsert(11, 2).WithTxnContext(*begin.TxnCtx))
	s.Require().NoError(err)
	_, err = producer.Append(ctx, message.NewCommitTxnMessageBuilderV2().WithVChannel(c.vchannel).
		WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(*begin.TxnCtx).
		WithBarrierTimeTick(tsoutil.AddPhysicalDurationOnTs(body.TimeTick, begin.TxnCtx.Keepalive+time.Second)))
	s.Require().Error(err)
	s.Require().True(status.AsStreamingError(err).IsTxnExpired())
	s.assertRows(ctx, c, 11, 1, 1)
	s.assertRows(ctx, c, 11, 2, 0)
}

func (s *UpsertWALSuite) TestRawProduceRejectsForeignVChannelTransaction() {
	for _, recovery := range []bool{false, true} {
		s.Run("recovery_"+strconv.FormatBool(recovery), func() { s.rawProduceScope(recovery) })
	}
}

func (s *UpsertWALSuite) rawProduceScope(recovery bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	first := s.create(ctx)
	var second collection
	for attempt := 0; attempt < 8; attempt++ {
		candidate := s.create(ctx)
		if funcutil.ToPhysicalChannel(candidate.vchannel) == funcutil.ToPhysicalChannel(first.vchannel) {
			second = candidate
			break
		}
	}
	s.Require().NotEmpty(second.name, "the fixture must exercise two VChannels on the same PChannel")
	producer := rawProducer(s.T(), ctx, s.Cluster, first.vchannel)
	begin := func() message.TxnContext {
		result, err := producer.Append(ctx, message.NewBeginTxnMessageBuilderV2().WithVChannel(first.vchannel).
			WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 600000}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable())
		s.Require().NoError(err)
		s.Require().NotNil(result.TxnCtx)
		return *result.TxnCtx
	}
	txn := begin()
	_, err := producer.Append(ctx, second.upsert(11, 1).WithTxnContext(txn))
	s.Require().Error(err, "body scope must fail before session admission")
	s.Require().ErrorContains(err, "invalid vchannel scope")
	_, err = producer.Append(ctx, message.NewRollbackTxnMessageBuilderV2().WithVChannel(first.vchannel).
		WithHeader(&message.RollbackTxnMessageHeader{}).WithBody(&message.RollbackTxnMessageBody{}).MustBuildMutable().WithTxnContext(txn))
	s.Require().NoError(err)
	s.assertRows(ctx, first, 11, 1, 0)
	s.assertRows(ctx, second, 11, 1, 0)
	if recovery {
		txn = begin()
		_, err = producer.Append(ctx, first.upsert(11, 3).WithTxnContext(txn))
		s.Require().NoError(err)
		producer.Close()
		s.restartStreamingNode()
		producer = rawProducer(s.T(), ctx, s.Cluster, first.vchannel)
		_, err = producer.Append(ctx, message.NewCommitTxnMessageBuilderV2().WithVChannel(first.vchannel).WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(txn))
		s.Require().NoError(err)
		s.waitForQueryRecovery(ctx, first)
		s.assertRows(ctx, first, 11, 3, 1)
	}
}

func (s *UpsertWALSuite) TestRollbackAndOverlappingBodies() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	for _, rollback := range []bool{true, false} {
		begin, err := producer.Append(ctx, message.NewBeginTxnMessageBuilderV2().WithVChannel(c.vchannel).
			WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 60000}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable())
		s.Require().NoError(err)
		s.Require().NotNil(begin.TxnCtx)
		_, err = producer.Append(ctx, c.upsert(11, 2).WithTxnContext(*begin.TxnCtx))
		s.Require().NoError(err)
		if rollback {
			_, err = producer.Append(ctx, message.NewRollbackTxnMessageBuilderV2().WithVChannel(c.vchannel).
				WithHeader(&message.RollbackTxnMessageHeader{}).WithBody(&message.RollbackTxnMessageBody{}).MustBuildMutable().WithTxnContext(*begin.TxnCtx))
			s.Require().NoError(err)
		} else {
			_, err = producer.Append(ctx, c.upsert(11, 3).WithTxnContext(*begin.TxnCtx))
			s.Require().NoError(err)
			_, err = producer.Append(ctx, message.NewCommitTxnMessageBuilderV2().WithVChannel(c.vchannel).
				WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 2}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(*begin.TxnCtx))
			s.Require().NoError(err)
		}
		if rollback {
			s.assertRows(ctx, c, 11, 1, 1)
			s.assertRows(ctx, c, 11, 2, 0)
			s.assertRows(ctx, c, 11, 3, 0)
		} else {
			s.assertRows(ctx, c, 11, 1, 0)
			s.waitForQueryRecovery(ctx, c)
		}
	}
}

func (s *UpsertWALSuite) TestPendingTransactionSurvivesWALReopen() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	begin, err := producer.Append(ctx, message.NewBeginTxnMessageBuilderV2().WithVChannel(c.vchannel).
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 600000}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable())
	s.Require().NoError(err)
	s.Require().NotNil(begin.TxnCtx)
	_, err = producer.Append(ctx, c.upsert(11, 2).WithTxnContext(*begin.TxnCtx))
	s.Require().NoError(err)
	producer.Close()
	s.restartStreamingNode()
	producer = rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err = producer.Append(ctx, message.NewCommitTxnMessageBuilderV2().WithVChannel(c.vchannel).
		WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(*begin.TxnCtx))
	s.Require().NoError(err)
	s.waitForQueryRecovery(ctx, c)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
}

func (s *UpsertWALSuite) TestTransactionReplacesInterveningWrite() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	begin, err := producer.Append(ctx, message.NewBeginTxnMessageBuilderV2().WithVChannel(c.vchannel).
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 60000}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable())
	s.Require().NoError(err)
	s.Require().NotNil(begin.TxnCtx)
	_, err = producer.Append(ctx, c.upsert(11, 3).WithTxnContext(*begin.TxnCtx))
	s.Require().NoError(err)
	s.assertRows(ctx, c, 11, 3, 0)
	intervening, err := producer.Append(ctx, c.upsert(11, 2))
	s.Require().NoError(err)
	s.assertRows(ctx, c, 11, 2, 1)
	commit, err := producer.Append(ctx, message.NewCommitTxnMessageBuilderV2().WithVChannel(c.vchannel).
		WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(*begin.TxnCtx))
	s.Require().NoError(err)
	s.Require().Greater(commit.TimeTick, intervening.TimeTick)
	s.assertRows(ctx, c, 11, 2, 0)
	s.assertRows(ctx, c, 11, 3, 1)
}
