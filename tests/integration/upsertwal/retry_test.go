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
	"os"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/proto"

	walpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/failpoint"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func (s *UpsertWALSuite) TestSequentialKeyedUpsertResultsSurviveReopen() {
	s.sequentialKeyedUpsertResults()
}

func (s *UpsertWALCASDisabledSuite) TestSequentialKeyedUpsertResultsSurviveReopen() {
	s.sequentialKeyedUpsertResults()
}

func (s *upsertWALFixture) sequentialKeyedUpsertResults() {
	for _, mixed := range []bool{false, true} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		c := s.create(ctx)
		producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
		for _, pk := range []int64{11, 12} {
			_, err := producer.Append(ctx, c.upsert(pk, 1))
			s.Require().NoError(err)
		}
		begin, err := producer.Append(ctx, message.NewBeginTxnMessageBuilderV2().WithVChannel(c.vchannel).
			WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 600000}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable())
		s.Require().NoError(err)
		// Each Append returns successfully before the next body or Commit is sent.
		_, err = producer.Append(ctx, c.upsert(11, 2, "sequential-keyed").WithTxnContext(*begin.TxnCtx))
		s.Require().NoError(err)
		second := c.upsert(12, 3, "sequential-keyed")
		typed := message.MustAsMutableUpsertMessageV2(second)
		resultBody := typed.MustBody()
		resultBody.Rows.RequestRowMapping.RowOffsets = []uint32{1}
		typed.OverwriteBody(resultBody)
		if mixed {
			second = c.legacy("Insert", 12, 3)
			insert := message.MustAsMutableInsertMessageV1(second)
			header := insert.Header()
			message.SetInsertHeaderIdempotentInsertResult(header, &messagespb.IdempotentInsertResult{Ids: resultBody.Rows.PrimaryKeys, RowOffsets: resultBody.Rows.RequestRowMapping.RowOffsets})
			insert.OverwriteHeader(header)
		}
		_, err = producer.Append(ctx, second.WithTxnContext(*begin.TxnCtx))
		s.Require().NoError(err)
		commit := message.NewCommitTxnMessageBuilderV2().WithVChannel(c.vchannel).
			WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(c.id, "sequential-keyed")).
			WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 2}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(*begin.TxnCtx)
		original, err := producer.Append(ctx, commit)
		s.Require().NoError(err)
		var result messagespb.IdempotentInsertResult
		s.Require().NoError(original.GetExtra(&result))
		s.Require().Equal([]int64{11, 12}, result.GetIds().GetIntId().GetData())
		s.Require().Equal([]uint32{0, 1}, result.GetRowOffsets())
		checkDuplicate := func(retried *types.AppendResult, err error) {
			s.Require().NoError(err)
			s.Require().True(original.MessageID.EQ(retried.MessageID))
			s.Require().Equal(original.TimeTick, retried.TimeTick)
			s.Require().True(original.LastConfirmedMessageID.EQ(retried.LastConfirmedMessageID))
			s.Require().True(proto.Equal(original.Extra, retried.Extra))
		}
		checkDuplicate(producer.Append(ctx, message.CloneMutableMessage(commit)))
		s.assertRows(ctx, c, 11, 1, 0)
		s.assertRows(ctx, c, 11, 2, 1)
		if mixed {
			s.assertRows(ctx, c, 12, 1, 1)
		} else {
			s.assertRows(ctx, c, 12, 1, 0)
		}
		s.assertRows(ctx, c, 12, 3, 1)
		producer.Close()
		s.restartStreamingNode(c)
		producer = rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
		checkDuplicate(producer.Append(ctx, message.CloneMutableMessage(commit)))
		s.assertRows(ctx, c, 11, 1, 0)
		s.assertRows(ctx, c, 11, 2, 1)
		s.assertRows(ctx, c, 12, 3, 1)
		s.assertPhysicalRows(ctx, c, 4)
	}
}

func (s *UpsertWALSuite) TestPendingKeyedUpsertUsesExistingRebuildAfterReopen() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	beginTxn := func() *types.AppendResult {
		begin, err := producer.Append(ctx, message.NewBeginTxnMessageBuilderV2().WithVChannel(c.vchannel).
			WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 600000}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable())
		s.Require().NoError(err)
		return begin
	}
	commitTxn := func(txnCtx message.TxnContext) (*types.AppendResult, error) {
		return producer.Append(ctx, message.NewCommitTxnMessageBuilderV2().WithVChannel(c.vchannel).
			WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(c.id, "pending-keyed")).
			WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(txnCtx))
	}
	first := beginTxn()
	_, err = producer.Append(ctx, c.upsert(11, 2, "pending-keyed").WithTxnContext(*first.TxnCtx))
	s.Require().NoError(err)
	producer.Close()
	s.restartStreamingNode(c)
	producer = rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err = commitTxn(*first.TxnCtx)
	s.Require().Error(err)
	s.Require().True(status.AsStreamingError(err).IsTxnExpired())
	s.assertRows(ctx, c, 11, 1, 1)
	s.assertRows(ctx, c, 11, 2, 0)
	rebuilt := beginTxn()
	s.Require().NotEqual(first.TxnCtx.TxnID, rebuilt.TxnCtx.TxnID)
	_, err = producer.Append(ctx, c.upsert(11, 2, "pending-keyed").WithTxnContext(*rebuilt.TxnCtx))
	s.Require().NoError(err)
	original, err := commitTxn(*rebuilt.TxnCtx)
	s.Require().NoError(err)
	var result messagespb.IdempotentInsertResult
	s.Require().NoError(original.GetExtra(&result))
	s.Require().Equal([]int64{11}, result.GetIds().GetIntId().GetData())
	retried, err := commitTxn(*rebuilt.TxnCtx)
	s.Require().NoError(err)
	s.Require().True(original.MessageID.EQ(retried.MessageID))
	s.Require().True(proto.Equal(original.Extra, retried.Extra))
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.assertPhysicalRows(ctx, c, 2)
}

func (s *UpsertWALSuite) TestRequestMappingAndInvalidMetadata() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	invalid := c.upsert(11, 999, "invalid-metadata")
	typed := message.MustAsMutableUpsertMessageV2(invalid)
	body := typed.MustBody()
	body.Rows.RequestRowMapping.RowOffsets = []uint32{0, 1}
	typed.OverwriteBody(body)
	_, err = producer.Append(ctx, invalid)
	s.Require().Error(err)
	s.assertRows(ctx, c, 11, 1, 1)
	s.assertRows(ctx, c, 11, 999, 0)

	// A complete mapping reconstructs the original result after reopen.
	write := c.upsert(11, 2, "mapped-result")
	original, err := producer.Append(ctx, write)
	s.Require().NoError(err)
	s.Require().NotNil(original.Extra)
	check := func() {
		retry, err := producer.Append(ctx, c.upsert(99, 999, "mapped-result"))
		s.Require().NoError(err)
		s.Require().True(original.MessageID.EQ(retry.MessageID))
		s.Require().Equal(original.TimeTick, retry.TimeTick)
		s.Require().True(proto.Equal(original.Extra, retry.Extra), "retry must return the original keys")
		s.assertRows(ctx, c, 11, 1, 0)
		s.assertRows(ctx, c, 11, 2, 1)
		s.assertRows(ctx, c, 99, 999, 0)
	}
	check()
	producer.Close()
	s.restartStreamingNode(c)
	producer = rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	check()
	s.assertPhysicalRows(ctx, c, 2)
}

func (s *UpsertWALSuite) TestUnknownAppendOutcomeRecoversOriginalResult() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	dir, err := os.MkdirTemp(s.controlDir, "unknown-")
	s.Require().NoError(err)
	nodes := s.Cluster.GetAllStreamingNodes()
	s.Require().Len(nodes, 1)
	key := message.NewCollectionScopedIdempotencyKey(c.id, "unknown")
	config := walpoint.Config{PID: nodes[0].PID(), Key: string(key), VChannel: c.vchannel, MessageType: message.MessageTypeUpsert.String(), Action: "pause", Directory: dir}
	payload, err := json.Marshal(config)
	s.Require().NoError(err)
	configPath := filepath.Join(s.controlDir, "backend.json")
	s.Require().NoError(os.WriteFile(configPath+".tmp", payload, 0o600))
	s.Require().NoError(os.Rename(configPath+".tmp", configPath))
	defer os.Remove(configPath)
	completed := make(chan error, 1)
	go func() { _, err := producer.Append(ctx, c.upsert(11, 2, "unknown")); completed <- err }()
	var receipt walpoint.Receipt
	s.Require().Eventually(func() bool {
		files, err := filepath.Glob(filepath.Join(dir, "*.json"))
		if err != nil || len(files) != 1 {
			return false
		}
		data, err := os.ReadFile(files[0])
		return err == nil && json.Unmarshal(data, &receipt) == nil && receipt.MessageID != "" && receipt.TimeTick > 0
	}, time.Minute, 10*time.Millisecond, "the selected Upsert must actually persist before its acknowledgement is lost")
	s.Require().Equal(nodes[0].PID(), receipt.PID)
	s.Require().Equal(string(key), receipt.Key)
	select {
	case err := <-completed:
		s.FailNow("append replied before the backend barrier was released", "%v", err)
	default:
	}
	s.restartStreamingNode()
	s.Require().Error(<-completed)
	producer = rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	result, err := producer.Append(ctx, c.upsert(99, 999, "unknown"))
	s.Require().NoError(err)
	s.Require().Equal(receipt.MessageID, result.MessageID.String())
	s.Require().Equal(receipt.TimeTick, result.TimeTick)
	var original messagespb.IdempotentInsertResult
	s.Require().NoError(result.GetExtra(&original))
	s.Require().Equal([]int64{11}, original.GetIds().GetIntId().GetData())
	s.waitForQueryRecovery(ctx, c)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.assertRows(ctx, c, 99, 999, 0)
	s.assertPhysicalRows(ctx, c, 2)
}

func (s *UpsertWALSuite) TestResentBodyCountMismatchRebuildsTransaction() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	streamCtx, disconnect := context.WithCancel(ctx)
	defer disconnect()
	producer := rawProducer(s.T(), streamCtx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	begin := func() *types.AppendResult {
		result, err := producer.Append(ctx, message.NewBeginTxnMessageBuilderV2().WithVChannel(c.vchannel).WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 600000}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable())
		s.Require().NoError(err)
		s.Require().NotNil(result.TxnCtx)
		return result
	}
	first := begin()
	dir := s.T().TempDir()
	nodes := s.Cluster.GetAllStreamingNodes()
	s.Require().Len(nodes, 1)
	data, err := json.Marshal(walpoint.Config{PID: nodes[0].PID(), VChannel: c.vchannel, MessageType: message.MessageTypeUpsert.String(), Action: "pause", Directory: dir})
	s.Require().NoError(err)
	configPath := filepath.Join(s.controlDir, "backend.json")
	s.Require().NoError(os.WriteFile(configPath, data, 0o600))
	defer os.Remove(configPath)
	defer os.WriteFile(filepath.Join(dir, "release"), nil, 0o600)
	body := c.upsert(11, 2).WithTxnContext(*first.TxnCtx)
	done := make(chan error, 1)
	go func() { _, err := producer.Append(ctx, body); done <- err }()
	var receipt walpoint.Receipt
	s.Require().Eventually(func() bool {
		files, err := filepath.Glob(filepath.Join(dir, "*.json"))
		if err != nil || len(files) != 1 {
			return false
		}
		data, err := os.ReadFile(files[0])
		return err == nil && json.Unmarshal(data, &receipt) == nil && receipt.MessageID != "" && receipt.TimeTick > 0
	}, time.Minute, 10*time.Millisecond, "the first body must be durable before its response is lost")
	s.Require().Equal(nodes[0].PID(), receipt.PID)
	disconnect()
	select {
	case err := <-done:
		s.Require().Error(err)
	case <-time.After(10 * time.Second):
		s.FailNow("disconnected Produce did not return")
	}
	s.Require().NoError(os.WriteFile(filepath.Join(dir, "release"), nil, 0o600))
	producer = rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	resent, err := producer.Append(ctx, message.CloneMutableMessage(body))
	s.Require().NoError(err)
	s.Require().NotEqual(receipt.MessageID, resent.MessageID.String(), "the same body was actually appended again")
	commit := func(txn message.TxnContext) (*types.AppendResult, error) {
		return producer.Append(ctx, message.NewCommitTxnMessageBuilderV2().WithVChannel(c.vchannel).WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(txn))
	}
	_, err = commit(*first.TxnCtx)
	s.Require().Error(err)
	s.Require().True(status.AsStreamingError(err).IsTxnExpired(), "%v", err)
	s.assertRows(ctx, c, 11, 1, 1)
	s.assertRows(ctx, c, 11, 2, 0)
	replacement := begin()
	s.Require().NotEqual(first.TxnCtx.TxnID, replacement.TxnCtx.TxnID)
	_, err = producer.Append(ctx, c.upsert(11, 2).WithTxnContext(*replacement.TxnCtx))
	s.Require().NoError(err)
	_, err = commit(*replacement.TxnCtx)
	s.Require().NoError(err)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.assertPhysicalRows(ctx, c, 2)
}

func (s *UpsertWALSuite) TestAutocommitReplacementAndRetry() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	first, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	s.Require().NotNil(first.MessageID)
	s.Require().Positive(first.TimeTick)
	s.assertRows(ctx, c, 11, 1, 1)
	newWrite := c.upsert(11, 2, "replace")
	replacement, err := producer.Append(ctx, newWrite)
	s.Require().NoError(err)
	s.Require().Greater(replacement.TimeTick, first.TimeTick)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	retry, err := producer.Append(ctx, c.upsert(11, 2, "replace"))
	s.Require().NoError(err)
	s.Require().True(retry.MessageID.EQ(replacement.MessageID))
	s.Require().Equal(replacement.TimeTick, retry.TimeTick)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.assertPhysicalRows(ctx, c, 2)
}
