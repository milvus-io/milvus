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

package producer

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestBatchCommitProduce(t *testing.T) {
	t.Run("EmptyTasks", func(t *testing.T) {
		resp := BatchCommitProduce(context.Background())
		assert.Equal(t, 0, len(resp.Responses))
	})

	t.Run("SingleTask_Success", func(t *testing.T) {
		p := mock_producer.NewMockProducer(t)
		p.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{}, nil)
		p.EXPECT().Available().Return(make(chan struct{})).Maybe()
		p.EXPECT().IsAvailable().Return(true).Maybe()
		p.EXPECT().Close().Return().Maybe()

		rp := NewResumableProducer(func(ctx context.Context, opts *handler.ProducerOptions) (producer.Producer, error) {
			return p, nil
		}, &ProducerOptions{PChannel: "test"})
		defer rp.Close()

		msg := createRealInsertMessage(t, "test-v")

		limiter := rate.NewLimiter(rate.Inf, 100)
		res := limiter.ReserveN(time.Now(), msg.EstimateSize())
		task := &ProduceGuard{producer: rp, r: res, msgs: []message.MutableMessage{msg}}

		resp := BatchCommitProduce(context.Background(), task)
		assert.Equal(t, 1, len(resp.Responses))
		assert.NoError(t, resp.UnwrapFirstError())
	})

	t.Run("MultipleTasks_Success", func(t *testing.T) {
		p := mock_producer.NewMockProducer(t)
		p.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{}, nil).Twice()
		p.EXPECT().Available().Return(make(chan struct{})).Maybe()
		p.EXPECT().IsAvailable().Return(true).Maybe()
		p.EXPECT().Close().Return().Maybe()

		rp := NewResumableProducer(func(ctx context.Context, opts *handler.ProducerOptions) (producer.Producer, error) {
			return p, nil
		}, &ProducerOptions{PChannel: "test"})
		defer rp.Close()

		msg1 := createRealInsertMessage(t, "test-v")
		msg2 := createRealInsertMessage(t, "test-v")

		limiter := rate.NewLimiter(rate.Inf, 200)
		res1 := limiter.ReserveN(time.Now(), msg1.EstimateSize())
		res2 := limiter.ReserveN(time.Now(), msg2.EstimateSize())

		task1 := &ProduceGuard{producer: rp, r: res1, msgs: []message.MutableMessage{msg1}}
		task2 := &ProduceGuard{producer: rp, r: res2, msgs: []message.MutableMessage{msg2}}

		resp := BatchCommitProduce(context.Background(), task1, task2)
		assert.Equal(t, 2, len(resp.Responses))
		assert.NoError(t, resp.UnwrapFirstError())
	})

	t.Run("SingleTask_CommitError", func(t *testing.T) {
		p := mock_producer.NewMockProducer(t)
		p.EXPECT().Append(mock.Anything, mock.Anything).Return(nil, status.NewUnrecoverableError("unrecoverable"))
		p.EXPECT().Available().Return(make(chan struct{})).Maybe()
		p.EXPECT().IsAvailable().Return(true).Maybe()
		p.EXPECT().Close().Return().Maybe()

		rp := NewResumableProducer(func(ctx context.Context, opts *handler.ProducerOptions) (producer.Producer, error) {
			return p, nil
		}, &ProducerOptions{PChannel: "test"})
		defer rp.Close()

		msg := createRealInsertMessage(t, "test-v")

		limiter := rate.NewLimiter(rate.Inf, 100)
		res := limiter.ReserveN(time.Now(), msg.EstimateSize())
		task := &ProduceGuard{producer: rp, r: res, msgs: []message.MutableMessage{msg}}

		resp := BatchCommitProduce(context.Background(), task)
		assert.Equal(t, 1, len(resp.Responses))
		assert.Error(t, resp.UnwrapFirstError())
	})
}

func TestWaitForReservationOK(t *testing.T) {
	t.Run("Immediate", func(t *testing.T) {
		limiter := rate.NewLimiter(rate.Inf, 100)
		res := limiter.ReserveN(time.Now(), 10)
		task := &ProduceGuard{r: res}
		err := waitForReservationOK(context.Background(), task)
		assert.NoError(t, err)
	})

	t.Run("WithDelay", func(t *testing.T) {
		limiter := rate.NewLimiter(10, 10)
		// Consume all burst
		limiter.ReserveN(time.Now(), 10)

		// This one should have delay
		res := limiter.ReserveN(time.Now(), 10)
		task := &ProduceGuard{
			r:        res,
			producer: &ResumableProducer{opts: &ProducerOptions{PChannel: "test"}},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		start := time.Now()
		err := waitForReservationOK(ctx, task)
		assert.NoError(t, err)
		assert.True(t, time.Since(start) >= 1*time.Second)
	})

	t.Run("ContextCanceled", func(t *testing.T) {
		limiter := rate.NewLimiter(1, 1)
		limiter.ReserveN(time.Now(), 1)
		res := limiter.ReserveN(time.Now(), 1)
		task := &ProduceGuard{r: res}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := waitForReservationOK(ctx, task)
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})
}

func TestProduceGuard_Cancel(t *testing.T) {
	limiter := rate.NewLimiter(1, 1)
	res := limiter.ReserveN(time.Now(), 1)
	task := &ProduceGuard{r: res}
	task.Cancel()
}

func TestProduceGuard_Commit_NoMessages(t *testing.T) {
	task := &ProduceGuard{msgs: []message.MutableMessage{}}
	assert.Panics(t, func() {
		task.commit(context.Background())
	})
}

func TestProduceGuardCommitPartialUpdateSingleMessageUsesTxn(t *testing.T) {
	rp := &ResumableProducer{opts: &ProducerOptions{PChannel: "p1"}}

	msg := createRealPartialUpdateInsertMessage(t, "test-v")

	var (
		mu           sync.Mutex
		seen         []message.MessageType
		bodyTxn      *message.TxnContext
		commitMsg    message.MutableMessage
		beginTxnCtx  = &message.TxnContext{TxnID: 1, Keepalive: time.Second}
		producePatch = mockey.Mock((*ResumableProducer).produceInternal).To(
			func(_ *ResumableProducer, _ context.Context, m message.MutableMessage) (*types.AppendResult, error) {
				mu.Lock()
				defer mu.Unlock()

				seen = append(seen, m.MessageType())
				switch m.MessageType() {
				case message.MessageTypeBeginTxn:
					return &types.AppendResult{TxnCtx: beginTxnCtx}, nil
				case message.MessageTypeInsert:
					bodyTxn = m.TxnContext()
					return &types.AppendResult{TxnCtx: m.TxnContext()}, nil
				case message.MessageTypeCommitTxn:
					commitMsg = m
					return &types.AppendResult{TxnCtx: m.TxnContext()}, nil
				default:
					return &types.AppendResult{}, nil
				}
			}).Build()
	)
	defer producePatch.UnPatch()

	guard := &ProduceGuard{producer: rp, msgs: []message.MutableMessage{msg}}
	result, err := guard.commit(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, []message.MessageType{
		message.MessageTypeBeginTxn,
		message.MessageTypeInsert,
		message.MessageTypeCommitTxn,
	}, seen)
	require.NotNil(t, bodyTxn)
	assert.Equal(t, beginTxnCtx.TxnID, bodyTxn.TxnID)
	require.NotNil(t, commitMsg)
	require.True(t, message.HasPartialUpdateCAS(commitMsg))
	marker, ok := commitMsg.Properties().Get("_puc")
	require.True(t, ok)
	require.Empty(t, marker)
}

func TestProduceGuardPartialUpdateTxnExpiredReturnsCASRetry(t *testing.T) {
	msg := createRealPartialUpdateInsertMessage(t, "test-v")
	guard := &ProduceGuard{
		producer: &ResumableProducer{opts: &ProducerOptions{PChannel: "p1"}},
		msgs:     []message.MutableMessage{msg},
	}

	attempts := 0
	m := mockey.Mock((*ProduceGuard).produceWithTxnOnce).To(
		func(*ProduceGuard, context.Context, ...message.MutableMessage) (*types.AppendResult, error) {
			attempts++
			if attempts == 1 {
				return nil, status.NewTransactionExpired("expired")
			}
			return &types.AppendResult{}, nil
		},
	).Build()
	defer m.UnPatch()

	_, err := guard.produceTxn(context.Background(), msg)

	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsPartialUpdateRetryableCAS())
	require.Equal(t, 1, attempts)
}

func TestProduceGuardOrdinaryTxnExpiredReplaysWholeTransaction(t *testing.T) {
	msgs := []message.MutableMessage{
		createRealInsertMessage(t, "test-v"),
		createRealInsertMessage(t, "test-v"),
	}
	guard := &ProduceGuard{
		producer: &ResumableProducer{opts: &ProducerOptions{PChannel: "p1"}},
		msgs:     msgs,
	}

	var (
		mu        sync.Mutex
		beginIDs  []message.TxnID
		bodyCount = make(map[message.TxnID]int)
		commitIDs []message.TxnID
	)
	nextTxnID := message.TxnID(1)
	producePatch := mockey.Mock((*ResumableProducer).produceInternal).To(
		func(_ *ResumableProducer, _ context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
			mu.Lock()
			defer mu.Unlock()

			switch msg.MessageType() {
			case message.MessageTypeBeginTxn:
				txnCtx := &message.TxnContext{TxnID: nextTxnID, Keepalive: time.Second}
				nextTxnID++
				beginIDs = append(beginIDs, txnCtx.TxnID)
				return &types.AppendResult{TxnCtx: txnCtx}, nil
			case message.MessageTypeCommitTxn:
				txnID := msg.TxnContext().TxnID
				commitIDs = append(commitIDs, txnID)
				if txnID == 1 {
					return nil, status.NewTransactionExpired("expired")
				}
				return &types.AppendResult{TxnCtx: msg.TxnContext()}, nil
			default:
				txnID := msg.TxnContext().TxnID
				bodyCount[txnID]++
				return &types.AppendResult{TxnCtx: msg.TxnContext()}, nil
			}
		},
	).Build()
	defer producePatch.UnPatch()

	result, err := guard.produceTxn(context.Background(), msgs...)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, []message.TxnID{1, 2}, beginIDs)
	require.Equal(t, []message.TxnID{1, 2}, commitIDs)
	require.Equal(t, len(msgs), bodyCount[1])
	require.Equal(t, len(msgs), bodyCount[2])
}

func TestProduceGuardCommitExistingPartialUpdateTxnIsNotRewrapped(t *testing.T) {
	rp := &ResumableProducer{opts: &ProducerOptions{PChannel: "p1"}}
	txnCtx := message.TxnContext{TxnID: 7, Keepalive: time.Second}
	replicateHeader := &message.ReplicateHeader{
		ClusterID:              "primary",
		MessageID:              walimplstest.NewTestMessageID(10),
		LastConfirmedMessageID: walimplstest.NewTestMessageID(9),
		TimeTick:               100,
		VChannel:               "test-v",
	}

	for _, tc := range []struct {
		name      string
		replicate bool
	}{
		{name: "local transactional message"},
		{name: "replicated transactional message", replicate: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := createRealPartialUpdateInsertMessage(t, "test-v").WithTxnContext(txnCtx)
			if tc.replicate {
				msg = msg.WithReplicateHeader(replicateHeader)
			}

			var seen []message.MutableMessage
			producePatch := mockey.Mock((*ResumableProducer).produceInternal).To(
				func(_ *ResumableProducer, _ context.Context, m message.MutableMessage) (*types.AppendResult, error) {
					seen = append(seen, m)
					if m.MessageType() == message.MessageTypeBeginTxn {
						return &types.AppendResult{TxnCtx: &message.TxnContext{TxnID: 99, Keepalive: time.Second}}, nil
					}
					return &types.AppendResult{TxnCtx: m.TxnContext()}, nil
				}).Build()
			defer producePatch.UnPatch()

			guard := &ProduceGuard{producer: rp, msgs: []message.MutableMessage{msg}}
			_, err := guard.commit(context.Background())
			require.NoError(t, err)
			require.Len(t, seen, 1)
			require.Equal(t, message.MessageTypeInsert, seen[0].MessageType())
			require.Equal(t, txnCtx.TxnID, seen[0].TxnContext().TxnID)
			if tc.replicate {
				require.Equal(t, replicateHeader.ClusterID, seen[0].ReplicateHeader().ClusterID)
			} else {
				require.Nil(t, seen[0].ReplicateHeader())
			}
		})
	}
}

func TestProduceGuardCommitOrdinarySingleMessageAutoCommit(t *testing.T) {
	rp := &ResumableProducer{opts: &ProducerOptions{PChannel: "p1"}}
	msg := createRealInsertMessage(t, "test-v")

	var seen []message.MessageType
	producePatch := mockey.Mock((*ResumableProducer).produceInternal).To(
		func(_ *ResumableProducer, _ context.Context, m message.MutableMessage) (*types.AppendResult, error) {
			seen = append(seen, m.MessageType())
			return &types.AppendResult{}, nil
		}).Build()
	defer producePatch.UnPatch()

	guard := &ProduceGuard{producer: rp, msgs: []message.MutableMessage{msg}}
	result, err := guard.commit(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, []message.MessageType{message.MessageTypeInsert}, seen)
}

func TestCommitTxnReturnsMarkerError(t *testing.T) {
	expected := status.NewUnrecoverableError("mark commit failed")
	patch := mockey.Mock(message.MarkPartialUpdateCASCommit).Return(expected).Build()
	defer patch.UnPatch()

	guard := &ProduceGuard{}
	result, err := guard.commitTxn(
		context.Background(),
		"test-v",
		&message.TxnContext{TxnID: 1},
		true,
	)
	require.Nil(t, result)
	require.ErrorIs(t, err, expected)
}

func TestBatchCommitProduce_ReservationError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	limiter := rate.NewLimiter(1, 1)
	limiter.ReserveN(time.Now(), 1)
	res := limiter.ReserveN(time.Now(), 1)

	msg := createRealInsertMessage(t, "test-v")
	task := &ProduceGuard{r: res, msgs: []message.MutableMessage{msg}}

	resp := BatchCommitProduce(ctx, task)
	assert.Equal(t, 1, len(resp.Responses))
	err := resp.UnwrapFirstError()
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestProduceGuard_Commit_Txn(t *testing.T) {
	p := mock_producer.NewMockProducer(t)

	msg1 := createRealInsertMessage(t, "test-v")
	msg2 := createRealInsertMessage(t, "test-v")

	// 1. beginTxn
	p.EXPECT().Append(mock.Anything, mock.MatchedBy(func(m message.MutableMessage) bool {
		return m.MessageType() == message.MessageTypeBeginTxn
	})).Return(&types.AppendResult{TxnCtx: &message.TxnContext{}}, nil)

	// 2. appendTxnBody
	p.EXPECT().Append(mock.Anything, mock.MatchedBy(func(m message.MutableMessage) bool {
		return m.MessageType() != message.MessageTypeBeginTxn && m.MessageType() != message.MessageTypeCommitTxn
	})).Return(&types.AppendResult{}, nil).Twice()

	// 3. commitTxn
	p.EXPECT().Append(mock.Anything, mock.MatchedBy(func(m message.MutableMessage) bool {
		if m.MessageType() != message.MessageTypeCommitTxn {
			return false
		}
		return message.IdempotencyKeyOf(m) == "key-1"
	})).Return(&types.AppendResult{}, nil)
	p.EXPECT().Available().Return(make(chan struct{})).Maybe()
	p.EXPECT().IsAvailable().Return(true).Maybe()
	p.EXPECT().Close().Return().Maybe()

	rp := NewResumableProducer(func(ctx context.Context, opts *handler.ProducerOptions) (producer.Producer, error) {
		return p, nil
	}, &ProducerOptions{PChannel: "test"})
	defer rp.Close()

	limiter := rate.NewLimiter(rate.Inf, 200)
	res := limiter.ReserveN(time.Now(), msg1.EstimateSize()+msg2.EstimateSize())
	task := &ProduceGuard{producer: rp, r: res, msgs: []message.MutableMessage{msg1, msg2}, opts: []ProduceOption{{IdempotencyKey: "key-1"}}}

	resAppend, err := task.commit(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, resAppend)
}

func TestProduceGuard_Commit_SingleMessageKeepsProxyIdempotencyKey(t *testing.T) {
	// The proxy single-sources the idempotency key of a single insert onto the
	// message property. The producer must not stamp it again on the autocommit
	// path: only the commit-txn message it synthesizes itself needs the key.
	p := mock_producer.NewMockProducer(t)
	msg := createRealInsertMessage(t, "test-v")
	require.Empty(t, message.IdempotencyKeyOf(msg))

	p.EXPECT().Append(mock.Anything, mock.MatchedBy(func(m message.MutableMessage) bool {
		return m.MessageType() == message.MessageTypeInsert && message.IdempotencyKeyOf(m) == ""
	})).Return(&types.AppendResult{}, nil)
	p.EXPECT().Available().Return(make(chan struct{})).Maybe()
	p.EXPECT().IsAvailable().Return(true).Maybe()
	p.EXPECT().Close().Return().Maybe()

	rp := NewResumableProducer(func(ctx context.Context, opts *handler.ProducerOptions) (producer.Producer, error) {
		return p, nil
	}, &ProducerOptions{PChannel: "test"})
	defer rp.Close()

	limiter := rate.NewLimiter(rate.Inf, 200)
	res := limiter.ReserveN(time.Now(), msg.EstimateSize())
	task := &ProduceGuard{producer: rp, r: res, msgs: []message.MutableMessage{msg}, opts: []ProduceOption{{IdempotencyKey: "key-1"}}}

	resAppend, err := task.commit(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, resAppend)
}

func TestIdempotencyKeyFromProduceOptions(t *testing.T) {
	assert.Empty(t, idempotencyKeyFromProduceOptions())
	assert.Empty(t, idempotencyKeyFromProduceOptions(ProduceOption{}))
	assert.Equal(t, "key-1", idempotencyKeyFromProduceOptions(ProduceOption{IdempotencyKey: "key-1"}))
	// The last non-empty option wins.
	assert.Equal(t, "key-2", idempotencyKeyFromProduceOptions(
		ProduceOption{IdempotencyKey: "key-1"},
		ProduceOption{IdempotencyKey: "key-2"},
	))

	// An empty key must not materialize the property at all: a keyless commit txn
	// message must carry no idempotency property, not an empty-valued one.
	msg := message.NewCommitTxnMessageBuilderV2().
		WithVChannel("test-v").
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		WithIdempotencyKey(idempotencyKeyFromProduceOptions()).
		MustBuildMutable()
	assert.Empty(t, message.IdempotencyKeyOf(msg))
	assert.NotContains(t, msg.Properties().ToRawMap(), "_ik")
}

func TestProduceGuard_Commit_TxnExpiredRetry(t *testing.T) {
	p := mock_producer.NewMockProducer(t)

	msg1 := createRealInsertMessage(t, "test-v")

	// First attempt fails with TxnExpired
	p.EXPECT().Append(mock.Anything, mock.Anything).Return(nil, status.NewTransactionExpired("expired"))
	p.EXPECT().Available().Return(make(chan struct{})).Maybe()
	p.EXPECT().IsAvailable().Return(true).Maybe()
	p.EXPECT().Close().Return().Maybe()

	rp := NewResumableProducer(func(ctx context.Context, opts *handler.ProducerOptions) (producer.Producer, error) {
		return p, nil
	}, &ProducerOptions{PChannel: "test"})
	defer rp.Close()

	limiter := rate.NewLimiter(rate.Inf, 200)
	res := limiter.ReserveN(time.Now(), msg1.EstimateSize()*2)
	task := &ProduceGuard{producer: rp, r: res, msgs: []message.MutableMessage{msg1, msg1}} // two messages to trigger txn

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := task.commit(ctx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestProduceGuard_Commit_Error(t *testing.T) {
	p := mock_producer.NewMockProducer(t)
	p.EXPECT().Append(mock.Anything, mock.Anything).Return(nil, status.NewUnrecoverableError("unrecoverable"))
	p.EXPECT().Available().Return(make(chan struct{})).Maybe()
	p.EXPECT().IsAvailable().Return(true).Maybe()
	p.EXPECT().Close().Return().Maybe()

	rp := NewResumableProducer(func(ctx context.Context, opts *handler.ProducerOptions) (producer.Producer, error) {
		return p, nil
	}, &ProducerOptions{PChannel: "test"})
	defer rp.Close()

	msg1 := createRealInsertMessage(t, "test-v")

	limiter := rate.NewLimiter(rate.Inf, 100)
	res := limiter.ReserveN(time.Now(), msg1.EstimateSize())
	task := &ProduceGuard{producer: rp, r: res, msgs: []message.MutableMessage{msg1}}

	resAppend, err := task.commit(context.Background())
	assert.Error(t, err)
	assert.Nil(t, resAppend)
}

func validProducerPartialUpdateCAS() *messagespb.PartialUpdateCAS {
	return &messagespb.PartialUpdateCAS{
		ReadTs:               100,
		ObservedPchannelTerm: 2,
	}
}

func createRealPartialUpdateInsertMessage(t *testing.T, vchannel string) message.MutableMessage {
	t.Helper()
	builder := message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{CollectionID: 1}).
		WithVChannel(vchannel)
	require.NoError(t, builder.AddPartialUpdateCAS(validProducerPartialUpdateCAS()))
	return builder.MustBuildMutable()
}
