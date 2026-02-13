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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
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
		return m.MessageType() == message.MessageTypeCommitTxn
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
	task := &ProduceGuard{producer: rp, r: res, msgs: []message.MutableMessage{msg1, msg2}}

	resAppend, err := task.commit(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, resAppend)
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
