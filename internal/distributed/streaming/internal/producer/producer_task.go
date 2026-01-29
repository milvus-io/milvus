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
	"time"

	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// BatchCommitProduce commits the produce tasks concurrently.
func BatchCommitProduce(ctx context.Context, tasks ...*ProduceGuard) types.AppendResponses {
	if len(tasks) == 0 {
		return types.NewAppendResponseN(0)
	}
	if err := waitForReservationOK(ctx, tasks...); err != nil {
		for _, task := range tasks {
			// return the quota of reservation to the limiter as much as possible.
			task.Cancel()
		}
		resp := types.NewAppendResponseN(len(tasks))
		resp.FillAllError(err)
		return resp
	}

	if len(tasks) == 1 {
		resp := types.NewAppendResponseN(1)
		appendResult, err := tasks[0].commit(ctx)
		resp.FillResponseAtIdx(types.AppendResponse{
			AppendResult: appendResult,
			Error:        err,
		}, 0)
		return resp
	}

	wg := sync.WaitGroup{}
	wg.Add(len(tasks))
	mu := sync.Mutex{}
	resp := types.NewAppendResponseN(len(tasks))
	for i, task := range tasks {
		go func(i int, task *ProduceGuard) (struct{}, error) {
			defer wg.Done()
			appendResult, err := task.commit(ctx)
			mu.Lock()
			resp.FillResponseAtIdx(types.AppendResponse{
				AppendResult: appendResult,
				Error:        err,
			}, i)
			mu.Unlock()
			return struct{}{}, nil
		}(i, task)
	}
	wg.Wait()
	return resp
}

// waitForReservationOK waits for all reservations of tasks to be ready.
func waitForReservationOK(ctx context.Context, tasks ...*ProduceGuard) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	var maxDelay time.Duration
	var pchannel string
	now := time.Now()
	for _, task := range tasks {
		if task.r == nil {
			continue
		}
		if delay := task.r.DelayFrom(now); delay > maxDelay {
			maxDelay = delay
			pchannel = task.producer.opts.PChannel
		}
	}
	if maxDelay == 0 {
		// all reservations are OK now.
		return nil
	}

	// Record the rate limit delay
	metrics.StreamingServiceClientProduceRateLimitDelaySeconds.WithLabelValues(paramtable.GetStringNodeID(), pchannel).Observe(maxDelay.Seconds())

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(maxDelay):
		// all reservations are OK now.
		return nil
	}
}

type ProduceGuard struct {
	producer *ResumableProducer
	msgs     []message.MutableMessage
	r        *rate.Reservation
}

// commit commit the produce task.
func (g *ProduceGuard) commit(ctx context.Context) (*types.AppendResult, error) {
	if len(g.msgs) == 0 {
		panic("append task with no messages")
	}
	// auto commit if there's only one message.
	if len(g.msgs) == 1 {
		return g.producer.produceInternal(ctx, g.msgs[0])
	}
	// produce with transaction.
	return g.produceTxn(ctx, g.msgs...)
}

// produceTxn produces the messages with a transaction, retry if the transaction is expired.
func (g *ProduceGuard) produceTxn(ctx context.Context, msgs ...message.MutableMessage) (*types.AppendResult, error) {
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		result, err := g.produceWithTxnOnce(ctx, msgs...)
		if err := status.AsStreamingError(err); err != nil && err.IsTxnExpired() {
			// if the transaction is expired,
			// there may be wal is transferred to another streaming node,
			// retry it with new transaction.
			g.producer.Logger().Warn("transaction expired, retrying", zap.String("vchannel", msgs[0].VChannel()), zap.Error(err))
			continue
		}
		if err != nil {
			return nil, err
		}
		return result, err
	}
}

// produceWithTxnOnce produces the messages with a transaction once.
func (g *ProduceGuard) produceWithTxnOnce(ctx context.Context, msgs ...message.MutableMessage) (*types.AppendResult, error) {
	// a txn batch should always belong to one vchannel.
	txn, err := g.beginTxn(ctx, msgs[0].VChannel())
	if err != nil {
		return nil, err
	}
	if err := g.appendTxnBody(ctx, txn, msgs...); err != nil {
		return nil, err
	}
	return g.commitTxn(ctx, msgs[0].VChannel(), txn)
}

// beginTxn begins a new transaction.
func (g *ProduceGuard) beginTxn(ctx context.Context, vchannel string) (*message.TxnContext, error) {
	// Create a new transaction, send the begin txn message.
	beginTxn := message.NewBeginTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable()

	result, err := g.producer.produceInternal(ctx, beginTxn)
	if err != nil {
		return nil, err
	}
	return result.TxnCtx, nil
}

// appendTxnBody appends the body of the transaction.
func (g *ProduceGuard) appendTxnBody(ctx context.Context, txn *message.TxnContext, msgs ...message.MutableMessage) error {
	// concurrent produce here.
	wg := sync.WaitGroup{}
	wg.Add(len(msgs))
	resp := types.NewAppendResponseN(len(msgs))
	mu := sync.Mutex{}
	for i, msg := range msgs {
		i := i
		msg := msg
		go func() (struct{}, error) {
			defer wg.Done()

			result, err := g.producer.produceInternal(ctx, msg.WithTxnContext(*txn))
			mu.Lock()
			resp.FillResponseAtIdx(types.AppendResponse{
				AppendResult: result,
				Error:        err,
			}, i)
			mu.Unlock()
			return struct{}{}, nil
		}()
	}
	wg.Wait()
	return resp.UnwrapFirstError()
}

// commitTxn commits the transaction.
func (g *ProduceGuard) commitTxn(ctx context.Context, vchannel string, txn *message.TxnContext) (*types.AppendResult, error) {
	commitTxn := message.NewCommitTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable()

	return g.producer.produceInternal(ctx, commitTxn.WithTxnContext(*txn))
}

// Cancel cancel the produce task.
func (g *ProduceGuard) Cancel() {
	// return the quota of reservation to the limiter as much as possible.
	g.r.Cancel()
}
