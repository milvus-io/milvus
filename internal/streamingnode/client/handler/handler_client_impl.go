package handler

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/assignment"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/consumer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var errWaitNextBackoff = errors.New("wait for next backoff")

type handlerClientImpl struct {
	lifetime         *typeutil.Lifetime
	service          lazygrpc.Service[streamingpb.StreamingNodeHandlerServiceClient]
	rb               resolver.Builder
	watcher          assignment.Watcher
	rebalanceTrigger types.AssignmentRebalanceTrigger
	newProducer      func(ctx context.Context, opts *producer.ProducerOptions, handler streamingpb.StreamingNodeHandlerServiceClient) (Producer, error)
	newConsumer      func(ctx context.Context, opts *consumer.ConsumerOptions, handlerClient streamingpb.StreamingNodeHandlerServiceClient) (Consumer, error)
}

// CreateProducer creates a producer.
func (hc *handlerClientImpl) CreateProducer(ctx context.Context, opts *ProducerOptions) (Producer, error) {
	if !hc.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrClientClosed
	}
	defer hc.lifetime.Done()

	p, err := hc.createHandlerAfterStreamingNodeReady(ctx, opts.PChannel, func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error) {
		// Wait for handler service is ready.
		handlerService, err := hc.service.GetService(ctx)
		if err != nil {
			return nil, err
		}
		return hc.newProducer(ctx, &producer.ProducerOptions{
			Assignment: assign,
		}, handlerService)
	})
	if err != nil {
		return nil, err
	}
	return p.(Producer), nil
}

// CreateConsumer creates a consumer.
func (hc *handlerClientImpl) CreateConsumer(ctx context.Context, opts *ConsumerOptions) (Consumer, error) {
	if !hc.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrClientClosed
	}
	defer hc.lifetime.Done()

	c, err := hc.createHandlerAfterStreamingNodeReady(ctx, opts.PChannel, func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error) {
		// Wait for handler service is ready.
		handlerService, err := hc.service.GetService(ctx)
		if err != nil {
			return nil, err
		}
		return hc.newConsumer(ctx, &consumer.ConsumerOptions{
			Assignment:     assign,
			VChannel:       opts.VChannel,
			DeliverPolicy:  opts.DeliverPolicy,
			DeliverFilters: opts.DeliverFilters,
			MessageHandler: opts.MessageHandler,
		}, handlerService)
	})
	if err != nil {
		return nil, err
	}
	return c.(Consumer), nil
}

// createHandlerAfterStreamingNodeReady creates a handler until streaming node ready.
// If streaming node is not ready, it will block until new assignment term is coming or context timeout.
func (hc *handlerClientImpl) createHandlerAfterStreamingNodeReady(ctx context.Context, pchannel string, create func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error)) (any, error) {
	logger := log.With(zap.String("pchannel", pchannel))
	// TODO: backoff should be configurable.
	backoff := backoff.NewExponentialBackOff()
	for {
		assign := hc.watcher.Get(ctx, pchannel)
		if assign != nil {
			// Find assignment, try to create producer on this assignment.
			c, err := create(ctx, assign)
			if err == nil {
				return c, nil
			}
			logger.Warn("create handler failed", zap.Any("assignment", assign), zap.Error(err))

			// Check if the error is permanent failure until new assignment.
			if isPermanentFailureUntilNewAssignment(err) {
				reportErr := hc.rebalanceTrigger.ReportAssignmentError(ctx, assign.Channel, err)
				logger.Info("report assignment error", zap.NamedError("assignmentError", err), zap.Error(reportErr))
			}
		} else {
			log.Warn("assignment not found")
		}

		start := time.Now()
		nextBackoff := backoff.NextBackOff()
		logger.Info("wait for next backoff", zap.Duration("nextBackoff", nextBackoff))
		isAssignemtChange, err := hc.waitForNextBackoff(ctx, pchannel, assign, nextBackoff)
		cost := time.Since(start)
		if err != nil {
			logger.Warn("wait for next backoff failed", zap.Error(err), zap.Duration("cost", cost))
			return nil, err
		}
		logger.Info("wait for next backoff done", zap.Bool("isAssignmentChange", isAssignemtChange), zap.Duration("cost", cost))
	}
}

// waitForNextBackoff waits for next backoff.
func (hc *handlerClientImpl) waitForNextBackoff(ctx context.Context, pchannel string, assign *types.PChannelInfoAssigned, nextBackoff time.Duration) (bool, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, nextBackoff, errWaitNextBackoff)
	defer cancel()
	// Block until new assignment term is coming.
	err := hc.watcher.Watch(ctx, pchannel, assign)
	if err == nil || errors.Is(context.Cause(ctx), errWaitNextBackoff) {
		return err == nil, nil
	}
	return false, err
}

// Close closes the handler client.
func (hc *handlerClientImpl) Close() {
	hc.lifetime.SetState(typeutil.LifetimeStateStopped)
	hc.lifetime.Wait()

	hc.watcher.Close()
	hc.service.Close()
	hc.rb.Close()
}

// isPermanentFailureUntilNewAssignment checks if the error is permanent failure until new assignment.
// If the encounter this error, client should notify the assignment service to rebalance the assignment and update discovery result.
// block until new assignment term is coming or context timeout.
func isPermanentFailureUntilNewAssignment(err error) bool {
	if err == nil {
		return false
	}
	// The error is reported by grpc balancer at client that the sub connection is not exist (remote server is down at view of session).
	if picker.IsErrSubConnNoExist(err) {
		return true
	}
	// The error is reported by remote server that the wal is not exist at remote server.
	streamingServiceErr := status.AsStreamingError(err)
	return streamingServiceErr.IsWrongStreamingNode()
}
