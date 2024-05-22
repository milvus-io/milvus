package consumer

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/lognode/client/handler"
	"github.com/milvus-io/milvus/internal/lognode/client/handler/consumer"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/options"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"go.uber.org/zap"
)

// NewResumableConsumer creates a new resumable consumer.
func NewResumableConsumer(factory factory, opts *options.ConsumerOptions) ResumableConsumer {
	ctx, cancel := context.WithCancel(context.Background())
	consumer := &resumableConsumerImpl{
		ctx:            ctx,
		cancel:         cancel,
		stopResumingCh: make(chan struct{}),
		resumingExitCh: make(chan struct{}),
		logger:         log.With(zap.String("channel", opts.Channel), zap.Any("initialDeliverPolicy", opts.DeliverPolicy)),

		opts: opts,
		mh: &messageHandler{
			inner:         opts.MessageHandler,
			lastMessageID: nil,
		},
		factory: factory,
	}
	go consumer.resumeLoop()
	return consumer
}

// resumableConsumerImpl is a consumer implementation.
type resumableConsumerImpl struct {
	ctx            context.Context
	cancel         context.CancelFunc
	logger         *log.MLogger
	stopResumingCh chan struct{}
	resumingExitCh chan struct{}

	opts    *options.ConsumerOptions
	mh      *messageHandler
	factory factory
}

type factory = func(ctx context.Context, opts *options.ConsumerOptions) (consumer.Consumer, error)

// Channel returns the channel name.
func (rc *resumableConsumerImpl) Channel() string {
	return rc.opts.Channel
}

// resumeLoop starts a background task to consume messages from log service to message handler.
func (rc *resumableConsumerImpl) resumeLoop() {
	defer func() {
		// close the message handler.
		rc.mh.Close()
		rc.logger.Info("resumable consumer is closed")
		close(rc.resumingExitCh)
	}()

	// Use the initialized deliver policy at first running.
	deliverPolicy := rc.opts.DeliverPolicy
	// consumer need to resume when error occur, so message handler shouldn't close if the internal consumer encounter failure.
	nopCloseMH := message.NopCloseHandler{
		Handler: rc.mh,
	}

	for {
		// Get last checkpoint sent.
		if rc.mh.lastMessageID != nil {
			// set the deliver policy start after the last message id.
			deliverPolicy = options.DeliverStartAfter(rc.mh.lastMessageID)
		}
		opts := &options.ConsumerOptions{
			Channel:        rc.opts.Channel,
			DeliverPolicy:  deliverPolicy,
			MessageHandler: nopCloseMH,
		}

		// Create a new consumer.
		// Return if context canceled.
		consumer, err := rc.createNewConsumer(opts)
		if err != nil {
			return
		}

		// Wait until the consumer is unavailable or context canceled.
		if err := rc.waitUntilUnavailable(consumer); err != nil {
			return
		}
	}
}

func (rc *resumableConsumerImpl) createNewConsumer(opts *options.ConsumerOptions) (consumer.Consumer, error) {
	// Mark as unavailable.
	metrics.LogServiceClientConsumerTotal.WithLabelValues(paramtable.GetStringNodeID(), metrics.LogServiceClientProducerUnAvailable).Inc()
	defer metrics.LogServiceClientConsumerTotal.WithLabelValues(paramtable.GetStringNodeID(), metrics.LogServiceClientProducerUnAvailable).Dec()

	logger := rc.logger.With(zap.Any("deliverPolicy", opts.DeliverPolicy))

	for {
		// Create a new consumer.
		// a underlying stream consumer life time should be equal to the resumable producer.
		// so ctx of resumable consumer is passed to underlying stream producer creation.
		consumer, err := rc.factory(rc.ctx, opts)
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		if err != nil {
			logger.Warn("create consumer failed", zap.Error(err))
			// TODO: backoff.
			time.Sleep(50 * time.Millisecond)
			continue
		}

		logger.Info("resume on new consumer at new start message id")
		return consumer, nil
	}
}

// waitUntilUnavailable is used to wait until the consumer is unavailable or context canceled.
func (rc *resumableConsumerImpl) waitUntilUnavailable(consumer handler.Consumer) error {
	// Mark as available.
	metrics.LogServiceClientConsumerTotal.WithLabelValues(paramtable.GetStringNodeID(), metrics.LogServiceClientProducerAvailable).Inc()
	defer func() {
		consumer.Close()
		metrics.LogServiceClientConsumerTotal.WithLabelValues(paramtable.GetStringNodeID(), metrics.LogServiceClientProducerAvailable).Dec()
	}()

	select {
	case <-rc.stopResumingCh:
		return errors.New("stop resuming")
	case <-rc.ctx.Done():
		return rc.ctx.Err()
	case <-consumer.Done():
		rc.logger.Warn("consumer is done or encounter error, try to resume...", zap.Error(consumer.Error()), zap.Any("lastMessageID", rc.mh.lastMessageID))
		return nil
	}
}

// gracefulClose graceful close the consumer.
func (rc *resumableConsumerImpl) gracefulClose() error {
	close(rc.stopResumingCh)
	select {
	case <-rc.resumingExitCh:
		return nil
	case <-time.After(50 * time.Millisecond):
		return context.DeadlineExceeded
	}
}

// Close the scanner, release the underlying resources.
func (rc *resumableConsumerImpl) Close() {
	if err := rc.gracefulClose(); err != nil {
		rc.logger.Warn("graceful close a consumer fail, force close is applied")
	}

	// cancel is always need to be called, even graceful close is success.
	// force close is applied by cancel context if graceful close is failed.
	rc.cancel()
	<-rc.resumingExitCh
}

// Done returns a channel which will be closed when scanner is finished or closed.
func (rc *resumableConsumerImpl) Done() <-chan struct{} {
	return rc.resumingExitCh
}
