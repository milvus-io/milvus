package producer

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/errs"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

var errGracefulShutdown = errors.New("graceful shutdown")

// ProducerOptions is the options for creating a producer.
type ProducerOptions struct {
	PChannel string
}

// NewResumableProducer creates a new producer.
// Provide an auto resuming producer.
func NewResumableProducer(f factory, opts *ProducerOptions) *ResumableProducer {
	ctx, cancel := context.WithCancel(context.Background())
	p := &ResumableProducer{
		ctx:            ctx,
		cancel:         cancel,
		stopResumingCh: make(chan struct{}),
		resumingExitCh: make(chan struct{}),
		lifetime:       lifetime.NewLifetime(lifetime.Working),
		logger:         log.With(zap.String("pchannel", opts.PChannel)),
		opts:           opts,

		producer: newProducerWithResumingError(), // lazy initialized.
		cond:     syncutil.NewContextCond(&sync.Mutex{}),
		factory:  f,
	}
	go p.resumeLoop()
	return p
}

// factory is a factory used to create a new underlying streamingnode producer.
type factory func(ctx context.Context, opts *handler.ProducerOptions) (producer.Producer, error)

// ResumableProducer is implementation for producing message to streaming service.
// ResumableProducer select a right streaming node to produce automatically.
// ResumableProducer will do automatic resume from stream broken and streaming node re-balance.
// All error in these package should be marked by streaming/errs package.
type ResumableProducer struct {
	ctx            context.Context
	cancel         context.CancelFunc
	resumingExitCh chan struct{}
	stopResumingCh chan struct{} // Q: why do not use ctx directly?
	// A: cancel the ctx will cancel the underlying running producer.
	// Use producer Close is better way to stop producer.

	lifetime lifetime.Lifetime[lifetime.State]
	logger   *log.MLogger
	opts     *ProducerOptions

	producer producerWithResumingError
	cond     *syncutil.ContextCond

	// factory is used to create a new producer.
	factory factory
}

// Produce produce a new message to log service.
func (p *ResumableProducer) Produce(ctx context.Context, msg message.MutableMessage) (*producer.ProduceResult, error) {
	if p.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, errors.Wrapf(errs.ErrClosed, "produce on closed producer")
	}
	defer p.lifetime.Done()

	for {
		// get producer.
		producerHandler, err := p.producer.GetProducerAfterAvailable(ctx)
		if err != nil {
			return nil, err
		}

		produceResult, err := producerHandler.Produce(ctx, msg)
		if err == nil {
			return produceResult, nil
		}
		// It's ok to stop retry if the error is canceled or deadline exceed.
		if status.IsCanceled(err) {
			return nil, errors.Mark(err, errs.ErrCanceledOrDeadlineExceed)
		}
		if sErr := status.AsStreamingError(err); sErr != nil {
			// if the error is txn unavailable or unrecoverable error,
			// it cannot be retried forever.
			// we should mark it and return.
			if sErr.IsUnrecoverable() {
				return nil, errors.Mark(err, errs.ErrUnrecoverable)
			}
		}
	}
}

// resumeLoop is used to resume producer from error.
func (p *ResumableProducer) resumeLoop() {
	defer func() {
		p.logger.Info("stop resuming")
		close(p.resumingExitCh)
	}()

	for {
		producer, err := p.createNewProducer()
		p.producer.SwapProducer(producer, err)
		if err != nil {
			return
		}

		// Wait until the new producer is unavailable, trigger a new swap operation.
		if err := p.waitUntilUnavailable(producer); err != nil {
			p.producer.SwapProducer(nil, err)
			return
		}
	}
}

// waitUntilUnavailable is used to wait until the producer is unavailable or context canceled.
func (p *ResumableProducer) waitUntilUnavailable(producer handler.Producer) error {
	// Mark as available.
	metrics.StreamingServiceClientProducerTotal.WithLabelValues(paramtable.GetStringNodeID(), metrics.StreamingServiceClientProducerAvailable).Inc()
	defer metrics.StreamingServiceClientProducerTotal.WithLabelValues(paramtable.GetStringNodeID(), metrics.StreamingServiceClientProducerAvailable).Dec()

	select {
	case <-p.stopResumingCh:
		return errGracefulShutdown
	case <-p.ctx.Done():
		return p.ctx.Err()
	case <-producer.Available():
		// Wait old producer unavailable, trigger a new resuming operation.
		p.logger.Warn("producer encounter error, try to resume...")
		return nil
	}
}

// createNewProducer is used to open a new stream producer with backoff.
func (p *ResumableProducer) createNewProducer() (producer.Producer, error) {
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 100 * time.Millisecond
	backoff.MaxInterval = 2 * time.Second
	for {
		// Create a new producer.
		// a underlying stream producer life time should be equal to the resumable producer.
		// so ctx of resumable producer is passed to underlying stream producer creation.
		producerHandler, err := p.factory(p.ctx, &handler.ProducerOptions{
			PChannel: p.opts.PChannel,
		})

		// Can not resumable:
		// 1. context canceled: the resumable producer is closed.
		// 2. ErrClientClosed: the underlying handlerClient is closed.
		if errors.IsAny(err, context.Canceled, handler.ErrClientClosed) {
			return nil, err
		}

		// Otherwise, perform a resuming operation.
		if err != nil {
			nextBackoff := backoff.NextBackOff()
			p.logger.Warn("create producer failed, retry...", zap.Error(err), zap.Duration("nextRetryInterval", nextBackoff))
			time.Sleep(nextBackoff)
			continue
		}
		return producerHandler, nil
	}
}

// gracefulClose graceful close the producer.
func (p *ResumableProducer) gracefulClose() error {
	p.lifetime.SetState(lifetime.Stopped)
	p.lifetime.Wait()
	// close the stop resuming background to avoid create new producer.
	close(p.stopResumingCh)

	select {
	case <-p.resumingExitCh:
		return nil
	case <-time.After(50 * time.Millisecond):
		return context.DeadlineExceeded
	}
}

// Close close the producer.
func (p *ResumableProducer) Close() {
	if err := p.gracefulClose(); err != nil {
		p.logger.Warn("graceful close a producer fail, force close is applied")
	}

	// cancel is always need to be called, even graceful close is success.
	// force close is applied by cancel context if graceful close is failed.
	p.cancel()
	<-p.resumingExitCh
}
