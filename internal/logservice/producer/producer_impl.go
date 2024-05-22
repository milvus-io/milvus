package producer

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/lognode/client/handler"
	"github.com/milvus-io/milvus/internal/lognode/client/handler/producer"
	"github.com/milvus-io/milvus/internal/logservice/errs"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/options"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"go.uber.org/zap"
)

// NewResumableProducer creates a new producer.
func NewResumableProducer(f factory, opts *options.ProducerOptions) ResumableProducer {
	ctx, cancel := context.WithCancel(context.Background())
	p := &resumableProducerImpl{
		ctx:            ctx,
		cancel:         cancel,
		stopResumingCh: make(chan struct{}),
		resumingExitCh: make(chan struct{}),
		lifetime:       lifetime.NewLifetime(lifetime.Working),
		logger:         log.With(zap.String("channel", opts.Channel)),
		opts:           opts,

		producer: nil, // lazy initialized.
		cond:     syncutil.NewContextCond(&sync.Mutex{}),
		factory:  f,
	}
	go p.resumeLoop()
	return p
}

// factory is a factory used to create a new underlying lognode producer.
type factory func(ctx context.Context, opts *options.ProducerOptions) (producer.Producer, error)

// resumableProducerImpl is a producer implementation.
type resumableProducerImpl struct {
	ctx            context.Context
	cancel         context.CancelFunc
	resumingExitCh chan struct{}
	stopResumingCh chan struct{} // Q: why do not use ctx directly?
	// A: cancel the ctx will cancel the underlying running producer.
	// Use producer Close is better way to stop producer.

	lifetime lifetime.Lifetime[lifetime.State]
	logger   *log.MLogger
	opts     *options.ProducerOptions

	// TODO: we should use more stream producer to support high throughput.
	producer handler.Producer
	cond     *syncutil.ContextCond
	// factory is used to create a new producer.
	factory func(ctx context.Context, opts *options.ProducerOptions) (handler.Producer, error)
}

// Channel returns the channel of producer.
func (p *resumableProducerImpl) Channel() string {
	return p.opts.Channel
}

// Produce produce a new message to log service.
func (p *resumableProducerImpl) Produce(ctx context.Context, msg message.MutableMessage) (msgID message.MessageID, err error) {
	if p.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, errors.Wrapf(errs.ErrClosed, "produce on closed producer")
	}
	messageSize := msg.EstimateSize()
	now := time.Now()
	defer func() {
		p.lifetime.Done()

		// Do a metric collect.
		if err == nil {
			metrics.LogServiceClientProduceBytes.WithLabelValues(paramtable.GetStringNodeID()).Observe(float64(messageSize))
		}
		metrics.LogServiceClientProduceDurationSeconds.WithLabelValues(paramtable.GetStringNodeID(), getStatusLabel(err)).Observe(float64(time.Since(now).Seconds()))
	}()

	for {
		// get producer.
		producerHandler, err := p.getProducerOrWaitProducerReady(ctx)
		if err != nil {
			return nil, errors.Mark(err, errors.Wrapf(errs.ErrCanceled, "wait for producer ready"))
		}
		msgID, err := producerHandler.Produce(ctx, msg)
		if err == nil {
			return msgID, nil
		}
		// It's ok to stop retry if the error is canceled or deadline exceed.
		if status.IsCanceled(err) {
			return nil, errors.Mark(err, errs.ErrCanceled)
		}
	}
}

// getProducerOrWaitProducerReady get producer or wait the new producer is available.
func (p *resumableProducerImpl) getProducerOrWaitProducerReady(ctx context.Context) (producer.Producer, error) {
	p.cond.L.Lock()

	for p.producer == nil || !p.producer.IsAvailable() {
		if err := p.cond.Wait(ctx); err != nil {
			return nil, err
		}
	}

	p.cond.L.Unlock()
	return p.producer, nil
}

// resumeLoop is used to resume producer from error.
func (p *resumableProducerImpl) resumeLoop() {
	defer func() {
		p.logger.Info("stop resuming")
		close(p.resumingExitCh)
	}()

	for {
		// Do a underlying producer swap.
		// Return if context canceled.
		newProducer, err := p.swapProducer()
		if err != nil {
			return
		}

		// Wait until the new producer is unavailable, trigger a new swap operation.
		if err := p.waitUntilUnavailable(newProducer); err != nil {
			return
		}
	}
}

// swapProducer is used to swap a new producer.
// It will block until the new producer is available.
// It will close the old producer if the old producer is not nil.
// It will return error if context canceled.
func (p *resumableProducerImpl) swapProducer() (handler.Producer, error) {
	// Mark as unavailable.
	metrics.LogServiceClientProducerTotal.WithLabelValues(paramtable.GetStringNodeID(), metrics.LogServiceClientProducerUnAvailable).Inc()
	defer metrics.LogServiceClientProducerTotal.WithLabelValues(paramtable.GetStringNodeID(), metrics.LogServiceClientProducerUnAvailable).Dec()

	producer, err := p.createNewProducer()
	if err != nil {
		return nil, err
	}

	// swap new producer.
	p.cond.LockAndBroadcast()
	oldProducer := p.producer
	p.producer = producer
	p.cond.L.Unlock()

	p.logger.Info("swap producer success")
	if oldProducer != nil {
		oldProducer.Close()
	}
	p.logger.Info("old producer closed")
	return producer, nil
}

// waitUntilUnavailable is used to wait until the producer is unavailable or context canceled.
func (p *resumableProducerImpl) waitUntilUnavailable(producer handler.Producer) error {
	// Mark as available.
	metrics.LogServiceClientProducerTotal.WithLabelValues(paramtable.GetStringNodeID(), metrics.LogServiceClientProducerAvailable).Inc()
	defer metrics.LogServiceClientProducerTotal.WithLabelValues(paramtable.GetStringNodeID(), metrics.LogServiceClientProducerAvailable).Dec()

	select {
	case <-p.stopResumingCh:
		return errors.New("stop resuming")
	case <-p.ctx.Done():
		return p.ctx.Err()
	case <-producer.Available():
		// Wait old producer unavailable, trigger a new resuming operation.
		p.logger.Warn("producer encounter error, try to resume...")
		return nil
	}
}

// createNewProducer is used to open a new stream producer with backoff.
func (p *resumableProducerImpl) createNewProducer() (producer.Producer, error) {
	for {
		// Create a new producer.
		// a underlying stream producer life time should be equal to the resumable producer.
		// so ctx of resumable producer is passed to underlying stream producer creation.
		producerHandler, err := p.factory(p.ctx, p.opts)
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		if err != nil {
			p.logger.Warn("create producer failed", zap.Error(err))
			// TODO: add backoff.
			time.Sleep(50 * time.Millisecond)
			continue
		}
		return producerHandler, nil
	}
}

// gracefulClose graceful close the producer.
func (p *resumableProducerImpl) gracefulClose() error {
	p.lifetime.SetState(lifetime.Stopped)
	p.lifetime.Wait()
	// close the stop resuming background to avoid create new producer.
	close(p.stopResumingCh)

	select {
	case <-p.resumingExitCh:
		p.closeProducer()
		return nil
	case <-time.After(50 * time.Millisecond):
		return context.DeadlineExceeded
	}
}

// Close close the producer.
func (p *resumableProducerImpl) Close() {
	if err := p.gracefulClose(); err != nil {
		p.logger.Warn("graceful close a producer fail, force close is applied")
	}

	// cancel is always need to be called, even graceful close is success.
	// force close is applied by cancel context if graceful close is failed.
	p.cancel()
	<-p.resumingExitCh
	p.closeProducer()

	return
}

func (p *resumableProducerImpl) closeProducer() {
	p.cond.L.Lock()
	if p.producer != nil {
		p.producer.Close()
	}
	p.producer = nil
	p.cond.L.Unlock()
}

// getStatusLabel returns the status label of error.
func getStatusLabel(err error) string {
	if errors.Is(err, errs.ErrCanceled) {
		return metrics.CancelLabel
	}
	if err != nil {
		return metrics.FailLabel
	}
	return metrics.SuccessLabel
}
