package producer

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/errs"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
)

// newProducerWithResumingError creates a new producer with resuming error.
func newProducerWithResumingError() producerWithResumingError {
	return producerWithResumingError{
		cond: syncutil.NewContextCond(&sync.Mutex{}),
	}
}

// producerWithResumingError is a producer that can be resumed.
type producerWithResumingError struct {
	cond     *syncutil.ContextCond
	producer handler.Producer
	err      error
}

// GetProducerAfterAvailable gets the producer after it is available.
func (p *producerWithResumingError) GetProducerAfterAvailable(ctx context.Context) (handler.Producer, error) {
	p.cond.L.Lock()
	for p.err == nil && (p.producer == nil || !p.producer.IsAvailable()) {
		if err := p.cond.Wait(ctx); err != nil {
			return nil, errors.Mark(err, errs.ErrCanceledOrDeadlineExceed)
		}
	}
	err := p.err
	producer := p.producer

	p.cond.L.Unlock()
	if err != nil {
		return nil, errors.Mark(err, errs.ErrClosed)
	}
	return producer, nil
}

// SwapProducer swaps the producer with a new one.
func (p *producerWithResumingError) SwapProducer(producer handler.Producer, err error) {
	p.cond.LockAndBroadcast()
	oldProducer := p.producer
	p.producer = producer
	p.err = err
	p.cond.L.Unlock()

	if oldProducer != nil {
		oldProducer.Close()
	}
}
