package handler

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/assignment"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/consumer"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type handlerClientImpl struct {
	lifetime lifetime.Lifetime[lifetime.State]
	service  lazygrpc.Service[streamingpb.StreamingNodeHandlerServiceClient]
	// TODO: use conn pool if there's huge amount of stream.
	// But there's low possibility that there's 100 stream on one streamingnode.
	rb                    resolver.Builder
	watcher               assignment.Watcher
	sharedProducers       map[string]*typeutil.WeakReference[Producer] // map the pchannel to shared producer.
	sharedProducerKeyLock *lock.KeyLock[string]
	newProducer           func(ctx context.Context, opts *producer.ProducerOptions, handler streamingpb.StreamingNodeHandlerServiceClient) (Producer, error)
	newConsumer           func(ctx context.Context, opts *consumer.ConsumerOptions, handlerClient streamingpb.StreamingNodeHandlerServiceClient) (Consumer, error)
}

// CreateProducer creates a producer.
func (hc *handlerClientImpl) CreateProducer(ctx context.Context, opts *options.ProducerOptions) (Producer, error) {
	if hc.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("handler client is closed")
	}
	defer hc.lifetime.Done()

	pChannel := funcutil.ToPhysicalChannel(opts.VChannel)
	p, err := hc.createHandlerUntilStreamingNodeReady(ctx, pChannel, func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error) {
		// Wait for handler service is ready.
		handlerService, err := hc.service.GetService(ctx)
		if err != nil {
			return nil, err
		}
		return hc.createOrGetSharedProducer(ctx, &producer.ProducerOptions{Assignment: assign}, handlerService)
	})
	if err != nil {
		return nil, err
	}
	return p.(Producer), nil
}

// CreateConsumer creates a consumer.
func (hc *handlerClientImpl) CreateConsumer(ctx context.Context, opts *options.ConsumerOptions) (Consumer, error) {
	if hc.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("handler client is closed")
	}
	defer hc.lifetime.Done()

	pChannel := funcutil.ToPhysicalChannel(opts.VChannel)
	c, err := hc.createHandlerUntilStreamingNodeReady(ctx, pChannel, func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error) {
		// Wait for handler service is ready.
		handlerService, err := hc.service.GetService(ctx)
		if err != nil {
			return nil, err
		}
		filters := append(opts.DeliverFilters, options.DeliverFilterVChannel(opts.VChannel))
		return hc.newConsumer(ctx, &consumer.ConsumerOptions{
			Assignment:     assign,
			DeliverPolicy:  opts.DeliverPolicy,
			DeliverFilters: filters,
			MessageHandler: opts.MessageHandler,
		}, handlerService)
	})
	if err != nil {
		return nil, err
	}
	return c.(Consumer), nil
}

// createHandlerUntilStreamingNodeReady creates a handler until streaming node ready.
// If streaming node is not ready, it will block until new assignment term is coming or context timeout.
func (hc *handlerClientImpl) createHandlerUntilStreamingNodeReady(ctx context.Context, pchannel string, create func(ctx context.Context, assign *types.PChannelInfoAssigned) (any, error)) (any, error) {
	for {
		assign := hc.watcher.Get(ctx, pchannel)
		if assign != nil {
			// Find assignment, try to create producer on this assignment.
			c, err := create(ctx, assign)
			if err == nil {
				return c, nil
			}

			// Check if wrong streaming node.
			streamingServiceErr := status.AsStreamingError(err)
			if !streamingServiceErr.IsWrongStreamingNode() {
				// stop retry if not wrong log node error.
				return nil, streamingServiceErr
			}
		}

		// Block until new assignment term is coming if wrong log node or no assignment.
		if err := hc.watcher.Watch(ctx, pchannel, assign); err != nil {
			// Context timeout
			log.Warn("wait for watch channel assignment timeout", zap.String("channel", pchannel), zap.Any("oldAssign", assign))
			return nil, err
		}
	}
}

// getFromSharedProducers gets a shared producer from shared producers.A
func (hc *handlerClientImpl) getFromSharedProducers(channelInfo types.PChannelInfo) Producer {
	weakProducerRef, ok := hc.sharedProducers[channelInfo.Name]
	if !ok {
		return nil
	}

	strongProducerRef := weakProducerRef.Upgrade()
	if strongProducerRef == nil {
		// upgrade failure means the outer producer is all closed.
		// remove the weak ref and create again.
		delete(hc.sharedProducers, channelInfo.Name)
		return nil
	}

	p := newSharedProducer(strongProducerRef)
	if !p.IsAvailable() || p.Assignment().Channel.Term < channelInfo.Term {
		// if the producer is not available or the term is less than expected.
		// close it and return to create new one.
		p.Close()
		delete(hc.sharedProducers, channelInfo.Name)
		return nil
	}
	return p
}

// createOrGetSharedProducer creates or get a shared producer.
// because vchannel in same pchannel can share the same producer.
func (hc *handlerClientImpl) createOrGetSharedProducer(
	ctx context.Context,
	opts *producer.ProducerOptions,
	handlerService streamingpb.StreamingNodeHandlerServiceClient,
) (Producer, error) {
	hc.sharedProducerKeyLock.Lock(opts.Assignment.Channel.Name)
	defer hc.sharedProducerKeyLock.Unlock(opts.Assignment.Channel.Name)

	// check if shared producer is created within key lock.
	if p := hc.getFromSharedProducers(opts.Assignment.Channel); p != nil {
		return p, nil
	}

	// create a new producer and insert it into shared producers.
	newProducer, err := hc.newProducer(ctx, opts, handlerService)
	if err != nil {
		return nil, err
	}
	newStrongProducerRef := typeutil.NewSharedReference(newProducer)
	// store a weak ref and return a strong ref.
	returned := newStrongProducerRef.Clone()
	stored := newStrongProducerRef.Downgrade()
	hc.sharedProducers[opts.Assignment.Channel.Name] = stored
	return newSharedProducer(returned), nil
}

// Close closes the handler client.
func (hc *handlerClientImpl) Close() {
	hc.lifetime.SetState(lifetime.Stopped)
	hc.lifetime.Wait()
	hc.lifetime.Close()

	hc.watcher.Close()
	hc.service.Close()
	hc.rb.Close()
}
