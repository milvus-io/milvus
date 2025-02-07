package streaming

import (
	"context"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/consumer"
	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/internal/streamingcoord/client"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var ErrWALAccesserClosed = status.NewOnShutdownError("wal accesser closed")

// newWALAccesser creates a new wal accesser.
func newWALAccesser(c *clientv3.Client) *walAccesserImpl {
	// Create a new streaming coord client.
	streamingCoordClient := client.NewClient(c)
	// Create a new streamingnode handler client.
	var handlerClient handler.HandlerClient
	if streamingutil.IsStreamingServiceEnabled() {
		// streaming service is enabled, create the handler client for the streaming service.
		handlerClient = handler.NewHandlerClient(streamingCoordClient.Assignment())
	}
	return &walAccesserImpl{
		lifetime:             typeutil.NewLifetime(),
		streamingCoordClient: streamingCoordClient,
		handlerClient:        handlerClient,
		producerMutex:        sync.Mutex{},
		producers:            make(map[string]*producer.ResumableProducer),

		// TODO: optimize the pool size, use the streaming api but not goroutines.
		appendExecutionPool:   conc.NewPool[struct{}](10),
		dispatchExecutionPool: conc.NewPool[struct{}](10),
	}
}

// walAccesserImpl is the implementation of WALAccesser.
type walAccesserImpl struct {
	lifetime *typeutil.Lifetime

	// All services
	streamingCoordClient client.Client
	handlerClient        handler.HandlerClient

	producerMutex         sync.Mutex
	producers             map[string]*producer.ResumableProducer
	appendExecutionPool   *conc.Pool[struct{}]
	dispatchExecutionPool *conc.Pool[struct{}]
}

func (w *walAccesserImpl) WALName() string {
	return util.MustSelectWALName()
}

// RawAppend writes a record to the log.
func (w *walAccesserImpl) RawAppend(ctx context.Context, msg message.MutableMessage, opts ...AppendOption) (*types.AppendResult, error) {
	assertValidMessage(msg)
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrWALAccesserClosed
	}
	defer w.lifetime.Done()

	msg = applyOpt(msg, opts...)
	return w.appendToWAL(ctx, msg)
}

// Read returns a scanner for reading records from the wal.
func (w *walAccesserImpl) Read(_ context.Context, opts ReadOption) Scanner {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		newErrScanner(ErrWALAccesserClosed)
	}
	defer w.lifetime.Done()

	if opts.VChannel == "" {
		return newErrScanner(status.NewInvaildArgument("vchannel is required"))
	}

	// TODO: optimize the consumer into pchannel level.
	pchannel := funcutil.ToPhysicalChannel(opts.VChannel)
	rc := consumer.NewResumableConsumer(w.handlerClient.CreateConsumer, &consumer.ConsumerOptions{
		PChannel:       pchannel,
		VChannel:       opts.VChannel,
		DeliverPolicy:  opts.DeliverPolicy,
		DeliverFilters: opts.DeliverFilters,
		MessageHandler: opts.MessageHandler,
	})
	return rc
}

// Broadcast returns a broadcast for broadcasting records to the wal.
func (w *walAccesserImpl) Broadcast() Broadcast {
	return broadcast{w}
}

func (w *walAccesserImpl) Txn(ctx context.Context, opts TxnOption) (Txn, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, ErrWALAccesserClosed
	}

	if opts.VChannel == "" {
		w.lifetime.Done()
		return nil, status.NewInvaildArgument("vchannel is required")
	}
	if opts.Keepalive != 0 && opts.Keepalive < 1*time.Millisecond {
		w.lifetime.Done()
		return nil, status.NewInvaildArgument("ttl must be greater than or equal to 1ms")
	}

	// Create a new transaction, send the begin txn message.
	beginTxn, err := message.NewBeginTxnMessageBuilderV2().
		WithVChannel(opts.VChannel).
		WithHeader(&message.BeginTxnMessageHeader{
			KeepaliveMilliseconds: opts.Keepalive.Milliseconds(),
		}).
		WithBody(&message.BeginTxnMessageBody{}).
		BuildMutable()
	if err != nil {
		w.lifetime.Done()
		return nil, err
	}

	appendResult, err := w.appendToWAL(ctx, beginTxn)
	if err != nil {
		w.lifetime.Done()
		return nil, err
	}

	// Create new transaction success.
	return &txnImpl{
		mu:              sync.Mutex{},
		state:           message.TxnStateInFlight,
		opts:            opts,
		txnCtx:          appendResult.TxnCtx,
		walAccesserImpl: w,
	}, nil
}

// Close closes all the wal accesser.
func (w *walAccesserImpl) Close() {
	w.lifetime.SetState(typeutil.LifetimeStateStopped)
	w.lifetime.Wait()

	w.producerMutex.Lock()
	for _, p := range w.producers {
		p.Close()
	}
	w.producerMutex.Unlock()

	if w.handlerClient != nil {
		w.handlerClient.Close()
	}
	w.streamingCoordClient.Close()
}

// newErrScanner creates a scanner that returns an error.
func newErrScanner(err error) Scanner {
	ch := make(chan struct{})
	return errScanner{
		closedCh: ch,
		err:      err,
	}
}

type errScanner struct {
	closedCh chan struct{}
	err      error
}

func (s errScanner) Done() <-chan struct{} {
	return s.closedCh
}

func (s errScanner) Error() error {
	return s.err
}

func (s errScanner) Close() {
}
