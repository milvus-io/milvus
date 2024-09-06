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
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
)

// newWALAccesser creates a new wal accesser.
func newWALAccesser(c *clientv3.Client) *walAccesserImpl {
	// Create a new streaming coord client.
	streamingCoordClient := client.NewClient(c)
	// Create a new streamingnode handler client.
	handlerClient := handler.NewHandlerClient(streamingCoordClient.Assignment())
	return &walAccesserImpl{
		lifetime:                       lifetime.NewLifetime(lifetime.Working),
		streamingCoordAssignmentClient: streamingCoordClient,
		handlerClient:                  handlerClient,
		producerMutex:                  sync.Mutex{},
		producers:                      make(map[string]*producer.ResumableProducer),

		// TODO: optimize the pool size, use the streaming api but not goroutines.
		appendExecutionPool:   conc.NewPool[struct{}](10),
		dispatchExecutionPool: conc.NewPool[struct{}](10),
	}
}

// walAccesserImpl is the implementation of WALAccesser.
type walAccesserImpl struct {
	lifetime lifetime.Lifetime[lifetime.State]

	// All services
	streamingCoordAssignmentClient client.Client
	handlerClient                  handler.HandlerClient

	producerMutex         sync.Mutex
	producers             map[string]*producer.ResumableProducer
	appendExecutionPool   *conc.Pool[struct{}]
	dispatchExecutionPool *conc.Pool[struct{}]
}

// RawAppend writes a record to the log.
func (w *walAccesserImpl) RawAppend(ctx context.Context, msg message.MutableMessage, opts ...AppendOption) (*types.AppendResult, error) {
	assertNoSystemMessage(msg)
	if err := w.lifetime.Add(lifetime.IsWorking); err != nil {
		return nil, status.NewOnShutdownError("wal accesser closed, %s", err.Error())
	}
	defer w.lifetime.Done()

	msg = applyOpt(msg, opts...)
	return w.appendToWAL(ctx, msg)
}

// Read returns a scanner for reading records from the wal.
func (w *walAccesserImpl) Read(_ context.Context, opts ReadOption) Scanner {
	if err := w.lifetime.Add(lifetime.IsWorking); err != nil {
		newErrScanner(status.NewOnShutdownError("wal accesser closed, %s", err.Error()))
	}
	defer w.lifetime.Done()

	// TODO: optimize the consumer into pchannel level.
	pchannel := funcutil.ToPhysicalChannel(opts.VChannel)
	filters := append(opts.DeliverFilters, options.DeliverFilterVChannel(opts.VChannel))
	rc := consumer.NewResumableConsumer(w.handlerClient.CreateConsumer, &consumer.ConsumerOptions{
		PChannel:       pchannel,
		DeliverPolicy:  opts.DeliverPolicy,
		DeliverFilters: filters,
		MessageHandler: opts.MessageHandler,
	})
	return rc
}

func (w *walAccesserImpl) Txn(ctx context.Context, opts TxnOption) (Txn, error) {
	if err := w.lifetime.Add(lifetime.IsWorking); err != nil {
		return nil, status.NewOnShutdownError("wal accesser closed, %s", err.Error())
	}

	if opts.VChannel == "" {
		return nil, status.NewInvaildArgument("vchannel is required")
	}
	if opts.Keepalive < 1*time.Millisecond {
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
	w.lifetime.SetState(lifetime.Stopped)
	w.lifetime.Wait()

	w.producerMutex.Lock()
	for _, p := range w.producers {
		p.Close()
	}
	w.producerMutex.Unlock()

	w.handlerClient.Close()
	w.streamingCoordAssignmentClient.Close()
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
