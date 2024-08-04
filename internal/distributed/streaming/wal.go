package streaming

import (
	"context"
	"sync"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/consumer"
	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/internal/streamingcoord/client"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
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
		// TODO: make the pool size configurable.
		appendExecutionPool: conc.NewPool[struct{}](10),
	}
}

// walAccesserImpl is the implementation of WALAccesser.
type walAccesserImpl struct {
	lifetime lifetime.Lifetime[lifetime.State]

	// All services
	streamingCoordAssignmentClient client.Client
	handlerClient                  handler.HandlerClient

	producerMutex       sync.Mutex
	producers           map[string]*producer.ResumableProducer
	appendExecutionPool *conc.Pool[struct{}]
}

// Append writes a record to the log.
func (w *walAccesserImpl) Append(ctx context.Context, msgs ...message.MutableMessage) AppendResponses {
	if err := w.lifetime.Add(lifetime.IsWorking); err != nil {
		err := status.NewOnShutdownError("wal accesser closed, %s", err.Error())
		resp := newAppendResponseN(len(msgs))
		resp.fillAllError(err)
		return resp
	}
	defer w.lifetime.Done()

	// If there is only one message, append it to the corresponding pchannel is ok.
	if len(msgs) <= 1 {
		return w.appendToPChannel(ctx, funcutil.ToPhysicalChannel(msgs[0].VChannel()), msgs...)
	}
	return w.dispatchByPChannel(ctx, msgs...)
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
