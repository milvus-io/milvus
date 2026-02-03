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
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var ErrWALAccesserClosed = status.NewOnShutdownError("wal accesser closed")

// newWALAccesser creates a new wal accesser.
func newWALAccesser(c *clientv3.Client) *walAccesserImpl {
	// Create a new streaming coord client.
	streamingCoordClient := client.NewClient(c)
	// Create a new streamingnode handler client.
	handlerClient := handler.NewHandlerClient(streamingCoordClient.Assignment())
	w := &walAccesserImpl{
		lifetime:             typeutil.NewLifetime(),
		clusterID:            paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
		streamingCoordClient: streamingCoordClient,
		handlerClient:        handlerClient,
		producerMutex:        sync.Mutex{},
		producers:            make(map[string]*producer.ResumableProducer),

		// TODO: optimize the pool size, use the streaming api but not goroutines.
		appendExecutionPool:   conc.NewPool[struct{}](0),
		dispatchExecutionPool: conc.NewPool[struct{}](0),

		forwardService: newForwardService(streamingCoordClient),
	}
	w.SetLogger(log.With(log.FieldComponent("wal-accesser")))
	return w
}

// walAccesserImpl is the implementation of WALAccesser.
type walAccesserImpl struct {
	log.Binder
	lifetime  *typeutil.Lifetime
	clusterID string

	// All services
	streamingCoordClient client.Client
	handlerClient        handler.HandlerClient

	producerMutex         sync.Mutex
	producers             map[string]*producer.ResumableProducer
	appendExecutionPool   *conc.Pool[struct{}]
	dispatchExecutionPool *conc.Pool[struct{}]

	forwardService *forwardServiceImpl
}

func (w *walAccesserImpl) ForwardService() ForwardService {
	return w.forwardService
}

func (w *walAccesserImpl) Replicate() ReplicateService {
	return replicateService{w}
}

func (w *walAccesserImpl) Balancer() Balancer {
	return balancerImpl{w}
}

func (w *walAccesserImpl) Local() Local {
	return localServiceImpl{w}
}

// ControlChannel returns the control channel name of the wal.
func (w *walAccesserImpl) ControlChannel() string {
	last, err := w.streamingCoordClient.Assignment().GetLatestAssignments(context.Background())
	if err != nil {
		panic(err)
	}
	return funcutil.GetControlChannel(last.PChannelOfCChannel())
}

// RawAppend writes a record to the log.
func (w *walAccesserImpl) RawAppend(ctx context.Context, msg message.MutableMessage, opts ...AppendOption) (*types.AppendResult, error) {
	assertValidMessage(msg)

	msg = applyOpt(msg, opts...)

	resp := w.AppendMessages(ctx, msg)
	return resp.Responses[0].AppendResult, resp.Responses[0].Error
}

// Read returns a scanner for reading records from the wal.
func (w *walAccesserImpl) Read(ctx context.Context, opts ReadOption) Scanner {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return newErrScanner(ErrWALAccesserClosed)
	}
	defer w.lifetime.Done()

	if opts.VChannel == "" && opts.PChannel == "" {
		panic("pchannel is required if vchannel is not set")
	}

	if opts.VChannel != "" {
		pchannel := funcutil.ToPhysicalChannel(opts.VChannel)
		if opts.PChannel != "" && opts.PChannel != pchannel {
			panic("pchannel is not match with vchannel")
		}
		opts.PChannel = pchannel
	}
	// TODO: optimize the consumer into pchannel level.
	rc := consumer.NewResumableConsumer(w.handlerClient.CreateConsumer, &consumer.ConsumerOptions{
		PChannel:               opts.PChannel,
		VChannel:               opts.VChannel,
		DeliverPolicy:          opts.DeliverPolicy,
		DeliverFilters:         opts.DeliverFilters,
		MessageHandler:         opts.MessageHandler,
		IgnorePauseConsumption: opts.IgnorePauseConsumption,
	})
	return rc
}

// Broadcast returns a broadcast for broadcasting records to the wal.
func (w *walAccesserImpl) Broadcast() Broadcast {
	return broadcast{w}
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
	if w.appendExecutionPool != nil {
		w.appendExecutionPool.Release()
	}
	if w.dispatchExecutionPool != nil {
		w.dispatchExecutionPool.Release()
	}
}

// newErrScanner creates a scanner that returns an error.
func newErrScanner(err error) Scanner {
	ch := make(chan struct{})
	close(ch)
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
