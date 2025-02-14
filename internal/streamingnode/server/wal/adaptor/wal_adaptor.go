package adaptor

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ wal.WAL = (*walAdaptorImpl)(nil)

type gracefulCloseFunc func()

// adaptImplsToWAL creates a new wal from wal impls.
func adaptImplsToWAL(
	basicWAL walimpls.WALImpls,
	builders []interceptors.InterceptorBuilder,
	cleanup func(),
) wal.WAL {
	param := interceptors.InterceptorBuildParam{
		WALImpls: basicWAL,
		WAL:      syncutil.NewFuture[wal.WAL](),
	}
	wal := &walAdaptorImpl{
		lifetime:    typeutil.NewLifetime(),
		available:   make(chan struct{}),
		idAllocator: typeutil.NewIDAllocator(),
		inner:       basicWAL,
		// TODO: make the pool size configurable.
		appendExecutionPool:    conc.NewPool[struct{}](10),
		interceptorBuildResult: buildInterceptor(builders, param),
		scannerRegistry: scannerRegistry{
			channel:     basicWAL.Channel(),
			idAllocator: typeutil.NewIDAllocator(),
		},
		scanners:     typeutil.NewConcurrentMap[int64, wal.Scanner](),
		cleanup:      cleanup,
		writeMetrics: metricsutil.NewWriteMetrics(basicWAL.Channel(), basicWAL.WALName()),
		scanMetrics:  metricsutil.NewScanMetrics(basicWAL.Channel()),
		logger: resource.Resource().Logger().With(
			log.FieldComponent("wal"),
			zap.Any("channel", basicWAL.Channel()),
		),
	}
	param.WAL.Set(wal)
	return wal
}

// walAdaptorImpl is a wrapper of WALImpls to extend it into a WAL interface.
type walAdaptorImpl struct {
	lifetime               *typeutil.Lifetime
	available              chan struct{}
	idAllocator            *typeutil.IDAllocator
	inner                  walimpls.WALImpls
	appendExecutionPool    *conc.Pool[struct{}]
	interceptorBuildResult interceptorBuildResult
	scannerRegistry        scannerRegistry
	scanners               *typeutil.ConcurrentMap[int64, wal.Scanner]
	cleanup                func()
	writeMetrics           *metricsutil.WriteMetrics
	scanMetrics            *metricsutil.ScanMetrics
	logger                 *log.MLogger
}

func (w *walAdaptorImpl) WALName() string {
	return w.inner.WALName()
}

// Channel returns the channel info of wal.
func (w *walAdaptorImpl) Channel() types.PChannelInfo {
	return w.inner.Channel()
}

// Append writes a record to the log.
func (w *walAdaptorImpl) Append(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	// Check if interceptor is ready.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.interceptorBuildResult.Interceptor.Ready():
	}
	// Setup the term of wal.
	msg = msg.WithWALTerm(w.Channel().Term)

	// Metrics for append message.
	metricsGuard := w.writeMetrics.StartAppend(msg.MessageType(), msg.EstimateSize())

	// Execute the interceptor and wal append.
	var extraAppendResult utility.ExtraAppendResult
	ctx = utility.WithExtraAppendResult(ctx, &extraAppendResult)
	messageID, err := w.interceptorBuildResult.Interceptor.DoAppend(ctx, msg,
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			if notPersistHint := utility.GetNotPersisted(ctx); notPersistHint != nil {
				// do not persist the message if the hint is set.
				return notPersistHint.MessageID, nil
			}
			metricsGuard.StartWALImplAppend()
			msgID, err := w.inner.Append(ctx, msg)
			metricsGuard.FinishWALImplAppend()
			return msgID, err
		})
	if err != nil {
		metricsGuard.Finish(err)
		return nil, err
	}
	var extra *anypb.Any
	if extraAppendResult.Extra != nil {
		var err error
		if extra, err = anypb.New(extraAppendResult.Extra); err != nil {
			panic("unreachable: failed to marshal extra append result")
		}
	}

	// unwrap the messageID if needed.
	r := &wal.AppendResult{
		MessageID: messageID,
		TimeTick:  extraAppendResult.TimeTick,
		TxnCtx:    extraAppendResult.TxnCtx,
		Extra:     extra,
	}
	metricsGuard.Finish(nil)
	return r, nil
}

// AppendAsync writes a record to the log asynchronously.
func (w *walAdaptorImpl) AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(*wal.AppendResult, error)) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		cb(nil, status.NewOnShutdownError("wal is on shutdown"))
		return
	}

	// Submit async append to a background execution pool.
	_ = w.appendExecutionPool.Submit(func() (struct{}, error) {
		defer w.lifetime.Done()

		msgID, err := w.Append(ctx, msg)
		cb(msgID, err)
		return struct{}{}, nil
	})
}

// Read returns a scanner for reading records from the wal.
func (w *walAdaptorImpl) Read(ctx context.Context, opts wal.ReadOption) (wal.Scanner, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	name, err := w.scannerRegistry.AllocateScannerName()
	if err != nil {
		return nil, err
	}
	// wrap the scanner with cleanup function.
	id := w.idAllocator.Allocate()
	s := newScannerAdaptor(
		name,
		w.inner,
		opts,
		w.scanMetrics.NewScannerMetrics(),
		func() { w.scanners.Remove(id) })
	w.scanners.Insert(id, s)
	return s, nil
}

// IsAvailable returns whether the wal is available.
func (w *walAdaptorImpl) IsAvailable() bool {
	select {
	case <-w.available:
		return false
	default:
		return true
	}
}

// Available returns a channel that will be closed when the wal is shut down.
func (w *walAdaptorImpl) Available() <-chan struct{} {
	return w.available
}

// Close overrides Scanner Close function.
func (w *walAdaptorImpl) Close() {
	logger := w.logger.With(zap.String("processing", "WALClose"))
	logger.Info("wal begin to close, start graceful close...")
	// graceful close the interceptors before wal closing.
	w.interceptorBuildResult.GracefulCloseFunc()

	logger.Info("wal graceful close done, wait for operation to be finished...")

	// begin to close the wal.
	w.lifetime.SetState(typeutil.LifetimeStateStopped)
	w.lifetime.Wait()
	close(w.available)

	logger.Info("wal begin to close scanners...")

	// close all wal instances.
	w.scanners.Range(func(id int64, s wal.Scanner) bool {
		s.Close()
		log.Info("close scanner by wal adaptor", zap.Int64("id", id), zap.Any("channel", w.Channel()))
		return true
	})

	logger.Info("scanner close done, close inner wal...")
	w.inner.Close()

	logger.Info("wal close done, close interceptors...")
	w.interceptorBuildResult.Close()
	w.appendExecutionPool.Free()

	logger.Info("call wal cleanup function...")
	w.cleanup()
	logger.Info("wal closed")

	// close all metrics.
	w.scanMetrics.Close()
	w.writeMetrics.Close()
}

type interceptorBuildResult struct {
	Interceptor       interceptors.InterceptorWithReady
	GracefulCloseFunc gracefulCloseFunc
}

func (r interceptorBuildResult) Close() {
	r.Interceptor.Close()
}

// newWALWithInterceptors creates a new wal with interceptors.
func buildInterceptor(builders []interceptors.InterceptorBuilder, param interceptors.InterceptorBuildParam) interceptorBuildResult {
	// Build all interceptors.
	builtIterceptors := make([]interceptors.Interceptor, 0, len(builders))
	for _, b := range builders {
		builtIterceptors = append(builtIterceptors, b.Build(param))
	}
	return interceptorBuildResult{
		Interceptor: interceptors.NewChainedInterceptor(builtIterceptors...),
		GracefulCloseFunc: func() {
			for _, i := range builtIterceptors {
				if c, ok := i.(interceptors.InterceptorWithGracefulClose); ok {
					c.GracefulClose()
				}
			}
		},
	}
}
