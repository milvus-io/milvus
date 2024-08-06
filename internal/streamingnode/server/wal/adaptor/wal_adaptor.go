package adaptor

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ wal.WAL = (*walAdaptorImpl)(nil)

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
	interceptor := buildInterceptor(builders, param)

	wal := &walAdaptorImpl{
		lifetime:    lifetime.NewLifetime(lifetime.Working),
		idAllocator: typeutil.NewIDAllocator(),
		inner:       basicWAL,
		// TODO: make the pool size configurable.
		appendExecutionPool: conc.NewPool[struct{}](10),
		interceptor:         interceptor,
		scannerRegistry: scannerRegistry{
			channel:     basicWAL.Channel(),
			idAllocator: typeutil.NewIDAllocator(),
		},
		scanners: typeutil.NewConcurrentMap[int64, wal.Scanner](),
		cleanup:  cleanup,
	}
	param.WAL.Set(wal)
	return wal
}

// walAdaptorImpl is a wrapper of WALImpls to extend it into a WAL interface.
type walAdaptorImpl struct {
	lifetime            lifetime.Lifetime[lifetime.State]
	idAllocator         *typeutil.IDAllocator
	inner               walimpls.WALImpls
	appendExecutionPool *conc.Pool[struct{}]
	interceptor         interceptors.InterceptorWithReady
	scannerRegistry     scannerRegistry
	scanners            *typeutil.ConcurrentMap[int64, wal.Scanner]
	cleanup             func()
}

func (w *walAdaptorImpl) WALName() string {
	return w.inner.WALName()
}

// Channel returns the channel info of wal.
func (w *walAdaptorImpl) Channel() types.PChannelInfo {
	return w.inner.Channel()
}

// Append writes a record to the log.
func (w *walAdaptorImpl) Append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	if w.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	// Check if interceptor is ready.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.interceptor.Ready():
	}

	// Execute the interceptor and wal append.
	return w.interceptor.DoAppend(ctx, msg, w.inner.Append)
}

// AppendAsync writes a record to the log asynchronously.
func (w *walAdaptorImpl) AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(message.MessageID, error)) {
	if w.lifetime.Add(lifetime.IsWorking) != nil {
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
	if w.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	name, err := w.scannerRegistry.AllocateScannerName()
	if err != nil {
		return nil, err
	}
	// wrap the scanner with cleanup function.
	id := w.idAllocator.Allocate()
	s := newScannerAdaptor(name, w.inner, opts, func() {
		w.scanners.Remove(id)
	})
	w.scanners.Insert(id, s)
	return s, nil
}

// IsAvailable returns whether the wal is available.
func (w *walAdaptorImpl) IsAvailable() bool {
	return !w.lifetime.IsClosed()
}

// Available returns a channel that will be closed when the wal is shut down.
func (w *walAdaptorImpl) Available() <-chan struct{} {
	return w.lifetime.CloseCh()
}

// Close overrides Scanner Close function.
func (w *walAdaptorImpl) Close() {
	w.lifetime.SetState(lifetime.Stopped)
	w.lifetime.Wait()
	w.lifetime.Close()

	// close all wal instances.
	w.scanners.Range(func(id int64, s wal.Scanner) bool {
		s.Close()
		log.Info("close scanner by wal extend", zap.Int64("id", id), zap.Any("channel", w.Channel()))
		return true
	})
	w.inner.Close()
	w.interceptor.Close()
	w.appendExecutionPool.Free()
	w.cleanup()
}

// newWALWithInterceptors creates a new wal with interceptors.
func buildInterceptor(builders []interceptors.InterceptorBuilder, param interceptors.InterceptorBuildParam) interceptors.InterceptorWithReady {
	// Build all interceptors.
	builtIterceptors := make([]interceptors.BasicInterceptor, 0, len(builders))
	for _, b := range builders {
		builtIterceptors = append(builtIterceptors, b.Build(param))
	}
	return interceptors.NewChainedInterceptor(builtIterceptors...)
}
