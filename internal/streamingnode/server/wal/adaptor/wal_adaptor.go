package adaptor

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher/flusherimpl"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ wal.WAL = (*walAdaptorImpl)(nil)

type gracefulCloseFunc func()

// adaptImplsToROWAL creates a new readonly wal from wal impls.
func adaptImplsToROWAL(
	basicWAL walimpls.WALImpls,
	cleanup func(),
) *roWALAdaptorImpl {
	logger := resource.Resource().Logger().With(
		log.FieldComponent("wal"),
		zap.String("channel", basicWAL.Channel().String()),
	)
	roWAL := &roWALAdaptorImpl{
		roWALImpls:  basicWAL,
		lifetime:    typeutil.NewLifetime(),
		available:   lifetime.NewSafeChan(),
		idAllocator: typeutil.NewIDAllocator(),
		scannerRegistry: scannerRegistry{
			channel:     basicWAL.Channel(),
			idAllocator: typeutil.NewIDAllocator(),
		},
		scanners:    typeutil.NewConcurrentMap[int64, wal.Scanner](),
		cleanup:     cleanup,
		scanMetrics: metricsutil.NewScanMetrics(basicWAL.Channel()),
	}
	roWAL.SetLogger(logger)
	return roWAL
}

// adaptImplsToRWWAL creates a new wal from wal impls.
func adaptImplsToRWWAL(
	roWAL *roWALAdaptorImpl,
	builders []interceptors.InterceptorBuilder,
	interceptorParam *interceptors.InterceptorBuildParam,
	flusher *flusherimpl.WALFlusherImpl,
) *walAdaptorImpl {
	if roWAL.Channel().AccessMode != types.AccessModeRW {
		panic("wal should be read-write")
	}
	// build append interceptor for a wal.
	wal := &walAdaptorImpl{
		roWALAdaptorImpl: roWAL,
		rwWALImpls:       roWAL.roWALImpls.(walimpls.WALImpls),
		// TODO: remove the pool, use a queue instead.
		appendExecutionPool:    conc.NewPool[struct{}](0),
		param:                  interceptorParam,
		interceptorBuildResult: buildInterceptor(builders, interceptorParam),
		flusher:                flusher,
		writeMetrics:           metricsutil.NewWriteMetrics(roWAL.Channel(), roWAL.WALName()),
		isFenced:               atomic.NewBool(false),
	}
	wal.writeMetrics.SetLogger(wal.roWALAdaptorImpl.Logger())
	interceptorParam.WAL.Set(wal)
	return wal
}

// walAdaptorImpl is a wrapper of WALImpls to extend it into a WAL interface.
type walAdaptorImpl struct {
	*roWALAdaptorImpl

	rwWALImpls             walimpls.WALImpls
	appendExecutionPool    *conc.Pool[struct{}]
	param                  *interceptors.InterceptorBuildParam
	interceptorBuildResult interceptorBuildResult
	flusher                *flusherimpl.WALFlusherImpl
	writeMetrics           *metricsutil.WriteMetrics
	isFenced               *atomic.Bool
}

// GetLatestMVCCTimestamp get the latest mvcc timestamp of the wal at vchannel.
func (w *walAdaptorImpl) GetLatestMVCCTimestamp(ctx context.Context, vchannel string) (uint64, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return 0, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()
	currentMVCC := w.param.MVCCManager.GetMVCCOfVChannel(vchannel)
	if !currentMVCC.Confirmed {
		// if the mvcc is not confirmed, trigger a sync operation to make it confirmed as soon as possible.
		resource.Resource().TimeTickInspector().TriggerSync(w.rwWALImpls.Channel(), false)
	}
	return currentMVCC.Timetick, nil
}

// Append writes a record to the log.
func (w *walAdaptorImpl) Append(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	if w.isFenced.Load() {
		// if the wal is fenced, we should reject all append operations.
		return nil, status.NewChannelFenced(w.Channel().String())
	}

	// Check if interceptor is ready.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.available.CloseCh():
		return nil, status.NewOnShutdownError("wal is on shutdown")
	case <-w.interceptorBuildResult.Interceptor.Ready():
	}

	// Setup the term of wal.
	msg = msg.WithWALTerm(w.Channel().Term)

	appendMetrics := w.writeMetrics.StartAppend(msg)
	ctx = utility.WithAppendMetricsContext(ctx, appendMetrics)

	// Metrics for append message.
	metricsGuard := appendMetrics.StartAppendGuard()

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
			msgID, err := w.retryAppendWhenRecoverableError(ctx, msg)
			metricsGuard.FinishWALImplAppend()
			return msgID, err
		})
	metricsGuard.FinishAppend()
	if err != nil {
		appendMetrics.Done(nil, err)
		if errors.Is(err, walimpls.ErrFenced) {
			// if the append operation of wal is fenced, we should report the error to the client.
			if w.isFenced.CompareAndSwap(false, true) {
				w.available.Close()
				w.Logger().Warn("wal is fenced, mark as unavailable, all append opertions will be rejected", zap.Error(err))
			}
			return nil, status.NewChannelFenced(w.Channel().String())
		}
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
	appendMetrics.Done(r, nil)
	return r, nil
}

// retryAppendWhenRecoverableError retries the append operation when recoverable error occurs.
func (w *walAdaptorImpl) retryAppendWhenRecoverableError(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = 5 * time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()

	// An append operation should be retried until it succeeds or some unrecoverable error occurs.
	for i := 0; ; i++ {
		msgID, err := w.rwWALImpls.Append(ctx, msg)
		if err == nil {
			return msgID, nil
		}
		if errors.IsAny(err, context.Canceled, context.DeadlineExceeded, walimpls.ErrFenced) {
			return nil, err
		}
		w.writeMetrics.ObserveRetry()
		nextInterval := backoff.NextBackOff()
		w.Logger().Warn("append message into wal impls failed, retrying...", log.FieldMessage(msg), zap.Int("retry", i), zap.Duration("nextInterval", nextInterval), zap.Error(err))

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-w.available.CloseCh():
			return nil, status.NewOnShutdownError("wal is on shutdown")
		case <-time.After(nextInterval):
		}
	}
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

// Close overrides Scanner Close function.
func (w *walAdaptorImpl) Close() {
	w.Logger().Info("wal begin to close, start graceful close...")
	// graceful close the interceptors before wal closing.
	w.interceptorBuildResult.GracefulCloseFunc()
	w.Logger().Info("wal graceful close done, wait for operation to be finished...")

	// begin to close the wal.
	w.lifetime.SetState(typeutil.LifetimeStateStopped)
	w.available.Close()
	w.lifetime.Wait()

	// close the flusher.
	w.Logger().Info("wal begin to close flusher...")
	if w.flusher != nil {
		// only in test, the flusher is nil.
		w.flusher.Close()
	}

	w.Logger().Info("wal begin to close scanners...")

	// close all wal instances.
	w.scanners.Range(func(id int64, s wal.Scanner) bool {
		s.Close()
		log.Info("close scanner by wal adaptor", zap.Int64("id", id), zap.Any("channel", w.Channel()))
		return true
	})

	w.Logger().Info("scanner close done, close inner wal...")
	w.rwWALImpls.Close()

	w.Logger().Info("wal close done, close interceptors...")
	w.interceptorBuildResult.Close()
	w.appendExecutionPool.Free()

	w.Logger().Info("close the write ahead buffer...")
	w.param.WriteAheadBuffer.Close()

	w.Logger().Info("close the segment assignment manager...")
	w.param.ShardManager.Close()

	w.Logger().Info("call wal cleanup function...")
	w.cleanup()
	w.Logger().Info("wal closed")

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
func buildInterceptor(builders []interceptors.InterceptorBuilder, param *interceptors.InterceptorBuildParam) interceptorBuildResult {
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
