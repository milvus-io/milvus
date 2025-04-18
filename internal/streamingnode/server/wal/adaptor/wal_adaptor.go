package adaptor

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher/flusherimpl"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ wal.WAL = (*walAdaptorImpl)(nil)

type gracefulCloseFunc func()

// adaptImplsToWAL creates a new wal from wal impls.
func adaptImplsToWAL(
	ctx context.Context,
	basicWAL walimpls.WALImpls,
	builders []interceptors.InterceptorBuilder,
	cleanup func(),
) (wal.WAL, error) {
	logger := resource.Resource().Logger().With(
		log.FieldComponent("wal"),
		zap.String("channel", basicWAL.Channel().String()),
	)
	roWAL := &roWALAdaptorImpl{
		roWALImpls:  basicWAL,
		lifetime:    typeutil.NewLifetime(),
		available:   make(chan struct{}),
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
	if basicWAL.Channel().AccessMode == types.AccessModeRO {
		// if the wal is read-only, return it directly.
		return roWAL, nil
	}

	// recover the wal state.
	param, err := buildInterceptorParams(ctx, basicWAL)
	if err != nil {
		return nil, err
	}
	rs, snapshot, err := recovery.RecoverRecoveryStorage(ctx, newRecoveryStreamBuilder(roWAL), param.LastTimeTickMessage)
	if err != nil {
		return nil, err
	}
	param.InitialRecoverSnapshot = snapshot
	param.TxnManager = txn.NewTxnManager(param.ChannelInfo, snapshot.TxnBuffer)

	// start the flusher to flush and generate recovery info.
	flusher := flusherimpl.RecoverWALFlusher(&flusherimpl.RecoverWALFlusherParam{
		WAL:             param.WAL,
		RecoveryStorage: rs,
		ChannelInfo:     basicWAL.Channel(),
	})

	// build append interceptor for a wal.
	wal := &walAdaptorImpl{
		roWALAdaptorImpl: roWAL,
		rwWALImpls:       basicWAL,
		// TODO: remove the pool, use a queue instead.
		appendExecutionPool:    conc.NewPool[struct{}](0),
		param:                  param,
		interceptorBuildResult: buildInterceptor(builders, param),
		flusher:                flusher,
		writeMetrics:           metricsutil.NewWriteMetrics(basicWAL.Channel(), basicWAL.WALName()),
	}
	param.WAL.Set(wal)
	return wal, nil
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

	// Check if interceptor is ready.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.available:
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
				appendMetrics.NotPersisted()
				return notPersistHint.MessageID, nil
			}
			metricsGuard.StartWALImplAppend()
			msgID, err := w.rwWALImpls.Append(ctx, msg)
			metricsGuard.FinishWALImplAppend()
			return msgID, err
		})
	metricsGuard.FinishAppend()
	if err != nil {
		appendMetrics.Done(nil, err)
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
	close(w.available)
	w.lifetime.Wait()

	// close the flusher.
	w.Logger().Info("wal begin to close flusher...")
	w.flusher.Close()

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
