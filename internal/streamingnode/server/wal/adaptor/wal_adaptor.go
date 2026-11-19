package adaptor

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/atomic"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/adaptor/rate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ wal.WAL = (*walAdaptorImpl)(nil)

type gracefulCloseFunc func()

// adaptImplsToROWAL creates a new readonly wal from wal impls.
func adaptImplsToROWAL(
	basicWAL walimpls.WALImpls,
	cleanup func(),
) *roWALAdaptorImpl {
	logger := resource.Resource().Logger().With(
		mlog.FieldComponent("wal"),
		mlog.String("channel", basicWAL.Channel().String()),
	)
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // cancel is stored in availableCancel and called in Close()
	roWAL := &roWALAdaptorImpl{
		WALRateLimitComponent: rate.NewWALRateLimitComponent(basicWAL.Channel()),

		roWALImpls:      basicWAL,
		lifetime:        typeutil.NewLifetime(),
		availableCtx:    ctx,
		availableCancel: cancel,
		idAllocator:     typeutil.NewIDAllocator(),
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
		interceptorBuildResult: buildInterceptorsAndReleaseInitialSnapshot(builders, interceptorParam),
		writeMetrics:           metricsutil.NewWriteMetrics(roWAL.Channel(), roWAL.WALName()),
		isFenced:               atomic.NewBool(false),
		appendRateCounter:      utility.NewAverageRateCounter(10 * time.Second), // 10 second sliding window
	}
	wal.writeMetrics.SetLogger(wal.Logger())
	interceptorParam.WAL.Set(wal)
	wal.RegisterMemoryObserver()
	wal.RegisterAppendRateObserver(wal.appendRateCounter)
	return wal
}

// walAdaptorImpl is a wrapper of WALImpls to extend it into a WAL interface.
type walAdaptorImpl struct {
	queryViewHandler    *snview.SNQueryViewHandler
	viewResourceManager *vchannel.PChannelRecoveryManager
	*roWALAdaptorImpl

	rwWALImpls             walimpls.WALImpls
	appendExecutionPool    *conc.Pool[struct{}]
	param                  *interceptors.InterceptorBuildParam
	interceptorBuildResult interceptorBuildResult
	writeMetrics           *metricsutil.WriteMetrics
	isFenced               *atomic.Bool
	appendRateCounter      *utility.AverageRateCounter // tracks append rate (bytes/sec)
	chunkPayloadSizeValue  atomic.Int64                // cached payload budget per chunked WAL record; 0 until first computed.
	chunkPayloadSizeAt     atomic.Int64                // unixnano of the last chunk-payload-size recompute.
}

// Metrics returns the metrics of the wal.
func (w *walAdaptorImpl) Metrics() types.WALMetrics {
	confirmedTimeTick := w.param.MVCCManager.GetConfirmedTimeTick()
	recoveryTimeTick := uint64(0)
	if w.param.RecoveryStorage != nil {
		recoveryTimeTick = w.param.RecoveryStorage.Metrics().RecoveryTimeTick
	}
	return types.RWWALMetrics{
		ChannelInfo:      w.Channel(),
		MVCCTimeTick:     confirmedTimeTick,
		RecoveryTimeTick: recoveryTimeTick,
	}
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

// GetReplicateCheckpoint returns the replicate checkpoint of the wal.
func (w *walAdaptorImpl) GetReplicateCheckpoint() (*utility.ReplicateCheckpoint, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	return w.param.ReplicateManager.GetReplicateCheckpoint()
}

// GetSalvageCheckpoint returns all salvage checkpoints captured during force promote.
func (w *walAdaptorImpl) GetSalvageCheckpoint() []*utility.ReplicateCheckpoint {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil
	}
	defer w.lifetime.Done()

	return w.param.ReplicateManager.GetSalvageCheckpoint()
}

// Append writes a record to the log.
func (w *walAdaptorImpl) Append(ctx context.Context, msg message.MutableMessage) (_ *wal.AppendResult, err error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	ctx, span := message.StartSpanForMessage(ctx, msg, message.SpanNameWALAppend)
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()

	if w.isFenced.Load() {
		// if the wal is fenced, we should reject all append operations.
		return nil, status.NewChannelFenced(w.Channel().String())
	}

	if msg.MessageType().IsDMLMessageType() && w.IsRejected() {
		// if the wal is rate limit rejected, we reject all the DML operation to protect the wal from being overloaded.
		return nil, status.NewRateLimitRejected("")
	}

	// Check if interceptor is ready.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-w.availableCtx.Done():
		return nil, status.NewOnShutdownError("wal is on shutdown")
	case <-w.interceptorBuildResult.Interceptor.Ready():
	}

	// Setup the term of wal.
	msg = msg.WithWALTerm(w.Channel().Term)
	ctx = redo.WithCallerContext(ctx)

	// we need to promise the state of wal kept consistent with the memory state of streamingnode.
	// So we don't allow the append operation can be canceled by the append caller to avoid leave a inconsistent state of alive wal,
	// the wal append operation can only be canceled when the wal is shutting down.
	ctx, cancel := contextutil.MergeContext(context.WithoutCancel(ctx), w.availableCtx)
	defer cancel()

	appendMetrics := w.writeMetrics.StartAppend(msg)
	ctx = utility.WithAppendMetricsContext(ctx, appendMetrics)

	// Metrics for append message.
	metricsGuard := appendMetrics.StartAppendGuard()

	// Execute the interceptor and wal append.
	var extraAppendResult utility.ExtraAppendResult
	ctx = utility.WithExtraAppendResult(ctx, &extraAppendResult)
	messageID, err := w.interceptorBuildResult.Interceptor.DoAppend(ctx, msg,
		func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
			// The writer may have been fenced while this append waited for the
			// interceptor lock. Recheck before attempting a physical write.
			if w.isFenced.Load() {
				return nil, walimpls.ErrFenced
			}

			if notPersistHint := utility.GetNotPersisted(ctx); notPersistHint != nil {
				// do not persist the message if the hint is set.
				return notPersistHint.MessageID, nil
			}

			metricsGuard.StartWALImplAppend()
			msgID, err := w.appendWithOptionalChunking(ctx, msg)
			metricsGuard.FinishWALImplAppend()
			if err != nil {
				return msgID, err
			}

			if message.IsWALSwitch(msg) {
				// Publish the new backend's budget before releasing the exclusive
				// PChannel lock and admitting any waiting business append.
				w.chunkPayloadSizeAt.Store(0)
			}

			return msgID, nil
		})
	metricsGuard.FinishAppend()
	if err != nil {
		appendMetrics.Done(ctx, nil, err)
		if errors.Is(err, walimpls.ErrFenced) {
			// if the append operation of wal is fenced, we should report the error to the client.
			if w.isFenced.CompareAndSwap(false, true) {
				w.forceCancelAfterGracefulTimeout()
				w.Logger().Warn(ctx, "wal is fenced, mark as unavailable, all append opertions will be rejected", mlog.Err(err))
			}
			return nil, status.NewChannelFenced(w.Channel().String())
		}
		return nil, err
	}
	w.appendRateCounter.Add(int64(msg.EstimateSize()))

	var extra *anypb.Any
	if extraAppendResult.Extra != nil {
		var err error
		if extra, err = anypb.New(extraAppendResult.Extra); err != nil {
			panic("unreachable: failed to marshal extra append result")
		}
	}

	// unwrap the messageID if needed.
	r := &wal.AppendResult{
		MessageID:              messageID,
		LastConfirmedMessageID: extraAppendResult.LastConfirmedMessageID,
		TimeTick:               extraAppendResult.TimeTick,
		TxnCtx:                 extraAppendResult.TxnCtx,
		Extra:                  extra,
	}
	appendMetrics.Done(ctx, r, nil)
	return r, nil
}

// appendWithOptionalChunking appends a logical message, splitting it into WAL
// records only when streaming.splitChunkSN is enabled. During a rolling
// upgrade, proxy.splitChunk stays enabled until every StreamingNode has been
// upgraded and observed streaming.splitChunkSN=true.
//
// The timetick durability barrier does not depend on record adjacency: a
// TimeTick(T) message is only published after every message with ts <= T has
// been fully appended and acknowledged (the ack manager advances its
// watermark over the consecutive acknowledged prefix, and a chunked message
// acknowledges exactly once -- when its whole run is persisted), so no
// downstream component can ever observe a half-written pack.
func (w *walAdaptorImpl) appendWithOptionalChunking(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	// A switch link must be one complete physical record: splitting it would
	// switch the backend before its remaining chunks were appended.
	if msg.MessageType() == message.MessageTypeAlterWAL || !paramtable.Get().StreamingCfg.SplitChunkSN.GetAsBool() {
		return w.appendOne(ctx, msg)
	}

	chunkPayloadSize := w.chunkPayloadSize()
	if chunkPayloadSize <= 0 || len(msg.IntoMessageProto().GetPayload()) <= chunkPayloadSize {
		return w.appendOne(ctx, msg)
	}

	// Split into chunks when the payload exceeds the backend's per-message
	// limit, so the backend never sees an oversized record. Backends without
	// a per-record cap (RocksMQ) get a zero budget and keep the pre-chunking
	// single-record behavior. The successful append attempt of the first chunk
	// supplies the logical message ID returned to the caller. On durable replay,
	// the assembler keeps the later payload-identical retry observation, so its
	// reassembled ID matches the acknowledged one.
	chunks := message.SplitIntoChunks(msg, chunkPayloadSize)

	var firstID message.MessageID
	for i, chunk := range chunks {
		msgID, err := w.appendOne(ctx, chunk)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			firstID = msgID
		}
	}
	return firstID, nil
}

// chunkPayloadSizeRefresh bounds how stale the cached chunk-payload budget may
// be. The underlying config items (pulsar.maxMessageSize,
// pulsar.messageReserveSize) are refreshable; a live DOWNWARD refresh takes
// effect for chunking within this window. Note a message already stuck in
// autowalimpls' physical retry loop never re-reads config regardless --
// that exposure is pre-existing, and the cache only widens it by this window
// for newly arriving appends.
const chunkPayloadSizeRefresh = time.Second

// chunkPayloadSize reports the payload-size budget per WAL record: the active
// backend's enforced limit, or Woodpecker's configured Milvus chunk threshold,
// minus reserve headroom for properties, cipher metadata, and the backend
// envelope. A zero value disables chunking because no limit/threshold is
// configured (RocksMQ or an unrecognized WAL); bounded WAL configuration is
// normalized to at least 256 KiB before reaching this path.
//
// Served from a cache refreshed at most once per chunkPayloadSizeRefresh:
// GetConfig traverses two concurrent lookup structures plus source resolution,
// which is not per-append work the hot path should pay.
func (w *walAdaptorImpl) chunkPayloadSize() int {
	now := time.Now().UnixNano()
	last := w.chunkPayloadSizeAt.Load()
	if last != 0 && now-last < int64(chunkPayloadSizeRefresh) {
		return int(w.chunkPayloadSizeValue.Load())
	}
	size := w.computeChunkPayloadSize()
	w.chunkPayloadSizeValue.Store(int64(size))
	w.chunkPayloadSizeAt.Store(now)
	return size
}

func (w *walAdaptorImpl) computeChunkPayloadSize() int {
	walName := w.rwWALImpls.WALName()
	params := paramtable.Get()
	maxMessageSize := params.WALMaxMessageSize(walName.String())
	_, reserve := params.PulsarCfg.GetMessageSizeLimitsFor(maxMessageSize)
	if maxMessageSize <= 0 || reserve < 0 || reserve >= maxMessageSize {
		return 0
	}
	return maxMessageSize - reserve
}

// appendOne observes one call into the WAL implementation, including all of
// autowalimpls' internal retries. A successful append must remain successful so
// the interceptors can complete their in-memory state transitions.
func (w *walAdaptorImpl) appendOne(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	ctx, span := message.StartSpanForMessage(ctx, msg, message.SpanNameWALAppendImpl)
	defer span.End()
	message.OverwriteTraceContext(ctx, msg)
	id, err := w.rwWALImpls.Append(ctx, msg)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return id, err
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
	w.Logger().Info(context.TODO(), "wal begin to close, start graceful close...")
	// graceful close the interceptors before wal closing.
	w.interceptorBuildResult.GracefulCloseFunc()
	w.Logger().Info(context.TODO(), "wal graceful close done, wait for operation to be finished...")

	// begin to close the wal.
	w.lifetime.SetState(typeutil.LifetimeStateStopped)
	w.forceCancelAfterGracefulTimeout()
	w.lifetime.Wait()
	if w.queryViewHandler != nil {
		w.queryViewHandler.CloseForHandoff()
	}

	// close the recovery-owned persistence path.
	w.Logger().Info(context.TODO(), "wal begin to close recovery storage...")
	if w.param.RecoveryStorage != nil {
		w.param.RecoveryStorage.Close()
	}

	w.Logger().Info(context.TODO(), "wal begin to close scanners...")

	// close all wal instances.
	w.scanners.Range(func(id int64, s wal.Scanner) bool {
		s.Close()
		mlog.Info(context.TODO(), "close scanner by wal adaptor", mlog.Int64("id", id), mlog.Any("channel", w.Channel()))
		return true
	})

	w.Logger().Info(context.TODO(), "scanner close done, close inner wal...")
	w.rwWALImpls.Close()

	w.Logger().Info(context.TODO(), "wal close done, close interceptors...")
	w.interceptorBuildResult.Close()

	w.Logger().Info(context.TODO(), "close the write ahead buffer...")
	w.param.WriteAheadBuffer.Close()

	w.Logger().Info(context.TODO(), "close the segment assignment manager...")
	w.param.ShardManager.Close()

	w.Logger().Info(context.TODO(), "call wal cleanup function...")
	w.cleanup()
	w.Logger().Info(context.TODO(), "wal closed")

	// close all metrics.
	w.scanMetrics.Close()
	w.writeMetrics.Close()

	// close the rate limit component.
	w.WALRateLimitComponent.Close()

	if w.appendExecutionPool != nil {
		w.appendExecutionPool.Release()
	}
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

func buildInterceptorsAndReleaseInitialSnapshot(
	builders []interceptors.InterceptorBuilder,
	param *interceptors.InterceptorBuildParam,
) interceptorBuildResult {
	result := buildInterceptor(builders, param)
	param.InitialRecoverSnapshot = nil
	return result
}

func (w *walAdaptorImpl) TransformLog() wal.TransformLogAccesser {
	return w.viewResourceManager
}
