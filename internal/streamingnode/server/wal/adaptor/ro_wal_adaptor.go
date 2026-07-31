package adaptor

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/adaptor/rate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ wal.WAL = (*roWALAdaptorImpl)(nil)

const defaultHistoricalWALFallbackTimeout = 30 * time.Second

type historicalWALOpener func(
	ctx context.Context,
	walName message.WALName,
	channel types.PChannelInfo,
) (walimpls.ROWALImpls, error)

type resolvedReadWAL struct {
	wal                     walimpls.ROWALImpls
	deliverPolicy           options.DeliverPolicy
	exclusiveStartMessageID message.MessageID
}

type roWALAdaptorImpl struct {
	*rate.WALRateLimitComponent
	mlog.Binder

	lifetime            *typeutil.Lifetime
	availableCtx        context.Context
	availableCancel     context.CancelFunc
	idAllocator         *typeutil.IDAllocator
	roWALImpls          walimpls.ROWALImpls
	historicalWALOpener historicalWALOpener
	// historicalWALFallbackTimeout bounds retries against an old backend that
	// may already have been decommissioned. Zero uses the production default.
	historicalWALFallbackTimeout time.Duration
	scannerRegistry              scannerRegistry
	scanners                     *typeutil.ConcurrentMap[int64, wal.Scanner]
	cleanup                      func()
	scanMetrics                  *metricsutil.ScanMetrics
	forceRecovery                bool
}

func (w *roWALAdaptorImpl) WALName() message.WALName {
	return w.roWALImpls.WALName()
}

// Channel returns the channel info of wal.
func (w *roWALAdaptorImpl) Channel() types.PChannelInfo {
	return w.roWALImpls.Channel()
}

// Metrics returns the metrics of the wal.
func (w *roWALAdaptorImpl) Metrics() types.WALMetrics {
	return types.ROWALMetrics{
		ChannelInfo: w.Channel(),
	}
}

func (w *roWALAdaptorImpl) GetLatestMVCCTimestamp(ctx context.Context, vchannel string) (uint64, error) {
	panic("we cannot acquire lastest mvcc timestamp from a read only wal")
}

func (w *roWALAdaptorImpl) GetReplicateCheckpoint() (*wal.ReplicateCheckpoint, error) {
	panic("we cannot get replicate checkpoint from a read only wal")
}

func (w *roWALAdaptorImpl) GetSalvageCheckpoint() []*wal.ReplicateCheckpoint {
	panic("we cannot get salvage checkpoint from a read only wal")
}

// Append writes a record to the log.
func (w *roWALAdaptorImpl) Append(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
	panic("we cannot append message into a read only wal")
}

// Append a record to the log asynchronously.
func (w *roWALAdaptorImpl) AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(*wal.AppendResult, error)) {
	panic("we cannot append message into a read only wal")
}

// ForceRecovery force recovery wal, currently only used for Alter WAL
func (w *roWALAdaptorImpl) ForceRecovery(forceRecovery bool) {
	w.forceRecovery = forceRecovery
}

// Read returns a scanner for reading records from the wal.
func (w *roWALAdaptorImpl) Read(ctx context.Context, opts wal.ReadOption) (wal.Scanner, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()

	resolved, err := w.resolveReadWAL(ctx, opts)
	if err != nil {
		return nil, err
	}
	readWAL := resolved.wal
	opts.DeliverPolicy = resolved.deliverPolicy

	name, err := w.scannerRegistry.AllocateScannerName()
	if err != nil {
		if readWAL.WALName() != w.WALName() {
			readWAL.Close()
		}
		return nil, err
	}
	// wrap the scanner with cleanup function.
	id := w.idAllocator.Allocate()
	s := newScannerAdaptor(
		name,
		w.roWALImpls,
		readWAL,
		opts,
		switchableScannerOptions{
			historicalWALOpener:               w.openHistoricalWALWithFallback,
			historicalWALFallbackTimeout:      w.getHistoricalWALFallbackTimeout(),
			historicalStartExclusiveMessageID: resolved.exclusiveStartMessageID,
		},
		w.scanMetrics.NewScannerMetrics(),
		func() { w.scanners.Remove(id) },
		w.forceRecovery)
	w.scanners.Insert(id, s)
	return s, nil
}

func (w *roWALAdaptorImpl) resolveReadWAL(ctx context.Context, opts wal.ReadOption) (*resolvedReadWAL, error) {
	msgWALName, ok := getDeliverPolicyWALName(opts.DeliverPolicy)
	if !ok || msgWALName == w.WALName() {
		return &resolvedReadWAL{wal: w.roWALImpls, deliverPolicy: opts.DeliverPolicy}, nil
	}
	if msgWALName == message.WALNameUnknown || msgWALName.String() == "" {
		return nil, status.NewWALNameMismatchError(w.WALName().String(), "unknown")
	}

	deliverPolicy, exclusiveStartMessageID, err := prepareHistoricalDeliverPolicy(opts.DeliverPolicy)
	if err != nil {
		w.Logger().Info(ctx, "invalid historical WAL position",
			mlog.Stringer("historicalWALName", msgWALName),
			mlog.Stringer("currentWALName", w.WALName()),
			mlog.Err(err))
		return nil, status.NewWALNameMismatchError(w.WALName().String(), msgWALName.String())
	}

	if w.historicalWALOpener == nil {
		w.Logger().Info(ctx, "WAL name mismatch",
			mlog.String("msgIDWALName", msgWALName.String()),
			mlog.String("currentWALName", w.WALName().String()))
		return nil, status.NewWALNameMismatchError(w.WALName().String(), msgWALName.String())
	}

	historicalWAL, err := w.openHistoricalWALWithFallback(ctx, msgWALName, w.Channel())
	if err != nil {
		return nil, err
	}
	w.Logger().Info(ctx, "open historical WAL for existing reader",
		mlog.Stringer("historicalWALName", msgWALName),
		mlog.Stringer("currentWALName", w.WALName()))
	return &resolvedReadWAL{
		wal:                     historicalWAL,
		deliverPolicy:           deliverPolicy,
		exclusiveStartMessageID: exclusiveStartMessageID,
	}, nil
}

func prepareHistoricalDeliverPolicy(deliverPolicy options.DeliverPolicy) (options.DeliverPolicy, message.MessageID, error) {
	switch policy := deliverPolicy.GetPolicy().(type) {
	case *streamingpb.DeliverPolicy_StartFrom:
		_, err := message.UnmarshalMessageID(policy.StartFrom)
		return deliverPolicy, nil, err
	case *streamingpb.DeliverPolicy_StartAfter:
		messageID, err := message.UnmarshalMessageID(policy.StartAfter)
		if err != nil {
			return nil, nil, err
		}
		// Historical readers need to inspect the excluded message because it may
		// itself be the AlterWAL boundary. The scanner filters this message from
		// user output while still applying its control semantics.
		return options.DeliverPolicyStartFrom(messageID), messageID, nil
	default:
		return deliverPolicy, nil, nil
	}
}

func (w *roWALAdaptorImpl) openHistoricalWALWithFallback(
	ctx context.Context,
	walName message.WALName,
	channel types.PChannelInfo,
) (walimpls.ROWALImpls, error) {
	if w.historicalWALOpener == nil {
		return nil, status.NewWALNameMismatchError(w.WALName().String(), walName.String())
	}

	timeout := w.getHistoricalWALFallbackTimeout()
	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()
	backoffTimer := typeutil.NewBackoffTimer(typeutil.BackoffTimerConfig{
		Default: time.Second,
		Backoff: typeutil.BackoffConfig{
			InitialInterval: 100 * time.Millisecond,
			Multiplier:      2,
			MaxInterval:     5 * time.Second,
		},
	})
	backoffTimer.EnableBackoff()

	for {
		historicalWAL, err := w.historicalWALOpener(ctx, walName, channel)
		if err == nil {
			actualWALName := historicalWAL.WALName()
			if actualWALName != walName {
				historicalWAL.Close()
				w.Logger().Warn(ctx, "historical WAL opener returned unexpected WAL",
					mlog.Stringer("expectedWALName", walName),
					mlog.Stringer("actualWALName", actualWALName))
				return nil, status.NewWALNameMismatchError(w.WALName().String(), walName.String())
			}
			return historicalWAL, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if isHistoricalWALUnavailable(err) {
			w.Logger().Warn(ctx, "historical WAL is permanently unavailable, falling back to current WAL",
				mlog.Stringer("historicalWALName", walName),
				mlog.Stringer("currentWALName", w.WALName()),
				mlog.Err(err))
			return nil, status.NewWALNameMismatchError(w.WALName().String(), walName.String())
		}

		waker, nextInterval := backoffTimer.NextTimer()
		w.Logger().Warn(ctx, "failed to open historical WAL, retrying before fallback",
			mlog.Stringer("historicalWALName", walName),
			mlog.Duration("nextInterval", nextInterval),
			mlog.Err(err))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timeoutTimer.C:
			w.Logger().Warn(ctx, "historical WAL open retry timed out, falling back to current WAL",
				mlog.Stringer("historicalWALName", walName),
				mlog.Stringer("currentWALName", w.WALName()),
				mlog.Duration("timeout", timeout),
				mlog.Err(err))
			return nil, status.NewWALNameMismatchError(w.WALName().String(), walName.String())
		case <-waker:
		}
	}
}

func (w *roWALAdaptorImpl) getHistoricalWALFallbackTimeout() time.Duration {
	if w.historicalWALFallbackTimeout > 0 {
		return w.historicalWALFallbackTimeout
	}
	return defaultHistoricalWALFallbackTimeout
}

func isHistoricalWALUnavailable(err error) bool {
	return status.AsStreamingError(err).IsWALNameMismatch() || errors.Is(err, merr.ErrMqTopicNotFound)
}

func getDeliverPolicyWALName(deliverPolicy options.DeliverPolicy) (message.WALName, bool) {
	if deliverPolicy == nil {
		return message.WALNameUnknown, false
	}
	var msgID *commonpb.MessageID
	switch policy := deliverPolicy.GetPolicy().(type) {
	case *streamingpb.DeliverPolicy_StartFrom:
		msgID = policy.StartFrom
	case *streamingpb.DeliverPolicy_StartAfter:
		msgID = policy.StartAfter
	default:
		return message.WALNameUnknown, false
	}
	if msgID == nil {
		return message.WALNameUnknown, false
	}
	return message.WALName(msgID.WALName), true
}

// IsAvailable returns whether the wal is available.
func (w *roWALAdaptorImpl) IsAvailable() bool {
	return w.availableCtx.Err() == nil
}

// Available returns a channel that will be closed when the wal is shut down.
func (w *roWALAdaptorImpl) Available() <-chan struct{} {
	return w.availableCtx.Done()
}

// Close overrides Scanner Close function.
func (w *roWALAdaptorImpl) Close() {
	// begin to close the wal.
	w.Logger().Info(context.TODO(), "wal begin to close...")
	w.lifetime.SetState(typeutil.LifetimeStateStopped)
	w.forceCancelAfterGracefulTimeout()
	w.lifetime.Wait()

	w.Logger().Info(context.TODO(), "wal begin to close scanners...")

	// close all wal instances.
	w.scanners.Range(func(id int64, s wal.Scanner) bool {
		s.Close()
		mlog.Info(context.TODO(), "close scanner by wal adaptor", mlog.Int64("id", id), mlog.Any("channel", w.Channel()))
		return true
	})

	w.Logger().Info(context.TODO(), "scanner close done, close inner wal...")
	w.roWALImpls.Close()

	w.Logger().Info(context.TODO(), "call wal cleanup function...")
	w.cleanup()
	w.Logger().Info(context.TODO(), "wal closed")

	// close all metrics.
	w.scanMetrics.Close()

	// close the rate limit component.
	w.WALRateLimitComponent.Close()
}

// forceCancelAfterGracefulTimeout forces to cancel the context after the graceful timeout.
func (w *roWALAdaptorImpl) forceCancelAfterGracefulTimeout() {
	if w.availableCtx.Err() != nil {
		return
	}
	time.AfterFunc(3*time.Second, func() {
		// perform a force cancel to avoid resource leak.
		w.availableCancel()
	})
}
