package adaptor

import (
	"context"
	"time"

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
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ wal.WAL = (*roWALAdaptorImpl)(nil)

type historicalWALOpener func(
	ctx context.Context,
	walName message.WALName,
	channel types.PChannelInfo,
) (walimpls.ROWALImpls, error)

type roWALAdaptorImpl struct {
	*rate.WALRateLimitComponent
	mlog.Binder

	lifetime            *typeutil.Lifetime
	availableCtx        context.Context
	availableCancel     context.CancelFunc
	idAllocator         *typeutil.IDAllocator
	roWALImpls          walimpls.ROWALImpls
	historicalWALOpener historicalWALOpener
	scannerRegistry     scannerRegistry
	scanners            *typeutil.ConcurrentMap[int64, wal.Scanner]
	cleanup             func()
	scanMetrics         *metricsutil.ScanMetrics
	forceRecovery       bool
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

	readWAL, err := w.resolveReadWAL(ctx, opts)
	if err != nil {
		return nil, err
	}

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
		w.scanMetrics.NewScannerMetrics(),
		func() { w.scanners.Remove(id) },
		w.forceRecovery)
	w.scanners.Insert(id, s)
	return s, nil
}

func (w *roWALAdaptorImpl) resolveReadWAL(ctx context.Context, opts wal.ReadOption) (walimpls.ROWALImpls, error) {
	msgWALName, ok := getDeliverPolicyWALName(opts.DeliverPolicy)
	if !ok || msgWALName == w.WALName() {
		return w.roWALImpls, nil
	}
	if msgWALName == message.WALNameUnknown || msgWALName.String() == "" {
		return nil, status.NewWALNameMismatchError(w.WALName().String(), "unknown")
	}

	if w.historicalWALOpener == nil {
		w.Logger().Info(ctx, "WAL name mismatch",
			mlog.String("msgIDWALName", msgWALName.String()),
			mlog.String("currentWALName", w.WALName().String()))
		return nil, status.NewWALNameMismatchError(w.WALName().String(), msgWALName.String())
	}

	historicalWAL, err := w.historicalWALOpener(ctx, msgWALName, w.Channel())
	if err != nil {
		w.Logger().Warn(ctx, "failed to open historical WAL",
			mlog.Stringer("historicalWALName", msgWALName),
			mlog.Stringer("currentWALName", w.WALName()),
			mlog.Err(err))
		return nil, status.NewInner(
			"failed to open historical %s WAL for channel %s: %s",
			msgWALName,
			w.Channel().Name,
			err.Error(),
		)
	}
	if historicalWAL.WALName() != msgWALName {
		historicalWAL.Close()
		return nil, status.NewInner(
			"historical WAL opener returned unexpected WAL: expected %s, actual %s",
			msgWALName,
			historicalWAL.WALName(),
		)
	}
	w.Logger().Info(ctx, "open historical WAL for existing reader",
		mlog.Stringer("historicalWALName", msgWALName),
		mlog.Stringer("currentWALName", w.WALName()))
	return historicalWAL, nil
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
