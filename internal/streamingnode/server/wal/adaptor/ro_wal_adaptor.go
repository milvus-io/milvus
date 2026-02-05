package adaptor

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ wal.WAL = (*roWALAdaptorImpl)(nil)

type roWALAdaptorImpl struct {
	log.Binder
	lifetime        *typeutil.Lifetime
	availableCtx    context.Context
	availableCancel context.CancelFunc
	idAllocator     *typeutil.IDAllocator
	roWALImpls      walimpls.ROWALImpls
	scannerRegistry scannerRegistry
	scanners        *typeutil.ConcurrentMap[int64, wal.Scanner]
	cleanup         func()
	scanMetrics     *metricsutil.ScanMetrics
	forceRecovery   bool
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

func (w *roWALAdaptorImpl) GetSalvageCheckpoint() *wal.ReplicateCheckpoint {
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

	// Validate DeliverPolicy: if it's StartFrom or StartAfter, check that the message ID's WALName matches the current WAL name
	if mismatchWALNameErr := w.checkReadOptWALName(opts); mismatchWALNameErr != nil {
		return nil, mismatchWALNameErr
	}

	name, err := w.scannerRegistry.AllocateScannerName()
	if err != nil {
		return nil, err
	}
	// wrap the scanner with cleanup function.
	id := w.idAllocator.Allocate()
	s := newScannerAdaptor(
		name,
		w.roWALImpls,
		opts,
		w.scanMetrics.NewScannerMetrics(),
		func() { w.scanners.Remove(id) },
		w.forceRecovery)
	w.scanners.Insert(id, s)
	return s, nil
}

func (w *roWALAdaptorImpl) checkReadOptWALName(opts wal.ReadOption) error {
	if opts.DeliverPolicy != nil {
		currentWALName := w.WALName()
		var msgID *commonpb.MessageID

		switch t := opts.DeliverPolicy.GetPolicy().(type) {
		case *streamingpb.DeliverPolicy_StartFrom:
			msgID = t.StartFrom
		case *streamingpb.DeliverPolicy_StartAfter:
			msgID = t.StartAfter
		}

		if msgID != nil {
			msgWALName := message.WALName(msgID.WALName)
			if msgWALName != currentWALName {
				w.Logger().Info("WAL name mismatch", zap.String("msgIDWALName", msgWALName.String()), zap.String("currentWALName", currentWALName.String()))
				return status.NewWALNameMismatchError(currentWALName.String(), msgWALName.String())
			}
		}
	}
	return nil
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
	w.Logger().Info("wal begin to close...")
	w.lifetime.SetState(typeutil.LifetimeStateStopped)
	w.forceCancelAfterGracefulTimeout()
	w.lifetime.Wait()

	w.Logger().Info("wal begin to close scanners...")

	// close all wal instances.
	w.scanners.Range(func(id int64, s wal.Scanner) bool {
		s.Close()
		log.Info("close scanner by wal adaptor", zap.Int64("id", id), zap.Any("channel", w.Channel()))
		return true
	})

	w.Logger().Info("scanner close done, close inner wal...")
	w.roWALImpls.Close()

	w.Logger().Info("call wal cleanup function...")
	w.cleanup()
	w.Logger().Info("wal closed")

	// close all metrics.
	w.scanMetrics.Close()
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
