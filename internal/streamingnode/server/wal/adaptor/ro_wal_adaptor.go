package adaptor

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ wal.WAL = (*roWALAdaptorImpl)(nil)

type roWALAdaptorImpl struct {
	log.Binder
	lifetime        *typeutil.Lifetime
	available       chan struct{}
	idAllocator     *typeutil.IDAllocator
	roWALImpls      walimpls.ROWALImpls
	scannerRegistry scannerRegistry
	scanners        *typeutil.ConcurrentMap[int64, wal.Scanner]
	cleanup         func()
	scanMetrics     *metricsutil.ScanMetrics
}

func (w *roWALAdaptorImpl) WALName() string {
	return w.roWALImpls.WALName()
}

// Channel returns the channel info of wal.
func (w *roWALAdaptorImpl) Channel() types.PChannelInfo {
	return w.roWALImpls.Channel()
}

func (w *roWALAdaptorImpl) GetLatestMVCCTimestamp(ctx context.Context, vchannel string) (uint64, error) {
	panic("we cannot acquire lastest mvcc timestamp from a read only wal")
}

// Append writes a record to the log.
func (w *roWALAdaptorImpl) Append(ctx context.Context, msg message.MutableMessage) (*wal.AppendResult, error) {
	panic("we cannot append message into a read only wal")
}

// Append a record to the log asynchronously.
func (w *roWALAdaptorImpl) AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(*wal.AppendResult, error)) {
	panic("we cannot append message into a read only wal")
}

// Read returns a scanner for reading records from the wal.
func (w *roWALAdaptorImpl) Read(ctx context.Context, opts wal.ReadOption) (wal.Scanner, error) {
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
		w.roWALImpls,
		opts,
		w.scanMetrics.NewScannerMetrics(),
		func() { w.scanners.Remove(id) })
	w.scanners.Insert(id, s)
	return s, nil
}

// IsAvailable returns whether the wal is available.
func (w *roWALAdaptorImpl) IsAvailable() bool {
	select {
	case <-w.available:
		return false
	default:
		return true
	}
}

// Available returns a channel that will be closed when the wal is shut down.
func (w *roWALAdaptorImpl) Available() <-chan struct{} {
	return w.available
}

// Close overrides Scanner Close function.
func (w *roWALAdaptorImpl) Close() {
	// begin to close the wal.
	w.Logger().Info("wal begin to close...")
	w.lifetime.SetState(typeutil.LifetimeStateStopped)
	close(w.available)
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
