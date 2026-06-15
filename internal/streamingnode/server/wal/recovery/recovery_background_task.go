package recovery

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// isDirty checks if the recovery storage mem state is not consistent with the persisted recovery storage.
func (rs *recoveryStorageImpl) isDirty() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	// pendingRecoveryPersistSnapshot is written under rs.mu, so read it here too;
	// the rest of the dirty predicate lives in one place (hasDirtyRecoveryStateUnsafe).
	return rs.pendingRecoveryPersistSnapshot != nil || rs.hasDirtyRecoveryStateUnsafe()
}

// TODO: !!! all recovery persist operation should be a compare-and-swap operation to
// promise there's only one consumer of wal.
// But currently, we don't implement the CAS operation of meta interface.
// Should be fixed in future.
func (rs *recoveryStorageImpl) backgroundTask() {
	ticker := time.NewTicker(rs.cfg.persistInterval)
	defer func() {
		ticker.Stop()
		rs.Logger().Info("recovery storage background task, perform a graceful exit...")
		if err := rs.persistRecoverySnapshotWhenClosing(); err != nil {
			rs.Logger().Warn("failed to persist recovery snapshot when closing", zap.Error(err))
		}
		rs.backgroundTaskNotifier.Finish(struct{}{})
		rs.Logger().Info("recovery storage background task exit")
	}()

	for {
		select {
		case <-rs.backgroundTaskNotifier.Context().Done():
			return
		case <-rs.persistNotifier:
		case <-ticker.C:
		}
		if err := rs.persistRecoverySnapshot(rs.backgroundTaskNotifier.Context(), zap.DebugLevel); err != nil {
			return
		}
	}
}

// persistRecoverySnapshotWhenClosing persists the dirty recovery snapshot when closing the recovery storage.
func (rs *recoveryStorageImpl) persistRecoverySnapshotWhenClosing() error {
	ctx, cancel := context.WithTimeout(context.Background(), rs.cfg.gracefulTimeout)
	defer cancel()

	for rs.isDirty() {
		if err := rs.persistRecoverySnapshot(ctx, zap.InfoLevel); err != nil {
			return err
		}
	}
	rs.gracefulClosed = true
	return nil
}

func (rs *recoveryStorageImpl) persistRecoverySnapshot(ctx context.Context, lvl zapcore.Level) error {
	snapshot := rs.ensurePendingRecoveryPersistSnapshot()
	return rs.persistRecoverySnapshotData(ctx, lvl, snapshot)
}

func (rs *recoveryStorageImpl) ensurePendingRecoveryPersistSnapshot() *RecoverySnapshot {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.pendingRecoveryPersistSnapshot == nil {
		rs.pendingRecoveryPersistSnapshot = rs.consumeDirtySnapshotLocked()
	}
	return rs.pendingRecoveryPersistSnapshot
}

func (rs *recoveryStorageImpl) clearPendingRecoveryPersistSnapshot() {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.pendingRecoveryPersistSnapshot = nil
}

func (rs *recoveryStorageImpl) persistRecoverySnapshotData(ctx context.Context, lvl zapcore.Level, snapshot *RecoverySnapshot) (err error) {
	if snapshot == nil {
		return nil
	}

	rs.metrics.ObserveIsOnPersisting(true)
	logger := rs.Logger().With(
		zap.String("checkpoint", snapshot.Checkpoint.MessageID.String()),
		zap.Uint64("checkpointTimeTick", snapshot.Checkpoint.TimeTick),
		zap.Int("vchannelCount", len(snapshot.VChannels)),
		zap.Int("segmentCount", len(snapshot.SegmentAssignments)),
	)
	defer func() {
		rs.metrics.ObserveIsOnPersisting(false)
		if err != nil {
			logger.Warn("failed to persist dirty snapshot", zap.Error(err))
			return
		}
		rs.clearPendingRecoveryPersistSnapshot()
		logger.Log(lvl, "persist dirty snapshot")
	}()

	if err := rs.dropAllVirtualChannel(ctx, snapshot.VChannels); err != nil {
		logger.Warn("failed to drop all virtual channels", zap.Error(err))
		return err
	}

	futures := make([]*conc.Future[struct{}], 0, 2)
	if len(snapshot.SegmentAssignments) > 0 {
		future := conc.Go(func() (struct{}, error) {
			err := retryOperationWithBackoff(ctx,
				logger.With(zap.String("op", "persistSegmentAssignments"), zap.Int64s("segmentIds", lo.Keys(snapshot.SegmentAssignments))),
				func(ctx context.Context) error {
					return resource.Resource().StreamingNodeCatalog().SaveSegmentAssignments(ctx, rs.channel.Name, snapshot.SegmentAssignments)
				})
			return struct{}{}, err
		})
		futures = append(futures, future)
	}
	if len(snapshot.VChannels) > 0 {
		future := conc.Go(func() (struct{}, error) {
			err := retryOperationWithBackoff(ctx,
				logger.With(zap.String("op", "persistVChannels"), zap.Strings("vchannels", lo.Keys(snapshot.VChannels))),
				func(ctx context.Context) error {
					return resource.Resource().StreamingNodeCatalog().SaveVChannels(ctx, rs.channel.Name, snapshot.VChannels)
				})
			return struct{}{}, err
		})
		futures = append(futures, future)
	}
	if err := conc.BlockOnAll(futures...); err != nil {
		return err
	}

	// Salvage checkpoint must be persisted before the consume checkpoint to guarantee ordering:
	// if the node crashes between these two writes, the next snapshot retry will re-persist both.
	if snapshot.SalvageCheckpoint != nil {
		if err := retryOperationWithBackoff(ctx, rs.Logger().With(zap.String("op", "persistSalvageCheckpoint")), func(ctx context.Context) error {
			return resource.Resource().StreamingNodeCatalog().SaveSalvageCheckpoint(ctx, rs.channel.Name, snapshot.SalvageCheckpoint.IntoProto())
		}); err != nil {
			return err
		}
	}

	// checkpoint updates should always be persisted after other updates success.
	effectiveCheckpoint := rs.windowManager.effectivePersistCheckpoint(snapshot, rs.getFlusherCheckpoint())
	if err := retryOperationWithBackoff(ctx, rs.Logger().With(zap.String("op", "persistCheckpoint")), func(ctx context.Context) error {
		return resource.Resource().StreamingNodeCatalog().
			SaveConsumeCheckpoint(ctx, rs.channel.Name, effectiveCheckpoint.IntoProto())
	}); err != nil {
		return err
	}
	rs.windowManager.markConsumeCheckpointPersisted(effectiveCheckpoint)

	// sample the checkpoint for truncator to make wal truncation.
	rs.metrics.ObServePersistedMetrics(effectiveCheckpoint.TimeTick)
	rs.simpleTruncateCheckpoint(ctx, effectiveCheckpoint)
	return
}

func (rs *recoveryStorageImpl) simpleTruncateCheckpoint(ctx context.Context, checkpoint *WALCheckpoint) {
	flusherCP := rs.getFlusherCheckpoint()
	if flusherCP == nil {
		return
	}
	// use the smaller one to truncate the wal.
	if flusherCP.MessageID.LTE(checkpoint.MessageID) {
		_ = rs.truncator.Truncate(ctx, flusherCP.MessageID)
	} else {
		_ = rs.truncator.Truncate(ctx, checkpoint.MessageID)
	}
}

// dropAllVirtualChannel drops all virtual channels that are in the dropped state.
// TODO: DropVirtualChannel will be called twice here,
// call it in recovery storage is used to promise the drop virtual channel must be called after recovery.
// In future, the flowgraph will be deprecated, all message operation will be implement here.
// So the DropVirtualChannel will only be called once after that.
func (rs *recoveryStorageImpl) dropAllVirtualChannel(ctx context.Context, vcs map[string]*streamingpb.VChannelMeta) error {
	channels := make([]string, 0, len(vcs))
	for channelName, vc := range vcs {
		if vc.State == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
			channels = append(channels, channelName)
		}
	}
	if len(channels) == 0 {
		return nil
	}

	mixCoordClient, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return err
	}

	for _, channelName := range channels {
		if err := retryOperationWithBackoff(ctx, rs.Logger().With(zap.String("op", "dropAllVirtualChannel")), func(ctx context.Context) error {
			resp, err := mixCoordClient.DropVirtualChannel(ctx, &datapb.DropVirtualChannelRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				ChannelName: channelName,
			})
			return merr.CheckRPCCall(resp, err)
		}); err != nil {
			return err
		}
	}
	return nil
}

// retryOperationWithBackoff retries the operation with exponential backoff.
func retryOperationWithBackoff(ctx context.Context, logger *log.MLogger, op func(ctx context.Context) error) error {
	backoff := newBackoff()
	for {
		err := op(ctx)
		if err == nil {
			return nil
		}
		// because underlying kv may report the context.Canceled, context.DeadlineExceeded even if the ctx is not canceled.
		// so we cannot use errors.IsAny(err, context.Canceled, context.DeadlineExceeded) to check the error.
		if ctx.Err() != nil {
			return ctx.Err()
		}

		nextInterval := backoff.NextBackOff()
		logger.Warn("failed to persist operation, wait for retry...", zap.Duration("nextRetryInterval", nextInterval), zap.Error(err))
		select {
		case <-time.After(nextInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// newBackoff creates a new backoff instance with the default settings.
func newBackoff() *backoff.ExponentialBackOff {
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = 1 * time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()

	return backoff
}
