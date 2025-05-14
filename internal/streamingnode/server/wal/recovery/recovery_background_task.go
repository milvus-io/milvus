package recovery

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// TODO: !!! all recovery persist operation should be a compare-and-swap operation to
// promise there's only one consumer of wal.
// But currently, we don't implement the CAS operation of meta interface.
// Should be fixed in future.
func (rs *RecoveryStorage) backgroundTask() {
	ticker := time.NewTicker(rs.cfg.persistInterval)
	defer func() {
		ticker.Stop()
		rs.backgroundTaskNotifier.Finish(struct{}{})
		rs.Logger().Info("recovery storage background task exit")
	}()

	for {
		select {
		case <-rs.backgroundTaskNotifier.Context().Done():
			// If the background task is exiting when on-operating persist operation,
			// We can try to do a graceful exit.
			rs.Logger().Info("recovery storage background task, perform a graceful exit...")
			rs.persistDritySnapshotWhenClosing()
			rs.gracefulClosed = true
			return // exit the background task
		case <-rs.persistNotifier:
		case <-ticker.C:
		}
		snapshot := rs.consumeDirtySnapshot()
		if err := rs.persistDirtySnapshot(rs.backgroundTaskNotifier.Context(), snapshot, zap.DebugLevel); err != nil {
			return
		}
	}
}

// persistDritySnapshotWhenClosing persists the dirty snapshot when closing the recovery storage.
func (rs *RecoveryStorage) persistDritySnapshotWhenClosing() {
	ctx, cancel := context.WithTimeout(context.Background(), rs.cfg.gracefulTimeout)
	defer cancel()

	snapshot := rs.consumeDirtySnapshot()
	_ = rs.persistDirtySnapshot(ctx, snapshot, zap.InfoLevel)
}

// persistDirtySnapshot persists the dirty snapshot to the catalog.
func (rs *RecoveryStorage) persistDirtySnapshot(ctx context.Context, snapshot *RecoverySnapshot, lvl zapcore.Level) (err error) {
	rs.metrics.ObserveIsOnPersisting(true)
	logger := rs.Logger().With(
		zap.String("checkpoint", snapshot.Checkpoint.MessageID.String()),
		zap.Uint64("checkpointTimeTick", snapshot.Checkpoint.TimeTick),
		zap.Int("vchannelCount", len(snapshot.VChannels)),
		zap.Int("segmentCount", len(snapshot.SegmentAssignments)),
	)
	defer func() {
		if err != nil {
			logger.Warn("failed to persist dirty snapshot", zap.Error(err))
			return
		}
		logger.Log(lvl, "persist dirty snapshot")
		defer rs.metrics.ObserveIsOnPersisting(false)
	}()

	if err := rs.dropAllVirtualChannel(ctx, snapshot.VChannels); err != nil {
		logger.Warn("failed to drop all virtual channels", zap.Error(err))
		return err
	}

	futures := make([]*conc.Future[struct{}], 0, 2)
	if len(snapshot.SegmentAssignments) > 0 {
		future := conc.Go(func() (struct{}, error) {
			err := rs.retryOperationWithBackoff(ctx,
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
			err := rs.retryOperationWithBackoff(ctx,
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

	// checkpoint updates should always be persisted after other updates success.
	if err := rs.retryOperationWithBackoff(ctx, rs.Logger().With(zap.String("op", "persistCheckpoint")), func(ctx context.Context) error {
		return resource.Resource().StreamingNodeCatalog().
			SaveConsumeCheckpoint(ctx, rs.channel.Name, snapshot.Checkpoint.IntoProto())
	}); err != nil {
		return err
	}

	// sample the checkpoint for truncator to make wal truncation.
	rs.metrics.ObServePersistedMetrics(snapshot.Checkpoint.TimeTick)
	rs.truncator.SampleCheckpoint(snapshot.Checkpoint)
	return
}

// dropAllVirtualChannel drops all virtual channels that are in the dropped state.
// TODO: DropVirtualChannel will be called twice here,
// call it in recovery storage is used to promise the drop virtual channel must be called after recovery.
// In future, the flowgraph will be deprecated, all message operation will be implement here.
// So the DropVirtualChannel will only be called once after that.
func (rs *RecoveryStorage) dropAllVirtualChannel(ctx context.Context, vcs map[string]*streamingpb.VChannelMeta) error {
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
		if err := rs.retryOperationWithBackoff(ctx, rs.Logger().With(zap.String("op", "dropAllVirtualChannel")), func(ctx context.Context) error {
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
func (rs *RecoveryStorage) retryOperationWithBackoff(ctx context.Context, logger *log.MLogger, op func(ctx context.Context) error) error {
	backoff := rs.newBackoff()
	for {
		err := op(ctx)
		if err == nil {
			return nil
		}
		if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
			return err
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
func (rs *RecoveryStorage) newBackoff() *backoff.ExponentialBackOff {
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = 1 * time.Second
	backoff.MaxElapsedTime = 0
	return backoff
}
