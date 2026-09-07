package recovery

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// isDirty checks if the recovery storage mem state is not consistent with the persisted recovery storage.
func (rs *recoveryStorageImpl) isDirty() bool {
	if rs.pendingPersistSnapshot != nil {
		return true
	}

	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.dirtyCounter > 0 || rs.pendingSalvageCheckpoint != nil
}

// TODO: !!! all recovery persist operation should be a compare-and-swap operation to
// promise there's only one consumer of wal.
// But currently, we don't implement the CAS operation of meta interface.
// Should be fixed in future.
// The compound SaveRecoverySnapshot already gathers the whole snapshot into
// one catalog call, paving the way for a future single-point CAS commit.
func (rs *recoveryStorageImpl) backgroundTask() {
	ticker := time.NewTicker(rs.cfg.persistInterval)
	defer func() {
		ticker.Stop()
		rs.Logger().Info(context.TODO(), "recovery storage background task, perform a graceful exit...")
		if err := rs.persistDritySnapshotWhenClosing(); err != nil {
			rs.Logger().Warn(context.TODO(), "failed to persist dirty snapshot when closing", mlog.Err(err))
		}
		rs.backgroundTaskNotifier.Finish(struct{}{})
		rs.Logger().Info(context.TODO(), "recovery storage background task exit")
	}()

	for {
		select {
		case <-rs.backgroundTaskNotifier.Context().Done():
			return
		case <-rs.persistNotifier:
		case <-ticker.C:
		}
		if err := rs.persistDirtySnapshot(rs.backgroundTaskNotifier.Context(), mlog.DebugLevel); err != nil {
			return
		}
	}
}

// persistDritySnapshotWhenClosing persists the dirty snapshot when closing the recovery storage.
func (rs *recoveryStorageImpl) persistDritySnapshotWhenClosing() error {
	ctx, cancel := context.WithTimeout(context.Background(), rs.cfg.gracefulTimeout)
	defer cancel()

	for rs.isDirty() {
		if err := rs.persistDirtySnapshot(ctx, mlog.InfoLevel); err != nil {
			return err
		}
	}
	rs.gracefulClosed = true
	return nil
}

// persistDirtySnapshot persists the dirty snapshot to the catalog.
func (rs *recoveryStorageImpl) persistDirtySnapshot(ctx context.Context, lvl mlog.Level) (err error) {
	if rs.pendingPersistSnapshot == nil {
		// if there's no dirty snapshot, generate a new one.
		rs.pendingPersistSnapshot = rs.consumeDirtySnapshot()
	}
	if rs.pendingPersistSnapshot == nil {
		return nil
	}

	snapshot := rs.pendingPersistSnapshot
	rs.metrics.ObserveIsOnPersisting(true)
	logger := rs.Logger().With(
		mlog.String("checkpoint", snapshot.Checkpoint.MessageID.String()),
		mlog.Uint64("checkpointTimeTick", snapshot.Checkpoint.TimeTick),
		mlog.Int("vchannelCount", len(snapshot.VChannels)),
		mlog.Int("segmentCount", len(snapshot.SegmentAssignments)),
	)
	defer func() {
		if err != nil {
			logger.Warn(ctx, "failed to persist dirty snapshot", mlog.Err(err))
			return
		}
		rs.pendingPersistSnapshot = nil
		logger.Log(ctx, lvl, "persist dirty snapshot")
		rs.metrics.ObserveIsOnPersisting(false)
	}()

	if err := rs.dropAllVirtualChannel(ctx, snapshot.VChannels); err != nil {
		logger.Warn(ctx, "failed to drop all virtual channels", mlog.Err(err))
		return err
	}

	// The catalog persists the whole snapshot as a single compound write, with
	// the consume checkpoint always the last/commit-marker op - so a
	// whole-snapshot retry is always safe (every part is an idempotent put).
	recoverySnapshot := &metastore.WALRecoverySnapshot{
		SegmentAssignments: snapshot.SegmentAssignments,
		VChannels:          snapshot.VChannels,
		ConsumeCheckpoint:  snapshot.Checkpoint.IntoProto(),
	}
	if snapshot.SalvageCheckpoint != nil {
		recoverySnapshot.SalvageCheckpoint = snapshot.SalvageCheckpoint.IntoProto()
	}
	if err := rs.retryOperationWithBackoff(ctx,
		logger.With(
			mlog.String("op", "persistRecoverySnapshot"),
			mlog.Int64s("segmentIds", lo.Keys(snapshot.SegmentAssignments)),
			mlog.Strings("vchannels", lo.Keys(snapshot.VChannels)),
		),
		func(ctx context.Context) error {
			return resource.Resource().StreamingNodeCatalog().SaveRecoverySnapshot(ctx, rs.channel.Name, recoverySnapshot)
		}); err != nil {
		return err
	}

	// sample the checkpoint for truncator to make wal truncation.
	rs.metrics.ObServePersistedMetrics(snapshot.Checkpoint.TimeTick)
	rs.simpleTruncateCheckpoint(ctx, snapshot.Checkpoint)
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
		if err := rs.retryOperationWithBackoff(ctx, rs.Logger().With(mlog.String("op", "dropAllVirtualChannel")), func(ctx context.Context) error {
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
func (rs *recoveryStorageImpl) retryOperationWithBackoff(ctx context.Context, logger *mlog.Logger, op func(ctx context.Context) error) error {
	backoff := rs.newBackoff()
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
		logger.Warn(ctx, "failed to persist operation, wait for retry...", mlog.Duration("nextRetryInterval", nextInterval), mlog.Err(err))
		select {
		case <-time.After(nextInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// newBackoff creates a new backoff instance with the default settings.
func (rs *recoveryStorageImpl) newBackoff() *backoff.ExponentialBackOff {
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = 1 * time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()

	return backoff
}
