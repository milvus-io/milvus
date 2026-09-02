package recovery

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// isDirty checks if the recovery storage mem state is not consistent with the persisted recovery storage.
func (rs *recoveryStorageImpl) isDirty() bool {
	if rs.pendingPersistSnapshot != nil {
		return true
	}

	rs.mu.Lock()
	dirty := rs.dirtyCounter > 0 || rs.moduleDirty || rs.pendingSalvageCheckpoint != nil || rs.checkpointDirty
	persistedCheckpoint := rs.persistedCheckpoint
	ackTracker := rs.ackTracker
	rs.mu.Unlock()
	if !dirty && ackTracker != nil {
		completed := ackTracker.CompletedPoint()
		dirty = persistedCheckpoint == nil || !consumeCheckpointEqual(persistedCheckpoint.DataCheckpoint, &completed)
	}
	if dirty {
		return true
	}

	if rs.vchannelManager != nil && rs.vchannelManager.HasPendingCleanup() {
		return true
	}
	return false
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

	persistIfAny := func() (bool, error) {
		if rs.pendingPersistSnapshot == nil {
			rs.pendingPersistSnapshot = rs.consumeDirtySnapshot()
		}
		// Cleanup candidates may remain pending until a future physical
		// checkpoint passes their tombstone. They are safe to leave for the
		// next WAL owner when no persistable snapshot can be produced.
		if rs.pendingPersistSnapshot == nil {
			return false, nil
		}
		if err := rs.persistDirtySnapshot(ctx, mlog.InfoLevel); err != nil {
			return false, err
		}
		return true, nil
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if rs.taskScheduler != nil {
			if err := rs.taskScheduler.WaitIdle(ctx); err != nil {
				return err
			}
		}
		persisted, err := persistIfAny()
		if err != nil {
			return err
		}
		if persisted {
			continue
		}

		// Wait once more before declaring no progress, so a task submitted
		// concurrently with the first idle observation can still publish its
		// final dirty state.
		if rs.taskScheduler != nil {
			if err := rs.taskScheduler.WaitIdle(ctx); err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !rs.isDirty() {
			break
		}
		persisted, err = persistIfAny()
		if err != nil {
			return err
		}
		if !persisted {
			break
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
	)
	defer func() {
		if err != nil {
			logger.Warn(context.TODO(), "failed to persist dirty snapshot", mlog.Err(err))
			return
		}
		rs.pendingPersistSnapshot = nil
		logger.Log(context.TODO(), lvl, "persist dirty snapshot")
		rs.metrics.ObserveIsOnPersisting(false)
	}()

	if err := rs.persistModuleDirtySnapshots(ctx, snapshot); err != nil {
		return err
	}
	if err := rs.persistCheckpointSnapshot(ctx, snapshot, lvl >= mlog.InfoLevel); err != nil {
		return err
	}
	return
}

func (rs *recoveryStorageImpl) persistModuleDirtySnapshots(ctx context.Context, snapshot *dirtyPersistSnapshot) error {
	if snapshot.ModuleSnapshotsAck || len(snapshot.ModuleDirtySnaps) == 0 {
		return nil
	}
	for _, dirtySnapshot := range snapshot.ModuleDirtySnaps {
		if err := rs.persistModuleDirtySnapshot(ctx, dirtySnapshot); err != nil {
			return err
		}
	}
	for _, dirtySnapshot := range snapshot.ModuleDirtySnaps {
		dirtySnapshot.MarkPersisted()
	}
	snapshot.ModuleSnapshotsAck = true
	return nil
}

func (rs *recoveryStorageImpl) persistModuleDirtySnapshot(ctx context.Context, snapshot moduleapi.DirtySnapshot) error {
	key := snapshot.Key()
	if key.PChannel == "" {
		key.PChannel = rs.channel.Name
	}
	catalog := resource.Resource().StreamingNodeCatalog()
	logger := rs.Logger().With(
		mlog.String("op", "persistModuleSnapshot"),
		mlog.String("module", string(snapshot.ModuleName())),
		mlog.Int("snapshotOp", int(snapshot.Op())),
		mlog.String("pchannel", key.PChannel),
		mlog.String("vchannel", key.VChannel),
		mlog.Int64("segmentID", key.SegmentID),
		mlog.Uint64("metaTimeTick", snapshot.MetaTimeTick()),
		mlog.Uint64("dataTimeTick", snapshot.DataTimeTick()),
	)
	return rs.retryOperationWithBackoff(ctx, logger, func(ctx context.Context) error {
		switch snapshot.ModuleName() {
		case moduleapi.ModuleNameVChannel:
			switch snapshot.Op() {
			case moduleapi.SnapshotOpUpsert:
				meta, ok := snapshot.Payload().(*streamingpb.VChannelMeta)
				if !ok || meta == nil {
					return errors.New("vchannel dirty snapshot payload is not VChannelMeta")
				}
				return catalog.SaveVChannels(ctx, key.PChannel, map[string]*streamingpb.VChannelMeta{key.VChannel: meta})
			case moduleapi.SnapshotOpUpsertBase:
				meta, ok := snapshot.Payload().(*streamingpb.VChannelMeta)
				if !ok || meta == nil {
					return merr.WrapErrServiceInternalMsg("vchannel base dirty snapshot payload is not VChannelMeta")
				}
				return catalog.SaveVChannelBaseMetas(ctx, key.PChannel, map[string]*streamingpb.VChannelMeta{key.VChannel: meta})
			case moduleapi.SnapshotOpDelete:
				meta, _ := snapshot.Payload().(*streamingpb.VChannelMeta)
				if meta == nil {
					meta = &streamingpb.VChannelMeta{Vchannel: key.VChannel}
				}
				return catalog.DropVChannels(ctx, key.PChannel, map[string]*streamingpb.VChannelMeta{key.VChannel: meta})
			default:
				return errors.Errorf("unknown vchannel snapshot op: %d", snapshot.Op())
			}
		case moduleapi.ModuleNameSegment:
			switch snapshot.Op() {
			case moduleapi.SnapshotOpUpsert:
				payload, ok := snapshot.Payload().(*streamingpb.SegmentAssignmentMeta)
				if !ok || payload == nil {
					return merr.WrapErrServiceInternalMsg("segment dirty snapshot payload is not SegmentAssignmentMeta")
				}
				return catalog.SaveSegmentAssignments(ctx, key.PChannel, map[int64]*streamingpb.SegmentAssignmentMeta{key.SegmentID: payload})
			case moduleapi.SnapshotOpDelete:
				return catalog.DropSegmentAssignments(ctx, key.PChannel, []int64{key.SegmentID})
			default:
				return errors.Errorf("unknown segment snapshot op: %d", snapshot.Op())
			}
		case moduleapi.ModuleNameTransformLog:
			switch snapshot.Op() {
			case moduleapi.SnapshotOpUpsert:
				meta, ok := snapshot.Payload().(*streamingpb.VChannelTransformLogMeta)
				if !ok || meta == nil {
					return errors.New("transformlog dirty snapshot payload is not VChannelTransformLogMeta")
				}
				return catalog.SaveTransformLogMeta(ctx, key.PChannel, map[string]*streamingpb.VChannelTransformLogMeta{key.VChannel: meta})
			case moduleapi.SnapshotOpDelete:
				return catalog.DropTransformLogMeta(ctx, key.PChannel, []string{key.VChannel})
			default:
				return errors.Errorf("unknown transformlog snapshot op: %d", snapshot.Op())
			}
		default:
			return errors.Errorf("unknown module dirty snapshot: %s", snapshot.ModuleName())
		}
	})
}

func (rs *recoveryStorageImpl) persistCheckpointSnapshot(ctx context.Context, snapshot *dirtyPersistSnapshot, _ bool) error {
	recoverySnapshot := &metastore.WALRecoverySnapshot{}
	if snapshot.SalvageCheckpoint != nil {
		recoverySnapshot.SalvageCheckpoint = snapshot.SalvageCheckpoint.IntoProto()
	}
	if snapshot.CheckpointDirty {
		recoverySnapshot.ConsumeCheckpoint = snapshot.Checkpoint.IntoProto()
	}
	if recoverySnapshot.SalvageCheckpoint == nil && recoverySnapshot.ConsumeCheckpoint == nil {
		return nil
	}
	if err := retryOperationWithBackoff(ctx, rs.Logger().With(mlog.String("op", "persistCheckpoint")), func(ctx context.Context) error {
		return resource.Resource().StreamingNodeCatalog().SaveRecoverySnapshot(ctx, rs.channel.Name, recoverySnapshot)
	}); err != nil {
		return err
	}
	if snapshot.CheckpointDirty {
		rs.mu.Lock()
		rs.persistedCheckpoint = snapshot.Checkpoint.Clone()
		rs.mu.Unlock()
		rs.metrics.ObServePersistedMetrics(snapshot.Checkpoint.TimeTick)
		rs.simpleTruncateCheckpoint(ctx, snapshot.Checkpoint)
	}
	return nil
}

func (rs *recoveryStorageImpl) simpleTruncateCheckpoint(ctx context.Context, checkpoint *WALCheckpoint) {
	if rs.truncator == nil || checkpoint.DataCheckpoint == nil || checkpoint.DataCheckpoint.MessageID == nil {
		return
	}
	_ = rs.truncator.Truncate(ctx, checkpoint.DataCheckpoint.MessageID)
}

// retryOperationWithBackoff retries the operation with exponential backoff.
func (rs *recoveryStorageImpl) retryOperationWithBackoff(ctx context.Context, logger *mlog.Logger, op func(ctx context.Context) error) error {
	return retryOperationWithBackoff(ctx, logger, op)
}

func retryOperationWithBackoff(ctx context.Context, logger *mlog.Logger, op func(ctx context.Context) error) error {
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
		logger.Warn(context.TODO(), "failed to persist operation, wait for retry...", mlog.Duration("nextRetryInterval", nextInterval), mlog.Err(err))
		select {
		case <-time.After(nextInterval):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func newBackoff() *backoff.ExponentialBackOff {
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = 1 * time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()
	return backoff
}
