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

	recoverySnapshot, err := rs.buildRecoverySnapshot(snapshot)
	if err != nil {
		return err
	}
	if err := rs.saveRecoverySnapshot(ctx, recoverySnapshot); err != nil {
		return err
	}
	for _, dirtySnapshot := range snapshot.ModuleDirtySnaps {
		dirtySnapshot.MarkPersisted()
	}
	if snapshot.CheckpointDirty {
		rs.mu.Lock()
		rs.persistedCheckpoint = snapshot.Checkpoint.Clone()
		rs.mu.Unlock()
		rs.metrics.ObServePersistedMetrics(snapshot.Checkpoint.TimeTick)
		rs.simpleTruncateCheckpoint(ctx, snapshot.Checkpoint)
	}
	return
}

func (rs *recoveryStorageImpl) buildRecoverySnapshot(snapshot *dirtyPersistSnapshot) (*metastore.WALRecoverySnapshot, error) {
	recoverySnapshot := &metastore.WALRecoverySnapshot{}
	type snapshotIdentity struct {
		module    moduleapi.ModuleName
		vchannel  string
		segmentID int64
	}
	seen := make(map[snapshotIdentity]struct{}, len(snapshot.ModuleDirtySnaps))
	for _, dirtySnapshot := range snapshot.ModuleDirtySnaps {
		key := dirtySnapshot.Key()
		if key.PChannel != "" && key.PChannel != rs.channel.Name {
			return nil, merr.WrapErrServiceInternalMsg(
				"dirty snapshot pchannel mismatch: expected %s, got %s",
				rs.channel.Name,
				key.PChannel,
			)
		}
		identity := snapshotIdentity{
			module:    dirtySnapshot.ModuleName(),
			vchannel:  key.VChannel,
			segmentID: key.SegmentID,
		}
		if identity.module == moduleapi.ModuleNameSegment {
			identity.vchannel = ""
		} else {
			identity.segmentID = 0
		}
		if _, ok := seen[identity]; ok {
			return nil, merr.WrapErrServiceInternalMsg(
				"duplicate dirty snapshot for module %s, vchannel %s, segment %d",
				identity.module,
				identity.vchannel,
				identity.segmentID,
			)
		}
		seen[identity] = struct{}{}

		switch dirtySnapshot.ModuleName() {
		case moduleapi.ModuleNameVChannel:
			if key.VChannel == "" {
				return nil, merr.WrapErrServiceInternalMsg("vchannel dirty snapshot is missing vchannel key")
			}
			meta, ok := dirtySnapshot.Payload().(*streamingpb.VChannelMeta)
			if !ok || meta == nil {
				return nil, merr.WrapErrServiceInternalMsg("vchannel dirty snapshot payload is not VChannelMeta")
			}
			if meta.GetVchannel() != key.VChannel {
				return nil, merr.WrapErrServiceInternalMsg(
					"vchannel dirty snapshot key mismatch: expected %s, got %s",
					key.VChannel,
					meta.GetVchannel(),
				)
			}
			switch dirtySnapshot.Op() {
			case moduleapi.SnapshotOpUpsert:
				if recoverySnapshot.VChannels == nil {
					recoverySnapshot.VChannels = make(map[string]*streamingpb.VChannelMeta)
				}
				recoverySnapshot.VChannels[key.VChannel] = meta
			case moduleapi.SnapshotOpUpsertBase:
				if recoverySnapshot.VChannelBaseMetas == nil {
					recoverySnapshot.VChannelBaseMetas = make(map[string]*streamingpb.VChannelMeta)
				}
				recoverySnapshot.VChannelBaseMetas[key.VChannel] = meta
			case moduleapi.SnapshotOpDelete:
				if recoverySnapshot.RemovedVChannels == nil {
					recoverySnapshot.RemovedVChannels = make(map[string]*streamingpb.VChannelMeta)
				}
				recoverySnapshot.RemovedVChannels[key.VChannel] = meta
			default:
				return nil, merr.WrapErrServiceInternalMsg("unknown vchannel snapshot op: %d", dirtySnapshot.Op())
			}
		case moduleapi.ModuleNameSegment:
			switch dirtySnapshot.Op() {
			case moduleapi.SnapshotOpUpsert:
				meta, ok := dirtySnapshot.Payload().(*streamingpb.SegmentAssignmentMeta)
				if !ok || meta == nil {
					return nil, merr.WrapErrServiceInternalMsg("segment dirty snapshot payload is not SegmentAssignmentMeta")
				}
				if meta.GetSegmentId() != key.SegmentID {
					return nil, merr.WrapErrServiceInternalMsg(
						"segment dirty snapshot key mismatch: expected %d, got %d",
						key.SegmentID,
						meta.GetSegmentId(),
					)
				}
				if recoverySnapshot.SegmentAssignments == nil {
					recoverySnapshot.SegmentAssignments = make(map[int64]*streamingpb.SegmentAssignmentMeta)
				}
				recoverySnapshot.SegmentAssignments[key.SegmentID] = meta
			case moduleapi.SnapshotOpDelete:
				recoverySnapshot.RemovedSegmentIDs = append(recoverySnapshot.RemovedSegmentIDs, key.SegmentID)
			default:
				return nil, merr.WrapErrServiceInternalMsg("unknown segment snapshot op: %d", dirtySnapshot.Op())
			}
		case moduleapi.ModuleNameTransformLog:
			if key.VChannel == "" {
				return nil, merr.WrapErrServiceInternalMsg("transformlog dirty snapshot is missing vchannel key")
			}
			switch dirtySnapshot.Op() {
			case moduleapi.SnapshotOpUpsert:
				meta, ok := dirtySnapshot.Payload().(*streamingpb.VChannelTransformLogMeta)
				if !ok || meta == nil {
					return nil, merr.WrapErrServiceInternalMsg("transformlog dirty snapshot payload is not VChannelTransformLogMeta")
				}
				if recoverySnapshot.TransformLogMetas == nil {
					recoverySnapshot.TransformLogMetas = make(map[string]*streamingpb.VChannelTransformLogMeta)
				}
				recoverySnapshot.TransformLogMetas[key.VChannel] = meta
			case moduleapi.SnapshotOpDelete:
				recoverySnapshot.RemovedTransformLogs = append(recoverySnapshot.RemovedTransformLogs, key.VChannel)
			default:
				return nil, merr.WrapErrServiceInternalMsg("unknown transformlog snapshot op: %d", dirtySnapshot.Op())
			}
		default:
			return nil, merr.WrapErrServiceInternalMsg("unknown module dirty snapshot: %s", dirtySnapshot.ModuleName())
		}
	}
	if snapshot.SalvageCheckpoint != nil {
		recoverySnapshot.SalvageCheckpoint = snapshot.SalvageCheckpoint.IntoProto()
	}
	if snapshot.CheckpointDirty {
		recoverySnapshot.ConsumeCheckpoint = snapshot.Checkpoint.IntoProto()
	}
	return recoverySnapshot, nil
}

func (rs *recoveryStorageImpl) saveRecoverySnapshot(ctx context.Context, snapshot *metastore.WALRecoverySnapshot) error {
	return retryOperationWithBackoff(ctx, rs.Logger().With(mlog.String("op", "persistRecoverySnapshot")), func(ctx context.Context) error {
		return resource.Resource().StreamingNodeCatalog().SaveRecoverySnapshot(ctx, rs.channel.Name, snapshot)
	})
}

func (rs *recoveryStorageImpl) simpleTruncateCheckpoint(ctx context.Context, checkpoint *WALCheckpoint) {
	if rs.truncator == nil || checkpoint.DataCheckpoint == nil || checkpoint.DataCheckpoint.MessageID == nil {
		return
	}
	_ = rs.truncator.Truncate(ctx, checkpoint.DataCheckpoint.MessageID)
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
		if !isRetryableRecoveryPersistError(err) {
			return err
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

func isRetryableRecoveryPersistError(err error) bool {
	if merr.IsRetryableErr(err) {
		return true
	}
	if merr.IsNonRetryableErr(err) || errors.IsAny(
		err,
		merr.ErrServiceInternal,
		merr.ErrDataIntegrity,
		merr.ErrSerializationFailed,
		merr.ErrParameterInvalid,
	) {
		return false
	}
	// MetaKv implementations can return untyped transport/backend errors.
	// Preserve the existing retry behavior for those unknown errors.
	return true
}

func newBackoff() *backoff.ExponentialBackOff {
	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = 1 * time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()
	return backoff
}
