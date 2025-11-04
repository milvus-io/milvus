package adaptor

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher/flusherimpl"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ wal.Opener = (*openerAdaptorImpl)(nil)

// adaptImplsToOpener creates a new wal opener with opener impls.
// Deprecated: Use NewOpenerAdaptor instead.
func adaptImplsToOpener(basicOpener walimpls.OpenerImpls, interceptorBuilders []interceptors.InterceptorBuilder) wal.Opener {
	o := &openerAdaptorImpl{
		lifetime:            typeutil.NewLifetime(),
		openerCache:         make(map[message.WALName]walimpls.OpenerImpls),
		idAllocator:         typeutil.NewIDAllocator(),
		walInstances:        typeutil.NewConcurrentMap[int64, wal.WAL](),
		interceptorBuilders: interceptorBuilders,
	}
	o.openerCache[message.WALNameTest] = basicOpener
	o.SetLogger(resource.Resource().Logger().With(log.FieldComponent("wal-opener")))
	return o
}

// NewOpenerAdaptor creates a new dynamic wal opener that can open different MQ types at runtime.
// It doesn't bind to a specific walName at construction time, instead it selects the appropriate
// wal implementation based on the walName in OpenOption when Open() is called.
func NewOpenerAdaptor(builders []interceptors.InterceptorBuilder) wal.Opener {
	o := &openerAdaptorImpl{
		lifetime:            typeutil.NewLifetime(),
		openerCache:         make(map[message.WALName]walimpls.OpenerImpls),
		idAllocator:         typeutil.NewIDAllocator(),
		walInstances:        typeutil.NewConcurrentMap[int64, wal.WAL](),
		interceptorBuilders: builders,
	}
	o.SetLogger(resource.Resource().Logger().With(log.FieldComponent("wal-opener")))
	return o
}

// openerAdaptorImpl is the wrapper that adapts walimpls.OpenerImpls to wal.Opener.
// It supports opening different WALImpls dynamically at runtime.
type openerAdaptorImpl struct {
	log.Binder

	lifetime            *typeutil.Lifetime
	mu                  sync.RWMutex                             // protects openerCache
	openerCache         map[message.WALName]walimpls.OpenerImpls // cache of opened walimpls.OpenerImpls, dynamically created based on walName
	idAllocator         *typeutil.IDAllocator
	walInstances        *typeutil.ConcurrentMap[int64, wal.WAL] // store all wal instances allocated by these allocator.
	interceptorBuilders []interceptors.InterceptorBuilder
}

// Open opens a wal instance for the channel.
func (o *openerAdaptorImpl) Open(ctx context.Context, opt *wal.OpenOption) (wal.WAL, error) {
	if !o.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal opener is on shutdown")
	}
	defer o.lifetime.Done()

	// Determine which walName to use
	walName, err := o.resolveWALName(ctx, opt)
	if err != nil {
		return nil, err
	}

	logger := o.Logger().With(zap.String("channel", opt.Channel.String()), zap.Stringer("walName", walName))

	// Get or create the underlying walimpls.OpenerImpls for this walName
	openerImpl, err := o.getOrCreateOpenerImpl(walName)
	if err != nil {
		logger.Warn("get or create underlying wal impls opener failed", zap.Error(err))
		return nil, err
	}

	// Open the underlying WAL implementation
	l, err := openerImpl.Open(ctx, &walimpls.OpenOption{
		Channel: opt.Channel,
	})
	if err != nil {
		logger.Warn("open wal impls failed", zap.Error(err))
		return nil, err
	}

	var wal wal.WAL
	switch opt.Channel.AccessMode {
	case types.AccessModeRW:
		wal, err = o.openRWWAL(ctx, l, opt)
	case types.AccessModeRO:
		wal, err = o.openROWAL(l)
	default:
		panic("unknown access mode")
	}
	if err != nil {
		logger.Warn("open wal failed", zap.Error(err))
		return nil, err
	}
	logger.Info("open wal done", zap.Stringer("walName", walName), zap.String("pchannel", opt.Channel.Name))
	return wal, nil
}

// resolveWALName determines the WAL name to use.
// It checks if there's an alter WAL message in the checkpoint and switches to the target WAL if found.
// Otherwise, it uses the WAL name from checkpoint or falls back to the default from config.
func (o *openerAdaptorImpl) resolveWALName(ctx context.Context, opt *wal.OpenOption) (message.WALName, error) {
	walName := message.WALNameUnknown
	catalog := resource.Resource().StreamingNodeCatalog()
	cpProto, err := catalog.GetConsumeCheckpoint(ctx, opt.Channel.Name)
	if err != nil {
		return message.WALNameUnknown, errors.Wrap(err, "failed to get checkpoint from catalog")
	}
	if cpProto != nil {
		checkpoint := utility.NewWALCheckpointFromProto(cpProto)
		walName = checkpoint.MessageID.WALName()
		if checkpoint.AlterWalMsgFound {
			// Switch to target WAL if alter WAL message found in checkpoint
			originalWALName := walName
			walName = message.NewWALName(checkpoint.TargetWalName)
			o.Logger().Info("alter WAL checkpoint found, switching to target WAL",
				zap.String("channel", opt.Channel.Name),
				zap.String("checkpoint", checkpoint.MessageID.String()),
				zap.Uint64("checkpointTimeTick", checkpoint.TimeTick),
				zap.Stringer("originalWAL", originalWALName),
				zap.String("targetWAL", checkpoint.TargetWalName))
		}
	}

	if walName == message.WALNameUnknown {
		// Use default WAL from config if not specified in checkpoint
		walName = util.MustSelectWALName()
	}
	return walName, nil
}

// getOrCreateOpenerImpl gets an existing opener from cache or creates a new one.
func (o *openerAdaptorImpl) getOrCreateOpenerImpl(walName message.WALName) (walimpls.OpenerImpls, error) {
	// Fast path: check cache with read lock
	o.mu.RLock()
	if opener, ok := o.openerCache[walName]; ok {
		o.mu.RUnlock()
		return opener, nil
	}
	o.mu.RUnlock()

	// Slow path: create with write lock
	o.mu.Lock()
	defer o.mu.Unlock()

	// Double-check after acquiring write lock
	if opener, ok := o.openerCache[walName]; ok {
		return opener, nil
	}

	// Build and cache new opener
	builder := registry.MustGetBuilder(walName)
	opener, err := builder.Build()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to build walimpls opener for %s", walName)
	}

	o.openerCache[walName] = opener
	o.Logger().Info("created and cached new walimpls opener", zap.Stringer("walName", walName))
	return opener, nil
}

// openRWWAL opens a read write wal instance for the channel.
func (o *openerAdaptorImpl) openRWWAL(ctx context.Context, l walimpls.WALImpls, opt *wal.OpenOption) (wal.WAL, error) {
	id := o.idAllocator.Allocate()
	roWAL := adaptImplsToROWAL(l, func() {
		o.walInstances.Remove(id)
	})
	cpProto, err := resource.Resource().StreamingNodeCatalog().GetConsumeCheckpoint(ctx, opt.Channel.Name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get checkpoint from catalog")
	}
	cp := utility.NewWALCheckpointFromProto(cpProto)

	// recover the wal state.
	param, err := buildInterceptorParams(ctx, l, cp)
	if err != nil {
		roWAL.Close()
		return nil, errors.Wrap(err, "when building interceptor params")
	}
	rs, snapshot, err := recovery.RecoverRecoveryStorage(ctx, newRecoveryStreamBuilder(roWAL), cp, param.LastTimeTickMessage)
	if err != nil {
		param.Clear()
		roWAL.Close()
		return nil, errors.Wrap(err, "when recovering recovery storage")
	}

	// Handle alter WAL if found in snapshot
	// This flushes all remaining data and triggers WAL switch to the target implementation
	if snapshot.FoundAlterWALMsg {
		return o.handleAlterWAL(ctx, l, opt, roWAL, param, rs, snapshot)
	}

	param.LastConfirmedMessageID = determineLastConfirmedMessageID(param.LastTimeTickMessage.MessageID(), snapshot.TxnBuffer)
	param.InitialRecoverSnapshot = snapshot
	param.TxnManager = txn.NewTxnManager(param.ChannelInfo, snapshot.TxnBuffer.GetUncommittedMessageBuilder())
	param.ShardManager = shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
		ChannelInfo:            param.ChannelInfo,
		WAL:                    param.WAL,
		InitialRecoverSnapshot: snapshot,
		TxnManager:             param.TxnManager,
	})
	if param.ReplicateManager, err = replicates.RecoverReplicateManager(
		&replicates.ReplicateManagerRecoverParam{
			ChannelInfo:            param.ChannelInfo,
			CurrentClusterID:       paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
			InitialRecoverSnapshot: snapshot,
		},
	); err != nil {
		return nil, err
	}

	// start the flusher to flush and generate recovery info.
	var flusher *flusherimpl.WALFlusherImpl
	if !opt.DisableFlusher {
		flusher = flusherimpl.RecoverWALFlusher(&flusherimpl.RecoverWALFlusherParam{
			WAL:              param.WAL,
			RecoveryStorage:  rs,
			ChannelInfo:      l.Channel(),
			RecoverySnapshot: snapshot,
		})
	}
	wal := adaptImplsToRWWAL(roWAL, o.interceptorBuilders, param, flusher)
	o.walInstances.Insert(id, wal)
	return wal, nil
}

// determineLastConfirmedMessageID determines the last confirmed message id after recovery.
// The last confirmed message id is the minimum last confirmed message id of all uncommitted txn messages.
func determineLastConfirmedMessageID(lastTimeTickMessageID message.MessageID, txnBuffer *utility.TxnBuffer) message.MessageID {
	// From here, we can read all messages which timetick is greater than timetick of LastTimeTickMessage sent at these term.
	lastConfirmedMessageID := lastTimeTickMessageID
	for _, builder := range txnBuffer.GetUncommittedMessageBuilder() {
		if builder.LastConfirmedMessageID().LT(lastConfirmedMessageID) {
			// use the minimum last confirmed message id of all uncommitted txn messages to protect the `LastConfirmedMessageID` promise.
			lastConfirmedMessageID = builder.LastConfirmedMessageID()
		}
	}
	return lastConfirmedMessageID
}

// handleAlterWAL handles WAL switch by flushing all remaining data and triggering WAL re-opening.
// Steps: 1. Start flusher to flush all growing segments
//  2. Wait for all data up to LastTimeTickMessage to be flushed
//  3. Close recovery storage to persist checkpoint
//  4. Return error to trigger WAL re-opening with new implementation
func (o *openerAdaptorImpl) handleAlterWAL(ctx context.Context, l walimpls.WALImpls, opt *wal.OpenOption,
	roWAL *roWALAdaptorImpl, param *interceptors.InterceptorBuildParam, rs recovery.RecoveryStorage, snapshot *recovery.RecoverySnapshot,
) (wal.WAL, error) {
	logger := o.Logger().With(
		zap.String("channel", opt.Channel.String()),
		zap.String("targetWAL", snapshot.TargetWALName))

	logger.Info("detected alter WAL message in snapshot",
		zap.String("checkpointMessageID", snapshot.Checkpoint.MessageID.String()),
		zap.Uint64("checkpointTimeTick", snapshot.Checkpoint.TimeTick),
		zap.Any("alterWALConfig", snapshot.AlterWALConfig))

	// Start flusher to flush all segments
	var flusher *flusherimpl.WALFlusherImpl
	if !opt.DisableFlusher {
		flusher = flusherimpl.RecoverWALFlusher(&flusherimpl.RecoverWALFlusherParam{
			WAL:              param.WAL,
			RecoveryStorage:  rs,
			ChannelInfo:      l.Channel(),
			RecoverySnapshot: snapshot,
		})
	}

	defer func() {
		// Close recovery storage to persist the final checkpoint and segment states
		logger.Info("closing recovery storage to persist WAL switch snapshot")
		rs.Close()
		flusher.Close()
		param.Clear()
		roWAL.Close()
	}()

	// Wait for all data up to LastTimeTickMessage to be flushed
	targetTimeTick := param.LastTimeTickMessage.TimeTick()
	logger.Info("waiting for all data to be flushed before WAL switch",
		zap.Uint64("targetTimeTick", targetTimeTick))

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	const defaultWALSwitchFlushTimeout = 1 * time.Minute
	checkCtx, checkCancel := context.WithTimeout(context.Background(), defaultWALSwitchFlushTimeout)
	defer checkCancel()

	// Periodically check flush progress
	for {
		select {
		case <-ticker.C:
			flusherCP := rs.GetFlusherCheckpointByTimeTick()
			if flusherCP == nil {
				logger.Info("waiting for flusher checkpoint initialization")
				continue
			}

			if flusherCP.TimeTick >= targetTimeTick {
				logger.Info("all data flushed, triggering re-opening with new WAL implementation",
					zap.Uint64("flusherCheckpointTimeTick", flusherCP.TimeTick),
					zap.Uint64("targetTimeTick", targetTimeTick),
					zap.String("flusherCheckpoint", flusherCP.MessageID.String()),
					zap.String("newWAL", snapshot.TargetWALName),
					zap.String("newCheckpoint", snapshot.Checkpoint.MessageID.String()))
				return nil, errors.Errorf(
					"WAL switch detected: switch to %s at checkpoint %s (timetick: %d), re-opening required",
					snapshot.TargetWALName,
					snapshot.Checkpoint.MessageID.String(),
					snapshot.Checkpoint.TimeTick)
			}

			remaining := targetTimeTick - flusherCP.TimeTick
			logger.Info("waiting for flush completion",
				zap.Uint64("flusherCheckpointTimeTick", flusherCP.TimeTick),
				zap.Uint64("targetTimeTick", targetTimeTick),
				zap.Uint64("remainingTimeTick", remaining),
				zap.String("flusherCheckpoint", flusherCP.MessageID.String()))

		case <-checkCtx.Done():
			logger.Warn("timeout waiting for flush completion during WAL switch",
				zap.Error(checkCtx.Err()),
				zap.Duration("timeout", defaultWALSwitchFlushTimeout))
			return nil, errors.Wrap(checkCtx.Err(), "timeout waiting for flush completion during WAL switch")

		case <-ctx.Done():
			logger.Warn("context canceled while waiting for flush completion during WAL switch",
				zap.Error(ctx.Err()))
			return nil, errors.Wrap(ctx.Err(), "context canceled during WAL switch flush waiting")
		}
	}
}

// openROWAL opens a read only wal instance for the channel.
func (o *openerAdaptorImpl) openROWAL(l walimpls.WALImpls) (wal.WAL, error) {
	id := o.idAllocator.Allocate()
	wal := adaptImplsToROWAL(l, func() {
		o.walInstances.Remove(id)
	})
	o.walInstances.Insert(id, wal)
	return wal, nil
}

// Close the wal opener, release the underlying resources.
func (o *openerAdaptorImpl) Close() {
	o.lifetime.SetState(typeutil.LifetimeStateStopped)
	o.lifetime.Wait()

	o.Logger().Info("wal opener closing...")

	// close all wal instances.
	o.walInstances.Range(func(id int64, l wal.WAL) bool {
		l.Close()
		o.Logger().Info("close wal by opener", zap.Int64("id", id), zap.String("channel", l.Channel().String()))
		return true
	})

	// close all cached opener impls
	o.mu.Lock()
	defer o.mu.Unlock()
	for walName, opener := range o.openerCache {
		o.Logger().Info("closing underlying walimpls opener", zap.Stringer("walName", walName))
		opener.Close()
	}
	o.openerCache = nil

	o.Logger().Info("wal opener closed")
}
