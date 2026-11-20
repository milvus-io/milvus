package adaptor

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/autowalimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ wal.Opener = (*openerAdaptorImpl)(nil)

// adaptImplsToOpener creates a new wal opener with opener impls.
// Test Only
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
	o.SetLogger(resource.Resource().Logger().With(mlog.FieldComponent("wal-opener")))
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
	o.SetLogger(resource.Resource().Logger().With(mlog.FieldComponent("wal-opener")))
	return o
}

// openerAdaptorImpl is the wrapper that adapts walimpls.OpenerImpls to wal.Opener.
// It supports opening different WALImpls dynamically at runtime.
type openerAdaptorImpl struct {
	mlog.Binder

	lifetime            *typeutil.Lifetime
	mu                  sync.Mutex                               // protects openerCache
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
	walName, err := o.determineWALName(ctx, opt)
	if err != nil {
		return nil, err
	}

	logger := mlog.With(mlog.String("channel", opt.Channel.String()), mlog.Stringer("walName", walName))

	// Get or create the underlying walimpls.OpenerImpls for this walName
	openerImpl, err := o.getOrCreateOpenerImpl(ctx, walName)
	if err != nil {
		logger.Warn(ctx, "get or create underlying wal impls opener failed", mlog.Err(err))
		return nil, err
	}

	// Open the underlying WAL implementation
	l, err := openerImpl.Open(ctx, &walimpls.OpenOption{
		Channel: opt.Channel,
	})
	if err != nil {
		logger.Warn(ctx, "open wal impls failed", mlog.Err(err))
		return nil, err
	}

	var wal wal.WAL
	switch opt.Channel.AccessMode {
	case types.AccessModeRW:
		wal, err = o.openRWWAL(ctx, l, opt)
	case types.AccessModeRO:
		if l.WALName() != message.WALNameTest {
			l = autowalimpls.New(l, o.backendOpener(opt.Channel))
		}
		wal, err = o.openROWAL(l)
	default:
		panic("unknown access mode")
	}
	if err != nil {
		logger.Warn(ctx, "open wal failed", mlog.Err(err))
		return nil, err
	}
	logger.Info(ctx, "open wal done", mlog.Stringer("walName", walName), mlog.String("pchannel", opt.Channel.Name))
	return wal, nil
}

// backendOpener supplies physical handles to auto during recovery and live use.
func (o *openerAdaptorImpl) backendOpener(channel types.PChannelInfo) autowalimpls.OpenBackend {
	return func(ctx context.Context, name message.WALName, mode types.AccessMode) (walimpls.WALImpls, error) {
		opener, err := o.getOrCreateOpenerImpl(ctx, name)
		if err != nil {
			return nil, err
		}
		info := channel
		info.AccessMode = mode
		return opener.Open(ctx, &walimpls.OpenOption{Channel: info})
	}
}

// determineWALName determines which walName to use for the given channel.
func (o *openerAdaptorImpl) determineWALName(ctx context.Context, opt *wal.OpenOption) (message.WALName, error) {
	walName := message.WALNameUnknown
	catalog := resource.Resource().StreamingNodeCatalog()
	cpProto, err := catalog.GetConsumeCheckpoint(ctx, opt.Channel.Name)
	if err != nil {
		return message.WALNameUnknown, errors.Wrap(err, "failed to get checkpoint from catalog")
	}
	if cpProto != nil {
		checkpoint := utility.NewWALCheckpointFromProto(cpProto)
		fields := []mlog.Field{
			mlog.String("channel", opt.Channel.Name),
			mlog.Stringer("checkpoint", checkpoint.MessageID),
			mlog.Uint64("checkpointTimeTick", checkpoint.TimeTick),
			mlog.Stringer("currentWAL", checkpoint.MessageID.WALName()),
		}
		mlog.Info(ctx, "get checkpoint from catalog", fields...)
		walName = checkpoint.MessageID.WALName()
	}

	if walName == message.WALNameUnknown {
		// Use default WAL if already register
		walName = message.GetDefaultWALName()
	}

	if walName == message.WALNameUnknown {
		// Use wal selector to choose one
		walName = util.MustSelectWALName()
	}
	return walName, nil
}

// getOrCreateOpenerImpl gets an existing opener from cache or creates a new one.
func (o *openerAdaptorImpl) getOrCreateOpenerImpl(ctx context.Context, walName message.WALName) (walimpls.OpenerImpls, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if opener, ok := o.openerCache[walName]; ok {
		return opener, nil
	}

	// Double-check after acquiring write lock
	if opener, ok := o.openerCache[walName]; ok {
		return opener, nil
	}

	// Build and cache new opener
	builderImpl := registry.MustGetBuilder(walName)
	opener, err := builderImpl.Build()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to build walimpls opener for %s", walName)
	}

	o.openerCache[walName] = opener
	mlog.Info(ctx, "created and cached new walimpls opener", mlog.Stringer("walName", walName))
	return opener, nil
}

// openRWWAL opens a read write wal instance for the channel.
func (o *openerAdaptorImpl) openRWWAL(ctx context.Context, l walimpls.WALImpls, opt *wal.OpenOption) (wal.WAL, error) {
	l = autowalimpls.New(l, o.backendOpener(opt.Channel))
	id := o.idAllocator.Allocate()
	roWAL := adaptImplsToROWAL(l, func() {
		o.walInstances.Remove(id)
	})
	resources := &walOpenResources{roWAL: roWAL}
	defer resources.Close()

	cpProto, err := resource.Resource().StreamingNodeCatalog().GetConsumeCheckpoint(ctx, opt.Channel.Name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get checkpoint from catalog")
	}
	cp := utility.NewWALCheckpointFromProto(cpProto)

	var firstPosition message.MessageID
	if cp != nil {
		firstPosition = cp.MessageID
	}
	firstBarrier, err := sendFirstTimeTick(ctx, l, firstPosition)
	if err != nil {
		return nil, err
	}
	var param *interceptors.InterceptorBuildParam
	queryViewCatalog := resource.Resource().StreamingNodeCatalog()
	persistedViews, err := queryViewCatalog.ListQueryViews(ctx, opt.Channel.Name)
	if err != nil {
		return nil, merr.Wrap(err, "load streaming node query views")
	}
	liveReady := make(chan struct{})
	builder := newRecoveryStreamBuilder(roWAL, nil, liveReady)
	rs, snapshot, err := recovery.RecoverRecoveryStorage(
		ctx,
		builder,
		cp,
		firstBarrier,
		recovery.WithRecoveryTailRateLimiter(roWAL.RecoveryStorage),
		recovery.WithConsumedThrough(func(tt uint64) {
			// Bounded recovery precedes WAB construction. Live input starts only
			// after the completion callback has bound the final buffer.
			if param != nil {
				param.WriteAheadBuffer.ReleaseThrough(tt)
			}
		}),
		recovery.WithRecoveryComplete(func(snapshot *recovery.RecoverySnapshot) {
			param = newInterceptorParams(ctx, l, snapshot.RecoveryBarrier)
			resources.param = param
			param.WriteAheadBuffer.SetPressureController(roWAL.MuxRateLimitObserverRegistryImpl)
			builder.writeAheadBuffer = param.WriteAheadBuffer
		}),
		recovery.WithRecoveryFatalHandler(roWAL.markUnavailable),
		recovery.WithQueryViewRecovery(func(manager *vchannel.PChannelRecoveryManager) {
			resources.queryViewHandler = snview.RecoverPChannelSNQueryViewHandler(roWAL.availableCtx, opt.Channel.Name, queryViewCatalog, manager, persistedViews)
		}),
	)
	if err != nil {
		return nil, errors.Wrap(err, "when recovering recovery storage")
	}
	param.RecoveryStorage = rs

	param.LastConfirmedMessageID = determineLastConfirmedMessageID(param.LastTimeTickMessage.MessageID(), snapshot.TxnBuffer)
	param.InitialRecoverSnapshot = snapshot
	param.TxnManager = txn.NewTxnManager(param.ChannelInfo, snapshot.TxnBuffer.GetUncommittedMessageBuilder())
	param.ShardManager = shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
		ChannelInfo:            param.ChannelInfo,
		WAL:                    param.WAL,
		InitialRecoverSnapshot: snapshot,
		TxnManager:             param.TxnManager,
	})
	// Load salvage checkpoints from etcd (one per source cluster that was force-promoted from).
	var salvageCheckpoints []*utility.ReplicateCheckpoint
	if salvageCPProtos, err := resource.Resource().StreamingNodeCatalog().GetSalvageCheckpoint(ctx, param.ChannelInfo.Name); err != nil {
		mlog.Info(ctx, "failed to load salvage checkpoints", mlog.Err(err))
	} else {
		for _, proto := range salvageCPProtos {
			salvageCheckpoints = append(salvageCheckpoints, utility.NewReplicateCheckpointFromProto(proto))
		}
	}

	if param.ReplicateManager, err = replicates.RecoverReplicateManager(
		&replicates.ReplicateManagerRecoverParam{
			ChannelInfo:            param.ChannelInfo,
			CurrentClusterID:       paramtable.Get().CommonCfg.ClusterPrefix.GetValue(),
			InitialRecoverSnapshot: snapshot,
			SalvageCheckpoints:     salvageCheckpoints,
		},
	); err != nil {
		return nil, err
	}

	for vchannel := range snapshot.WritePathRecovery.VChannels {
		param.MVCCManager.ApplyRecoveryBarrier(vchannel, param.LastTimeTickMessage.TimeTick())
	}
	wal := adaptImplsToRWWAL(roWAL, o.interceptorBuilders, param)
	wal.queryViewHandler = resources.queryViewHandler
	wal.viewResourceManager = rs.VChannelManager()
	close(liveReady)
	o.walInstances.Insert(id, wal)
	resources.Release()
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

	o.Logger().Info(context.TODO(), "wal opener closing...")

	// close all wal instances.
	o.walInstances.Range(func(id int64, l wal.WAL) bool {
		l.Close()
		o.Logger().Info(context.TODO(), "close wal by opener", mlog.Int64("id", id), mlog.String("channel", l.Channel().String()))
		return true
	})

	// close all cached opener impls
	o.mu.Lock()
	defer o.mu.Unlock()
	for walName, opener := range o.openerCache {
		o.Logger().Info(context.TODO(), "closing underlying walimpls opener", mlog.Stringer("walName", walName))
		opener.Close()
	}
	o.openerCache = nil

	o.Logger().Info(context.TODO(), "wal opener closed")
}
