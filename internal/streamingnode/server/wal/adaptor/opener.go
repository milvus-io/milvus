package adaptor

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate/replicates"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
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
		wal, err = o.openReadReplica(ctx, l)
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
	if opt.Channel.WALReplicaID == 0 {
		return o.openWriter(ctx, l, opt, nil, nil, nil)
	}
	readTarget := opt.Channel
	readTarget.AccessMode = types.AccessModeRO
	readTarget.Term = 0
	reader, err := o.Open(ctx, &wal.OpenOption{Channel: readTarget})
	if err != nil {
		l.Close()
		return nil, err
	}
	readSide := reader.(*roWALAdaptorImpl)
	cursor, _, err := readSide.replicaBuffer.Pin(true)
	if err != nil {
		reader.Close()
		l.Close()
		return nil, err
	}
	writer, err := o.openWriter(ctx, l, opt, readSide, cursor, nil)
	if err != nil {
		cursor.Close()
		reader.Close()
	}
	return writer, err
}

func (o *openerAdaptorImpl) openWriter(ctx context.Context, l walimpls.WALImpls, opt *wal.OpenOption, readSide *roWALAdaptorImpl, cursor *replica.RecoveryCursor, frozen *recovery.FrozenSnapshot) (opened wal.WAL, openErr error) {
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
	if cpProto != nil && opt.Channel.WALReplicaID != 0 && cpProto.Term != opt.Channel.Term {
		previous := cpProto
		cpProto = proto.CloneOf(cpProto)
		cpProto.Term = opt.Channel.Term
		if err := resource.Resource().StreamingNodeCatalog().SaveRecoverySnapshot(ctx, opt.Channel.Name, &metastore.WALRecoverySnapshot{ConsumeCheckpoint: cpProto, ExpectedCheckpoint: previous}); err != nil {
			return nil, merr.Wrap(err, "claim consume checkpoint before writer barrier")
		}
	}
	cp := utility.NewWALCheckpointFromProto(cpProto)
	if opt.Channel.WALReplicaID != 0 {
		roWAL.writerPersistence = &replica.Admission{}

		roWAL.replicaPublisher = replica.NewPublisher(cpProto)
	}

	var firstPosition message.MessageID
	if cp != nil {
		firstPosition = cp.MessageID
	}
	var param *interceptors.InterceptorBuildParam
	queryViewCatalog := resource.Resource().StreamingNodeCatalog()
	persistedViews, err := queryViewCatalog.ListQueryViews(ctx, opt.Channel.Name)
	if err != nil {
		return nil, merr.Wrap(err, "load streaming node query views")
	}
	if roWAL.replicaPublisher != nil {
		desired, err := resource.Resource().QueryViewCatalog().ListQueryViews(ctx)
		if err != nil {
			return nil, merr.Wrap(err, "recover replica QueryView targets")
		}
		seedReplicaViews(roWAL.replicaPublisher, opt.Channel.Name, persistedViews, desired)
	}
	// Recovery may publish this term's manifest before Open returns. Cleanup
	// discards the writer on failure, so Sync must not reopen the same term.
	if opt.Channel.WALReplicaID != 0 {
		defer func() {
			if openErr != nil {
				openErr = errors.Join(openErr, wal.ErrWriterTermConsumed)
			}
		}()
	}
	liveReady := make(chan struct{})
	builder := newRecoveryStreamBuilder(roWAL, nil, liveReady)
	var firstBarrier message.ImmutableMessage
	if cursor == nil {
		firstBarrier, err = sendFirstTimeTick(ctx, l, firstPosition)
	} else {
		firstBarrier, err = prepareReplicaWriter(ctx, builder, cursor, readSide.replicaBuffer, firstPosition)
	}
	if err != nil {
		return nil, err
	}
	var streamBuilder recovery.RecoveryStreamBuilder = builder
	if cursor != nil {
		streamBuilder = &replicaRecoveryBuilder{WALImpls: l, cursor: cursor, ready: liveReady, onFatal: roWAL.markUnavailable}
	}
	rs, snapshot, err := frozen.Recover(
		ctx,
		streamBuilder,
		cp,
		firstBarrier,
		recovery.WithWriterPersistence(roWAL.writerPersistence),
		recovery.WithObservedWriterBarrier(cursor != nil),
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
		recovery.WithPublishedCheckpoint(func(cp *utility.WALCheckpoint) {
			if readSide != nil {
				readSide.replicaBuffer.Truncate(cp.TimeTick)
				roWAL.replicaPublisher.PublishCheckpoint(cp.IntoProto())
			}
		}),
		recovery.WithRecoveryFatalHandler(roWAL.markUnavailable),
		recovery.WithQueryViewRecovery(func(manager *vchannel.PChannelRecoveryManager) {
			if readSide != nil {
				return
			}
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
	wal.readSide = readSide
	wal.queryViewHandler = resources.queryViewHandler

	wal.viewResourceManager = rs.VChannelManager()
	if readSide != nil {
		readSide.stopFollowing()
		wal.queryViewHandler = readSide.readViews
		wal.replicaViews = newWriterViewControl(roWAL.availableCtx, readSide, roWAL.replicaPublisher, persistedViews)
	}
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

// openReadReplica starts one ordered reader from the checkpoint's physical anchor.
func (o *openerAdaptorImpl) openReadReplica(ctx context.Context, l walimpls.WALImpls) (wal.WAL, error) {
	if l.Channel().WALReplicaID == 0 {
		return o.openROWAL(l)
	}
	stored, err := resource.Resource().StreamingNodeCatalog().GetConsumeCheckpoint(ctx, l.Channel().Name)
	if err != nil {
		l.Close()
		return nil, err
	}
	policy := options.DeliverPolicyAll()
	if cp := utility.NewWALCheckpointFromProto(stored); cp != nil {
		policy = options.DeliverPolicyStartFrom(cp.MessageID)
	}
	id := o.idAllocator.Allocate()
	r := adaptImplsToROWAL(l, func() { o.walInstances.Remove(id) })
	r.replicaOpener = o
	r.replicaBuffer = replica.NewBuffer(stored.GetTimeTick())
	persisted, err := resource.Resource().StreamingNodeCatalog().ListQueryViews(ctx, l.Channel().Name)
	if err != nil {
		r.Close()
		return nil, err
	}
	desired, err := resource.Resource().QueryViewCatalog().ListQueryViews(ctx)
	if err != nil {
		r.Close()
		return nil, err
	}
	wanted := make(map[qviews.QueryViewKey]bool)
	for _, v := range desired {
		if v.Meta.State == viewpb.QueryViewState_QueryViewStateUp {
			wanted[viewKey(v)] = true
		}
	}
	kept := slices.DeleteFunc(persisted, func(v *viewpb.QueryViewOfShard) bool { return !wanted[viewKey(v)] })
	r.readQueries = vchannel.NewReplicaQueries(r.availableCtx, l.Channel().Name, r.replicaBuffer, stored.GetTimeTick())
	r.readViews = snview.RecoverPChannelSNQueryViewHandler(r.availableCtx, l.Channel().Name, nil, r.readQueries, kept)
	r.follower = newFollower(l.Channel().Name, r.readViews, r.readQueries, kept)
	r.replicaReaderDone = make(chan struct{})
	r.replicaScanner = newScannerAdaptor(fmt.Sprintf("replica-%d", l.Channel().WALReplicaID), l,
		wal.ReadOption{DeliverPolicy: policy, IgnorePauseConsumption: true}, r.scanMetrics.NewScannerMetrics(), nil, scannerConfig{replicaBuffer: r.replicaBuffer, replicaObserve: r.readQueries.Observe})
	go func() {
		defer close(r.replicaReaderDone)
		for range r.replicaScanner.Chan() {
		}
		r.replicaBuffer.Close()
		if err := r.replicaScanner.Error(); err != nil && r.availableCtx.Err() == nil {
			r.markUnavailable(err)
		}
	}()
	o.walInstances.Insert(id, r)
	r.startFollowing()
	return r, nil
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

// Promote installs a writer beside the long-lived read side. The source
// persistence fence is already in effect when seal is supplied by the caller.
func (r *roWALAdaptorImpl) Promote(ctx context.Context, target types.PChannelInfo, cooperative bool, activate func(context.Context) error) (wal.WAL, error) {
	cursor, floor, err := r.replicaBuffer.Pin(true)
	if err != nil {
		return nil, err
	}
	var writer wal.WAL
	defer func() {
		if writer == nil {
			cursor.Close()
		}
	}()
	var snapshot *recovery.FrozenSnapshot
	if cooperative {
		snapshot, err = recovery.ReadFrozenSnapshot(ctx, target)
		if err != nil {
			return nil, err
		}
		if snapshot.Checkpoint != nil && floor > snapshot.Checkpoint.TimeTick {
			return nil, merr.WrapErrServiceUnavailable("replica buffer no longer covers writer recovery checkpoint")
		}
		defer snapshot.Close()
		if err := snapshot.Prepare(ctx, target, cursor); err != nil {
			return nil, err
		}
	}
	if err := activate(ctx); err != nil {
		return nil, err
	}
	name, err := r.replicaOpener.determineWALName(ctx, &wal.OpenOption{Channel: target})
	if err != nil {
		return nil, err
	}
	if _, start := r.replicaBuffer.WriterPosition(); start != nil {
		name = start.WALName()
	}
	impl, err := r.replicaOpener.getOrCreateOpenerImpl(ctx, name)
	if err != nil {
		return nil, err
	}
	backend, err := impl.Open(ctx, &walimpls.OpenOption{Channel: target})
	if err != nil {
		return nil, err
	}
	writer, err = r.replicaOpener.openWriter(ctx, backend, &wal.OpenOption{Channel: target}, r, cursor, snapshot)
	return writer, err
}
