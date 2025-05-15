package adaptor

import (
	"context"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/flusher/flusherimpl"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ wal.Opener = (*openerAdaptorImpl)(nil)

// adaptImplsToOpener creates a new wal opener with opener impls.
func adaptImplsToOpener(opener walimpls.OpenerImpls, builders []interceptors.InterceptorBuilder) wal.Opener {
	o := &openerAdaptorImpl{
		lifetime:            typeutil.NewLifetime(),
		opener:              opener,
		idAllocator:         typeutil.NewIDAllocator(),
		walInstances:        typeutil.NewConcurrentMap[int64, wal.WAL](),
		interceptorBuilders: builders,
	}
	o.SetLogger(resource.Resource().Logger().With(log.FieldComponent("wal-opener")))
	return o
}

// openerAdaptorImpl is the wrapper of OpenerImpls to Opener.
type openerAdaptorImpl struct {
	log.Binder

	lifetime            *typeutil.Lifetime
	opener              walimpls.OpenerImpls
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

	logger := o.Logger().With(zap.String("channel", opt.Channel.String()))

	l, err := o.opener.Open(ctx, &walimpls.OpenOption{
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
	logger.Info("open wal done")
	return wal, nil
}

// openRWWAL opens a read write wal instance for the channel.
func (o *openerAdaptorImpl) openRWWAL(ctx context.Context, l walimpls.WALImpls, opt *wal.OpenOption) (wal.WAL, error) {
	id := o.idAllocator.Allocate()
	roWAL := adaptImplsToROWAL(l, func() {
		o.walInstances.Remove(id)
	})

	// recover the wal state.
	param, err := buildInterceptorParams(ctx, l)
	if err != nil {
		roWAL.Close()
		return nil, errors.Wrap(err, "when building interceptor params")
	}
	rs, snapshot, err := recovery.RecoverRecoveryStorage(ctx, newRecoveryStreamBuilder(roWAL), param.LastTimeTickMessage)
	if err != nil {
		param.Clear()
		roWAL.Close()
		return nil, errors.Wrap(err, "when recovering recovery storage")
	}
	param.InitialRecoverSnapshot = snapshot
	param.TxnManager = txn.NewTxnManager(param.ChannelInfo, snapshot.TxnBuffer.GetUncommittedMessageBuilder())
	param.ShardManager = shards.RecoverShardManager(&shards.ShardManagerRecoverParam{
		ChannelInfo:            param.ChannelInfo,
		WAL:                    param.WAL,
		InitialRecoverSnapshot: snapshot,
		TxnManager:             param.TxnManager,
	})

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

	// close all wal instances.
	o.walInstances.Range(func(id int64, l wal.WAL) bool {
		l.Close()
		o.Logger().Info("close wal by opener", zap.Int64("id", id), zap.String("channel", l.Channel().String()))
		return true
	})
	// close the opener
	o.opener.Close()
}
