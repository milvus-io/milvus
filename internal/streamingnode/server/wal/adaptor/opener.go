package adaptor

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var _ wal.Opener = (*openerAdaptorImpl)(nil)

// adaptImplsToOpener creates a new wal opener with opener impls.
func adaptImplsToOpener(opener walimpls.OpenerImpls, builders []interceptors.InterceptorBuilder) wal.Opener {
	return &openerAdaptorImpl{
		lifetime:            typeutil.NewLifetime(),
		opener:              opener,
		idAllocator:         typeutil.NewIDAllocator(),
		walInstances:        typeutil.NewConcurrentMap[int64, wal.WAL](),
		interceptorBuilders: builders,
		logger:              log.With(log.FieldComponent("opener")),
	}
}

// openerAdaptorImpl is the wrapper of OpenerImpls to Opener.
type openerAdaptorImpl struct {
	lifetime            *typeutil.Lifetime
	opener              walimpls.OpenerImpls
	idAllocator         *typeutil.IDAllocator
	walInstances        *typeutil.ConcurrentMap[int64, wal.WAL] // store all wal instances allocated by these allocator.
	interceptorBuilders []interceptors.InterceptorBuilder
	logger              *log.MLogger
}

// Open opens a wal instance for the channel.
func (o *openerAdaptorImpl) Open(ctx context.Context, opt *wal.OpenOption) (wal.WAL, error) {
	if !o.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, status.NewOnShutdownError("wal opener is on shutdown")
	}
	defer o.lifetime.Done()

	id := o.idAllocator.Allocate()
	logger := o.logger.With(zap.String("channel", opt.Channel.String()), zap.Int64("id", id))

	l, err := o.opener.Open(ctx, &walimpls.OpenOption{
		Channel: opt.Channel,
	})
	if err != nil {
		logger.Warn("open wal failed", zap.Error(err))
		return nil, err
	}

	// wrap the wal into walExtend with cleanup function and interceptors.
	wal := adaptImplsToWAL(l, o.interceptorBuilders, func() {
		o.walInstances.Remove(id)
		logger.Info("wal deleted from opener")
	})

	o.walInstances.Insert(id, wal)
	logger.Info("new wal created")
	return wal, nil
}

// Close the wal opener, release the underlying resources.
func (o *openerAdaptorImpl) Close() {
	o.lifetime.SetState(typeutil.LifetimeStateStopped)
	o.lifetime.Wait()

	// close all wal instances.
	o.walInstances.Range(func(id int64, l wal.WAL) bool {
		l.Close()
		o.logger.Info("close wal by opener", zap.Int64("id", id), zap.Any("channel", l.Channel()))
		return true
	})
	// close the opener
	o.opener.Close()
}
