package extends

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

var _ wal.Opener = (*openerExtendImpl)(nil)

// NewOpenerWithBasicOpener creates a new wal opener with basic opener.
func NewOpenerWithBasicOpener(opener wal.BasicOpener) wal.Opener {
	return &openerExtendImpl{
		lifetime:     lifetime.NewLifetime(lifetime.Working),
		opener:       opener,
		idAllocator:  util.NewIDAllocator(),
		walInstances: typeutil.NewConcurrentMap[int64, wal.WAL](),
	}
}

// openerExtendImpl is the wrapper of BasicOpener to implement Opener.
type openerExtendImpl struct {
	lifetime     lifetime.Lifetime[lifetime.State]
	opener       wal.BasicOpener
	idAllocator  *util.IDAllocator
	walInstances *typeutil.ConcurrentMap[int64, wal.WAL] // store all wal instances allocated by these allocator.
}

// Open opens a wal instance for the channel.
func (o *openerExtendImpl) Open(ctx context.Context, opt *wal.OpenOption) (wal.WAL, error) {
	if o.lifetime.Add(lifetime.IsWorking) != nil {
		return nil, status.NewOnShutdownError("wal opener is on shutdown")
	}
	defer o.lifetime.Done()

	id := o.idAllocator.Allocate()
	log := log.With(zap.Any("channel", opt.Channel), zap.Int64("id", id))

	l, err := o.opener.Open(ctx, &opt.BasicOpenOption)
	if err != nil {
		log.Warn("open wal failed", zap.Error(err))
		return nil, err
	}

	// wrap the wal into walExtend with cleanup function and interceptors.
	wal := WALWithCleanup(
		newWALExtend(newWALWithInterceptors(l, opt.InterceptorBuilders...)),
		func() {
			o.walInstances.Remove(id)
			log.Info("wal deleted from allocator")
			metrics.LogNodeWALTotal.WithLabelValues(paramtable.GetStringNodeID()).Dec()
		})
	o.walInstances.Insert(id, wal)

	log.Info("new wal created")
	metrics.LogNodeWALTotal.WithLabelValues(paramtable.GetStringNodeID()).Inc()
	return wal, nil
}

// Close the wal opener, release the underlying resources.
func (o *openerExtendImpl) Close() {
	o.lifetime.SetState(lifetime.Stopped)
	o.lifetime.Wait()
	o.lifetime.Close()

	// close all wal instances.
	o.walInstances.Range(func(id int64, l wal.WAL) bool {
		l.Close()
		log.Info("close wal by opener", zap.Int64("id", id), zap.Any("channel", l.Channel()))
		return true
	})
	// close the opener
	o.opener.Close()
}
