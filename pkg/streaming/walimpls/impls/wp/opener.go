package wp

import (
	"context"

	"github.com/zilliztech/woodpecker/woodpecker"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/helper"
)

var _ walimpls.OpenerImpls = (*openerImpl)(nil)

// openerImpl is the implementation of walimpls.Opener interface.
type openerImpl struct {
	c woodpecker.Client
}

// Open opens a new wal.
func (o *openerImpl) Open(ctx context.Context, opt *walimpls.OpenOption) (walimpls.WALImpls, error) {
	exists, err := o.c.LogExists(ctx, opt.Channel.Name)
	if err != nil {
		log.Ctx(ctx).Error("failed to check log exists", zap.String("log_name", opt.Channel.Name), zap.Error(err))
		return nil, err
	}
	if !exists {
		if err := o.c.CreateLog(ctx, opt.Channel.Name); err != nil {
			log.Ctx(ctx).Error("failed to create log", zap.String("log_name", opt.Channel.Name), zap.Error(err))
			return nil, err
		}
	}
	l, err := o.c.OpenLog(ctx, opt.Channel.Name)
	if err != nil {
		log.Ctx(ctx).Error("failed to open log", zap.String("log_name", opt.Channel.Name), zap.Error(err))
		return nil, err
	}
	p, err := l.OpenLogWriter(ctx)
	if err != nil {
		log.Ctx(ctx).Error("failed to open log writer", zap.String("log_name", opt.Channel.Name), zap.Error(err))
		return nil, err
	}
	log.Ctx(ctx).Info("finish to open log writer", zap.String("log_name", opt.Channel.Name), zap.Error(err))
	return &walImpl{
		WALHelper: helper.NewWALHelper(opt),
		p:         p,
		l:         l,
	}, nil
}

// Close closes the opener resources.
func (o *openerImpl) Close() {
	err := o.c.Close()
	if err != nil {
		log.Ctx(context.Background()).Error("failed to close woodpecker client", zap.Error(err))
	}
}
