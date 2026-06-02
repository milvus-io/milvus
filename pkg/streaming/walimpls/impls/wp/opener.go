package wp

import (
	"context"

	"github.com/zilliztech/woodpecker/woodpecker"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
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
		mlog.Error(ctx, "failed to check log exists", mlog.String("log_name", opt.Channel.Name), mlog.Err(err))
		return nil, err
	}
	if !exists {
		if err := o.c.CreateLog(ctx, opt.Channel.Name); err != nil {
			mlog.Error(ctx, "failed to create log", mlog.String("log_name", opt.Channel.Name), mlog.Err(err))
			return nil, err
		}
	}
	l, err := o.c.OpenLog(ctx, opt.Channel.Name)
	if err != nil {
		mlog.Error(ctx, "failed to open log", mlog.String("log_name", opt.Channel.Name), mlog.Err(err))
		return nil, err
	}
	p, err := l.OpenLogWriter(ctx)
	if err != nil {
		mlog.Error(ctx, "failed to open log writer", mlog.String("log_name", opt.Channel.Name), mlog.Err(err))
		return nil, err
	}
	mlog.Info(ctx, "finish to open log writer", mlog.String("log_name", opt.Channel.Name))
	return &walImpl{
		WALHelper: helper.NewWALHelper(opt),
		p:         p,
		l:         l,
	}, nil
}

// Close closes the opener resources.
func (o *openerImpl) Close() {
	ctx := context.Background()
	err := o.c.Close(ctx)
	if err != nil {
		mlog.Error(ctx, "failed to close woodpecker client", mlog.Err(err))
	}
}
