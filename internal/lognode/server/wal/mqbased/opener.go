package mqbased

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"go.uber.org/zap"
)

var _ wal.BasicOpener = (*mqBasedOpener)(nil)

// mqBasedOpener is the opener implementation based on message queue.
type mqBasedOpener struct {
	c mqwrapper.Client
}

// Open creates a new wal instance based on message queue.
func (o *mqBasedOpener) Open(ctx context.Context, opt *wal.BasicOpenOption) (wal.BasicWAL, error) {
	p, err := o.c.CreateProducer(mqwrapper.ProducerOptions{
		Topic: opt.Channel.Name,
	})
	if err != nil {
		return nil, status.NewInner(err.Error())
	}
	return &walImpl{
		logger:   log.With(zap.Any("channel", opt.Channel)),
		channel:  opt.Channel,
		registry: newScannerRegistry(opt.Channel),
		scanners: typeutil.NewConcurrentMap[string, *mqBasedScanner](),
		c:        o.c,
		p:        p,
	}, nil
}

// Close closes the opener resources.
func (o *mqBasedOpener) Close() {
	o.c.Close()
}
