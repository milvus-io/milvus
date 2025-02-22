package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/helper"
)

var _ walimpls.OpenerImpls = (*openerImpl)(nil)

// openerImpl is the opener for pulsar wal.
type openerImpl struct {
	c pulsar.Client
}

// Open opens a wal instance.
func (o *openerImpl) Open(ctx context.Context, opt *walimpls.OpenOption) (walimpls.WALImpls, error) {
	p, err := o.c.CreateProducer(pulsar.ProducerOptions{
		// TODO: configurations.
		Topic: opt.Channel.Name,
	})
	if err != nil {
		return nil, err
	}
	return &walImpl{
		WALHelper: helper.NewWALHelper(opt),
		p:         p,
		c:         o.c,
	}, nil
}

// Close closes the opener resources.
func (o *openerImpl) Close() {
	o.c.Close()
}
