package pulsar

import (
	"context"
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	truncateCursorSubscriptionName = "truncate-cursor"
	defaultBacklogSize             = 100 * 1024 * 1024 // default 100MB
)

var _ walimpls.OpenerImpls = (*openerImpl)(nil)

// tenant is the tenant of pulsar.
type tenant struct {
	tenant    string
	namespace string
}

// MustGetFullTopicName gets the full topic name of pulsar.
// If the tenant or namespace or topic is empty, it will panic.
func (t tenant) MustGetFullTopicName(topic string) string {
	if len(t.tenant) == 0 || len(t.namespace) == 0 || len(topic) == 0 {
		panic("tenant or namespace or topic is empty")
	}
	return fmt.Sprintf("%s/%s/%s", t.tenant, t.namespace, topic)
}

// openerImpl is the opener for pulsar wal.
type openerImpl struct {
	tenant            tenant
	c                 pulsar.Client
	newProducerClient clientFactory // creates the client dedicated to the producer of each read-write wal.
}

// Open opens a wal instance.
func (o *openerImpl) Open(ctx context.Context, opt *walimpls.OpenOption) (walimpls.WALImpls, error) {
	if err := opt.Validate(); err != nil {
		return nil, err
	}

	w := &walImpl{
		WALHelper: helper.NewWALHelper(opt),
		c:         o.c,
		tenant:    o.tenant,
	}
	if opt.Channel.AccessMode != types.AccessModeRW {
		return w, nil
	}

	backlogAutoClearBytes := paramtable.Get().PulsarCfg.BacklogAutoClearBytes.GetAsSize()
	if backlogAutoClearBytes <= 0 {
		backlogAutoClearBytes = defaultBacklogSize
	}
	// The backlog clear helper starts before the producer, because a backlog exceeded topic rejects producer creation.
	w.backlogClearHelper = newBacklogClearHelper(o.c, opt.Channel, backlogAutoClearBytes, o.tenant)
	producer, err := newWALProducer(ctx, o.newProducerClient, o.tenant.MustGetFullTopicName(opt.Channel.Name), w.Log())
	if err != nil {
		w.Close()
		return nil, err
	}
	w.producer = producer
	return w, nil
}

// Close closes the opener resources.
func (o *openerImpl) Close() {
	o.c.Close()
}
