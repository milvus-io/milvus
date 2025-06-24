package pulsar

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

const (
	truncateCursorSubscriptionName = "truncate-cursor"
)

var _ walimpls.OpenerImpls = (*openerImpl)(nil)

// openerImpl is the opener for pulsar wal.
type openerImpl struct {
	c pulsar.Client
}

// Open opens a wal instance.
func (o *openerImpl) Open(ctx context.Context, opt *walimpls.OpenOption) (walimpls.WALImpls, error) {
	if err := opt.Validate(); err != nil {
		return nil, err
	}

	var backlogClearHelper *backlogClearHelper
	if opt.Channel.AccessMode == types.AccessModeRW {
		backlogAutoClearBytes := paramtable.Get().PulsarCfg.BacklogAutoClearBytes.GetAsSize()
		if backlogAutoClearBytes > 0 {
			backlogClearHelper = newBacklogClearHelper(o.c, opt.Channel, backlogAutoClearBytes)
		} else {
			// Initialize a persistent cursor to protect the topic from being retention.
			cursor, err := o.c.Subscribe(pulsar.ConsumerOptions{
				Topic:                       opt.Channel.Name,
				SubscriptionName:            truncateCursorSubscriptionName,
				Type:                        pulsar.Exclusive,
				MaxPendingChunkedMessage:    0,
				SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
			})
			if err != nil {
				return nil, err
			}
			cursor.Close()
		}
	}
	w := &walImpl{
		WALHelper:          helper.NewWALHelper(opt),
		c:                  o.c,
		p:                  syncutil.NewFuture[pulsar.Producer](),
		notifier:           syncutil.NewAsyncTaskNotifier[struct{}](),
		backlogClearHelper: backlogClearHelper,
	}
	// because the producer of pulsar cannot be created if the topic is backlog exceeded,
	// so we need to set the producer at background with backoff retry.
	w.initProducerAtBackground()
	return w, nil
}

// Close closes the opener resources.
func (o *openerImpl) Close() {
	o.c.Close()
}
