package pulsar

import (
	"context"
	"fmt"
	"net/http"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/rest"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/helper"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

const (
	truncateCursorSubscriptionName = "truncate-cursor"
	defaultBacklogSize             = 100 * 1024 * 1024 // default 100MB
)

var _ walimpls.OpenerImpls = (*openerImpl)(nil)

type pulsarTopicAdmin interface {
	GetStatsWithContext(context.Context, utils.TopicName) (utils.TopicStats, error)
}

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
	tenant tenant
	c      pulsar.Client
	topics pulsarTopicAdmin
}

// Open opens a wal instance.
func (o *openerImpl) Open(ctx context.Context, opt *walimpls.OpenOption) (walimpls.WALImpls, error) {
	if err := opt.Validate(); err != nil {
		return nil, err
	}
	if opt.Channel.AccessMode == types.AccessModeRO {
		topic := o.tenant.MustGetFullTopicName(opt.Channel.Name)
		if err := o.checkTopicExists(ctx, topic); err != nil {
			return nil, err
		}
	}

	var backlogClearHelper *backlogClearHelper
	if opt.Channel.AccessMode == types.AccessModeRW {
		backlogAutoClearBytes := paramtable.Get().PulsarCfg.BacklogAutoClearBytes.GetAsSize()
		if backlogAutoClearBytes <= 0 {
			backlogAutoClearBytes = defaultBacklogSize
		}
		backlogClearHelper = newBacklogClearHelper(o.c, opt.Channel, backlogAutoClearBytes, o.tenant)
	}
	w := &walImpl{
		WALHelper:          helper.NewWALHelper(opt),
		c:                  o.c,
		p:                  syncutil.NewFuture[pulsar.Producer](),
		notifier:           syncutil.NewAsyncTaskNotifier[struct{}](),
		backlogClearHelper: backlogClearHelper,
		tenant:             o.tenant,
	}
	// because the producer of pulsar cannot be created if the topic is backlog exceeded,
	// so we need to set the producer at background with backoff retry.
	w.initProducerAtBackground()
	return w, nil
}

func (o *openerImpl) checkTopicExists(ctx context.Context, topic string) error {
	if o.topics == nil {
		return merr.WrapErrMqInternalMsg("pulsar topic admin is unavailable")
	}
	topicName, err := utils.GetTopicName(topic)
	if err != nil {
		return merr.WrapErrMqInternal(err, "parse pulsar topic name")
	}
	if _, err := o.topics.GetStatsWithContext(ctx, *topicName); err != nil {
		var adminErr rest.Error
		if errors.As(err, &adminErr) && adminErr.Code == http.StatusNotFound {
			return merr.WrapErrMqTopicNotFound(topic, adminErr.Reason)
		}
		return merr.WrapErrMqInternal(err, "check pulsar topic existence")
	}
	return nil
}

// Close closes the opener resources.
func (o *openerImpl) Close() {
	o.c.Close()
}
