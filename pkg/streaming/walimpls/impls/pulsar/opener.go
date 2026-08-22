package pulsar

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

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

	topicAdminMu  sync.Mutex
	topicAdmin    pulsarTopicAdmin
	newTopicAdmin func() (pulsarTopicAdmin, error)
}

// Open opens a wal instance.
func (o *openerImpl) Open(ctx context.Context, opt *walimpls.OpenOption) (walimpls.WALImpls, error) {
	if err := opt.Validate(); err != nil {
		return nil, err
	}
	if opt.Channel.AccessMode == types.AccessModeRO {
		// A WAL name identifies the backend, not the migration epoch. Even if
		// the requested name matches the current backend, an A->B->A migration
		// can still make this a historical read, so every RO open is checked.
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
	if err := ctx.Err(); err != nil {
		return err
	}
	topicName, err := utils.GetTopicName(topic)
	if err != nil {
		return merr.WrapErrMqInternal(err, "parse pulsar topic name")
	}
	topicAdmin, err := o.getTopicAdmin()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return merr.Wrap(err, "get pulsar topic admin")
	}

	checkCtx, cancel := context.WithTimeout(ctx, getPulsarTopicCheckTimeout())
	defer cancel()
	if _, err := topicAdmin.GetStatsWithContext(checkCtx, *topicName); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if isExplicitPulsarTopicNotFound(err) {
			return merr.WrapErrMqTopicNotFound(topic, err.Error())
		}
		return merr.WrapErrMqInternal(err, "check pulsar topic existence")
	}
	return nil
}

func getPulsarTopicCheckTimeout() time.Duration {
	timeout := paramtable.Get().PulsarCfg.RequestTimeout.GetAsDuration(time.Second)
	if timeout <= 0 {
		return 60 * time.Second
	}
	return timeout
}

func (o *openerImpl) getTopicAdmin() (pulsarTopicAdmin, error) {
	o.topicAdminMu.Lock()
	defer o.topicAdminMu.Unlock()
	if o.topicAdmin != nil {
		return o.topicAdmin, nil
	}
	if o.newTopicAdmin == nil {
		return nil, merr.WrapErrMqInternalMsg("pulsar topic admin is unavailable")
	}
	topicAdmin, err := o.newTopicAdmin()
	if err != nil {
		return nil, err
	}
	if topicAdmin == nil {
		return nil, merr.WrapErrMqInternalMsg("pulsar topic admin returned no topic service")
	}
	o.topicAdmin = topicAdmin
	return o.topicAdmin, nil
}

func isExplicitPulsarTopicNotFound(err error) bool {
	var adminErr rest.Error
	if !errors.As(err, &adminErr) || adminErr.Code != http.StatusNotFound {
		return false
	}
	reason := strings.ToLower(strings.TrimSpace(adminErr.Reason))
	return strings.Contains(reason, "topic") &&
		(strings.Contains(reason, "not found") || strings.Contains(reason, "does not exist"))
}

// Close closes the opener resources.
func (o *openerImpl) Close() {
	o.c.Close()
}
