package pulsar

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin"
	pulsaradminconfig "github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/pulsar/pulsarlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func init() {
	// register the builder to the wal registry.
	registry.RegisterBuilder(&builderImpl{})
	// register the unmarshaler to the message registry.
	message.RegisterMessageIDUnmsarshaler(message.WALNamePulsar, UnmarshalMessageID)
}

// builderImpl is the builder for pulsar wal.
type builderImpl struct{}

// Name returns the name of the wal.
func (b *builderImpl) Name() message.WALName {
	return message.WALNamePulsar
}

// Build build a wal instance.
func (b *builderImpl) Build() (walimpls.OpenerImpls, error) {
	options, tenant, err := b.getPulsarClientOptions()
	if err != nil {
		return nil, errors.Wrapf(err, "build pulsar client options failed")
	}
	c, err := pulsar.NewClient(options)
	if err != nil {
		return nil, err
	}
	return &openerImpl{
		tenant:        tenant,
		c:             c,
		newTopicAdmin: b.getPulsarTopicAdmin,
	}, nil
}

func (b *builderImpl) getPulsarTopicAdmin() (pulsarTopicAdmin, error) {
	cfg := &paramtable.Get().PulsarCfg
	adminClient, err := admin.New(&pulsaradminconfig.Config{
		WebServiceURL: cfg.WebAddress.GetValue(),
		AuthPlugin:    cfg.AuthPlugin.GetValue(),
		AuthParams:    cfg.AuthParams.GetValue(),
	})
	if err != nil {
		return nil, merr.WrapErrMqInternal(err, "build pulsar admin client")
	}
	return adminClient.Topics(), nil
}

// getPulsarClientOptions gets the pulsar client options from the config.
func (b *builderImpl) getPulsarClientOptions() (pulsar.ClientOptions, tenant, error) {
	cfg := &paramtable.Get().PulsarCfg
	auth, err := pulsar.NewAuthentication(cfg.AuthPlugin.GetValue(), cfg.AuthParams.GetValue())
	if err != nil {
		return pulsar.ClientOptions{}, tenant{}, merr.WrapErrParameterInvalidMsg("build authencation from config failed")
	}
	options := pulsar.ClientOptions{
		URL:              cfg.Address.GetValue(),
		OperationTimeout: cfg.RequestTimeout.GetAsDuration(time.Second),
		Authentication:   auth,
		Logger:           pulsarlog.NewLogger(),
	}
	if cfg.EnableClientMetrics.GetAsBool() {
		// Enable client metrics if config.EnableClientMetrics is true, use pkg-defined registerer.
		options.MetricsRegisterer = metrics.GetRegisterer()
	}
	return options, tenant{
		namespace: cfg.Namespace.GetValue(),
		tenant:    cfg.Tenant.GetValue(),
	}, nil
}
