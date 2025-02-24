package pulsar

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	walName = "pulsar"
)

func init() {
	// register the builder to the wal registry.
	registry.RegisterBuilder(&builderImpl{})
	// register the unmarshaler to the message registry.
	message.RegisterMessageIDUnmsarshaler(walName, UnmarshalMessageID)
}

// builderImpl is the builder for pulsar wal.
type builderImpl struct{}

// Name returns the name of the wal.
func (b *builderImpl) Name() string {
	return walName
}

// Build build a wal instance.
func (b *builderImpl) Build() (walimpls.OpenerImpls, error) {
	options, err := b.getPulsarClientOptions()
	if err != nil {
		return nil, errors.Wrapf(err, "build pulsar client options failed")
	}
	c, err := pulsar.NewClient(options)
	if err != nil {
		return nil, err
	}
	return &openerImpl{
		c: c,
	}, nil
}

// getPulsarClientOptions gets the pulsar client options from the config.
func (b *builderImpl) getPulsarClientOptions() (pulsar.ClientOptions, error) {
	cfg := &paramtable.Get().PulsarCfg
	auth, err := pulsar.NewAuthentication(cfg.AuthPlugin.GetValue(), cfg.AuthParams.GetValue())
	if err != nil {
		return pulsar.ClientOptions{}, errors.New("build authencation from config failed")
	}
	options := pulsar.ClientOptions{
		URL:              cfg.Address.GetValue(),
		OperationTimeout: cfg.RequestTimeout.GetAsDuration(time.Second),
		Authentication:   auth,
	}
	if cfg.EnableClientMetrics.GetAsBool() {
		// Enable client metrics if config.EnableClientMetrics is true, use pkg-defined registerer.
		options.MetricsRegisterer = metrics.GetRegisterer()
	}
	return options, nil
}
