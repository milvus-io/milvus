package rmq

import (
	"github.com/milvus-io/milvus/pkg/v3/mq/mqimpl/rocksmq/client"
	"github.com/milvus-io/milvus/pkg/v3/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func init() {
	// register the builder to the registry.
	registry.RegisterBuilder(&builderImpl{})
	// register the unmarshaler to the message registry.
	message.RegisterMessageIDUnmsarshaler(message.WALNameRocksmq, UnmarshalMessageID)
}

// builderImpl is the builder for rmq opener.
type builderImpl struct{}

// Name of the wal builder, should be a lowercase string.
func (b *builderImpl) Name() message.WALName {
	return message.WALNameRocksmq
}

// Build build a wal instance.
func (b *builderImpl) Build() (walimpls.OpenerImpls, error) {
	if !paramtable.IsStandalone() {
		return nil, merr.WrapErrServiceInternalMsg("RocksMQ WAL is only supported in standalone mode")
	}
	if server.Rmq == nil {
		path := paramtable.Get().RocksmqCfg.Path.GetValue()
		if err := server.InitRocksMQ(path); err != nil {
			return nil, merr.WrapErrServiceInternalErr(err, "failed to initialize RocksMQ at %q", path)
		}
		if server.Rmq == nil {
			return nil, merr.WrapErrServiceInternalMsg("RocksMQ is unavailable after initialization at %q", path)
		}
	}
	c, err := client.NewClient(client.Options{
		Server: server.Rmq,
	})
	if err != nil {
		return nil, err
	}
	return &openerImpl{
		c:   c,
		rmq: server.Rmq,
	}, nil
}
