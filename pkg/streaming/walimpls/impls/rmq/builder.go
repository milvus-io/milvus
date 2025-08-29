package rmq

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/mq/mqimpl/rocksmq/client"
	"github.com/milvus-io/milvus/pkg/v2/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/registry"
)

const (
	WALName = "rocksmq"
)

func init() {
	// register the builder to the registry.
	registry.RegisterBuilder(&builderImpl{})
	// register the unmarshaler to the message registry.
	message.RegisterMessageIDUnmsarshaler(WALName, UnmarshalMessageID)
	message.RegisterMessageIDUnmsarshaler(commonpb.WALName_RocksMQ.String(), UnmarshalMessageID)
}

// builderImpl is the builder for rmq opener.
type builderImpl struct{}

// Name of the wal builder, should be a lowercase string.
func (b *builderImpl) Name() string {
	return WALName
}

// Build build a wal instance.
func (b *builderImpl) Build() (walimpls.OpenerImpls, error) {
	c, err := client.NewClient(client.Options{
		Server: server.Rmq,
	})
	if err != nil {
		return nil, err
	}
	return &openerImpl{
		c: c,
	}, nil
}
