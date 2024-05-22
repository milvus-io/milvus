package mqbased

import (
	"context"

	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/extends"
	smsgstream "github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/util"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

const (
	walName = "mqBasedWAL"
)

var _ wal.OpenerBuilder = (*builder)(nil)

// builder is the builder for mqBasedOpener.
type builder struct {
	name string
}

// Name of the wal builder, should be a lowercase string.
func (b *builder) Name() string {
	return b.name
}

// Build build a wal instance.
func (b *builder) Build() (wal.Opener, error) {
	mqClient, err := newMQClient(b.name)
	if err != nil {
		return nil, err
	}
	return extends.NewOpenerWithBasicOpener(&mqBasedOpener{
		c: mqClient,
	}), nil
}

// newMQClient creates a new mq client.
func newMQClient(mqType string) (mqwrapper.Client, error) {
	// TODO: Too many singleton in the code, clean it in future.
	params := paramtable.Get()
	switch mqType {
	case util.MQTypeNatsmq:
		return msgstream.NewNatsmqFactory().NewClient(context.TODO())
	case util.MQTypeRocksmq:
		return smsgstream.NewRocksmqFactory(params.RocksmqCfg.Path.GetValue(), &params.ServiceParam).NewClient(context.TODO())
	case util.MQTypePulsar:
		return msgstream.NewPmsFactory(&params.ServiceParam).NewClient(context.TODO())
	case util.MQTypeKafka:
		return msgstream.NewKmsFactory(&params.ServiceParam).NewClient(context.TODO())
	default:
		panic("unknown mq type")
	}
}
