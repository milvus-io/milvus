package msgstream

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/logservice"
	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	logservicemq "github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/logservice"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/rmq"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// NewRocksmqFactory creates a new message stream factory based on rocksmq.
func NewRocksmqFactory(path string, cfg *paramtable.ServiceParam) *msgstream.CommonFactory {
	if err := server.InitRocksMQ(path); err != nil {
		log.Fatal("fail to init rocksmq", zap.Error(err))
	}
	log.Info("init rocksmq msgstream success", zap.String("path", path))

	return &msgstream.CommonFactory{
		Newer:             rmq.NewClientWithDefaultOptions,
		DispatcherFactory: msgstream.ProtoUDFactory{},
	}
}

// TODO: remove in future.
func NewLogServiceFactory() *msgstream.CommonFactory {
	var mqClient mqwrapper.Client
	once := &sync.Once{}
	return &msgstream.CommonFactory{
		Newer: func(ctx context.Context) (mqwrapper.Client, error) {
			once.Do(func() {
				logServiceClient := logservice.DialContext(ctx, kvfactory.GetEtcd())
				// TODO: Bad implementation of outside msgstream ( recreate underlying client every new msgstream is created ).
				// We use a client singleton here, and refactor the outside msgstream in future.
				// However, the client can never be close until the milvus node exit,
				// logServiceClient is leak.
				// see logservicemq.logServiceClient.Close()
				mqClient = logservicemq.NewLogServiceClient(logServiceClient)
			})
			return mqClient, nil
		},
		DispatcherFactory: msgstream.ProtoUDFactory{},
		CustomDisposer: func(channels []string, _ string) error {
			return nil
		}, // logservice clean up by itself, do nothing here.
	}
}
