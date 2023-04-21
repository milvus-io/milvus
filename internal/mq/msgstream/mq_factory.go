package msgstream

import (
	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/rmq"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"go.uber.org/zap"
)

// NewRocksmqFactory creates a new message stream factory based on rocksmq.
func NewRocksmqFactory(path string) msgstream.Factory {
	if err := server.InitRocksMQ(path); err != nil {
		log.Fatal("fail to init rocksmq", zap.Error(err))
	}
	log.Info("init rocksmq msgstream success", zap.String("path", path))

	return &msgstream.CommonFactory{
		Newer:             rmq.NewClientWithDefaultOptions,
		DispatcherFactory: msgstream.ProtoUDFactory{},
		ReceiveBufSize:    1024,
		MQBufSize:         1024,
	}
}
