package msgstream

import (
	"go.uber.org/zap"

	pmqimplserver "github.com/milvus-io/milvus/internal/mq/mqimpl/pebblemq/server"
	rmqimplserver "github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	pmqwrapper "github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/pmq"
	rmqwrapper "github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper/rmq"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// NewRocksmqFactory creates a new message stream factory based on rocksmq.
func NewRocksmqFactory(path string, cfg *paramtable.ServiceParam) msgstream.Factory {
	if err := rmqimplserver.InitRocksMQ(path); err != nil {
		log.Fatal("fail to init rocksmq", zap.Error(err))
	}
	log.Info("init rocksmq msgstream success", zap.String("path", path))

	return &msgstream.CommonFactory{
		Newer:             rmqwrapper.NewClientWithDefaultOptions,
		DispatcherFactory: msgstream.ProtoUDFactory{},
		ReceiveBufSize:    cfg.MQCfg.ReceiveBufSize.GetAsInt64(),
		MQBufSize:         cfg.MQCfg.MQBufSize.GetAsInt64(),
	}
}

// NewPebblemqFactory creates a new message stream factory based on pebblemq.
func NewPebblemqFactory(path string, cfg *paramtable.ServiceParam) msgstream.Factory {
	if err := pmqimplserver.InitPebbleMQ(path); err != nil {
		log.Fatal("fail to init pebblemq", zap.Error(err))
	}
	log.Info("init pebblemq msgstream success", zap.String("path", path))

	return &msgstream.CommonFactory{
		Newer:             pmqwrapper.NewClientWithDefaultOptions,
		DispatcherFactory: msgstream.ProtoUDFactory{},
		ReceiveBufSize:    cfg.MQCfg.ReceiveBufSize.GetAsInt64(),
		MQBufSize:         cfg.MQCfg.MQBufSize.GetAsInt64(),
	}
}
