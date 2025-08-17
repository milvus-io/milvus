package configuration

import (
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/cdcpb"
)

type ReplicateConfiguration interface {
	GetReplicateID() string
	GetState() cdcpb.ReplicateState
	GetReason() string
	GetConfiguration() *milvuspb.ReplicateConfiguration
}

func WrapConfigLog(config ReplicateConfiguration, fields ...zap.Field) []zap.Field {
	return append(fields, []zap.Field{
		zap.String("replicateID", config.GetReplicateID()),
		zap.String("state", config.GetState().String()),
		zap.String("reason", config.GetReason()),
	}...)
}
