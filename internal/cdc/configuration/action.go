package configuration

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/cdcpb"
)

type UpdateAction = func(config ReplicateConfiguration) error

func UpdateState(state cdcpb.ReplicateState) UpdateAction {
	return func(config ReplicateConfiguration) error {
		config.(*cdcpb.ReplicateConfiguration).State = state
		return nil
	}
}

func UpdateReason(reason string) UpdateAction {
	return func(config ReplicateConfiguration) error {
		config.(*cdcpb.ReplicateConfiguration).Reason = reason
		return nil
	}
}

func UpdateConfiguration(configuration *milvuspb.ReplicateConfiguration) UpdateAction {
	return func(config ReplicateConfiguration) error {
		config.(*cdcpb.ReplicateConfiguration).Configuration = configuration
		return nil
	}
}
