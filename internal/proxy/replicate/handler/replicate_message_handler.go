package handler

import (
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

var (
	handler ReplicateMessageHandler
	once    sync.Once
)

func GetMsgHandler() ReplicateMessageHandler {
	once.Do(func() {
		handler = &replicateMessageHandler{}
	})
	return handler
}

// ReplicateMessageHandler is the handler for replicate message.
type ReplicateMessageHandler interface {
	// ToTargetMessage converts the replicate message to the target message by:
	// 1. replacing the pchannel to target pchannel by pchannel mapping.
	// 2. appending the replicate message ID to the target message.
	ToTargetMessage(msg *milvuspb.ReplicateRequest_ReplicateMessage) (message.MutableMessage, error)
}

type replicateMessageHandler struct{}

func (h *replicateMessageHandler) ToTargetMessage(msg *milvuspb.ReplicateRequest_ReplicateMessage) (message.MutableMessage, error) {
	return nil, nil
}
