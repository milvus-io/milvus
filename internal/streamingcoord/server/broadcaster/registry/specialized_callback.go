package registry

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// init the message ack callbacks
func init() {
	resetMessageAckCallbacks()
	resetMessageCheckCallbacks()
}

// resetMessageCheckCallbacks resets the message check callbacks.
func resetMessageCheckCallbacks() {
	messageCheckCallbacks = map[message.MessageTypeWithVersion]*syncutil.Future[messageInnerCheckCallback]{
		message.MessageTypeImportV1: syncutil.NewFuture[messageInnerCheckCallback](),
	}
}

var (
	RegisterDropPartitionMessageV1AckCallback = registerMessageAckCallback[*message.DropPartitionMessageHeader, *msgpb.DropPartitionRequest]
	RegisterImportMessageV1AckCallback        = registerMessageAckCallback[*message.ImportMessageHeader, *msgpb.ImportMsg]
)

// resetMessageAckCallbacks resets the message ack callbacks.
func resetMessageAckCallbacks() {
	messageAckCallbacks = map[message.MessageTypeWithVersion]*syncutil.Future[messageInnerAckCallback]{
		message.MessageTypeDropPartitionV1: syncutil.NewFuture[messageInnerAckCallback](),
		message.MessageTypeImportV1:        syncutil.NewFuture[messageInnerAckCallback](),
	}
}

var RegisterImportMessageV1CheckCallback = registerMessageCheckCallback[*message.ImportMessageHeader, *msgpb.ImportMsg]
