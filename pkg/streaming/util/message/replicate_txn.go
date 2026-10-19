package message

import (
	"maps"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
)

// CDC adds this transport property to a copy of the source message. The receiving
// Proxy moves it into ReplicateHeader before the message enters the local WAL.
const messageReplicateTxnPrevious = "_rtprev"

// WithReplicateTxnPrevious builds a wire copy without changing the source WAL
// message or the pending transaction retained by CDC for reconnect.
func WithReplicateTxnPrevious(msg ImmutableMessage, previous MessageID) *commonpb.ImmutableMessage {
	result := msg.IntoImmutableMessageProto()
	properties := maps.Clone(msg.Properties().ToRawMap())
	value, err := EncodeProto(previous.IntoProto())
	if err != nil {
		panic("cannot encode transaction predecessor")
	}
	properties[messageReplicateTxnPrevious] = value
	result.Properties = properties
	return result
}
