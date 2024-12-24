package client

import (
	"bytes"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/pkg/streaming/proto/messagespb"
)

var (
	// magicPrefix is used to identify the rocksmq legacy message and new message for streaming service.
	// Make a low probability of collision with the legacy proto message.
	magicPrefix                   = append([]byte{0xFF, 0xFE, 0xFD, 0xFC}, []byte("STREAM")...)
	errNotStreamingServiceMessage = errors.New("not a streaming service message")
)

// marshalStreamingMessage marshals a streaming message to bytes.
func marshalStreamingMessage(message *common.ProducerMessage) ([]byte, error) {
	rmqMessage := &messagespb.RMQMessageLayout{
		Payload:    message.Payload,
		Properties: message.Properties,
	}
	payload, err := proto.Marshal(rmqMessage)
	if err != nil {
		return nil, err
	}
	finalPayload := make([]byte, len(payload)+len(magicPrefix))
	copy(finalPayload, magicPrefix)
	copy(finalPayload[len(magicPrefix):], payload)
	return finalPayload, nil
}

// unmarshalStreamingMessage unmarshals a streaming message from bytes.
func unmarshalStreamingMessage(topic string, msg server.ConsumerMessage) (*RmqMessage, error) {
	if !bytes.HasPrefix(msg.Payload, magicPrefix) {
		return nil, errNotStreamingServiceMessage
	}

	var rmqMessage messagespb.RMQMessageLayout
	if err := proto.Unmarshal(msg.Payload[len(magicPrefix):], &rmqMessage); err != nil {
		return nil, err
	}
	return &RmqMessage{
		msgID:      msg.MsgID,
		payload:    rmqMessage.Payload,
		properties: rmqMessage.Properties,
		topic:      topic,
	}, nil
}
