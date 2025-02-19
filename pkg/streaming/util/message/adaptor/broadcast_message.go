package adaptor

import (
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

func NewMsgPackFromMutableMessageV1(msg message.MutableMessage) (msgstream.TsMsg, error) {
	if msg.Version() != message.VersionV1 {
		return nil, errors.New("Invalid message version")
	}

	tsMsg, err := UnmashalerDispatcher.Unmarshal(msg.Payload(), MustGetCommonpbMsgTypeFromMessageType(msg.MessageType()))
	if err != nil {
		return nil, errors.Wrap(err, "Failed to unmarshal message")
	}
	return recoverMutableMessageFromHeader(tsMsg, msg)
}

func recoverMutableMessageFromHeader(tsMsg msgstream.TsMsg, _ message.MutableMessage) (msgstream.TsMsg, error) {
	// TODO: fillback the header information to tsMsg
	return tsMsg, nil
}
