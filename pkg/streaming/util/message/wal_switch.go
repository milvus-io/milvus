package message

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SetWALSwitchStart installs the durable target anchor on an exclusively owned
// AlterWAL. The target anchor and the old boundary share one logical timestamp.
func SetWALSwitchStart(msg MutableMessage, id MessageID) {
	alter := MustAsMutableAlterWALMessageV2(msg)
	// Each PChannel owns its anchor; do not mutate a shared broadcast header.
	header := proto.Clone(alter.Header()).(*AlterWALMessageHeader)
	header.TargetStartMessageId = id.IntoProto()
	alter.OverwriteHeader(header)
}

// IsWALSwitch distinguishes the linked protocol from legacy AlterWAL records.
func IsWALSwitch(msg BasicMessage) bool {
	return msg.MessageType() == MessageTypeAlterWAL && walSwitchHeader(msg).GetTargetStartMessageId() != nil
}

// WALSwitchStart returns nil for a legacy AlterWAL, otherwise the target anchor.
func WALSwitchStart(msg BasicMessage) (MessageID, error) {
	if msg.MessageType() != MessageTypeAlterWAL {
		return nil, nil
	}
	header := walSwitchHeader(msg)
	if header.GetTargetStartMessageId() == nil {
		return nil, nil
	}
	start, err := UnmarshalMessageID(header.GetTargetStartMessageId())
	if err != nil {
		return nil, merr.Wrap(err, "decode WAL switch message ID")
	}
	if start.WALName() != WALName(header.GetTargetWalName()) {
		return nil, merr.WrapErrDataIntegrityMsg("WAL switch anchor does not match target")
	}
	return start, nil
}

// Read the serialized header through BasicMessage: scanner source markers may
// wrap immutable messages without exposing their concrete message implementation.
func walSwitchHeader(msg BasicMessage) *AlterWALMessageHeader {
	value, ok := msg.Properties().Get(messageHeader)
	if !ok {
		panic("lost AlterWAL message header")
	}
	header := &AlterWALMessageHeader{}
	if err := DecodeProto(value, header); err != nil {
		panic(err)
	}
	return header
}
