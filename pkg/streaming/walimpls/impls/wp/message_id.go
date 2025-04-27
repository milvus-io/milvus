package wp

import (
	"encoding/base64"
	"fmt"

	"github.com/cockroachdb/errors"
	wp "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

func NewWpID(id *wp.LogMessageId) message.MessageID {
	return wpID{
		logMsgId: id,
	}
}

func UnmarshalMessageID(data string) (message.MessageID, error) {
	id, err := unmarshalMessageID(data)
	if err != nil {
		return nil, err
	}
	return id, nil
}

func unmarshalMessageID(data string) (wpID, error) {
	val, err := base64.StdEncoding.DecodeString(data)
	if err != nil {
		return wpID{logMsgId: nil}, errors.Wrapf(message.ErrInvalidMessageID, "decode pulsar fail when decode base64 with err: %s, id: %s", err.Error(), data)
	}
	msgID, err := wp.DeserializeLogMessageId(val)
	if err != nil {
		return wpID{logMsgId: nil}, errors.Wrapf(message.ErrInvalidMessageID, "decode pulsar fail when deserialize with err: %s, id: %s", err.Error(), data)
	}
	return wpID{logMsgId: msgID}, nil
}

var _ message.MessageID = wpID{}

type wpID struct {
	logMsgId *wp.LogMessageId
}

func (id wpID) WoodpeckerID() *wp.LogMessageId {
	return id.logMsgId
}

func (id wpID) WoodpeckerMsgId() *wp.LogMessageId {
	return id.logMsgId
}

func (id wpID) WALName() string {
	return WALName
}

func (id wpID) LT(other message.MessageID) bool {
	id2 := other.(wpID)
	if id.logMsgId.SegmentId != id2.logMsgId.SegmentId {
		return id.logMsgId.SegmentId < id2.logMsgId.SegmentId
	}
	return id.logMsgId.EntryId < id2.logMsgId.EntryId
}

func (id wpID) LTE(other message.MessageID) bool {
	id2 := other.(wpID)
	if id.logMsgId.SegmentId < id2.logMsgId.SegmentId {
		return true
	} else if id.logMsgId.SegmentId > id2.logMsgId.SegmentId {
		return false
	}
	return id.logMsgId.EntryId <= id2.logMsgId.EntryId
}

func (id wpID) EQ(other message.MessageID) bool {
	id2 := other.(wpID)
	return id.logMsgId.SegmentId == id2.logMsgId.SegmentId && id.logMsgId.EntryId == id2.logMsgId.EntryId
}

func (id wpID) Marshal() string {
	return base64.StdEncoding.EncodeToString(id.logMsgId.Serialize())
}

func (id wpID) String() string {
	return fmt.Sprintf("%d/%d", id.logMsgId.SegmentId, id.logMsgId.EntryId)
}
