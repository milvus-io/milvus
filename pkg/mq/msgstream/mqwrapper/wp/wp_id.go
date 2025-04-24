package wp

import (
	"github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus/pkg/v2/mq/common"
)

func NewWoodpeckerID(id *log.LogMessageId) *woodpeckerID {
	return &woodpeckerID{
		messageID: id,
	}
}

type woodpeckerID struct {
	messageID *log.LogMessageId
}

func (w *woodpeckerID) WoodpeckerID() *log.LogMessageId {
	return w.messageID
}

func (w *woodpeckerID) Serialize() []byte {
	return w.messageID.Serialize()
}

func (w *woodpeckerID) AtEarliestPosition() bool {
	if w.messageID.SegmentId <= 0 && w.messageID.EntryId <= 0 {
		return true
	}
	return false
}

func (w *woodpeckerID) LessOrEqualThan(msgID []byte) (bool, error) {
	id2, err := log.DeserializeLogMessageId(msgID)
	if err != nil {
		return false, err
	}
	if w.messageID.SegmentId < id2.SegmentId {
		return true, nil
	}
	if w.messageID.SegmentId > id2.SegmentId {
		return false, nil
	}
	return w.messageID.EntryId <= id2.EntryId, nil
}

func (w *woodpeckerID) Equal(msgID []byte) (bool, error) {
	id2, err := log.DeserializeLogMessageId(msgID)
	if err != nil {
		return false, err
	}
	return w.messageID.SegmentId == id2.SegmentId && w.messageID.EntryId == id2.EntryId, nil
}

// Check if pulsarID implements and MessageID interface
var _ common.MessageID = &woodpeckerID{}

func DeserializeWoodpeckerMsgID(messageID []byte) (*log.LogMessageId, error) {
	return log.DeserializeLogMessageId(messageID)
}
