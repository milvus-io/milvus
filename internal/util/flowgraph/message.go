package flowgraph

import "github.com/zilliztech/milvus-distributed/internal/msgstream"

type Msg interface {
	TimeTick() Timestamp
}

type MsgStreamMsg struct {
	tsMessages     []msgstream.TsMsg
	timestampMin   Timestamp
	timestampMax   Timestamp
	startPositions []*MsgPosition
	endPositions   []*MsgPosition
}

func GenerateMsgStreamMsg(tsMessages []msgstream.TsMsg, timestampMin, timestampMax Timestamp, startPos []*MsgPosition, endPos []*MsgPosition) *MsgStreamMsg {
	return &MsgStreamMsg{
		tsMessages:     tsMessages,
		timestampMin:   timestampMin,
		timestampMax:   timestampMax,
		startPositions: startPos,
		endPositions:   endPos,
	}
}

func (msMsg *MsgStreamMsg) TimeTick() Timestamp {
	return msMsg.timestampMax
}

func (msMsg *MsgStreamMsg) DownStreamNodeIdx() int {
	return 0
}

func (msMsg *MsgStreamMsg) TsMessages() []msgstream.TsMsg {
	return msMsg.tsMessages
}

func (msMsg *MsgStreamMsg) TimestampMin() Timestamp {
	return msMsg.timestampMin
}

func (msMsg *MsgStreamMsg) TimestampMax() Timestamp {
	return msMsg.timestampMax
}

func (msMsg *MsgStreamMsg) StartPositions() []*MsgPosition {
	return msMsg.startPositions
}

func (msMsg *MsgStreamMsg) EndPositions() []*MsgPosition {
	return msMsg.endPositions
}
