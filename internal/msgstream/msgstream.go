package msgstream

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp
type IntPrimaryKey = typeutil.IntPrimaryKey
type MsgPosition = internalpb2.MsgPosition

type MsgPack struct {
	BeginTs        Timestamp
	EndTs          Timestamp
	Msgs           []TsMsg
	StartPositions []*MsgPosition
	endPositions   []*MsgPosition
}

type RepackFunc func(msgs []TsMsg, hashKeys [][]int32) (map[int32]*MsgPack, error)

type MsgStream interface {
	Start()
	Close()
	Chan() <-chan *MsgPack

	Produce(*MsgPack) error
	Broadcast(*MsgPack) error
	Consume() *MsgPack
	Seek(offset *MsgPosition) error
}
