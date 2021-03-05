package msgstream

import (
	"errors"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

type UnmarshalFunc func(interface{}) (TsMsg, error)

type UnmarshalDispatcher interface {
	Unmarshal(input interface{}, msgType commonpb.MsgType) (TsMsg, error)
	AddMsgTemplate(msgType commonpb.MsgType, unmarshalFunc UnmarshalFunc)
}

type UnmarshalDispatcherFactory interface {
	NewUnmarshalDispatcher() *UnmarshalDispatcher
}

// ProtoUnmarshalDispatcher ant its factory

type ProtoUnmarshalDispatcher struct {
	TempMap map[commonpb.MsgType]UnmarshalFunc
}

func (p *ProtoUnmarshalDispatcher) Unmarshal(input interface{}, msgType commonpb.MsgType) (TsMsg, error) {
	unmarshalFunc, ok := p.TempMap[msgType]
	if !ok {
		return nil, errors.New("not set unmarshalFunc for this messageType")
	}
	return unmarshalFunc(input)
}

func (p *ProtoUnmarshalDispatcher) AddMsgTemplate(msgType commonpb.MsgType, unmarshalFunc UnmarshalFunc) {
	p.TempMap[msgType] = unmarshalFunc
}

type ProtoUDFactory struct{}

func (pudf *ProtoUDFactory) NewUnmarshalDispatcher() *ProtoUnmarshalDispatcher {
	insertMsg := InsertMsg{}
	deleteMsg := DeleteMsg{}
	searchMsg := SearchMsg{}
	searchResultMsg := SearchResultMsg{}
	timeTickMsg := TimeTickMsg{}
	createCollectionMsg := CreateCollectionMsg{}
	dropCollectionMsg := DropCollectionMsg{}
	createPartitionMsg := CreatePartitionMsg{}
	dropPartitionMsg := DropPartitionMsg{}
	loadIndexMsg := LoadIndexMsg{}
	flushMsg := FlushMsg{}
	segmentInfoMsg := SegmentInfoMsg{}
	flushCompletedMsg := FlushCompletedMsg{}
	queryNodeSegStatsMsg := QueryNodeStatsMsg{}
	segmentStatisticsMsg := SegmentStatisticsMsg{}

	p := &ProtoUnmarshalDispatcher{}
	p.TempMap = make(map[commonpb.MsgType]UnmarshalFunc)
	p.TempMap[commonpb.MsgType_kInsert] = insertMsg.Unmarshal
	p.TempMap[commonpb.MsgType_kDelete] = deleteMsg.Unmarshal
	p.TempMap[commonpb.MsgType_kSearch] = searchMsg.Unmarshal
	p.TempMap[commonpb.MsgType_kSearchResult] = searchResultMsg.Unmarshal
	p.TempMap[commonpb.MsgType_kTimeTick] = timeTickMsg.Unmarshal
	p.TempMap[commonpb.MsgType_kQueryNodeStats] = queryNodeSegStatsMsg.Unmarshal
	p.TempMap[commonpb.MsgType_kCreateCollection] = createCollectionMsg.Unmarshal
	p.TempMap[commonpb.MsgType_kDropCollection] = dropCollectionMsg.Unmarshal
	p.TempMap[commonpb.MsgType_kCreatePartition] = createPartitionMsg.Unmarshal
	p.TempMap[commonpb.MsgType_kDropPartition] = dropPartitionMsg.Unmarshal
	p.TempMap[commonpb.MsgType_kLoadIndex] = loadIndexMsg.Unmarshal
	p.TempMap[commonpb.MsgType_kFlush] = flushMsg.Unmarshal
	p.TempMap[commonpb.MsgType_kSegmentInfo] = segmentInfoMsg.Unmarshal
	p.TempMap[commonpb.MsgType_kSegmentFlushDone] = flushCompletedMsg.Unmarshal
	p.TempMap[commonpb.MsgType_kSegmentStatistics] = segmentStatisticsMsg.Unmarshal

	return p
}

// MemUnmarshalDispatcher ant its factory

//type MemUDFactory struct {
//
//}
//func (mudf *MemUDFactory) NewUnmarshalDispatcher() *UnmarshalDispatcher {
//
//}
