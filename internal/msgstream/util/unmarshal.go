package util

import (
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

type MarshalFunc func(msgstream.TsMsg) ([]byte, error)
type UnmarshalFunc func([]byte) (msgstream.TsMsg, error)

type UnmarshalDispatcher struct {
	TempMap map[commonpb.MsgType]UnmarshalFunc
}

func (dispatcher *UnmarshalDispatcher) Unmarshal(input []byte, msgType commonpb.MsgType) (msgstream.TsMsg, error) {
	unmarshalFunc, ok := dispatcher.TempMap[msgType]
	if !ok {
		return nil, errors.New(string("Not set unmarshalFunc for this messageType."))
	}
	return unmarshalFunc(input)
}

func (dispatcher *UnmarshalDispatcher) AddMsgTemplate(msgType commonpb.MsgType, unmarshal UnmarshalFunc) {
	dispatcher.TempMap[msgType] = unmarshal
}

func (dispatcher *UnmarshalDispatcher) addDefaultMsgTemplates() {
	insertMsg := msgstream.InsertMsg{}
	deleteMsg := msgstream.DeleteMsg{}
	searchMsg := msgstream.SearchMsg{}
	searchResultMsg := msgstream.SearchResultMsg{}
	timeTickMsg := msgstream.TimeTickMsg{}
	createCollectionMsg := msgstream.CreateCollectionMsg{}
	dropCollectionMsg := msgstream.DropCollectionMsg{}
	createPartitionMsg := msgstream.CreatePartitionMsg{}
	dropPartitionMsg := msgstream.DropPartitionMsg{}
	loadIndexMsg := msgstream.LoadIndexMsg{}
	flushMsg := msgstream.FlushMsg{}

	queryNodeSegStatsMsg := msgstream.QueryNodeStatsMsg{}
	dispatcher.TempMap = make(map[commonpb.MsgType]UnmarshalFunc)
	dispatcher.TempMap[commonpb.MsgType_kInsert] = insertMsg.Unmarshal
	dispatcher.TempMap[commonpb.MsgType_kDelete] = deleteMsg.Unmarshal
	dispatcher.TempMap[commonpb.MsgType_kSearch] = searchMsg.Unmarshal
	dispatcher.TempMap[commonpb.MsgType_kSearchResult] = searchResultMsg.Unmarshal
	dispatcher.TempMap[commonpb.MsgType_kTimeTick] = timeTickMsg.Unmarshal
	dispatcher.TempMap[commonpb.MsgType_kQueryNodeStats] = queryNodeSegStatsMsg.Unmarshal
	dispatcher.TempMap[commonpb.MsgType_kCreateCollection] = createCollectionMsg.Unmarshal
	dispatcher.TempMap[commonpb.MsgType_kDropCollection] = dropCollectionMsg.Unmarshal
	dispatcher.TempMap[commonpb.MsgType_kCreatePartition] = createPartitionMsg.Unmarshal
	dispatcher.TempMap[commonpb.MsgType_kDropPartition] = dropPartitionMsg.Unmarshal
	dispatcher.TempMap[commonpb.MsgType_kLoadIndex] = loadIndexMsg.Unmarshal
	dispatcher.TempMap[commonpb.MsgType_kFlush] = flushMsg.Unmarshal
}

func NewUnmarshalDispatcher() *UnmarshalDispatcher {
	unmarshalDispatcher := UnmarshalDispatcher{}
	unmarshalDispatcher.addDefaultMsgTemplates()
	return &unmarshalDispatcher
}
