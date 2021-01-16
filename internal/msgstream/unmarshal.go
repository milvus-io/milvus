package msgstream

import (
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

type MarshalFunc func(TsMsg) ([]byte, error)
type UnmarshalFunc func([]byte) (TsMsg, error)

type UnmarshalDispatcher struct {
	tempMap map[commonpb.MsgType]UnmarshalFunc
}

func (dispatcher *UnmarshalDispatcher) Unmarshal(input []byte, msgType commonpb.MsgType) (TsMsg, error) {
	unmarshalFunc, ok := dispatcher.tempMap[msgType]
	if !ok {
		return nil, errors.New(string("Not set unmarshalFunc for this messageType."))
	}
	return unmarshalFunc(input)
}

func (dispatcher *UnmarshalDispatcher) AddMsgTemplate(msgType commonpb.MsgType, unmarshal UnmarshalFunc) {
	dispatcher.tempMap[msgType] = unmarshal
}

func (dispatcher *UnmarshalDispatcher) addDefaultMsgTemplates() {
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

	queryNodeSegStatsMsg := QueryNodeStatsMsg{}
	dispatcher.tempMap = make(map[commonpb.MsgType]UnmarshalFunc)
	dispatcher.tempMap[commonpb.MsgType_kInsert] = insertMsg.Unmarshal
	dispatcher.tempMap[commonpb.MsgType_kDelete] = deleteMsg.Unmarshal
	dispatcher.tempMap[commonpb.MsgType_kSearch] = searchMsg.Unmarshal
	dispatcher.tempMap[commonpb.MsgType_kSearchResult] = searchResultMsg.Unmarshal
	dispatcher.tempMap[commonpb.MsgType_kTimeTick] = timeTickMsg.Unmarshal
	dispatcher.tempMap[commonpb.MsgType_kQueryNodeStats] = queryNodeSegStatsMsg.Unmarshal
	dispatcher.tempMap[commonpb.MsgType_kCreateCollection] = createCollectionMsg.Unmarshal
	dispatcher.tempMap[commonpb.MsgType_kDropCollection] = dropCollectionMsg.Unmarshal
	dispatcher.tempMap[commonpb.MsgType_kCreatePartition] = createPartitionMsg.Unmarshal
	dispatcher.tempMap[commonpb.MsgType_kDropPartition] = dropPartitionMsg.Unmarshal
	dispatcher.tempMap[commonpb.MsgType_kLoadIndex] = loadIndexMsg.Unmarshal
	dispatcher.tempMap[commonpb.MsgType_kFlush] = flushMsg.Unmarshal
}

func NewUnmarshalDispatcher() *UnmarshalDispatcher {
	unmarshalDispatcher := UnmarshalDispatcher{}
	unmarshalDispatcher.addDefaultMsgTemplates()
	return &unmarshalDispatcher
}
