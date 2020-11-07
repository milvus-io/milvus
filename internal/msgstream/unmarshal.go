package msgstream

import (
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type MarshalFunc func(*TsMsg) ([]byte, error)
type UnmarshalFunc func([]byte) (*TsMsg, error)

type UnmarshalDispatcher struct {
	tempMap map[internalPb.MsgType]UnmarshalFunc
}

func (dispatcher *UnmarshalDispatcher) Unmarshal(input []byte, msgType internalPb.MsgType) (*TsMsg, error) {
	unmarshalFunc := dispatcher.tempMap[msgType]
	return unmarshalFunc(input)
}

func (dispatcher *UnmarshalDispatcher) AddMsgTemplate(msgType internalPb.MsgType, unmarshal UnmarshalFunc) {
	dispatcher.tempMap[msgType] = unmarshal
}

func (dispatcher *UnmarshalDispatcher) addDefaultMsgTemplates() {
	insertMsg := InsertMsg{}
	deleteMsg := DeleteMsg{}
	searchMsg := SearchMsg{}
	searchResultMsg := SearchResultMsg{}
	timeTickMsg := TimeTickMsg{}
	dispatcher.tempMap = make(map[internalPb.MsgType]UnmarshalFunc)
	dispatcher.tempMap[internalPb.MsgType_kInsert] = insertMsg.Unmarshal
	dispatcher.tempMap[internalPb.MsgType_kDelete] = deleteMsg.Unmarshal
	dispatcher.tempMap[internalPb.MsgType_kSearch] = searchMsg.Unmarshal
	dispatcher.tempMap[internalPb.MsgType_kSearchResult] = searchResultMsg.Unmarshal
	dispatcher.tempMap[internalPb.MsgType_kTimeTick] = timeTickMsg.Unmarshal
}

func NewUnmarshalDispatcher() *UnmarshalDispatcher {
	unmarshalDispatcher := UnmarshalDispatcher{}
	unmarshalDispatcher.addDefaultMsgTemplates()
	return &unmarshalDispatcher
}
