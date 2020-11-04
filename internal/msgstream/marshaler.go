package msgstream

import (
	"github.com/golang/protobuf/proto"
	commonPb "github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type TsMsgMarshaler interface {
	Marshal(input *TsMsg) ([]byte, commonPb.Status)
	Unmarshal(input []byte) (*TsMsg, commonPb.Status)
}

func GetMarshalers(inputMsgType MsgType, outputMsgType MsgType) (*TsMsgMarshaler, *TsMsgMarshaler) {
	return GetMarshaler(inputMsgType), GetMarshaler(outputMsgType)
}

func GetMarshaler(MsgType MsgType) *TsMsgMarshaler {
	switch MsgType {
	case KInsert:
		insertMarshaler := &InsertMarshaler{}
		var tsMsgMarshaller TsMsgMarshaler = insertMarshaler
		return &tsMsgMarshaller
	case KDelete:
		deleteMarshaler := &DeleteMarshaler{}
		var tsMsgMarshaller TsMsgMarshaler = deleteMarshaler
		return &tsMsgMarshaller
	case KSearch:
		searchMarshaler := &SearchMarshaler{}
		var tsMsgMarshaller TsMsgMarshaler = searchMarshaler
		return &tsMsgMarshaller
	case KSearchResult:
		searchResultMarshler := &SearchResultMarshaler{}
		var tsMsgMarshaller TsMsgMarshaler = searchResultMarshler
		return &tsMsgMarshaller
	case KTimeTick:
		timeSyncMarshaler := &TimeTickMarshaler{}
		var tsMsgMarshaller TsMsgMarshaler = timeSyncMarshaler
		return &tsMsgMarshaller
	default:
		return nil
	}
}

//////////////////////////////////////Insert///////////////////////////////////////////////

type InsertMarshaler struct{}

func (im *InsertMarshaler) Marshal(input *TsMsg) ([]byte, commonPb.Status) {
	insertTask := (*input).(InsertTask)
	insertRequest := &insertTask.InsertRequest
	mb, err := proto.Marshal(insertRequest)
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	return mb, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

func (im *InsertMarshaler) Unmarshal(input []byte) (*TsMsg, commonPb.Status) {
	insertRequest := internalPb.InsertRequest{}
	err := proto.Unmarshal(input, &insertRequest)
	insertTask := InsertTask{InsertRequest: insertRequest}

	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	var tsMsg TsMsg = insertTask
	return &tsMsg, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

/////////////////////////////////////Delete//////////////////////////////////////////////

type DeleteMarshaler struct{}

func (dm *DeleteMarshaler) Marshal(input *TsMsg) ([]byte, commonPb.Status) {
	deleteTask := (*input).(DeleteTask)
	deleteRequest := &deleteTask.DeleteRequest
	mb, err := proto.Marshal(deleteRequest)
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	return mb, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

func (dm *DeleteMarshaler) Unmarshal(input []byte) (*TsMsg, commonPb.Status) {
	deleteRequest := internalPb.DeleteRequest{}
	err := proto.Unmarshal(input, &deleteRequest)
	deleteTask := DeleteTask{DeleteRequest: deleteRequest}
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	var tsMsg TsMsg = deleteTask
	return &tsMsg, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

/////////////////////////////////////Search///////////////////////////////////////////////

type SearchMarshaler struct{}

func (sm *SearchMarshaler) Marshal(input *TsMsg) ([]byte, commonPb.Status) {
	searchTask := (*input).(SearchTask)
	searchRequest := &searchTask.SearchRequest
	mb, err := proto.Marshal(searchRequest)
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	return mb, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

func (sm *SearchMarshaler) Unmarshal(input []byte) (*TsMsg, commonPb.Status) {
	searchRequest := internalPb.SearchRequest{}
	err := proto.Unmarshal(input, &searchRequest)
	searchTask := SearchTask{SearchRequest: searchRequest}
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	var tsMsg TsMsg = searchTask
	return &tsMsg, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

/////////////////////////////////////SearchResult///////////////////////////////////////////////

type SearchResultMarshaler struct{}

func (srm *SearchResultMarshaler) Marshal(input *TsMsg) ([]byte, commonPb.Status) {
	searchResultTask := (*input).(SearchResultTask)
	searchResult := &searchResultTask.SearchResult
	mb, err := proto.Marshal(searchResult)
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	return mb, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

func (srm *SearchResultMarshaler) Unmarshal(input []byte) (*TsMsg, commonPb.Status) {
	searchResult := internalPb.SearchResult{}
	err := proto.Unmarshal(input, &searchResult)
	searchResultTask := SearchResultTask{SearchResult: searchResult}
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	var tsMsg TsMsg = searchResultTask
	return &tsMsg, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

/////////////////////////////////////TimeSync///////////////////////////////////////////////

type TimeTickMarshaler struct{}

func (tm *TimeTickMarshaler) Marshal(input *TsMsg) ([]byte, commonPb.Status) {
	timeSyncTask := (*input).(TimeTickMsg)
	timeSyncMsg := &timeSyncTask.TimeTickMsg
	mb, err := proto.Marshal(timeSyncMsg)
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	return mb, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

func (tm *TimeTickMarshaler) Unmarshal(input []byte) (*TsMsg, commonPb.Status) {
	timeSyncMsg := internalPb.TimeTickMsg{}
	err := proto.Unmarshal(input, &timeSyncMsg)
	timeSyncTask := TimeTickMsg{TimeTickMsg: timeSyncMsg}
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	var tsMsg TsMsg = timeSyncTask
	return &tsMsg, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

///////////////////////////////////////Key2Seg///////////////////////////////////////////////
//
//type Key2SegMarshaler struct{}
//
//func (km *Key2SegMarshaler) Marshal(input *TsMsg) ([]byte, commonPb.Status) {
//	key2SegTask := (*input).(Key2SegTask)
//	key2SegMsg := &key2SegTask.Key2SegMsg
//	mb, err := proto.Marshal(key2SegMsg)
//	if err != nil{
//		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
//	}
//	return mb, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
//}
//
//func (km *Key2SegMarshaler) Unmarshal(input []byte) (*TsMsg, commonPb.Status) {
//	key2SegMsg := internalPb.Key2SegMsg{}
//	err := proto.Unmarshal(input, &key2SegMsg)
//	key2SegTask := Key2SegTask{key2SegMsg}
//	if err != nil{
//		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
//	}
//	var tsMsg TsMsg = key2SegTask
//	return &tsMsg, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
//}
