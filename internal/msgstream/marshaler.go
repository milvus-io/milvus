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
	case internalPb.MsgType_kInsert:
		insertMarshaler := &InsertMarshaler{}
		var tsMsgMarshaller TsMsgMarshaler = insertMarshaler
		return &tsMsgMarshaller
	case internalPb.MsgType_kDelete:
		deleteMarshaler := &DeleteMarshaler{}
		var tsMsgMarshaller TsMsgMarshaler = deleteMarshaler
		return &tsMsgMarshaller
	case internalPb.MsgType_kSearch:
		searchMarshaler := &SearchMarshaler{}
		var tsMsgMarshaller TsMsgMarshaler = searchMarshaler
		return &tsMsgMarshaller
	case internalPb.MsgType_kSearchResult:
		searchResultMarshler := &SearchResultMarshaler{}
		var tsMsgMarshaller TsMsgMarshaler = searchResultMarshler
		return &tsMsgMarshaller
	case internalPb.MsgType_kTimeTick:
		timeTickMarshaler := &TimeTickMarshaler{}
		var tsMsgMarshaller TsMsgMarshaler = timeTickMarshaler
		return &tsMsgMarshaller
	default:
		return nil
	}
}

//////////////////////////////////////Insert///////////////////////////////////////////////

type InsertMarshaler struct{}

func (im *InsertMarshaler) Marshal(input *TsMsg) ([]byte, commonPb.Status) {
	insertTask := (*input).(*InsertMsg)
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
	insertMsg := &InsertMsg{InsertRequest: insertRequest}

	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	var tsMsg TsMsg = insertMsg
	return &tsMsg, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

/////////////////////////////////////Delete//////////////////////////////////////////////

type DeleteMarshaler struct{}

func (dm *DeleteMarshaler) Marshal(input *TsMsg) ([]byte, commonPb.Status) {
	deleteMsg := (*input).(*DeleteMsg)
	deleteRequest := &deleteMsg.DeleteRequest
	mb, err := proto.Marshal(deleteRequest)
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	return mb, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

func (dm *DeleteMarshaler) Unmarshal(input []byte) (*TsMsg, commonPb.Status) {
	deleteRequest := internalPb.DeleteRequest{}
	err := proto.Unmarshal(input, &deleteRequest)
	deleteMsg := &DeleteMsg{DeleteRequest: deleteRequest}
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	var tsMsg TsMsg = deleteMsg
	return &tsMsg, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

/////////////////////////////////////Search///////////////////////////////////////////////

type SearchMarshaler struct{}

func (sm *SearchMarshaler) Marshal(input *TsMsg) ([]byte, commonPb.Status) {
	searchMsg := (*input).(*SearchMsg)
	searchRequest := &searchMsg.SearchRequest
	mb, err := proto.Marshal(searchRequest)
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	return mb, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

func (sm *SearchMarshaler) Unmarshal(input []byte) (*TsMsg, commonPb.Status) {
	searchRequest := internalPb.SearchRequest{}
	err := proto.Unmarshal(input, &searchRequest)
	searchMsg := &SearchMsg{SearchRequest: searchRequest}
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	var tsMsg TsMsg = searchMsg
	return &tsMsg, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

/////////////////////////////////////SearchResult///////////////////////////////////////////////

type SearchResultMarshaler struct{}

func (srm *SearchResultMarshaler) Marshal(input *TsMsg) ([]byte, commonPb.Status) {
	searchResultMsg := (*input).(*SearchResultMsg)
	searchResult := &searchResultMsg.SearchResult
	mb, err := proto.Marshal(searchResult)
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	return mb, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

func (srm *SearchResultMarshaler) Unmarshal(input []byte) (*TsMsg, commonPb.Status) {
	searchResult := internalPb.SearchResult{}
	err := proto.Unmarshal(input, &searchResult)
	searchResultMsg := &SearchResultMsg{SearchResult: searchResult}
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	var tsMsg TsMsg = searchResultMsg
	return &tsMsg, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

/////////////////////////////////////TimeTick///////////////////////////////////////////////

type TimeTickMarshaler struct{}

func (tm *TimeTickMarshaler) Marshal(input *TsMsg) ([]byte, commonPb.Status) {
	timeTickMsg := (*input).(*TimeTickMsg)
	timeTick := &timeTickMsg.TimeTickMsg
	mb, err := proto.Marshal(timeTick)
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	return mb, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}

func (tm *TimeTickMarshaler) Unmarshal(input []byte) (*TsMsg, commonPb.Status) {
	timeTickMsg := internalPb.TimeTickMsg{}
	err := proto.Unmarshal(input, &timeTickMsg)
	timeTick := &TimeTickMsg{TimeTickMsg: timeTickMsg}
	if err != nil {
		return nil, commonPb.Status{ErrorCode: commonPb.ErrorCode_UNEXPECTED_ERROR}
	}
	var tsMsg TsMsg = timeTick
	return &tsMsg, commonPb.Status{ErrorCode: commonPb.ErrorCode_SUCCESS}
}
