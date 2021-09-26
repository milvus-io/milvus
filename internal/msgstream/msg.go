// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package msgstream

import (
	"context"
	"errors"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

// MsgType is an alias ofo commonpb.MsgType
type MsgType = commonpb.MsgType

// MarshalType is an empty interface
type MarshalType = interface{}

// TsMsg provides methods to get begin timestamp and end timestamp of a message pack
type TsMsg interface {
	TraceCtx() context.Context
	SetTraceCtx(ctx context.Context)
	ID() UniqueID
	BeginTs() Timestamp
	EndTs() Timestamp
	Type() MsgType
	SourceID() int64
	HashKeys() []uint32
	Marshal(TsMsg) (MarshalType, error)
	Unmarshal(MarshalType) (TsMsg, error)
	Position() *MsgPosition
	SetPosition(*MsgPosition)
}

// BaseMsg is a basic structure that contains begin timestamp, end timestamp and the position of msgstream
type BaseMsg struct {
	Ctx            context.Context
	BeginTimestamp Timestamp
	EndTimestamp   Timestamp
	HashValues     []uint32
	MsgPosition    *MsgPosition
}

// TraceCtx returns the context of opentracing
func (bm *BaseMsg) TraceCtx() context.Context {
	return bm.Ctx
}

// SetTraceCtx is used to set context for opentracing
func (bm *BaseMsg) SetTraceCtx(ctx context.Context) {
	bm.Ctx = ctx
}

// BeginTs returns the begin timestamp of this message pack
func (bm *BaseMsg) BeginTs() Timestamp {
	return bm.BeginTimestamp
}

// EndTs returns the end timestamp of this message pack
func (bm *BaseMsg) EndTs() Timestamp {
	return bm.EndTimestamp
}

// HashKeys returns the end timestamp of this message pack
func (bm *BaseMsg) HashKeys() []uint32 {
	return bm.HashValues
}

// Position returns the position of this message pack in msgstream
func (bm *BaseMsg) Position() *MsgPosition {
	return bm.MsgPosition
}

// SetPosition is used to set position of this message in msgstream
func (bm *BaseMsg) SetPosition(position *MsgPosition) {
	bm.MsgPosition = position
}

func convertToByteArray(input interface{}) ([]byte, error) {
	switch output := input.(type) {
	case []byte:
		return output, nil
	default:
		return nil, errors.New("cannot convert interface{} to []byte")
	}
}

/////////////////////////////////////////Insert//////////////////////////////////////////

// InsertMsg is a message pack that contains insert request
type InsertMsg struct {
	BaseMsg
	internalpb.InsertRequest
}

// interface implementation validation
var _ TsMsg = &InsertMsg{}

// ID returns the ID of this message pack
func (it *InsertMsg) ID() UniqueID {
	return it.Base.MsgID
}

// Type returns the type of this message pack
func (it *InsertMsg) Type() MsgType {
	return it.Base.MsgType
}

// SourceID indicated which component generated this message
func (it *InsertMsg) SourceID() int64 {
	return it.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (it *InsertMsg) Marshal(input TsMsg) (MarshalType, error) {
	insertMsg := input.(*InsertMsg)
	insertRequest := &insertMsg.InsertRequest
	mb, err := proto.Marshal(insertRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (it *InsertMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	insertRequest := internalpb.InsertRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &insertRequest)
	if err != nil {
		return nil, err
	}
	insertMsg := &InsertMsg{InsertRequest: insertRequest}
	for _, timestamp := range insertMsg.Timestamps {
		insertMsg.BeginTimestamp = timestamp
		insertMsg.EndTimestamp = timestamp
		break
	}
	for _, timestamp := range insertMsg.Timestamps {
		if timestamp > insertMsg.EndTimestamp {
			insertMsg.EndTimestamp = timestamp
		}
		if timestamp < insertMsg.BeginTimestamp {
			insertMsg.BeginTimestamp = timestamp
		}
	}

	return insertMsg, nil
}

/////////////////////////////////////////Delete//////////////////////////////////////////

// DeleteMsg is a message pack that contains delete request
type DeleteMsg struct {
	BaseMsg
	internalpb.DeleteRequest
}

// interface implementation validation
var _ TsMsg = &DeleteMsg{}

// ID returns the ID of this message pack
func (dt *DeleteMsg) ID() UniqueID {
	return dt.Base.MsgID
}

// Type returns the type of this message pack
func (dt *DeleteMsg) Type() MsgType {
	return dt.Base.MsgType
}

// SourceID indicated which component generated this message
func (dt *DeleteMsg) SourceID() int64 {
	return dt.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (dt *DeleteMsg) Marshal(input TsMsg) (MarshalType, error) {
	deleteMsg := input.(*DeleteMsg)
	deleteRequest := &deleteMsg.DeleteRequest
	mb, err := proto.Marshal(deleteRequest)
	if err != nil {
		return nil, err
	}

	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (dt *DeleteMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	deleteRequest := internalpb.DeleteRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &deleteRequest)
	if err != nil {
		return nil, err
	}
	deleteMsg := &DeleteMsg{DeleteRequest: deleteRequest}
	deleteMsg.BeginTimestamp = deleteMsg.Timestamp
	deleteMsg.EndTimestamp = deleteMsg.Timestamp

	return deleteMsg, nil
}

/////////////////////////////////////////Search//////////////////////////////////////////

// SearchMsg is a message pack that contains search request
type SearchMsg struct {
	BaseMsg
	internalpb.SearchRequest
}

// interface implementation validation
var _ TsMsg = &SearchMsg{}

// ID returns the ID of this message pack
func (st *SearchMsg) ID() UniqueID {
	return st.Base.MsgID
}

// Type returns the type of this message pack
func (st *SearchMsg) Type() MsgType {
	return st.Base.MsgType
}

// SourceID indicated which component generated this message
func (st *SearchMsg) SourceID() int64 {
	return st.Base.SourceID
}

// GuaranteeTs returns the guarantee timestamp that querynode can perform this search request. This timestamp
// filled in client(e.g. pymilvus). The timestamp will be 0 if client never execute any insert, otherwise equals
// the timestamp from last insert response.
func (st *SearchMsg) GuaranteeTs() Timestamp {
	return st.GetGuaranteeTimestamp()
}

// TravelTs returns the timestamp of a time travel search request
func (st *SearchMsg) TravelTs() Timestamp {
	return st.GetTravelTimestamp()
}

// Marshal is used to serializing a message pack to byte array
func (st *SearchMsg) Marshal(input TsMsg) (MarshalType, error) {
	searchTask := input.(*SearchMsg)
	searchRequest := &searchTask.SearchRequest
	mb, err := proto.Marshal(searchRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (st *SearchMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	searchRequest := internalpb.SearchRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &searchRequest)
	if err != nil {
		return nil, err
	}
	searchMsg := &SearchMsg{SearchRequest: searchRequest}
	searchMsg.BeginTimestamp = searchMsg.Base.Timestamp
	searchMsg.EndTimestamp = searchMsg.Base.Timestamp

	return searchMsg, nil
}

/////////////////////////////////////////SearchResult//////////////////////////////////////////

// SearchResultMsg is a message pack that contains the result of search request
type SearchResultMsg struct {
	BaseMsg
	internalpb.SearchResults
}

// interface implementation validation
var _ TsMsg = &SearchResultMsg{}

// ID returns the ID of this message pack
func (srt *SearchResultMsg) ID() UniqueID {
	return srt.Base.MsgID
}

// Type returns the type of this message pack
func (srt *SearchResultMsg) Type() MsgType {
	return srt.Base.MsgType
}

// SourceID indicated which component generated this message
func (srt *SearchResultMsg) SourceID() int64 {
	return srt.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (srt *SearchResultMsg) Marshal(input TsMsg) (MarshalType, error) {
	searchResultTask := input.(*SearchResultMsg)
	searchResultRequest := &searchResultTask.SearchResults
	mb, err := proto.Marshal(searchResultRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (srt *SearchResultMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	searchResultRequest := internalpb.SearchResults{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &searchResultRequest)
	if err != nil {
		return nil, err
	}
	searchResultMsg := &SearchResultMsg{SearchResults: searchResultRequest}
	searchResultMsg.BeginTimestamp = searchResultMsg.Base.Timestamp
	searchResultMsg.EndTimestamp = searchResultMsg.Base.Timestamp

	return searchResultMsg, nil
}

////////////////////////////////////////Retrieve/////////////////////////////////////////

// RetrieveMsg is a message pack that contains retrieve request
type RetrieveMsg struct {
	BaseMsg
	internalpb.RetrieveRequest
}

// interface implementation validation
var _ TsMsg = &RetrieveMsg{}

// ID returns the ID of this message pack
func (rm *RetrieveMsg) ID() UniqueID {
	return rm.Base.MsgID
}

// Type returns the type of this message pack
func (rm *RetrieveMsg) Type() MsgType {
	return rm.Base.MsgType
}

// SourceID indicated which component generated this message
func (rm *RetrieveMsg) SourceID() int64 {
	return rm.Base.SourceID
}

// GuaranteeTs returns the guarantee timestamp that querynode can perform this query request. This timestamp
// filled in client(e.g. pymilvus). The timestamp will be 0 if client never execute any insert, otherwise equals
// the timestamp from last insert response.
func (rm *RetrieveMsg) GuaranteeTs() Timestamp {
	return rm.GetGuaranteeTimestamp()
}

// TravelTs returns the timestamp of a time travel query request
func (rm *RetrieveMsg) TravelTs() Timestamp {
	return rm.GetTravelTimestamp()
}

// Marshal is used to serializing a message pack to byte array
func (rm *RetrieveMsg) Marshal(input TsMsg) (MarshalType, error) {
	retrieveTask := input.(*RetrieveMsg)
	retrieveRequest := &retrieveTask.RetrieveRequest
	mb, err := proto.Marshal(retrieveRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (rm *RetrieveMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	retrieveRequest := internalpb.RetrieveRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &retrieveRequest)
	if err != nil {
		return nil, err
	}
	retrieveMsg := &RetrieveMsg{RetrieveRequest: retrieveRequest}
	retrieveMsg.BeginTimestamp = retrieveMsg.Base.Timestamp
	retrieveMsg.EndTimestamp = retrieveMsg.Base.Timestamp

	return retrieveMsg, nil
}

//////////////////////////////////////RetrieveResult///////////////////////////////////////

// RetrieveResultMsg is a message pack that contains the result of query request
type RetrieveResultMsg struct {
	BaseMsg
	internalpb.RetrieveResults
}

// interface implementation validation
var _ TsMsg = &RetrieveResultMsg{}

// ID returns the ID of this message pack
func (rrm *RetrieveResultMsg) ID() UniqueID {
	return rrm.Base.MsgID
}

// Type returns the type of this message pack
func (rrm *RetrieveResultMsg) Type() MsgType {
	return rrm.Base.MsgType
}

// SourceID indicated which component generated this message
func (rrm *RetrieveResultMsg) SourceID() int64 {
	return rrm.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (rrm *RetrieveResultMsg) Marshal(input TsMsg) (MarshalType, error) {
	retrieveResultTask := input.(*RetrieveResultMsg)
	retrieveResultRequest := &retrieveResultTask.RetrieveResults
	mb, err := proto.Marshal(retrieveResultRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (rrm *RetrieveResultMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	retrieveResultRequest := internalpb.RetrieveResults{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &retrieveResultRequest)
	if err != nil {
		return nil, err
	}
	retrieveResultMsg := &RetrieveResultMsg{RetrieveResults: retrieveResultRequest}
	retrieveResultMsg.BeginTimestamp = retrieveResultMsg.Base.Timestamp
	retrieveResultMsg.EndTimestamp = retrieveResultMsg.Base.Timestamp

	return retrieveResultMsg, nil
}

/////////////////////////////////////////TimeTick//////////////////////////////////////////

// TimeTickMsg is a message pack that contains time tick only
type TimeTickMsg struct {
	BaseMsg
	internalpb.TimeTickMsg
}

// interface implementation validation
var _ TsMsg = &TimeTickMsg{}

// ID returns the ID of this message pack
func (tst *TimeTickMsg) ID() UniqueID {
	return tst.Base.MsgID
}

// Type returns the type of this message pack
func (tst *TimeTickMsg) Type() MsgType {
	return tst.Base.MsgType
}

// SourceID indicated which component generated this message
func (tst *TimeTickMsg) SourceID() int64 {
	return tst.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (tst *TimeTickMsg) Marshal(input TsMsg) (MarshalType, error) {
	timeTickTask := input.(*TimeTickMsg)
	timeTick := &timeTickTask.TimeTickMsg
	mb, err := proto.Marshal(timeTick)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (tst *TimeTickMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	timeTickMsg := internalpb.TimeTickMsg{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &timeTickMsg)
	if err != nil {
		return nil, err
	}
	timeTick := &TimeTickMsg{TimeTickMsg: timeTickMsg}
	timeTick.BeginTimestamp = timeTick.Base.Timestamp
	timeTick.EndTimestamp = timeTick.Base.Timestamp

	return timeTick, nil
}

/////////////////////////////////////////QueryNodeStats//////////////////////////////////////////

// QueryNodeStatsMsg is a message pack that contains statistic from querynode
// GOOSE TODO: remove QueryNodeStats
type QueryNodeStatsMsg struct {
	BaseMsg
	internalpb.QueryNodeStats
}

// interface implementation validation
var _ TsMsg = &QueryNodeStatsMsg{}

// TraceCtx returns the context of opentracing
func (qs *QueryNodeStatsMsg) TraceCtx() context.Context {
	return qs.BaseMsg.Ctx
}

// SetTraceCtx is used to set context for opentracing
func (qs *QueryNodeStatsMsg) SetTraceCtx(ctx context.Context) {
	qs.BaseMsg.Ctx = ctx
}

// ID returns the ID of this message pack
func (qs *QueryNodeStatsMsg) ID() UniqueID {
	return qs.Base.MsgID
}

// Type returns the type of this message pack
func (qs *QueryNodeStatsMsg) Type() MsgType {
	return qs.Base.MsgType
}

// SourceID indicated which component generated this message
func (qs *QueryNodeStatsMsg) SourceID() int64 {
	return qs.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (qs *QueryNodeStatsMsg) Marshal(input TsMsg) (MarshalType, error) {
	queryNodeSegStatsTask := input.(*QueryNodeStatsMsg)
	queryNodeSegStats := &queryNodeSegStatsTask.QueryNodeStats
	mb, err := proto.Marshal(queryNodeSegStats)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (qs *QueryNodeStatsMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	queryNodeSegStats := internalpb.QueryNodeStats{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &queryNodeSegStats)
	if err != nil {
		return nil, err
	}
	queryNodeSegStatsMsg := &QueryNodeStatsMsg{QueryNodeStats: queryNodeSegStats}

	return queryNodeSegStatsMsg, nil
}

/////////////////////////////////////////SegmentStatisticsMsg//////////////////////////////////////////

// SegmentStatisticsMsg is a message pack that contains segment statistic
type SegmentStatisticsMsg struct {
	BaseMsg
	internalpb.SegmentStatistics
}

// interface implementation validation
var _ TsMsg = &SegmentStatisticsMsg{}

// TraceCtx returns the context of opentracing
func (ss *SegmentStatisticsMsg) TraceCtx() context.Context {
	return ss.BaseMsg.Ctx
}

// SetTraceCtx is used to set context for opentracing
func (ss *SegmentStatisticsMsg) SetTraceCtx(ctx context.Context) {
	ss.BaseMsg.Ctx = ctx
}

// ID returns the ID of this message pack
func (ss *SegmentStatisticsMsg) ID() UniqueID {
	return ss.Base.MsgID
}

// Type returns the type of this message pack
func (ss *SegmentStatisticsMsg) Type() MsgType {
	return ss.Base.MsgType
}

// SourceID indicated which component generated this message
func (ss *SegmentStatisticsMsg) SourceID() int64 {
	return ss.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (ss *SegmentStatisticsMsg) Marshal(input TsMsg) (MarshalType, error) {
	segStatsTask := input.(*SegmentStatisticsMsg)
	segStats := &segStatsTask.SegmentStatistics
	mb, err := proto.Marshal(segStats)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (ss *SegmentStatisticsMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	segStats := internalpb.SegmentStatistics{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &segStats)
	if err != nil {
		return nil, err
	}
	segStatsMsg := &SegmentStatisticsMsg{SegmentStatistics: segStats}

	return segStatsMsg, nil
}

/////////////////////////////////////////CreateCollection//////////////////////////////////////////

// CreateCollectionMsg is a message pack that contains create collection request
type CreateCollectionMsg struct {
	BaseMsg
	internalpb.CreateCollectionRequest
}

// interface implementation validation
var _ TsMsg = &CreateCollectionMsg{}

// ID returns the ID of this message pack
func (cc *CreateCollectionMsg) ID() UniqueID {
	return cc.Base.MsgID
}

// Type returns the type of this message pack
func (cc *CreateCollectionMsg) Type() MsgType {
	return cc.Base.MsgType
}

// SourceID indicated which component generated this message
func (cc *CreateCollectionMsg) SourceID() int64 {
	return cc.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (cc *CreateCollectionMsg) Marshal(input TsMsg) (MarshalType, error) {
	createCollectionMsg := input.(*CreateCollectionMsg)
	createCollectionRequest := &createCollectionMsg.CreateCollectionRequest
	mb, err := proto.Marshal(createCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (cc *CreateCollectionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	createCollectionRequest := internalpb.CreateCollectionRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &createCollectionRequest)
	if err != nil {
		return nil, err
	}
	createCollectionMsg := &CreateCollectionMsg{CreateCollectionRequest: createCollectionRequest}
	createCollectionMsg.BeginTimestamp = createCollectionMsg.Base.Timestamp
	createCollectionMsg.EndTimestamp = createCollectionMsg.Base.Timestamp

	return createCollectionMsg, nil
}

/////////////////////////////////////////DropCollection//////////////////////////////////////////

// DropCollectionMsg is a message pack that contains drop collection request
type DropCollectionMsg struct {
	BaseMsg
	internalpb.DropCollectionRequest
}

// interface implementation validation
var _ TsMsg = &DropCollectionMsg{}

// ID returns the ID of this message pack
func (dc *DropCollectionMsg) ID() UniqueID {
	return dc.Base.MsgID
}

// Type returns the type of this message pack
func (dc *DropCollectionMsg) Type() MsgType {
	return dc.Base.MsgType
}

// SourceID indicated which component generated this message
func (dc *DropCollectionMsg) SourceID() int64 {
	return dc.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (dc *DropCollectionMsg) Marshal(input TsMsg) (MarshalType, error) {
	dropCollectionMsg := input.(*DropCollectionMsg)
	dropCollectionRequest := &dropCollectionMsg.DropCollectionRequest
	mb, err := proto.Marshal(dropCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (dc *DropCollectionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	dropCollectionRequest := internalpb.DropCollectionRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &dropCollectionRequest)
	if err != nil {
		return nil, err
	}
	dropCollectionMsg := &DropCollectionMsg{DropCollectionRequest: dropCollectionRequest}
	dropCollectionMsg.BeginTimestamp = dropCollectionMsg.Base.Timestamp
	dropCollectionMsg.EndTimestamp = dropCollectionMsg.Base.Timestamp

	return dropCollectionMsg, nil
}

/////////////////////////////////////////CreatePartition//////////////////////////////////////////

// CreatePartitionMsg is a message pack that contains create partition request
type CreatePartitionMsg struct {
	BaseMsg
	internalpb.CreatePartitionRequest
}

// interface implementation validation
var _ TsMsg = &CreatePartitionMsg{}

// ID returns the ID of this message pack
func (cp *CreatePartitionMsg) ID() UniqueID {
	return cp.Base.MsgID
}

// Type returns the type of this message pack
func (cp *CreatePartitionMsg) Type() MsgType {
	return cp.Base.MsgType
}

// SourceID indicated which component generated this message
func (cp *CreatePartitionMsg) SourceID() int64 {
	return cp.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (cp *CreatePartitionMsg) Marshal(input TsMsg) (MarshalType, error) {
	createPartitionMsg := input.(*CreatePartitionMsg)
	createPartitionRequest := &createPartitionMsg.CreatePartitionRequest
	mb, err := proto.Marshal(createPartitionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (cp *CreatePartitionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	createPartitionRequest := internalpb.CreatePartitionRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &createPartitionRequest)
	if err != nil {
		return nil, err
	}
	createPartitionMsg := &CreatePartitionMsg{CreatePartitionRequest: createPartitionRequest}
	createPartitionMsg.BeginTimestamp = createPartitionMsg.Base.Timestamp
	createPartitionMsg.EndTimestamp = createPartitionMsg.Base.Timestamp

	return createPartitionMsg, nil
}

/////////////////////////////////////////DropPartition//////////////////////////////////////////

// DropPartitionMsg is a message pack that contains drop partition request
type DropPartitionMsg struct {
	BaseMsg
	internalpb.DropPartitionRequest
}

// interface implementation validation
var _ TsMsg = &DropPartitionMsg{}

// ID returns the ID of this message pack
func (dp *DropPartitionMsg) ID() UniqueID {
	return dp.Base.MsgID
}

// Type returns the type of this message pack
func (dp *DropPartitionMsg) Type() MsgType {
	return dp.Base.MsgType
}

// SourceID indicated which component generated this message
func (dp *DropPartitionMsg) SourceID() int64 {
	return dp.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (dp *DropPartitionMsg) Marshal(input TsMsg) (MarshalType, error) {
	dropPartitionMsg := input.(*DropPartitionMsg)
	dropPartitionRequest := &dropPartitionMsg.DropPartitionRequest
	mb, err := proto.Marshal(dropPartitionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (dp *DropPartitionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	dropPartitionRequest := internalpb.DropPartitionRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &dropPartitionRequest)
	if err != nil {
		return nil, err
	}
	dropPartitionMsg := &DropPartitionMsg{DropPartitionRequest: dropPartitionRequest}
	dropPartitionMsg.BeginTimestamp = dropPartitionMsg.Base.Timestamp
	dropPartitionMsg.EndTimestamp = dropPartitionMsg.Base.Timestamp

	return dropPartitionMsg, nil
}

/////////////////////////////////////////LoadIndex//////////////////////////////////////////
// FIXME(wxyu): comment it until really needed
/*
type LoadIndexMsg struct {
	BaseMsg
	internalpb.LoadIndex
}

// TraceCtx returns the context of opentracing
func (lim *LoadIndexMsg) TraceCtx() context.Context {
	return lim.BaseMsg.Ctx
}

// SetTraceCtx is used to set context for opentracing
func (lim *LoadIndexMsg) SetTraceCtx(ctx context.Context) {
	lim.BaseMsg.Ctx = ctx
}

// ID returns the ID of this message pack
func (lim *LoadIndexMsg) ID() UniqueID {
	return lim.Base.MsgID
}

// Type returns the type of this message pack
func (lim *LoadIndexMsg) Type() MsgType {
	return lim.Base.MsgType
}

// SourceID indicated which component generated this message
func (lim *LoadIndexMsg) SourceID() int64 {
	return lim.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (lim *LoadIndexMsg) Marshal(input TsMsg) (MarshalType, error) {
	loadIndexMsg := input.(*LoadIndexMsg)
	loadIndexRequest := &loadIndexMsg.LoadIndex
	mb, err := proto.Marshal(loadIndexRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (lim *LoadIndexMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	loadIndexRequest := internalpb.LoadIndex{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &loadIndexRequest)
	if err != nil {
		return nil, err
	}
	loadIndexMsg := &LoadIndexMsg{LoadIndex: loadIndexRequest}

	return loadIndexMsg, nil
}
*/

/////////////////////////////////////////LoadBalanceSegments//////////////////////////////////////////

// LoadBalanceSegmentsMsg is a message pack that contains load balance segments request
type LoadBalanceSegmentsMsg struct {
	BaseMsg
	internalpb.LoadBalanceSegmentsRequest
}

// interface implementation validation
var _ TsMsg = &LoadBalanceSegmentsMsg{}

// ID returns the ID of this message pack
func (l *LoadBalanceSegmentsMsg) ID() UniqueID {
	return l.Base.MsgID
}

// Type returns the type of this message pack
func (l *LoadBalanceSegmentsMsg) Type() MsgType {
	return l.Base.MsgType
}

// SourceID indicated which component generated this message
func (l *LoadBalanceSegmentsMsg) SourceID() int64 {
	return l.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (l *LoadBalanceSegmentsMsg) Marshal(input TsMsg) (MarshalType, error) {
	load := input.(*LoadBalanceSegmentsMsg)
	loadReq := &load.LoadBalanceSegmentsRequest
	mb, err := proto.Marshal(loadReq)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (l *LoadBalanceSegmentsMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	loadReq := internalpb.LoadBalanceSegmentsRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &loadReq)
	if err != nil {
		return nil, err
	}
	loadMsg := &LoadBalanceSegmentsMsg{LoadBalanceSegmentsRequest: loadReq}
	loadMsg.BeginTimestamp = loadReq.Base.Timestamp
	loadMsg.EndTimestamp = loadReq.Base.Timestamp

	return loadMsg, nil
}

// DataNodeTtMsg is a message pack that contains datanode time tick
type DataNodeTtMsg struct {
	BaseMsg
	datapb.DataNodeTtMsg
}

// interface implementation validation
var _ TsMsg = &DataNodeTtMsg{}

// ID returns the ID of this message pack
func (m *DataNodeTtMsg) ID() UniqueID {
	return m.Base.MsgID
}

// Type returns the type of this message pack
func (m *DataNodeTtMsg) Type() MsgType {
	return m.Base.MsgType
}

// SourceID indicated which component generated this message
func (m *DataNodeTtMsg) SourceID() int64 {
	return m.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (m *DataNodeTtMsg) Marshal(input TsMsg) (MarshalType, error) {
	msg := input.(*DataNodeTtMsg)
	t, err := proto.Marshal(&msg.DataNodeTtMsg)
	if err != nil {
		return nil, err
	}
	return t, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (m *DataNodeTtMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	msg := datapb.DataNodeTtMsg{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &msg)
	if err != nil {
		return nil, err
	}
	return &DataNodeTtMsg{
		DataNodeTtMsg: msg,
	}, nil
}
