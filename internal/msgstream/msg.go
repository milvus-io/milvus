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

type MsgType = commonpb.MsgType
type MarshalType = interface{}

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

type BaseMsg struct {
	Ctx            context.Context
	BeginTimestamp Timestamp
	EndTimestamp   Timestamp
	HashValues     []uint32
	MsgPosition    *MsgPosition
}

func (bm *BaseMsg) BeginTs() Timestamp {
	return bm.BeginTimestamp
}

func (bm *BaseMsg) EndTs() Timestamp {
	return bm.EndTimestamp
}

func (bm *BaseMsg) HashKeys() []uint32 {
	return bm.HashValues
}

func (bm *BaseMsg) Position() *MsgPosition {
	return bm.MsgPosition
}

func (bm *BaseMsg) SetPosition(position *MsgPosition) {
	bm.MsgPosition = position
}

func ConvertToByteArray(input interface{}) ([]byte, error) {
	switch output := input.(type) {
	case []byte:
		return output, nil
	default:
		return nil, errors.New("cannot convert interface{} to []byte")
	}
}

/////////////////////////////////////////Insert//////////////////////////////////////////
type InsertMsg struct {
	BaseMsg
	internalpb.InsertRequest
}

func (it *InsertMsg) TraceCtx() context.Context {
	return it.BaseMsg.Ctx
}
func (it *InsertMsg) SetTraceCtx(ctx context.Context) {
	it.BaseMsg.Ctx = ctx
}

func (it *InsertMsg) ID() UniqueID {
	return it.Base.MsgID
}

func (it *InsertMsg) Type() MsgType {
	return it.Base.MsgType
}

func (it *InsertMsg) SourceID() int64 {
	return it.Base.SourceID
}

func (it *InsertMsg) Marshal(input TsMsg) (MarshalType, error) {
	insertMsg := input.(*InsertMsg)
	insertRequest := &insertMsg.InsertRequest
	mb, err := proto.Marshal(insertRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (it *InsertMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	insertRequest := internalpb.InsertRequest{}
	in, err := ConvertToByteArray(input)
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
// TODO(wxyu): comment it until really needed
type DeleteMsg struct {
	BaseMsg
	internalpb.DeleteRequest
}

func (dt *DeleteMsg) TraceCtx() context.Context {
	return dt.BaseMsg.Ctx
}

func (dt *DeleteMsg) SetTraceCtx(ctx context.Context) {
	dt.BaseMsg.Ctx = ctx
}

func (dt *DeleteMsg) ID() UniqueID {
	return dt.Base.MsgID
}

func (dt *DeleteMsg) Type() MsgType {
	return dt.Base.MsgType
}

func (dt *DeleteMsg) SourceID() int64 {
	return dt.Base.SourceID
}

func (dt *DeleteMsg) Marshal(input TsMsg) (MarshalType, error) {
	deleteMsg := input.(*DeleteMsg)
	deleteRequest := &deleteMsg.DeleteRequest
	mb, err := proto.Marshal(deleteRequest)
	if err != nil {
		return nil, err
	}

	return mb, nil
}

func (dt *DeleteMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	deleteRequest := internalpb.DeleteRequest{}
	in, err := ConvertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &deleteRequest)
	if err != nil {
		return nil, err
	}
	deleteMsg := &DeleteMsg{DeleteRequest: deleteRequest}
	for _, timestamp := range deleteMsg.Timestamps {
		deleteMsg.BeginTimestamp = timestamp
		deleteMsg.EndTimestamp = timestamp
		break
	}
	for _, timestamp := range deleteMsg.Timestamps {
		if timestamp > deleteMsg.EndTimestamp {
			deleteMsg.EndTimestamp = timestamp
		}
		if timestamp < deleteMsg.BeginTimestamp {
			deleteMsg.BeginTimestamp = timestamp
		}
	}

	return deleteMsg, nil
}

/////////////////////////////////////////Search//////////////////////////////////////////
type SearchMsg struct {
	BaseMsg
	internalpb.SearchRequest
}

func (st *SearchMsg) TraceCtx() context.Context {
	return st.BaseMsg.Ctx
}

func (st *SearchMsg) SetTraceCtx(ctx context.Context) {
	st.BaseMsg.Ctx = ctx
}

func (st *SearchMsg) ID() UniqueID {
	return st.Base.MsgID
}

func (st *SearchMsg) Type() MsgType {
	return st.Base.MsgType
}

func (st *SearchMsg) SourceID() int64 {
	return st.Base.SourceID
}

func (st *SearchMsg) GuaranteeTs() Timestamp {
	return st.GetGuaranteeTimestamp()
}

func (st *SearchMsg) TravelTs() Timestamp {
	return st.GetTravelTimestamp()
}

func (st *SearchMsg) Marshal(input TsMsg) (MarshalType, error) {
	searchTask := input.(*SearchMsg)
	searchRequest := &searchTask.SearchRequest
	mb, err := proto.Marshal(searchRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (st *SearchMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	searchRequest := internalpb.SearchRequest{}
	in, err := ConvertToByteArray(input)
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
type SearchResultMsg struct {
	BaseMsg
	internalpb.SearchResults
}

func (srt *SearchResultMsg) TraceCtx() context.Context {
	return srt.BaseMsg.Ctx
}

func (srt *SearchResultMsg) SetTraceCtx(ctx context.Context) {
	srt.BaseMsg.Ctx = ctx
}

func (srt *SearchResultMsg) ID() UniqueID {
	return srt.Base.MsgID
}

func (srt *SearchResultMsg) Type() MsgType {
	return srt.Base.MsgType
}

func (srt *SearchResultMsg) SourceID() int64 {
	return srt.Base.SourceID
}

func (srt *SearchResultMsg) Marshal(input TsMsg) (MarshalType, error) {
	searchResultTask := input.(*SearchResultMsg)
	searchResultRequest := &searchResultTask.SearchResults
	mb, err := proto.Marshal(searchResultRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (srt *SearchResultMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	searchResultRequest := internalpb.SearchResults{}
	in, err := ConvertToByteArray(input)
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
type RetrieveMsg struct {
	BaseMsg
	internalpb.RetrieveRequest
}

func (rm *RetrieveMsg) TraceCtx() context.Context {
	return rm.BaseMsg.Ctx
}

func (rm *RetrieveMsg) SetTraceCtx(ctx context.Context) {
	rm.BaseMsg.Ctx = ctx
}

func (rm *RetrieveMsg) ID() UniqueID {
	return rm.Base.MsgID
}

func (rm *RetrieveMsg) Type() MsgType {
	return rm.Base.MsgType
}

func (rm *RetrieveMsg) SourceID() int64 {
	return rm.Base.SourceID
}

func (rm *RetrieveMsg) GuaranteeTs() Timestamp {
	return rm.GetGuaranteeTimestamp()
}

func (rm *RetrieveMsg) TravelTs() Timestamp {
	return rm.GetTravelTimestamp()
}

func (rm *RetrieveMsg) Marshal(input TsMsg) (MarshalType, error) {
	retrieveTask := input.(*RetrieveMsg)
	retrieveRequest := &retrieveTask.RetrieveRequest
	mb, err := proto.Marshal(retrieveRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (rm *RetrieveMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	retrieveRequest := internalpb.RetrieveRequest{}
	in, err := ConvertToByteArray(input)
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
type RetrieveResultMsg struct {
	BaseMsg
	internalpb.RetrieveResults
}

func (rrm *RetrieveResultMsg) TraceCtx() context.Context {
	return rrm.BaseMsg.Ctx
}

func (rrm *RetrieveResultMsg) SetTraceCtx(ctx context.Context) {
	rrm.BaseMsg.Ctx = ctx
}

func (rrm *RetrieveResultMsg) ID() UniqueID {
	return rrm.Base.MsgID
}

func (rrm *RetrieveResultMsg) Type() MsgType {
	return rrm.Base.MsgType
}

func (rrm *RetrieveResultMsg) SourceID() int64 {
	return rrm.Base.SourceID
}

func (rrm *RetrieveResultMsg) Marshal(input TsMsg) (MarshalType, error) {
	retrieveResultTask := input.(*RetrieveResultMsg)
	retrieveResultRequest := &retrieveResultTask.RetrieveResults
	mb, err := proto.Marshal(retrieveResultRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (rrm *RetrieveResultMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	retrieveResultRequest := internalpb.RetrieveResults{}
	in, err := ConvertToByteArray(input)
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
type TimeTickMsg struct {
	BaseMsg
	internalpb.TimeTickMsg
}

func (tst *TimeTickMsg) TraceCtx() context.Context {
	return tst.BaseMsg.Ctx
}

func (tst *TimeTickMsg) SetTraceCtx(ctx context.Context) {
	tst.BaseMsg.Ctx = ctx
}

func (tst *TimeTickMsg) ID() UniqueID {
	return tst.Base.MsgID
}

func (tst *TimeTickMsg) Type() MsgType {
	return tst.Base.MsgType
}

func (tst *TimeTickMsg) SourceID() int64 {
	return tst.Base.SourceID
}

func (tst *TimeTickMsg) Marshal(input TsMsg) (MarshalType, error) {
	timeTickTask := input.(*TimeTickMsg)
	timeTick := &timeTickTask.TimeTickMsg
	mb, err := proto.Marshal(timeTick)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (tst *TimeTickMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	timeTickMsg := internalpb.TimeTickMsg{}
	in, err := ConvertToByteArray(input)
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
// GOOSE TODO: remove QueryNodeStats
type QueryNodeStatsMsg struct {
	BaseMsg
	internalpb.QueryNodeStats
}

func (qs *QueryNodeStatsMsg) TraceCtx() context.Context {
	return qs.BaseMsg.Ctx
}

func (qs *QueryNodeStatsMsg) SetTraceCtx(ctx context.Context) {
	qs.BaseMsg.Ctx = ctx
}

func (qs *QueryNodeStatsMsg) ID() UniqueID {
	return qs.Base.MsgID
}

func (qs *QueryNodeStatsMsg) Type() MsgType {
	return qs.Base.MsgType
}

func (qs *QueryNodeStatsMsg) SourceID() int64 {
	return qs.Base.SourceID
}

func (qs *QueryNodeStatsMsg) Marshal(input TsMsg) (MarshalType, error) {
	queryNodeSegStatsTask := input.(*QueryNodeStatsMsg)
	queryNodeSegStats := &queryNodeSegStatsTask.QueryNodeStats
	mb, err := proto.Marshal(queryNodeSegStats)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (qs *QueryNodeStatsMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	queryNodeSegStats := internalpb.QueryNodeStats{}
	in, err := ConvertToByteArray(input)
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
type SegmentStatisticsMsg struct {
	BaseMsg
	internalpb.SegmentStatistics
}

func (ss *SegmentStatisticsMsg) TraceCtx() context.Context {
	return ss.BaseMsg.Ctx
}

func (ss *SegmentStatisticsMsg) SetTraceCtx(ctx context.Context) {
	ss.BaseMsg.Ctx = ctx
}

func (ss *SegmentStatisticsMsg) ID() UniqueID {
	return ss.Base.MsgID
}

func (ss *SegmentStatisticsMsg) Type() MsgType {
	return ss.Base.MsgType
}

func (ss *SegmentStatisticsMsg) SourceID() int64 {
	return ss.Base.SourceID
}

func (ss *SegmentStatisticsMsg) Marshal(input TsMsg) (MarshalType, error) {
	segStatsTask := input.(*SegmentStatisticsMsg)
	segStats := &segStatsTask.SegmentStatistics
	mb, err := proto.Marshal(segStats)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (ss *SegmentStatisticsMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	segStats := internalpb.SegmentStatistics{}
	in, err := ConvertToByteArray(input)
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
type CreateCollectionMsg struct {
	BaseMsg
	internalpb.CreateCollectionRequest
}

func (cc *CreateCollectionMsg) TraceCtx() context.Context {
	return cc.BaseMsg.Ctx
}

func (cc *CreateCollectionMsg) SetTraceCtx(ctx context.Context) {
	cc.BaseMsg.Ctx = ctx
}

func (cc *CreateCollectionMsg) ID() UniqueID {
	return cc.Base.MsgID
}

func (cc *CreateCollectionMsg) Type() MsgType {
	return cc.Base.MsgType
}

func (cc *CreateCollectionMsg) SourceID() int64 {
	return cc.Base.SourceID
}

func (cc *CreateCollectionMsg) Marshal(input TsMsg) (MarshalType, error) {
	createCollectionMsg := input.(*CreateCollectionMsg)
	createCollectionRequest := &createCollectionMsg.CreateCollectionRequest
	mb, err := proto.Marshal(createCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (cc *CreateCollectionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	createCollectionRequest := internalpb.CreateCollectionRequest{}
	in, err := ConvertToByteArray(input)
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
type DropCollectionMsg struct {
	BaseMsg
	internalpb.DropCollectionRequest
}

func (dc *DropCollectionMsg) TraceCtx() context.Context {
	return dc.BaseMsg.Ctx
}

func (dc *DropCollectionMsg) SetTraceCtx(ctx context.Context) {
	dc.BaseMsg.Ctx = ctx
}

func (dc *DropCollectionMsg) ID() UniqueID {
	return dc.Base.MsgID
}

func (dc *DropCollectionMsg) Type() MsgType {
	return dc.Base.MsgType
}

func (dc *DropCollectionMsg) SourceID() int64 {
	return dc.Base.SourceID
}

func (dc *DropCollectionMsg) Marshal(input TsMsg) (MarshalType, error) {
	dropCollectionMsg := input.(*DropCollectionMsg)
	dropCollectionRequest := &dropCollectionMsg.DropCollectionRequest
	mb, err := proto.Marshal(dropCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (dc *DropCollectionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	dropCollectionRequest := internalpb.DropCollectionRequest{}
	in, err := ConvertToByteArray(input)
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
type CreatePartitionMsg struct {
	BaseMsg
	internalpb.CreatePartitionRequest
}

func (cp *CreatePartitionMsg) TraceCtx() context.Context {
	return cp.BaseMsg.Ctx
}

func (cp *CreatePartitionMsg) SetTraceCtx(ctx context.Context) {
	cp.BaseMsg.Ctx = ctx
}

func (cp *CreatePartitionMsg) ID() UniqueID {
	return cp.Base.MsgID
}

func (cp *CreatePartitionMsg) Type() MsgType {
	return cp.Base.MsgType
}

func (cp *CreatePartitionMsg) SourceID() int64 {
	return cp.Base.SourceID
}

func (cp *CreatePartitionMsg) Marshal(input TsMsg) (MarshalType, error) {
	createPartitionMsg := input.(*CreatePartitionMsg)
	createPartitionRequest := &createPartitionMsg.CreatePartitionRequest
	mb, err := proto.Marshal(createPartitionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (cp *CreatePartitionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	createPartitionRequest := internalpb.CreatePartitionRequest{}
	in, err := ConvertToByteArray(input)
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
type DropPartitionMsg struct {
	BaseMsg
	internalpb.DropPartitionRequest
}

func (dp *DropPartitionMsg) TraceCtx() context.Context {
	return dp.BaseMsg.Ctx
}

func (dp *DropPartitionMsg) SetTraceCtx(ctx context.Context) {
	dp.BaseMsg.Ctx = ctx
}

func (dp *DropPartitionMsg) ID() UniqueID {
	return dp.Base.MsgID
}

func (dp *DropPartitionMsg) Type() MsgType {
	return dp.Base.MsgType
}

func (dp *DropPartitionMsg) SourceID() int64 {
	return dp.Base.SourceID
}

func (dp *DropPartitionMsg) Marshal(input TsMsg) (MarshalType, error) {
	dropPartitionMsg := input.(*DropPartitionMsg)
	dropPartitionRequest := &dropPartitionMsg.DropPartitionRequest
	mb, err := proto.Marshal(dropPartitionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (dp *DropPartitionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	dropPartitionRequest := internalpb.DropPartitionRequest{}
	in, err := ConvertToByteArray(input)
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
// TODO(wxyu): comment it until really needed
/*
type LoadIndexMsg struct {
	BaseMsg
	internalpb.LoadIndex
}

func (lim *LoadIndexMsg) TraceCtx() context.Context {
	return lim.BaseMsg.Ctx
}

func (lim *LoadIndexMsg) SetTraceCtx(ctx context.Context) {
	lim.BaseMsg.Ctx = ctx
}

func (lim *LoadIndexMsg) ID() UniqueID {
	return lim.Base.MsgID
}

func (lim *LoadIndexMsg) Type() MsgType {
	return lim.Base.MsgType
}

func (lim *LoadIndexMsg) SourceID() int64 {
	return lim.Base.SourceID
}

func (lim *LoadIndexMsg) Marshal(input TsMsg) (MarshalType, error) {
	loadIndexMsg := input.(*LoadIndexMsg)
	loadIndexRequest := &loadIndexMsg.LoadIndex
	mb, err := proto.Marshal(loadIndexRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (lim *LoadIndexMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	loadIndexRequest := internalpb.LoadIndex{}
	in, err := ConvertToByteArray(input)
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
type LoadBalanceSegmentsMsg struct {
	BaseMsg
	internalpb.LoadBalanceSegmentsRequest
}

func (l *LoadBalanceSegmentsMsg) TraceCtx() context.Context {
	return l.BaseMsg.Ctx
}

func (l *LoadBalanceSegmentsMsg) SetTraceCtx(ctx context.Context) {
	l.BaseMsg.Ctx = ctx
}

func (l *LoadBalanceSegmentsMsg) ID() UniqueID {
	return l.Base.MsgID
}

func (l *LoadBalanceSegmentsMsg) Type() MsgType {
	return l.Base.MsgType
}

func (l *LoadBalanceSegmentsMsg) SourceID() int64 {
	return l.Base.SourceID
}

func (l *LoadBalanceSegmentsMsg) Marshal(input TsMsg) (MarshalType, error) {
	load := input.(*LoadBalanceSegmentsMsg)
	loadReq := &load.LoadBalanceSegmentsRequest
	mb, err := proto.Marshal(loadReq)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (l *LoadBalanceSegmentsMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	loadReq := internalpb.LoadBalanceSegmentsRequest{}
	in, err := ConvertToByteArray(input)
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

type DataNodeTtMsg struct {
	BaseMsg
	datapb.DataNodeTtMsg
}

func (m *DataNodeTtMsg) TraceCtx() context.Context {
	return m.BaseMsg.Ctx
}

func (m *DataNodeTtMsg) SetTraceCtx(ctx context.Context) {
	m.BaseMsg.Ctx = ctx
}

func (m *DataNodeTtMsg) ID() UniqueID {
	return m.Base.MsgID
}

func (m *DataNodeTtMsg) Type() MsgType {
	return m.Base.MsgType
}

func (m *DataNodeTtMsg) SourceID() int64 {
	return m.Base.SourceID
}

func (m *DataNodeTtMsg) Marshal(input TsMsg) (MarshalType, error) {
	msg := input.(*DataNodeTtMsg)
	t, err := proto.Marshal(&msg.DataNodeTtMsg)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (m *DataNodeTtMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	msg := datapb.DataNodeTtMsg{}
	in, err := ConvertToByteArray(input)
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
