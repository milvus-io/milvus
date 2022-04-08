// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querynode

import "C"
import (
	"bytes"
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/timerecord"
)

// MsgType is an alias of commonpb.MsgType
type MsgType = commonpb.MsgType

type BaseMsg = msgstream.BaseMsg

var _ queryMsg = (*searchMsg)(nil)
var _ queryMsg = (*retrieveMsg)(nil)

const (
	maxTopKMergeRatio = 10.0
)

type queryMsg interface {
	TraceCtx() context.Context
	SetTraceCtx(ctx context.Context)
	ID() UniqueID
	BeginTs() Timestamp
	EndTs() Timestamp

	Type() MsgType
	SourceID() int64

	GuaranteeTs() Timestamp
	TravelTs() Timestamp
	TimeoutTs() Timestamp

	SetTimeRecorder()
	GetTimeRecorder() *timerecord.TimeRecorder

	CombinePlaceHolderGroups()
	GetCollectionID() UniqueID

	GetNumMerged() int
}

type searchMsg struct {
	BaseMsg
	Base                  *commonpb.MsgBase
	ReqIDs                []int64
	DbID                  int64
	CollectionID          int64
	PartitionIDs          []int64
	Dsl                   string
	DslType               commonpb.DslType
	PlaceholderGroup      []byte
	OrigPlaceHolderGroups [][]byte
	SerializedExprPlan    []byte
	TravelTimestamp       uint64
	GuaranteeTimestamp    uint64
	TimeoutTimestamp      uint64
	NQ                    int64
	OrigNQs               []int64
	SourceIDs             []UniqueID
	TopK                  int64
	MetricType            string
	OrigTopKs             []int64
	tr                    *timerecord.TimeRecorder
}

func (st *searchMsg) GetTimeRecorder() *timerecord.TimeRecorder {
	return st.tr
}

func (st *searchMsg) GetNumMerged() int {
	return len(st.OrigNQs)
}

func (st *searchMsg) ID() UniqueID {
	return st.Base.MsgID
}

// Type returns the type of this message pack
func (st *searchMsg) Type() MsgType {
	return st.Base.MsgType
}

// SourceID indicates which component generated this message
func (st *searchMsg) SourceID() int64 {
	return st.Base.SourceID
}

func (st *searchMsg) GuaranteeTs() Timestamp {
	return st.GuaranteeTimestamp
}

// TravelTs returns the timestamp of a time travel search request
func (st *searchMsg) TravelTs() Timestamp {
	return st.TravelTimestamp
}

// TimeoutTs returns the timestamp of timeout
func (st *searchMsg) TimeoutTs() Timestamp {
	return st.TimeoutTimestamp
}

// SetTimeRecorder sets the timeRecorder for RetrieveMsg
func (st *searchMsg) SetTimeRecorder() {
	st.tr = timerecord.NewTimeRecorder("searchMsg")
}

// ElapseSpan returns the duration from the beginning
func (st *searchMsg) ElapseSpan() time.Duration {
	return st.tr.ElapseSpan()
}

// RecordSpan returns the duration from last record
func (st *searchMsg) RecordSpan() time.Duration {
	return st.tr.RecordSpan()
}

// CombinePlaceHolderGroups combine all the placeholder groups.
func (st *searchMsg) CombinePlaceHolderGroups() {
	if len(st.OrigPlaceHolderGroups) > 1 {
		ret := &milvuspb.PlaceholderGroup{}
		//retValues := ret.Placeholders[0].GetValues()
		_ = proto.Unmarshal(st.PlaceholderGroup, ret)
		for i, grp := range st.OrigPlaceHolderGroups {
			if i == 0 {
				continue
			}
			x := &milvuspb.PlaceholderGroup{}
			_ = proto.Unmarshal(grp, x)
			ret.Placeholders[0].Values = append(ret.Placeholders[0].Values, x.Placeholders[0].Values...)
		}
		st.PlaceholderGroup, _ = proto.Marshal(ret)
	}
}

// GetCollectionID return CollectionID.
func (st *searchMsg) GetCollectionID() UniqueID {
	return st.CollectionID
}

type retrieveMsg struct {
	BaseMsg
	Base               *commonpb.MsgBase
	ReqID              int64
	DbID               int64
	CollectionID       int64
	PartitionIDs       []int64
	SerializedExprPlan []byte
	TravelTimestamp    uint64
	GuaranteeTimestamp uint64
	TimeoutTimestamp   uint64
	tr                 *timerecord.TimeRecorder
}

func (st *retrieveMsg) GetTimeRecorder() *timerecord.TimeRecorder {
	return st.tr
}

func (st *retrieveMsg) ID() UniqueID {
	return st.Base.MsgID
}

// Type returns the type of this message pack
func (st *retrieveMsg) Type() MsgType {
	return st.Base.MsgType
}

// SourceID indicates which component generated this message
func (st *retrieveMsg) SourceID() int64 {
	return st.Base.SourceID
}

func (st *retrieveMsg) GuaranteeTs() Timestamp {
	return st.GuaranteeTimestamp
}

// TravelTs returns the timestamp of a time travel search request
func (st *retrieveMsg) TravelTs() Timestamp {
	return st.TravelTimestamp
}

// TimeoutTs returns the timestamp of timeout
func (st *retrieveMsg) TimeoutTs() Timestamp {
	return st.TimeoutTimestamp
}

// SetTimeRecorder sets the timeRecorder for RetrieveMsg
func (st *retrieveMsg) SetTimeRecorder() {
	st.tr = timerecord.NewTimeRecorder("retrieveMsg")
}

// ElapseSpan returns the duration from the beginning
func (st *retrieveMsg) ElapseSpan() time.Duration {
	return st.tr.ElapseSpan()
}

// RecordSpan returns the duration from last record
func (st *retrieveMsg) RecordSpan() time.Duration {
	return st.tr.RecordSpan()
}

// CombinePlaceHolderGroups does nothing.
func (st *retrieveMsg) CombinePlaceHolderGroups() {
}

// GetCollectionID return CollectionID.
func (st *retrieveMsg) GetCollectionID() UniqueID {
	return st.CollectionID
}

func (st *retrieveMsg) GetNumMerged() int {
	return 0
}

type pubSearchResults struct {
	results []*internalpb.SearchResults
	Blobs   searchResultDataBlobs
	tr      *timerecord.TimeRecorder
	err     error
}

func (p *pubSearchResults) Close() {
	if p.Blobs != nil {
		deleteSearchResultDataBlobs(p.Blobs)
		p.Blobs = nil
	}
}

func canMerge(msg1 queryMsg, msg2 queryMsg) bool {
	newMsg1, ok := msg1.(*searchMsg)
	if !ok {
		return false
	}

	newMsg2, ok := msg2.(*searchMsg)
	if !ok {
		return false
	}

	if newMsg1.DbID != newMsg2.DbID {
		return false
	}

	if newMsg1.CollectionID != newMsg2.CollectionID {
		return false
	}

	if newMsg1.DslType != newMsg2.DslType {
		return false
	}

	if newMsg1.MetricType != newMsg2.MetricType {
		return false
	}

	if !funcutil.SortedSliceEqual(newMsg1.PartitionIDs, newMsg2.PartitionIDs) {
		return false
	}

	if !bytes.Equal(newMsg1.SerializedExprPlan, newMsg2.SerializedExprPlan) {
		return false
	}

	if newMsg1.TravelTimestamp != newMsg2.TravelTimestamp {
		return false
	}

	pre := newMsg1.NQ * newMsg1.TopK * 1.0
	newTopK := newMsg1.TopK
	if newTopK < newMsg2.TopK {
		newTopK = newMsg2.TopK
	}
	after := (newMsg1.NQ + newMsg2.NQ) * newTopK * 1.0

	if pre == 0 {
		return false
	}
	if after/pre > maxTopKMergeRatio {
		return false
	}
	return true
}

func mergeSearchMsg(t queryMsg, s queryMsg) queryMsg {
	target, _ := t.(*searchMsg)
	src, _ := s.(*searchMsg)

	newTopK := target.TopK
	if newTopK < src.TopK {
		newTopK = src.TopK
	}

	target.TopK = newTopK
	target.ReqIDs = append(target.ReqIDs, src.ReqIDs...)
	target.OrigTopKs = append(target.OrigTopKs, src.OrigTopKs...)
	target.OrigNQs = append(target.OrigNQs, src.OrigNQs...)
	target.SourceIDs = append(target.SourceIDs, src.SourceIDs...)
	target.OrigPlaceHolderGroups = append(target.OrigPlaceHolderGroups, src.OrigPlaceHolderGroups...)
	target.NQ += src.NQ
	return target
}

func convertSearchMsg(src *msgstream.SearchMsg) queryMsg {
	target := &searchMsg{
		BaseMsg:               src.BaseMsg,
		Base:                  src.Base,
		ReqIDs:                []UniqueID{src.ReqID},
		DbID:                  src.DbID,
		CollectionID:          src.GetCollectionID(),
		PartitionIDs:          src.GetPartitionIDs(),
		Dsl:                   src.GetDsl(),
		DslType:               src.GetDslType(),
		PlaceholderGroup:      src.GetPlaceholderGroup(),
		OrigPlaceHolderGroups: [][]byte{src.GetPlaceholderGroup()},
		SerializedExprPlan:    src.GetSerializedExprPlan(),
		TravelTimestamp:       src.GetTravelTimestamp(),
		GuaranteeTimestamp:    src.GetGuaranteeTimestamp(),
		TimeoutTimestamp:      src.GetTimeoutTimestamp(),
		NQ:                    src.GetNq(),
		OrigNQs:               []int64{src.GetNq()},
		OrigTopKs:             []int64{src.GetTopk()},
		SourceIDs:             []UniqueID{src.SourceID()},
		TopK:                  src.GetTopk(),
		MetricType:            src.GetMetricType(),
	}
	return target
}

func convertToRetrieveMsg(src *msgstream.RetrieveMsg) queryMsg {
	target := &retrieveMsg{
		BaseMsg:            src.BaseMsg,
		Base:               src.Base,
		DbID:               src.DbID,
		CollectionID:       src.GetCollectionID(),
		PartitionIDs:       src.GetPartitionIDs(),
		SerializedExprPlan: src.GetSerializedExprPlan(),
		TravelTimestamp:    src.GetTravelTimestamp(),
		GuaranteeTimestamp: src.GetGuaranteeTimestamp(),
		TimeoutTimestamp:   src.GetTimeoutTimestamp(),
		ReqID:              src.GetReqID(),
	}
	return target
}

type pubRetrieveResults = internalpb.RetrieveResults

type pubResult struct {
	Msg         queryMsg
	Err         error
	RetrieveRet *pubRetrieveResults
	SearchRet   *pubSearchResults
}

func (p *pubResult) Type() commonpb.MsgType {
	return p.Msg.Type()
}
