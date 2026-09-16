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

package segcore

/*
#cgo pkg-config: milvus_core

#include "common/type_c.h"
#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "segcore/plan_c.h"
*/
import "C"

import (
	"unsafe"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SearchPlan is a wrapper of the underlying C-structure C.CSearchPlan
type SearchPlan struct {
	cSearchPlan C.CSearchPlan
}

func deletePlaceholderGroup(group unsafe.Pointer) {
	C.DeletePlaceholderGroup(C.CPlaceholderGroup(group))
}

func createSearchPlanByExpr(col *CCollection, expr []byte) (*SearchPlan, error) {
	if len(expr) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("empty expression plan")
	}
	var cPlan C.CSearchPlan
	status := C.CreateSearchPlanByExpr(col.rawPointer(), unsafe.Pointer(&expr[0]), (C.int64_t)(len(expr)), &cPlan)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, merr.Wrap(err, "Create Plan by expr failed")
	}
	return &SearchPlan{cSearchPlan: cPlan}, nil
}

func (plan *SearchPlan) GetTopK() int64 {
	topK := C.GetTopK(plan.cSearchPlan)
	return int64(topK)
}

func (plan *SearchPlan) setMetricType(metricType string) {
	cmt := C.CString(metricType)
	defer C.free(unsafe.Pointer(cmt))
	C.SetMetricType(plan.cSearchPlan, cmt)
}

func (plan *SearchPlan) GetMetricType() string {
	cMetricType := C.GetMetricType(plan.cSearchPlan)
	defer C.free(unsafe.Pointer(cMetricType))
	metricType := C.GoString(cMetricType)
	return metricType
}

func (plan *SearchPlan) HasTargetEntries() bool {
	return bool(C.HasTargetEntries(plan.cSearchPlan))
}

func (plan *SearchPlan) delete() {
	C.DeleteSearchPlan(plan.cSearchPlan)
}

type SearchRequest struct {
	plan                  *SearchPlan
	cPlaceholderGroup     C.CPlaceholderGroup
	msgID                 int64
	searchFieldID         int64
	mvccTimestamp         typeutil.Timestamp
	consistencyLevel      commonpb.ConsistencyLevel
	collectionTTL         typeutil.Timestamp
	entityTTLPhysicalTime typeutil.Timestamp
	filterOnly            bool // If true, only execute filter and return valid count (for two-stage search Stage 1)
	enableExprCache       bool // If true, enable expression filter cache for two-stage search
	textTermGenerations   map[int64]uint64
}

func validateTextTermGenerations(req *querypb.SearchRequest) (map[int64]uint64, error) {
	textTermGenerations := make(map[int64]uint64, len(req.GetTextTermGenerations()))
	for _, generation := range req.GetTextTermGenerations() {
		if generation == nil || generation.GetSegmentID() == 0 || generation.GetGeneration() == 0 {
			return nil, merr.WrapErrServiceInternalMsg("invalid text term generation in search request")
		}
		if existing, ok := textTermGenerations[generation.GetSegmentID()]; ok && existing != generation.GetGeneration() {
			return nil, merr.WrapErrServiceInternalMsg(
				"conflicting text term generations for segment %d", generation.GetSegmentID())
		}
		textTermGenerations[generation.GetSegmentID()] = generation.GetGeneration()
	}
	if req.GetReq().GetFuzzyBm25Options() == nil {
		if len(textTermGenerations) > 0 {
			return nil, merr.WrapErrServiceInternalMsg("text term generations require fuzzy BM25 options")
		}
	} else {
		targetSegments := make(map[int64]struct{}, len(req.GetSegmentIDs()))
		for _, segmentID := range req.GetSegmentIDs() {
			if segmentID == 0 {
				return nil, merr.WrapErrServiceInternalMsg("invalid fuzzy BM25 target segment 0")
			}
			targetSegments[segmentID] = struct{}{}
			if _, ok := textTermGenerations[segmentID]; !ok {
				return nil, merr.WrapErrServiceInternalMsg(
					"missing text term generation for fuzzy BM25 target segment %d", segmentID)
			}
		}
		if len(textTermGenerations) != len(targetSegments) {
			return nil, merr.WrapErrServiceInternalMsg("text term generations do not match fuzzy BM25 target segments")
		}
	}
	return textTermGenerations, nil
}

func NewSearchRequest(collection *CCollection, req *querypb.SearchRequest, placeholderGrp []byte) (*SearchRequest, error) {
	textTermGenerations, err := validateTextTermGenerations(req)
	if err != nil {
		return nil, err
	}
	metricType := req.GetReq().GetMetricType()
	expr := req.Req.SerializedExprPlan
	plan, err := createSearchPlanByExpr(collection, expr)
	if err != nil {
		return nil, err
	}

	if len(placeholderGrp) == 0 {
		plan.delete()
		return nil, merr.WrapErrParameterInvalidMsg("empty search request")
	}

	metricTypeInPlan := plan.GetMetricType()
	if len(metricType) != 0 && metricType != metricTypeInPlan {
		plan.delete()
		return nil, merr.WrapErrParameterInvalid(metricTypeInPlan, metricType, "metric type not match")
	}

	var fieldID C.int64_t
	status := C.GetFieldID(plan.cSearchPlan, &fieldID)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		plan.delete()
		return nil, merr.Wrap(err, "get fieldID from plan failed")
	}

	blobPtr := unsafe.Pointer(&placeholderGrp[0])
	blobSize := C.int64_t(len(placeholderGrp))
	var cPlaceholderGroup C.CPlaceholderGroup
	status = C.ParsePlaceholderGroup(plan.cSearchPlan, blobPtr, blobSize, &cPlaceholderGroup)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		plan.delete()
		return nil, merr.Wrap(err, "parser searchRequest failed")
	}

	cl := req.GetReq().GetConsistencyLevel()

	return &SearchRequest{
		plan:                  plan,
		cPlaceholderGroup:     cPlaceholderGroup,
		msgID:                 req.GetReq().GetBase().GetMsgID(),
		searchFieldID:         int64(fieldID),
		mvccTimestamp:         req.GetReq().GetMvccTimestamp(),
		consistencyLevel:      cl,
		collectionTTL:         req.GetReq().GetCollectionTtlTimestamps(),
		entityTTLPhysicalTime: req.GetReq().GetEntityTtlPhysicalTime(),
		filterOnly:            req.GetFilterOnly(),
		enableExprCache:       req.GetEnableExprCache(),
		textTermGenerations:   textTermGenerations,
	}, nil
}

func (r *SearchRequest) TextTermGeneration(segmentID int64) (uint64, bool) {
	if r == nil {
		return 0, false
	}
	generation, ok := r.textTermGenerations[segmentID]
	return generation, ok
}

func (r *SearchRequest) ValidateTextTermGeneration(segmentID int64, actual uint64) error {
	expected, ok := r.TextTermGeneration(segmentID)
	if !ok || expected == actual {
		return nil
	}
	return merr.WrapErrServiceUnavailableMsg(
		"text term generation changed for segment %d: expected=%d actual=%d",
		segmentID, expected, actual)
}

func (req *SearchRequest) GetNumOfQuery() int64 {
	numQueries := C.GetNumOfQueries(req.cPlaceholderGroup)
	return int64(numQueries)
}

func (req *SearchRequest) MVCC() typeutil.Timestamp {
	return req.mvccTimestamp
}

func (req *SearchRequest) Plan() *SearchPlan {
	return req.plan
}

func (req *SearchRequest) PlaceholderGroup() unsafe.Pointer {
	return unsafe.Pointer(req.cPlaceholderGroup)
}

func (req *SearchRequest) SearchFieldID() int64 {
	return req.searchFieldID
}

func (req *SearchRequest) FilterOnly() bool {
	return req.filterOnly
}

func (req *SearchRequest) EnableExprCache() bool {
	return req.enableExprCache
}

func (req *SearchRequest) Delete() {
	if req.plan != nil {
		req.plan.delete()
	}
	deletePlaceholderGroup(unsafe.Pointer(req.cPlaceholderGroup))
}

// RetrievePlan is a wrapper of the underlying C-structure C.CRetrievePlan
type RetrievePlan struct {
	cRetrievePlan         C.CRetrievePlan
	Timestamp             typeutil.Timestamp
	msgID                 int64 // only used to debug.
	maxLimitSize          int64
	ignoreNonPk           bool
	consistencyLevel      commonpb.ConsistencyLevel
	collectionTTL         typeutil.Timestamp
	entityTTLPhysicalTime typeutil.Timestamp
}

func NewRetrievePlan(col *CCollection,
	expr []byte,
	timestamp typeutil.Timestamp,
	msgID int64,
	consistencylevel commonpb.ConsistencyLevel,
	collectionTTL typeutil.Timestamp,
	entityTTLPhysicalTime typeutil.Timestamp,
) (*RetrievePlan, error) {
	if col.rawPointer() == nil {
		return nil, merr.WrapErrServiceInternalMsg("collection is released")
	}
	var cPlan C.CRetrievePlan
	status := C.CreateRetrievePlanByExpr(col.rawPointer(), unsafe.Pointer(&expr[0]), (C.int64_t)(len(expr)), &cPlan)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, merr.Wrap(err, "Create retrieve plan by expr failed")
	}
	maxLimitSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	return &RetrievePlan{
		cRetrievePlan:         cPlan,
		Timestamp:             timestamp,
		msgID:                 msgID,
		maxLimitSize:          maxLimitSize,
		consistencyLevel:      consistencylevel,
		collectionTTL:         collectionTTL,
		entityTTLPhysicalTime: entityTTLPhysicalTime,
	}, nil
}

func (plan *RetrievePlan) ShouldIgnoreNonPk() bool {
	return bool(C.ShouldIgnoreNonPk(plan.cRetrievePlan))
}

func (plan *RetrievePlan) SetIgnoreNonPk(ignore bool) {
	plan.ignoreNonPk = ignore
}

func (plan *RetrievePlan) IsIgnoreNonPk() bool {
	return plan.ignoreNonPk
}

func (plan *RetrievePlan) MsgID() int64 {
	return plan.msgID
}

func (plan *RetrievePlan) Delete() {
	C.DeleteRetrievePlan(plan.cRetrievePlan)
}
