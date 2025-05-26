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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// SearchPlan is a wrapper of the underlying C-structure C.CSearchPlan
type SearchPlan struct {
	cSearchPlan C.CSearchPlan
}

func createSearchPlanByExpr(col *CCollection, expr []byte) (*SearchPlan, error) {
	var cPlan C.CSearchPlan
	status := C.CreateSearchPlanByExpr(col.rawPointer(), unsafe.Pointer(&expr[0]), (C.int64_t)(len(expr)), &cPlan)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, errors.Wrap(err, "Create Plan by expr failed")
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

func (plan *SearchPlan) delete() {
	C.DeleteSearchPlan(plan.cSearchPlan)
}

type SearchRequest struct {
	plan              *SearchPlan
	cPlaceholderGroup C.CPlaceholderGroup
	msgID             int64
	searchFieldID     int64
	mvccTimestamp     typeutil.Timestamp
	consistencyLevel  commonpb.ConsistencyLevel
	collectionTTL     typeutil.Timestamp
}

func NewSearchRequest(collection *CCollection, req *querypb.SearchRequest, placeholderGrp []byte) (*SearchRequest, error) {
	metricType := req.GetReq().GetMetricType()
	expr := req.Req.SerializedExprPlan
	plan, err := createSearchPlanByExpr(collection, expr)
	if err != nil {
		return nil, err
	}

	if len(placeholderGrp) == 0 {
		plan.delete()
		return nil, errors.New("empty search request")
	}

	blobPtr := unsafe.Pointer(&placeholderGrp[0])
	blobSize := C.int64_t(len(placeholderGrp))
	var cPlaceholderGroup C.CPlaceholderGroup
	status := C.ParsePlaceholderGroup(plan.cSearchPlan, blobPtr, blobSize, &cPlaceholderGroup)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		plan.delete()
		return nil, errors.Wrap(err, "parser searchRequest failed")
	}

	metricTypeInPlan := plan.GetMetricType()
	if len(metricType) != 0 && metricType != metricTypeInPlan {
		plan.delete()
		return nil, merr.WrapErrParameterInvalid(metricTypeInPlan, metricType, "metric type not match")
	}

	var fieldID C.int64_t
	status = C.GetFieldID(plan.cSearchPlan, &fieldID)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		plan.delete()
		return nil, errors.Wrap(err, "get fieldID from plan failed")
	}

	return &SearchRequest{
		plan:              plan,
		cPlaceholderGroup: cPlaceholderGroup,
		msgID:             req.GetReq().GetBase().GetMsgID(),
		searchFieldID:     int64(fieldID),
		mvccTimestamp:     req.GetReq().GetMvccTimestamp(),
		consistencyLevel:  req.GetReq().GetConsistencyLevel(),
		collectionTTL:     req.GetReq().GetCollectionTtl(),
	}, nil
}

func (req *SearchRequest) GetNumOfQuery() int64 {
	numQueries := C.GetNumOfQueries(req.cPlaceholderGroup)
	return int64(numQueries)
}

func (req *SearchRequest) Plan() *SearchPlan {
	return req.plan
}

func (req *SearchRequest) SearchFieldID() int64 {
	return req.searchFieldID
}

func (req *SearchRequest) Delete() {
	if req.plan != nil {
		req.plan.delete()
	}
	C.DeletePlaceholderGroup(req.cPlaceholderGroup)
}

// RetrievePlan is a wrapper of the underlying C-structure C.CRetrievePlan
type RetrievePlan struct {
	cRetrievePlan    C.CRetrievePlan
	Timestamp        typeutil.Timestamp
	msgID            int64 // only used to debug.
	maxLimitSize     int64
	ignoreNonPk      bool
	consistencyLevel commonpb.ConsistencyLevel
	collectionTTL    typeutil.Timestamp
}

func NewRetrievePlan(col *CCollection, expr []byte, timestamp typeutil.Timestamp, msgID int64, consistencylevel commonpb.ConsistencyLevel, collectionTTL typeutil.Timestamp) (*RetrievePlan, error) {
	if col.rawPointer() == nil {
		return nil, errors.New("collection is released")
	}
	var cPlan C.CRetrievePlan
	status := C.CreateRetrievePlanByExpr(col.rawPointer(), unsafe.Pointer(&expr[0]), (C.int64_t)(len(expr)), &cPlan)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, errors.Wrap(err, "Create retrieve plan by expr failed")
	}
	maxLimitSize := paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64()
	return &RetrievePlan{
		cRetrievePlan:    cPlan,
		Timestamp:        timestamp,
		msgID:            msgID,
		maxLimitSize:     maxLimitSize,
		consistencyLevel: consistencylevel,
		collectionTTL:    collectionTTL,
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
