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

package segments

/*
#cgo pkg-config: milvus_core

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "segcore/plan_c.h"
*/
import "C"

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	. "github.com/milvus-io/milvus/pkg/util/typeutil"
)

// SearchPlan is a wrapper of the underlying C-structure C.CSearchPlan
type SearchPlan struct {
	cSearchPlan C.CSearchPlan
}

func createSearchPlanByExpr(ctx context.Context, col *Collection, expr []byte) (*SearchPlan, error) {
	if col.collectionPtr == nil {
		return nil, errors.New("nil collection ptr, collectionID = " + fmt.Sprintln(col.id))
	}
	var cPlan C.CSearchPlan
	status := C.CreateSearchPlanByExpr(col.collectionPtr, unsafe.Pointer(&expr[0]), (C.int64_t)(len(expr)), &cPlan)

	err1 := HandleCStatus(ctx, &status, "Create Plan by expr failed")
	if err1 != nil {
		return nil, err1
	}

	return &SearchPlan{cSearchPlan: cPlan}, nil
}

func (plan *SearchPlan) getTopK() int64 {
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
	msgID             UniqueID
	searchFieldID     UniqueID
	mvccTimestamp     Timestamp
}

func NewSearchRequest(ctx context.Context, collection *Collection, req *querypb.SearchRequest, placeholderGrp []byte) (*SearchRequest, error) {
	metricType := req.GetReq().GetMetricType()
	expr := req.Req.SerializedExprPlan
	plan, err := createSearchPlanByExpr(ctx, collection, expr)
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

	if err := HandleCStatus(ctx, &status, "parser searchRequest failed"); err != nil {
		plan.delete()
		return nil, err
	}

	metricTypeInPlan := plan.GetMetricType()
	if len(metricType) != 0 && metricType != metricTypeInPlan {
		plan.delete()
		return nil, merr.WrapErrParameterInvalid(metricTypeInPlan, metricType, "metric type not match")
	}

	var fieldID C.int64_t
	status = C.GetFieldID(plan.cSearchPlan, &fieldID)
	if err = HandleCStatus(ctx, &status, "get fieldID from plan failed"); err != nil {
		plan.delete()
		return nil, err
	}

	ret := &SearchRequest{
		plan:              plan,
		cPlaceholderGroup: cPlaceholderGroup,
		msgID:             req.GetReq().GetBase().GetMsgID(),
		searchFieldID:     int64(fieldID),
		mvccTimestamp:     req.GetReq().GetMvccTimestamp(),
	}

	return ret, nil
}

func (req *SearchRequest) getNumOfQuery() int64 {
	numQueries := C.GetNumOfQueries(req.cPlaceholderGroup)
	return int64(numQueries)
}

func (req *SearchRequest) Plan() *SearchPlan {
	return req.plan
}

func (req *SearchRequest) Delete() {
	if req.plan != nil {
		req.plan.delete()
	}
	C.DeletePlaceholderGroup(req.cPlaceholderGroup)
}

func parseSearchRequest(ctx context.Context, plan *SearchPlan, searchRequestBlob []byte) (*SearchRequest, error) {
	if len(searchRequestBlob) == 0 {
		return nil, fmt.Errorf("empty search request")
	}
	blobPtr := unsafe.Pointer(&searchRequestBlob[0])
	blobSize := C.int64_t(len(searchRequestBlob))
	var cPlaceholderGroup C.CPlaceholderGroup
	status := C.ParsePlaceholderGroup(plan.cSearchPlan, blobPtr, blobSize, &cPlaceholderGroup)

	if err := HandleCStatus(ctx, &status, "parser searchRequest failed"); err != nil {
		return nil, err
	}

	ret := &SearchRequest{cPlaceholderGroup: cPlaceholderGroup, plan: plan}
	return ret, nil
}

// RetrievePlan is a wrapper of the underlying C-structure C.CRetrievePlan
type RetrievePlan struct {
	cRetrievePlan C.CRetrievePlan
	Timestamp     Timestamp
	msgID         UniqueID // only used to debug.
	ignoreNonPk   bool
}

func NewRetrievePlan(ctx context.Context, col *Collection, expr []byte, timestamp Timestamp, msgID UniqueID) (*RetrievePlan, error) {
	col.mu.RLock()
	defer col.mu.RUnlock()

	if col.collectionPtr == nil {
		return nil, merr.WrapErrCollectionNotFound(col.id, "collection released")
	}

	var cPlan C.CRetrievePlan
	status := C.CreateRetrievePlanByExpr(col.collectionPtr, unsafe.Pointer(&expr[0]), (C.int64_t)(len(expr)), &cPlan)

	err := HandleCStatus(ctx, &status, "Create retrieve plan by expr failed")
	if err != nil {
		return nil, err
	}

	newPlan := &RetrievePlan{
		cRetrievePlan: cPlan,
		Timestamp:     timestamp,
		msgID:         msgID,
	}
	return newPlan, nil
}

func (plan *RetrievePlan) ShouldIgnoreNonPk() bool {
	return bool(C.ShouldIgnoreNonPk(plan.cRetrievePlan))
}

func (plan *RetrievePlan) Delete() {
	C.DeleteRetrievePlan(plan.cRetrievePlan)
}
