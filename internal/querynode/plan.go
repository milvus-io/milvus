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

/*
#cgo CFLAGS: -I${SRCDIR}/../core/output/include
#cgo darwin LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath,"${SRCDIR}/../core/output/lib"
#cgo linux LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib
#cgo windows LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "segcore/plan_c.h"
*/
import "C"
import (
	"errors"
	"fmt"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"unsafe"
)

// SearchPlan is a wrapper of the underlying C-structure C.CSearchPlan
type SearchPlan struct {
	cSearchPlan C.CSearchPlan
}

// createSearchPlan returns a new SearchPlan and error
func createSearchPlan(col *Collection, dsl string) (*SearchPlan, error) {
	if col.collectionPtr == nil {
		return nil, errors.New("nil collection ptr, collectionID = " + fmt.Sprintln(col.id))
	}

	cDsl := C.CString(dsl)
	defer C.free(unsafe.Pointer(cDsl))
	var cPlan C.CSearchPlan
	status := C.CreateSearchPlan(col.collectionPtr, cDsl, &cPlan)

	err1 := HandleCStatus(&status, "Create Plan failed")
	if err1 != nil {
		return nil, err1
	}

	var newPlan = &SearchPlan{cSearchPlan: cPlan}
	return newPlan, nil
}

func createSearchPlanByExpr(col *Collection, expr []byte) (*SearchPlan, error) {
	if col.collectionPtr == nil {
		return nil, errors.New("nil collection ptr, collectionID = " + fmt.Sprintln(col.id))
	}
	var cPlan C.CSearchPlan
	status := C.CreateSearchPlanByExpr(col.collectionPtr, unsafe.Pointer(&expr[0]), (C.int64_t)(len(expr)), &cPlan)

	err1 := HandleCStatus(&status, "Create Plan by expr failed")
	if err1 != nil {
		return nil, err1
	}

	var newPlan = &SearchPlan{cSearchPlan: cPlan}
	return newPlan, nil
}

func (plan *SearchPlan) getTopK() int64 {
	var topK C.int64_t
	status := C.GetTopK(plan.cSearchPlan, &topK)
	if err := HandleCStatus(&status, "getTopK failed"); err != nil {
		log.Error(err.Error())
		return 0
	}
	return int64(topK)
}

func (plan *SearchPlan) delete() {
	C.DeleteSearchPlan(plan.cSearchPlan)
}

type searchRequest struct {
	plan              *SearchPlan
	cPlaceholderGroup C.CPlaceholderGroup
	timestamp         Timestamp
}

func newSearchRequest(collection *Collection, req *querypb.SearchRequest, placeholderGrp []byte) (*searchRequest, error) {
	var err error
	var plan *SearchPlan
	if req.Req.GetDslType() == commonpb.DslType_BoolExprV1 {
		expr := req.Req.SerializedExprPlan
		plan, err = createSearchPlanByExpr(collection, expr)
		if err != nil {
			return nil, err
		}
	} else {
		dsl := req.Req.GetDsl()
		plan, err = createSearchPlan(collection, dsl)
		if err != nil {
			return nil, err
		}
	}

	if len(placeholderGrp) == 0 {
		plan.delete()
		return nil, errors.New("empty search request")
	}

	var blobPtr = unsafe.Pointer(&placeholderGrp[0])
	blobSize := C.int64_t(len(placeholderGrp))
	var cPlaceholderGroup C.CPlaceholderGroup
	status := C.ParsePlaceholderGroup(plan.cSearchPlan, blobPtr, blobSize, &cPlaceholderGroup)

	if err := HandleCStatus(&status, "parser searchRequest failed"); err != nil {
		plan.delete()
		return nil, err
	}

	ret := &searchRequest{
		plan:              plan,
		cPlaceholderGroup: cPlaceholderGroup,
		timestamp:         req.Req.GetTravelTimestamp(),
	}

	return ret, nil
}

func (sr *searchRequest) getNumOfQuery() int64 {
	var numQueries C.int64_t
	status := C.GetNumOfQueries(sr.cPlaceholderGroup, &numQueries)
	if err := HandleCStatus(&status, "getNumOfQuery failed"); err != nil {
		log.Error(err.Error())
		return 0
	}
	return int64(numQueries)
}

func (sr *searchRequest) delete() {
	if sr.plan != nil {
		sr.plan.delete()
	}
	C.DeletePlaceholderGroup(sr.cPlaceholderGroup)
}

func parseSearchRequest(plan *SearchPlan, searchRequestBlob []byte) (*searchRequest, error) {
	if len(searchRequestBlob) == 0 {
		return nil, fmt.Errorf("empty search request")
	}
	var blobPtr = unsafe.Pointer(&searchRequestBlob[0])
	blobSize := C.int64_t(len(searchRequestBlob))
	var cPlaceholderGroup C.CPlaceholderGroup
	status := C.ParsePlaceholderGroup(plan.cSearchPlan, blobPtr, blobSize, &cPlaceholderGroup)

	if err := HandleCStatus(&status, "parser searchRequest failed"); err != nil {
		return nil, err
	}

	var ret = &searchRequest{cPlaceholderGroup: cPlaceholderGroup, plan: plan}
	return ret, nil
}

// RetrievePlan is a wrapper of the underlying C-structure C.CRetrievePlan
type RetrievePlan struct {
	cRetrievePlan C.CRetrievePlan
	Timestamp     Timestamp
}

func createRetrievePlanByExpr(col *Collection, expr []byte, timestamp Timestamp) (*RetrievePlan, error) {
	var cPlan C.CRetrievePlan
	status := C.CreateRetrievePlanByExpr(col.collectionPtr, unsafe.Pointer(&expr[0]), (C.int64_t)(len(expr)), &cPlan)

	err := HandleCStatus(&status, "Create retrieve plan by expr failed")
	if err != nil {
		return nil, err
	}

	var newPlan = &RetrievePlan{
		cRetrievePlan: cPlan,
		Timestamp:     timestamp,
	}
	return newPlan, nil
}

func (plan *RetrievePlan) delete() {
	C.DeleteRetrievePlan(plan.cRetrievePlan)
}
