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

package querynode

/*
#cgo CFLAGS: -I${SRCDIR}/../core/output/include
#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
#include "segcore/plan_c.h"
*/
import "C"
import (
	"errors"
	"fmt"
	"unsafe"
)

// SearchPlan is a wrapper of the underlying C-structure C.CSearchPlan
type SearchPlan struct {
	cSearchPlan C.CSearchPlan
}

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
	status := C.CreateSearchPlanByExpr(col.collectionPtr, (*C.char)(unsafe.Pointer(&expr[0])), (C.int64_t)(len(expr)), &cPlan)

	err1 := HandleCStatus(&status, "Create Plan by expr failed")
	if err1 != nil {
		return nil, err1
	}

	var newPlan = &SearchPlan{cSearchPlan: cPlan}
	return newPlan, nil
}

func (plan *SearchPlan) getTopK() int64 {
	topK := C.GetTopK(plan.cSearchPlan)
	return int64(topK)
}

func (plan *SearchPlan) getMetricType() string {
	cMetricType := C.GetMetricType(plan.cSearchPlan)
	defer C.free(unsafe.Pointer(cMetricType))
	metricType := C.GoString(cMetricType)
	return metricType
}

func (plan *SearchPlan) delete() {
	C.DeleteSearchPlan(plan.cSearchPlan)
}

type searchRequest struct {
	cPlaceholderGroup C.CPlaceholderGroup
}

func parseSearchRequest(plan *SearchPlan, searchRequestBlob []byte) (*searchRequest, error) {
	if len(searchRequestBlob) == 0 {
		return nil, errors.New("empty search request")
	}
	var blobPtr = unsafe.Pointer(&searchRequestBlob[0])
	blobSize := C.long(len(searchRequestBlob))
	var cPlaceholderGroup C.CPlaceholderGroup
	status := C.ParsePlaceholderGroup(plan.cSearchPlan, blobPtr, blobSize, &cPlaceholderGroup)

	if err := HandleCStatus(&status, "parser searchRequest failed"); err != nil {
		return nil, err
	}

	var newSearchRequest = &searchRequest{cPlaceholderGroup: cPlaceholderGroup}
	return newSearchRequest, nil
}

func (pg *searchRequest) getNumOfQuery() int64 {
	numQueries := C.GetNumOfQueries(pg.cPlaceholderGroup)
	return int64(numQueries)
}

func (pg *searchRequest) delete() {
	C.DeletePlaceholderGroup(pg.cPlaceholderGroup)
}

// RetrievePlan is a wrapper of the underlying C-structure C.CRetrievePlan
type RetrievePlan struct {
	cRetrievePlan C.CRetrievePlan
	Timestamp     Timestamp
}

// func createRetrievePlan(col *Collection, msg *segcorepb.RetrieveRequest, timestamp uint64) (*RetrievePlan, error) {
// 	protoCGo, err := MarshalForCGo(msg)
// 	if err != nil {
// 		return nil, err
// 	}
// 	plan := new(RetrievePlan)
// 	plan.Timestamp = timestamp
// 	status := C.CreateRetrievePlan(col.collectionPtr, protoCGo.CProto, &plan.cRetrievePlan)
// 	err2 := HandleCStatus(&status, "create retrieve plan failed")
// 	if err2 != nil {
// 		return nil, err2
// 	}
// 	return plan, nil
// }

func createRetrievePlanByExpr(col *Collection, expr []byte, timestamp Timestamp) (*RetrievePlan, error) {
	var cPlan C.CRetrievePlan
	status := C.CreateRetrievePlanByExpr(col.collectionPtr, (*C.char)(unsafe.Pointer(&expr[0])),
		(C.int64_t)(len(expr)), &cPlan)

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
