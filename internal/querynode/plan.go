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
	"unsafe"

	"errors"
)

type Plan struct {
	cPlan C.CPlan
}

func createPlan(col Collection, dsl string) (*Plan, error) {
	cDsl := C.CString(dsl)
	defer C.free(unsafe.Pointer(cDsl))
	var cPlan C.CPlan
	status := C.CreatePlan(col.collectionPtr, cDsl, &cPlan)

	if err := HandleCStatus(&status, "Create Plan failed"); err != nil {
		return nil, err
	}

	var newPlan = &Plan{cPlan: cPlan}
	return newPlan, nil
}

func (plan *Plan) getTopK() int64 {
	topK := C.GetTopK(plan.cPlan)
	return int64(topK)
}

func (plan *Plan) getMetricType() string {
	cMetricType := C.GetMetricType(plan.cPlan)
	defer C.free(unsafe.Pointer(cMetricType))
	metricType := C.GoString(cMetricType)
	return metricType
}

func (plan *Plan) delete() {
	C.DeletePlan(plan.cPlan)
}

type searchRequest struct {
	cPlaceholderGroup C.CPlaceholderGroup
}

func parseSearchRequest(plan *Plan, searchRequestBlob []byte) (*searchRequest, error) {
	if len(searchRequestBlob) == 0 {
		return nil, errors.New("empty search request")
	}
	var blobPtr = unsafe.Pointer(&searchRequestBlob[0])
	blobSize := C.long(len(searchRequestBlob))
	var cPlaceholderGroup C.CPlaceholderGroup
	status := C.ParsePlaceholderGroup(plan.cPlan, blobPtr, blobSize, &cPlaceholderGroup)

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
