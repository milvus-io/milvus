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

type PlaceholderGroup struct {
	cPlaceholderGroup C.CPlaceholderGroup
}

func parserPlaceholderGroup(plan *Plan, placeHolderBlob []byte) (*PlaceholderGroup, error) {
	if len(placeHolderBlob) == 0 {
		return nil, errors.New("empty search request")
	}
	var blobPtr = unsafe.Pointer(&placeHolderBlob[0])
	blobSize := C.long(len(placeHolderBlob))
	var cPlaceholderGroup C.CPlaceholderGroup
	status := C.ParsePlaceholderGroup(plan.cPlan, blobPtr, blobSize, &cPlaceholderGroup)

	if err := HandleCStatus(&status, "parser placeholder group failed"); err != nil {
		return nil, err
	}

	var newPlaceholderGroup = &PlaceholderGroup{cPlaceholderGroup: cPlaceholderGroup}
	return newPlaceholderGroup, nil
}

func (pg *PlaceholderGroup) getNumOfQuery() int64 {
	numQueries := C.GetNumOfQueries(pg.cPlaceholderGroup)
	return int64(numQueries)
}

func (pg *PlaceholderGroup) delete() {
	C.DeletePlaceholderGroup(pg.cPlaceholderGroup)
}
