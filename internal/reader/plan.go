package reader

/*
#cgo CFLAGS: -I${SRCDIR}/../core/output/include
#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib
#include "collection_c.h"
#include "segment_c.h"
#include "plan_c.h"
*/
import "C"
import (
	"unsafe"
)

type Plan struct {
	cPlan C.CPlan
}

func CreatePlan(col Collection, dsl string) *Plan {
	cDsl := C.CString(dsl)
	cPlan := C.CreatePlan(col.collectionPtr, cDsl)
	var newPlan = &Plan{cPlan: cPlan}
	return newPlan
}

func (plan *Plan) GetTopK() int64 {
	topK := C.GetTopK(plan.cPlan)
	return int64(topK)
}

func (plan *Plan) DeletePlan() {
	C.DeletePlan(plan.cPlan)
}

type PlaceholderGroup struct {
	cPlaceholderGroup C.CPlaceholderGroup
}

func ParserPlaceholderGroup(plan *Plan, placeHolderBlob []byte) *PlaceholderGroup {
	var blobPtr = unsafe.Pointer(&placeHolderBlob[0])
	blobSize := C.long(len(placeHolderBlob))
	cPlaceholderGroup := C.ParsePlaceholderGroup(plan.cPlan, blobPtr, blobSize)
	var newPlaceholderGroup = &PlaceholderGroup{cPlaceholderGroup: cPlaceholderGroup}
	return newPlaceholderGroup
}

func (pg *PlaceholderGroup) GetNumOfQuery() int64 {
	numQueries := C.GetNumOfQueries(pg.cPlaceholderGroup)
	return int64(numQueries)
}

func (pg *PlaceholderGroup) DeletePlaceholderGroup() {
	C.DeletePlaceholderGroup(pg.cPlaceholderGroup)
}
