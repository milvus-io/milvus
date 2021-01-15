package indexbuilder

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_indexbuilder -Wl,-rpath=${SRCDIR}/../core/output/lib

#include <stdlib.h>	// free
#include "segcore/collection_c.h"
#include "indexbuilder/index_c.h"

*/
import "C"
import (
	"github.com/zilliztech/milvus-distributed/internal/errors"
	"strconv"
	"unsafe"
)

type QueryResult interface {
	Delete() error
	NQ() int64
	TOPK() int64
	IDs() []int64
	Distances() []float32
}

type CQueryResult struct {
	ptr C.CIndexQueryResult
}

type CFunc func() C.CStatus

func TryCatch(fn CFunc) error {
	status := fn()
	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}
	return nil
}

func CreateQueryResult() (QueryResult, error) {
	var ptr C.CIndexQueryResult
	fn := func() C.CStatus {
		return C.CreateQueryResult(&ptr)
	}
	err := TryCatch(fn)
	if err != nil {
		return nil, err
	}
	return &CQueryResult{
		ptr: ptr,
	}, nil
}

func (qs *CQueryResult) Delete() error {
	fn := func() C.CStatus {
		return C.DeleteQueryResult(qs.ptr)
	}
	return TryCatch(fn)
}

func (qs *CQueryResult) NQ() int64 {
	return int64(C.NqOfQueryResult(qs.ptr))
}

func (qs *CQueryResult) TOPK() int64 {
	return int64(C.TopkOfQueryResult(qs.ptr))
}

func (qs *CQueryResult) IDs() []int64 {
	nq := qs.NQ()
	topk := qs.TOPK()

	if nq <= 0 || topk <= 0 {
		return []int64{}
	}

	// TODO: how could we avoid memory copy every time when this called
	ids := make([]int64, nq*topk)
	C.GetIdsOfQueryResult(qs.ptr, (*C.int64_t)(&ids[0]))

	return ids
}

func (qs *CQueryResult) Distances() []float32 {
	nq := qs.NQ()
	topk := qs.TOPK()

	if nq <= 0 || topk <= 0 {
		return []float32{}
	}

	// TODO: how could we avoid memory copy every time when this called
	distances := make([]float32, nq*topk)
	C.GetDistancesOfQueryResult(qs.ptr, (*C.float)(&distances[0]))

	return distances
}
