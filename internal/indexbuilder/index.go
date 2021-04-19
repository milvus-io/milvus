package indexbuilder

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_indexbuilder -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/collection_c.h"
#include "indexbuilder/index_c.h"

*/
import "C"
import (
	"encoding/json"

	"github.com/zilliztech/milvus-distributed/internal/errors"
)

// TODO: use storage.Blob instead later
// type Blob = storage.Blob
type Blob struct {
	Key   string
	Value []byte
}

type Index interface {
	Serialize() ([]*Blob, error)
	Load([]*Blob) error
	BuildFloatVecIndex(vectors []float32) error
	Delete() error
}

type CIndex struct {
	indexPtr C.CIndex
}

func (index *CIndex) Serialize() ([]*Blob, error) {
	var cDumpedSlicedBuffer *C.char = C.SerializeToSlicedBuffer(index.indexPtr)
	var dumpedSlicedBuffer string = C.GoString(cDumpedSlicedBuffer)

	var data map[string]interface{}
	err := json.Unmarshal([]byte(dumpedSlicedBuffer), &data)
	if err != nil {
		return nil, errors.New("unmarshal sliced buffer failed")
	}

	ret := make([]*Blob, 0)
	for key, value := range data {
		valueString, ok := value.(string)
		if !ok {
			return nil, errors.New("unexpected data type of dumped sliced buffer")
		}
		ret = append(ret, &Blob{key, []byte(valueString)})
	}

	return ret, nil
}

func (index *CIndex) Load([]*Blob) error {
	return nil
}

func (index *CIndex) BuildFloatVecIndex(vectors []float32) error {
	return nil
}

func (index *CIndex) Delete() error {
	C.DeleteIndex(index.indexPtr)
	return nil
}

func NewCIndex(typeParams, indexParams map[string]string) (Index, error) {
	dumpedTypeParamsStr, err := json.Marshal(typeParams)
	if err != nil {
		return nil, err
	}

	dumpedIndexParamsStr, err := json.Marshal(indexParams)
	if err != nil {
		return nil, err
	}

	cDumpedTypeParamsStr := C.CString(string(dumpedTypeParamsStr))
	cDumpedIndexParamsStr := C.CString(string(dumpedIndexParamsStr))
	return &CIndex{
		indexPtr: C.CreateIndex(cDumpedTypeParamsStr, cDumpedIndexParamsStr),
	}, nil
}
