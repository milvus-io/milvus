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
	"errors"
	"strconv"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexcgopb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
)

// TODO: use storage.Blob instead later
type Blob = storage.Blob

type Index interface {
	Serialize() ([]*Blob, error)
	Load([]*Blob) error
	BuildFloatVecIndexWithoutIds(vectors []float32) error
	BuildBinaryVecIndexWithoutIds(vectors []byte) error
	Delete() error
}

type CIndex struct {
	indexPtr C.CIndex
}

func (index *CIndex) Serialize() ([]*Blob, error) {
	/*
		CStatus
		SerializeToSlicedBuffer(CIndex index, int32_t* buffer_size, char** res_buffer);
	*/

	var cDumpedSlicedBuffer *C.char
	var bufferSize int32
	status := C.SerializeToSlicedBuffer(index.indexPtr, (*C.int32_t)(unsafe.Pointer(&bufferSize)), &cDumpedSlicedBuffer)
	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return nil, errors.New("SerializeToSlicedBuffer failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}

	defer C.free(unsafe.Pointer(cDumpedSlicedBuffer))

	dumpedSlicedBuffer := C.GoBytes(unsafe.Pointer(cDumpedSlicedBuffer), (C.int32_t)(bufferSize))
	var blobs indexcgopb.BinarySet
	err := proto.Unmarshal(dumpedSlicedBuffer, &blobs)
	if err != nil {
		return nil, err
	}

	ret := make([]*Blob, 0)
	for _, data := range blobs.Datas {
		ret = append(ret, &Blob{Key: data.Key, Value: data.Value})
	}

	return ret, nil
}

func (index *CIndex) Load(blobs []*Blob) error {
	binarySet := &indexcgopb.BinarySet{Datas: make([]*indexcgopb.Binary, 0)}
	for _, blob := range blobs {
		binarySet.Datas = append(binarySet.Datas, &indexcgopb.Binary{Key: blob.Key, Value: blob.Value})
	}

	datas, err := proto.Marshal(binarySet)
	if err != nil {
		return err
	}

	/*
		CStatus
		LoadFromSlicedBuffer(CIndex index, const char* serialized_sliced_blob_buffer, int32_t size);
	*/
	status := C.LoadFromSlicedBuffer(index.indexPtr, (*C.char)(unsafe.Pointer(&datas[0])), (C.int32_t)(len(datas)))
	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("BuildFloatVecIndexWithoutIds failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}
	return nil
}

func (index *CIndex) BuildFloatVecIndexWithoutIds(vectors []float32) error {
	/*
		CStatus
		BuildFloatVecIndexWithoutIds(CIndex index, int64_t float_value_num, const float* vectors);
	*/
	status := C.BuildFloatVecIndexWithoutIds(index.indexPtr, (C.int64_t)(len(vectors)), (*C.float)(&vectors[0]))
	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("BuildFloatVecIndexWithoutIds failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}
	return nil
}

func (index *CIndex) BuildBinaryVecIndexWithoutIds(vectors []byte) error {
	/*
		CStatus
		BuildBinaryVecIndexWithoutIds(CIndex index, int64_t data_size, const uint8_t* vectors);
	*/
	status := C.BuildBinaryVecIndexWithoutIds(index.indexPtr, (C.int64_t)(len(vectors)), (*C.uint8_t)(&vectors[0]))
	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New(" failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}
	return nil
}

func (index *CIndex) Delete() error {
	/*
		void
		DeleteIndex(CIndex index);
	*/
	C.DeleteIndex(index.indexPtr)
	return nil
}

func NewCIndex(typeParams, indexParams map[string]string) (Index, error) {
	protoTypeParams := &indexcgopb.TypeParams{
		Params: make([]*commonpb.KeyValuePair, 0),
	}
	for key, value := range typeParams {
		protoTypeParams.Params = append(protoTypeParams.Params, &commonpb.KeyValuePair{Key: key, Value: value})
	}
	typeParamsStr := proto.MarshalTextString(protoTypeParams)

	protoIndexParams := &indexcgopb.IndexParams{
		Params: make([]*commonpb.KeyValuePair, 0),
	}
	for key, value := range indexParams {
		protoIndexParams.Params = append(protoIndexParams.Params, &commonpb.KeyValuePair{Key: key, Value: value})
	}
	indexParamsStr := proto.MarshalTextString(protoIndexParams)

	typeParamsPointer := C.CString(typeParamsStr)
	indexParamsPointer := C.CString(indexParamsStr)

	/*
		CStatus
		CreateIndex(const char* serialized_type_params,
					const char* serialized_index_params,
					CIndex* res_index);
	*/
	var indexPtr C.CIndex
	status := C.CreateIndex(typeParamsPointer, indexParamsPointer, &indexPtr)
	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return nil, errors.New(" failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}

	return &CIndex{
		indexPtr: indexPtr,
	}, nil
}
