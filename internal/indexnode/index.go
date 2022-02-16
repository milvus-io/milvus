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

package indexnode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo darwin LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_common -lmilvus_indexbuilder -Wl,-rpath,"${SRCDIR}/../core/output/lib"
#cgo linux LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_common -lmilvus_indexbuilder -Wl,-rpath=${SRCDIR}/../core/output/lib

#include <stdlib.h>	// free
#include "segcore/collection_c.h"
#include "indexbuilder/index_c.h"
#include "common/vector_index_c.h"

*/
import "C"

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"unsafe"

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexcgopb"
	"github.com/milvus-io/milvus/internal/storage"
)

// Blob is an alias for the storage.Blob type
type Blob = storage.Blob

// Index is an interface used to call the interface to build the index task in 'C'.
type Index interface {
	Serialize() ([]*Blob, error)
	Load([]*Blob) error
	BuildFloatVecIndexWithoutIds(vectors []float32) error
	BuildBinaryVecIndexWithoutIds(vectors []byte) error
	Delete() error
}

// CIndex is a pointer used to access 'CGO'.
type CIndex struct {
	indexPtr C.CIndex
	close    bool
}

func GetBinarySetKeys(cBinarySet C.CBinarySet) ([]string, error) {
	size := int(C.GetBinarySetSize(cBinarySet))
	if size == 0 {
		return nil, fmt.Errorf("BinarySet size is zero!")
	}
	datas := make([]unsafe.Pointer, size)

	C.GetBinarySetKeys(cBinarySet, unsafe.Pointer(&datas[0]))
	ret := make([]string, size)
	for i := 0; i < size; i++ {
		ret[i] = C.GoString((*C.char)(datas[i]))
	}

	return ret, nil
}

func GetBinarySetValue(cBinarySet C.CBinarySet, key string) ([]byte, error) {
	cIndexKey := C.CString(key)
	defer C.free(unsafe.Pointer(cIndexKey))
	ret := C.GetBinarySetValueSize(cBinarySet, cIndexKey)
	size := int(ret)
	if size == 0 {
		return nil, fmt.Errorf("GetBinarySetValueSize size is zero!")
	}
	value := make([]byte, size)
	status := C.CopyBinarySetValue(unsafe.Pointer(&value[0]), cIndexKey, cBinarySet)

	if err := HandleCStatus(&status, "CopyBinarySetValue failed"); err != nil {
		return nil, err
	}

	return value, nil
}

func (index *CIndex) Serialize() ([]*Blob, error) {
	var cBinarySet C.CBinarySet

	status := C.SerializeToBinarySet(index.indexPtr, &cBinarySet)
	defer func() {
		if cBinarySet != nil {
			C.DeleteBinarySet(cBinarySet)
		}
	}()
	if err := HandleCStatus(&status, "SerializeToBinarySet failed"); err != nil {
		return nil, err
	}

	keys, err := GetBinarySetKeys(cBinarySet)
	if err != nil {
		return nil, err
	}
	ret := make([]*Blob, 0)
	for _, key := range keys {
		value, err := GetBinarySetValue(cBinarySet, key)
		if err != nil {
			return nil, err
		}
		blob := &Blob{
			Key:   key,
			Value: value,
		}
		ret = append(ret, blob)
	}

	return ret, nil
}

// Serialize serializes vector data into bytes data so that it can be accessed in 'C'.
/*
func (index *CIndex) Serialize() ([]*Blob, error) {
	var cBinary C.CBinary

	status := C.SerializeToSlicedBuffer(index.indexPtr, &cBinary)
	defer func() {
		if cBinary != nil {
			C.DeleteCBinary(cBinary)
		}
	}()
	if err := HandleCStatus(&status, "SerializeToSlicedBuffer failed"); err != nil {
		return nil, err
	}

	binarySize := C.GetCBinarySize(cBinary)
	binaryData := make([]byte, binarySize)
	C.GetCBinaryData(cBinary, unsafe.Pointer(&binaryData[0]))

	var blobs indexcgopb.BinarySet
	err := proto.Unmarshal(binaryData, &blobs)
	if err != nil {
		return nil, err
	}

	ret := make([]*Blob, 0)
	for _, data := range blobs.Datas {
		ret = append(ret, &Blob{Key: data.Key, Value: data.Value})
	}

	return ret, nil
}
*/

func (index *CIndex) Load(blobs []*Blob) error {
	var cBinarySet C.CBinarySet
	status := C.NewBinarySet(&cBinarySet)
	defer C.DeleteBinarySet(cBinarySet)

	if err := HandleCStatus(&status, "CIndex Load2 NewBinarySet failed"); err != nil {
		return err
	}
	for _, blob := range blobs {
		key := blob.Key
		byteIndex := blob.Value
		indexPtr := unsafe.Pointer(&byteIndex[0])
		indexLen := C.int64_t(len(byteIndex))
		binarySetKey := filepath.Base(key)
		log.Debug("", zap.String("index key", binarySetKey))
		indexKey := C.CString(binarySetKey)
		status = C.AppendIndexBinary(cBinarySet, indexPtr, indexLen, indexKey)
		C.free(unsafe.Pointer(indexKey))
		if err := HandleCStatus(&status, "CIndex Load AppendIndexBinary failed"); err != nil {
			return err
		}
	}
	status = C.LoadFromBinarySet(index.indexPtr, cBinarySet)
	return HandleCStatus(&status, "AppendIndex failed")
}

// Load loads data from 'C'.
/*
func (index *CIndex) Load(blobs []*Blob) error {
	binarySet := &indexcgopb.BinarySet{Datas: make([]*indexcgopb.Binary, 0)}
	for _, blob := range blobs {
		binarySet.Datas = append(binarySet.Datas, &indexcgopb.Binary{Key: blob.Key, Value: blob.Value})
	}

	datas, err2 := proto.Marshal(binarySet)
	if err2 != nil {
		return err2
	}

	status := C.LoadFromSlicedBuffer(index.indexPtr, (*C.char)(unsafe.Pointer(&datas[0])), (C.int32_t)(len(datas)))
	return HandleCStatus(&status, "LoadFromSlicedBuffer failed")
}
*/

// BuildFloatVecIndexWithoutIds builds indexes for float vector.
func (index *CIndex) BuildFloatVecIndexWithoutIds(vectors []float32) error {
	/*
		CStatus
		BuildFloatVecIndexWithoutIds(CIndex index, int64_t float_value_num, const float* vectors);
	*/
	log.Debug("before BuildFloatVecIndexWithoutIds")
	status := C.BuildFloatVecIndexWithoutIds(index.indexPtr, (C.int64_t)(len(vectors)), (*C.float)(&vectors[0]))
	return HandleCStatus(&status, "BuildFloatVecIndexWithoutIds failed")
}

// BuildBinaryVecIndexWithoutIds builds indexes for binary vector.
func (index *CIndex) BuildBinaryVecIndexWithoutIds(vectors []byte) error {
	/*
		CStatus
		BuildBinaryVecIndexWithoutIds(CIndex index, int64_t data_size, const uint8_t* vectors);
	*/
	status := C.BuildBinaryVecIndexWithoutIds(index.indexPtr, (C.int64_t)(len(vectors)), (*C.uint8_t)(&vectors[0]))
	return HandleCStatus(&status, "BuildBinaryVecIndexWithoutIds failed")
}

// Delete removes the pointer to build the index in 'C'. we can ensure that it is idempotent.
func (index *CIndex) Delete() error {
	/*
		void
		DeleteIndex(CIndex index);
	*/
	if index.close {
		return nil
	}
	C.DeleteIndex(index.indexPtr)
	index.close = true
	return nil
}

// NewCIndex creates a new pointer to build the index in 'C'.
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
	defer C.free(unsafe.Pointer(typeParamsPointer))
	defer C.free(unsafe.Pointer(indexParamsPointer))

	/*
		CStatus
		CreateIndex(const char* serialized_type_params,
					const char* serialized_index_params,
					CIndex* res_index);
	*/
	var indexPtr C.CIndex
	log.Debug("Start to create index ...", zap.String("params", indexParamsStr))
	status := C.CreateIndex(typeParamsPointer, indexParamsPointer, &indexPtr)
	if err := HandleCStatus(&status, "CreateIndex failed"); err != nil {
		return nil, err
	}
	log.Debug("Successfully create index ...")

	index := &CIndex{
		indexPtr: indexPtr,
		close:    false,
	}
	runtime.SetFinalizer(index, func(index *CIndex) {
		if index != nil && !index.close {
			log.Error("there is leakage in index object, please check.")
		}
	})
	return index, nil
}

// HandleCStatus deal with the error returned from CGO
func HandleCStatus(status *C.CStatus, extraInfo string) error {
	if status.error_code == 0 {
		return nil
	}
	errorCode := status.error_code
	errorName, ok := commonpb.ErrorCode_name[int32(errorCode)]
	if !ok {
		errorName = "UnknownError"
	}
	errorMsg := C.GoString(status.error_msg)
	defer C.free(unsafe.Pointer(status.error_msg))

	finalMsg := fmt.Sprintf("[%s] %s", errorName, errorMsg)
	logMsg := fmt.Sprintf("%s, C Runtime Exception: %s\n", extraInfo, finalMsg)
	log.Warn(logMsg)
	return errors.New(finalMsg)
}
