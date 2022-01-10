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

#cgo darwin LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_indexbuilder -Wl,-rpath,"${SRCDIR}/../core/output/lib"
#cgo linux LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_indexbuilder -Wl,-rpath=${SRCDIR}/../core/output/lib

#include <stdlib.h>	// free
#include "segcore/collection_c.h"
#include "indexbuilder/index_c.h"

*/
import "C"

import (
	"errors"
	"fmt"
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
}

// Serialize serializes vector data into bytes data so that it can be accessed in 'C'.
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

// Load loads data from 'C'.
func (index *CIndex) Load(blobs []*Blob) error {
	binarySet := &indexcgopb.BinarySet{Datas: make([]*indexcgopb.Binary, 0)}
	for _, blob := range blobs {
		binarySet.Datas = append(binarySet.Datas, &indexcgopb.Binary{Key: blob.Key, Value: blob.Value})
	}

	datas, err2 := proto.Marshal(binarySet)
	if err2 != nil {
		return err2
	}

	/*
		CStatus
		LoadFromSlicedBuffer(CIndex index, const char* serialized_sliced_blob_buffer, int32_t size);
	*/
	status := C.LoadFromSlicedBuffer(index.indexPtr, (*C.char)(unsafe.Pointer(&datas[0])), (C.int32_t)(len(datas)))
	return HandleCStatus(&status, "LoadFromSlicedBuffer failed")
}

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

// Delete removes the pointer to build the index in 'C'.
func (index *CIndex) Delete() error {
	/*
		void
		DeleteIndex(CIndex index);
	*/
	C.DeleteIndex(index.indexPtr)
	// TODO: check if index.indexPtr will be released by golang, though it occupies little memory
	// C.free(index.indexPtr)
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

	return &CIndex{
		indexPtr: indexPtr,
	}, nil
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
