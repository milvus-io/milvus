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

package indexnode

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_indexbuilder -Wl,-rpath=${SRCDIR}/../core/output/lib

#include <stdlib.h>	// free
#include "segcore/collection_c.h"
#include "indexbuilder/index_c.h"

*/
import "C"

import (
	"fmt"
	"unsafe"

	"go.uber.org/zap"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexcgopb"
	"github.com/milvus-io/milvus/internal/storage"
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
	var cBinary C.CBinary

	status := C.SerializeToSlicedBuffer(index.indexPtr, &cBinary)
	defer func() {
		if cBinary != nil {
			C.DeleteCBinary(cBinary)
		}
	}()
	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return nil, fmt.Errorf("SerializeToSlicedBuffer failed, C runtime error detected, error code = %d, err msg = %s", errorCode, errorMsg)
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
	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return fmt.Errorf("BuildFloatVecIndexWithoutIds failed, C runtime error detected, error code = %d, err msg = %s", errorCode, errorMsg)
	}
	return nil
}

func (index *CIndex) BuildFloatVecIndexWithoutIds(vectors []float32) error {
	/*
		CStatus
		BuildFloatVecIndexWithoutIds(CIndex index, int64_t float_value_num, const float* vectors);
	*/
	log.Debug("before BuildFloatVecIndexWithoutIds")
	status := C.BuildFloatVecIndexWithoutIds(index.indexPtr, (C.int64_t)(len(vectors)), (*C.float)(&vectors[0]))
	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		log.Debug("indexnode", zap.String("BuildFloatVecIndexWithoutIds error msg: ", errorMsg))
		defer C.free(unsafe.Pointer(status.error_msg))
		return fmt.Errorf("BuildFloatVecIndexWithoutIds failed, C runtime error detected, error code = %d, err msg = %s", errorCode, errorMsg)
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
		return fmt.Errorf("BuildBinaryVecIndexWithoutIds failed, C runtime error detected, error code = %d, err msg = %s", errorCode, errorMsg)
	}
	return nil
}

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
	log.Debug("before create index ...")
	status := C.CreateIndex(typeParamsPointer, indexParamsPointer, &indexPtr)
	log.Debug("after create index ...")
	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		log.Debug("indexnode", zap.String("create index error msg", errorMsg))
		defer C.free(unsafe.Pointer(status.error_msg))
		return nil, fmt.Errorf(" failed, C runtime error detected, error code = %d, err msg = %s", errorCode, errorMsg)
	}

	return &CIndex{
		indexPtr: indexPtr,
	}, nil
}
