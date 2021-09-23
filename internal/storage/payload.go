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

package storage

/*
#cgo CFLAGS: -I${SRCDIR}/cwrapper

#cgo LDFLAGS: -L${SRCDIR}/cwrapper/output -lwrapper -L${SRCDIR}/../core/output/lib -lparquet -larrow -lstdc++ -lm -Wl,-rpath=${SRCDIR}/../core/output/lib
#include <stdlib.h>
#include "ParquetWrapper.h"
*/
import "C"
import (
	"errors"
	"unsafe"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

type PayloadWriterInterface interface {
	AddDataToPayload(msgs interface{}, dim ...int) error
	AddBoolToPayload(msgs []bool) error
	AddInt8ToPayload(msgs []int8) error
	AddInt16ToPayload(msgs []int16) error
	AddInt32ToPayload(msgs []int32) error
	AddInt64ToPayload(msgs []int64) error
	AddFloatToPayload(msgs []float32) error
	AddDoubleToPayload(msgs []float64) error
	AddOneStringToPayload(msgs string) error
	AddBinaryVectorToPayload(binVec []byte, dim int) error
	AddFloatVectorToPayload(binVec []float32, dim int) error
	FinishPayloadWriter() error
	GetPayloadBufferFromWriter() ([]byte, error)
	GetPayloadLengthFromWriter() (int, error)
	ReleasePayloadWriter() error
	Close() error
}

type PayloadReaderInterface interface {
	GetDataFromPayload(idx ...int) (interface{}, int, error)
	GetBoolFromPayload() ([]bool, error)
	GetInt8FromPayload() ([]int8, error)
	GetInt16FromPayload() ([]int16, error)
	GetInt32FromPayload() ([]int32, error)
	GetInt64FromPayload() ([]int64, error)
	GetFloatFromPayload() ([]float32, error)
	GetDoubleFromPayload() ([]float64, error)
	GetOneStringFromPayload(idx int) (string, error)
	GetBinaryVectorFromPayload() ([]byte, int, error)
	GetFloatVectorFromPayload() ([]float32, int, error)
	GetPayloadLengthFromReader() (int, error)
	ReleasePayloadReader() error
	Close() error
}

type PayloadWriter struct {
	payloadWriterPtr C.CPayloadWriter
	colType          schemapb.DataType
}

type PayloadReader struct {
	payloadReaderPtr C.CPayloadReader
	colType          schemapb.DataType
}

func NewPayloadWriter(colType schemapb.DataType) (*PayloadWriter, error) {
	w := C.NewPayloadWriter(C.int(colType))
	if w == nil {
		return nil, errors.New("create Payload writer failed")
	}
	return &PayloadWriter{payloadWriterPtr: w, colType: colType}, nil
}

func (w *PayloadWriter) AddDataToPayload(msgs interface{}, dim ...int) error {
	switch len(dim) {
	case 0:
		switch w.colType {
		case schemapb.DataType_Bool:
			val, ok := msgs.([]bool)
			if !ok {
				return errors.New("incorrect data type")
			}
			return w.AddBoolToPayload(val)
		case schemapb.DataType_Int8:
			val, ok := msgs.([]int8)
			if !ok {
				return errors.New("incorrect data type")
			}
			return w.AddInt8ToPayload(val)
		case schemapb.DataType_Int16:
			val, ok := msgs.([]int16)
			if !ok {
				return errors.New("incorrect data type")
			}
			return w.AddInt16ToPayload(val)
		case schemapb.DataType_Int32:
			val, ok := msgs.([]int32)
			if !ok {
				return errors.New("incorrect data type")
			}
			return w.AddInt32ToPayload(val)
		case schemapb.DataType_Int64:
			val, ok := msgs.([]int64)
			if !ok {
				return errors.New("incorrect data type")
			}
			return w.AddInt64ToPayload(val)
		case schemapb.DataType_Float:
			val, ok := msgs.([]float32)
			if !ok {
				return errors.New("incorrect data type")
			}
			return w.AddFloatToPayload(val)
		case schemapb.DataType_Double:
			val, ok := msgs.([]float64)
			if !ok {
				return errors.New("incorrect data type")
			}
			return w.AddDoubleToPayload(val)
		case schemapb.DataType_String:
			val, ok := msgs.(string)
			if !ok {
				return errors.New("incorrect data type")
			}
			return w.AddOneStringToPayload(val)
		default:
			return errors.New("incorrect datatype")
		}
	case 1:
		switch w.colType {
		case schemapb.DataType_BinaryVector:
			val, ok := msgs.([]byte)
			if !ok {
				return errors.New("incorrect data type")
			}
			return w.AddBinaryVectorToPayload(val, dim[0])
		case schemapb.DataType_FloatVector:
			val, ok := msgs.([]float32)
			if !ok {
				return errors.New("incorrect data type")
			}
			return w.AddFloatVectorToPayload(val, dim[0])
		default:
			return errors.New("incorrect datatype")
		}
	default:
		return errors.New("incorrect input numbers")
	}
}

func (w *PayloadWriter) AddBoolToPayload(msgs []bool) error {
	length := len(msgs)
	if length <= 0 {
		return errors.New("can't add empty msgs into payload")
	}

	cMsgs := (*C.bool)(unsafe.Pointer(&msgs[0]))
	cLength := C.int(length)

	status := C.AddBooleanToPayload(w.payloadWriterPtr, cMsgs, cLength)

	errCode := commonpb.ErrorCode(status.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New(msg)
	}
	return nil
}

func (w *PayloadWriter) AddInt8ToPayload(msgs []int8) error {
	length := len(msgs)
	if length <= 0 {
		return errors.New("can't add empty msgs into payload")
	}
	cMsgs := (*C.int8_t)(unsafe.Pointer(&msgs[0]))
	cLength := C.int(length)

	status := C.AddInt8ToPayload(w.payloadWriterPtr, cMsgs, cLength)

	errCode := commonpb.ErrorCode(status.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New(msg)
	}
	return nil
}

func (w *PayloadWriter) AddInt16ToPayload(msgs []int16) error {
	length := len(msgs)
	if length <= 0 {
		return errors.New("can't add empty msgs into payload")
	}

	cMsgs := (*C.int16_t)(unsafe.Pointer(&msgs[0]))
	cLength := C.int(length)

	status := C.AddInt16ToPayload(w.payloadWriterPtr, cMsgs, cLength)

	errCode := commonpb.ErrorCode(status.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New(msg)
	}
	return nil
}

func (w *PayloadWriter) AddInt32ToPayload(msgs []int32) error {
	length := len(msgs)
	if length <= 0 {
		return errors.New("can't add empty msgs into payload")
	}

	cMsgs := (*C.int32_t)(unsafe.Pointer(&msgs[0]))
	cLength := C.int(length)

	status := C.AddInt32ToPayload(w.payloadWriterPtr, cMsgs, cLength)

	errCode := commonpb.ErrorCode(status.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New(msg)
	}
	return nil
}

func (w *PayloadWriter) AddInt64ToPayload(msgs []int64) error {
	length := len(msgs)
	if length <= 0 {
		return errors.New("can't add empty msgs into payload")
	}

	cMsgs := (*C.int64_t)(unsafe.Pointer(&msgs[0]))
	cLength := C.int(length)

	status := C.AddInt64ToPayload(w.payloadWriterPtr, cMsgs, cLength)

	errCode := commonpb.ErrorCode(status.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New(msg)
	}
	return nil
}

func (w *PayloadWriter) AddFloatToPayload(msgs []float32) error {
	length := len(msgs)
	if length <= 0 {
		return errors.New("can't add empty msgs into payload")
	}

	cMsgs := (*C.float)(unsafe.Pointer(&msgs[0]))
	cLength := C.int(length)

	status := C.AddFloatToPayload(w.payloadWriterPtr, cMsgs, cLength)

	errCode := commonpb.ErrorCode(status.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New(msg)
	}
	return nil
}

func (w *PayloadWriter) AddDoubleToPayload(msgs []float64) error {
	length := len(msgs)
	if length <= 0 {
		return errors.New("can't add empty msgs into payload")
	}

	cMsgs := (*C.double)(unsafe.Pointer(&msgs[0]))
	cLength := C.int(length)

	status := C.AddDoubleToPayload(w.payloadWriterPtr, cMsgs, cLength)

	errCode := commonpb.ErrorCode(status.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New(msg)
	}
	return nil
}

func (w *PayloadWriter) AddOneStringToPayload(msg string) error {
	length := len(msg)
	if length == 0 {
		return errors.New("can't add empty string into payload")
	}

	cmsg := C.CString(msg)
	clength := C.int(length)
	defer C.free(unsafe.Pointer(cmsg))

	st := C.AddOneStringToPayload(w.payloadWriterPtr, cmsg, clength)

	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return errors.New(msg)
	}
	return nil
}

// dimension > 0 && (%8 == 0)
func (w *PayloadWriter) AddBinaryVectorToPayload(binVec []byte, dim int) error {
	length := len(binVec)
	if length <= 0 {
		return errors.New("can't add empty binVec into payload")
	}
	if dim <= 0 {
		return errors.New("dimension should be greater than 0")
	}

	cBinVec := (*C.uint8_t)(&binVec[0])
	cDim := C.int(dim)
	cLength := C.int(length / (dim / 8))

	st := C.AddBinaryVectorToPayload(w.payloadWriterPtr, cBinVec, cDim, cLength)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return errors.New(msg)
	}
	return nil
}

// dimension > 0 && (%8 == 0)
func (w *PayloadWriter) AddFloatVectorToPayload(floatVec []float32, dim int) error {
	length := len(floatVec)
	if length <= 0 {
		return errors.New("can't add empty floatVec into payload")
	}
	if dim <= 0 {
		return errors.New("dimension should be greater than 0")
	}

	cVec := (*C.float)(&floatVec[0])
	cDim := C.int(dim)
	cLength := C.int(length / dim)

	st := C.AddFloatVectorToPayload(w.payloadWriterPtr, cVec, cDim, cLength)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return errors.New(msg)
	}
	return nil
}

func (w *PayloadWriter) FinishPayloadWriter() error {
	st := C.FinishPayloadWriter(w.payloadWriterPtr)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return errors.New(msg)
	}
	return nil
}

func (w *PayloadWriter) GetPayloadBufferFromWriter() ([]byte, error) {
	cb := C.GetPayloadBufferFromWriter(w.payloadWriterPtr)
	pointer := unsafe.Pointer(cb.data)
	length := int(cb.length)
	if length <= 0 {
		return nil, errors.New("empty buffer")
	}
	// refer to: https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices
	slice := (*[1 << 28]byte)(pointer)[:length:length]
	return slice, nil
}

func (w *PayloadWriter) GetPayloadLengthFromWriter() (int, error) {
	length := C.GetPayloadLengthFromWriter(w.payloadWriterPtr)
	return int(length), nil
}

func (w *PayloadWriter) ReleasePayloadWriter() error {
	st := C.ReleasePayloadWriter(w.payloadWriterPtr)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return errors.New(msg)
	}
	return nil
}

func (w *PayloadWriter) Close() error {
	return w.ReleasePayloadWriter()
}

func NewPayloadReader(colType schemapb.DataType, buf []byte) (*PayloadReader, error) {
	if len(buf) == 0 {
		return nil, errors.New("create Payload reader failed, buffer is empty")
	}
	r := C.NewPayloadReader(C.int(colType), (*C.uint8_t)(unsafe.Pointer(&buf[0])), C.long(len(buf)))
	if r == nil {
		return nil, errors.New("failed to read parquet from buffer")
	}
	return &PayloadReader{payloadReaderPtr: r, colType: colType}, nil
}

// Params:
//      `idx`: String index
// Return:
//      `interface{}`: all types.
//      `int`: length, only meaningful to FLOAT/BINARY VECTOR type.
//      `error`: error.
func (r *PayloadReader) GetDataFromPayload(idx ...int) (interface{}, int, error) {
	switch len(idx) {
	case 1:
		switch r.colType {
		case schemapb.DataType_String:
			val, err := r.GetOneStringFromPayload(idx[0])
			return val, 0, err
		default:
			return nil, 0, errors.New("unknown type")
		}
	case 0:
		switch r.colType {
		case schemapb.DataType_Bool:
			val, err := r.GetBoolFromPayload()
			return val, 0, err
		case schemapb.DataType_Int8:
			val, err := r.GetInt8FromPayload()
			return val, 0, err
		case schemapb.DataType_Int16:
			val, err := r.GetInt16FromPayload()
			return val, 0, err
		case schemapb.DataType_Int32:
			val, err := r.GetInt32FromPayload()
			return val, 0, err
		case schemapb.DataType_Int64:
			val, err := r.GetInt64FromPayload()
			return val, 0, err
		case schemapb.DataType_Float:
			val, err := r.GetFloatFromPayload()
			return val, 0, err
		case schemapb.DataType_Double:
			val, err := r.GetDoubleFromPayload()
			return val, 0, err
		case schemapb.DataType_BinaryVector:
			return r.GetBinaryVectorFromPayload()
		case schemapb.DataType_FloatVector:
			return r.GetFloatVectorFromPayload()
		default:
			return nil, 0, errors.New("unknown type")
		}
	default:
		return nil, 0, errors.New("incorrect number of index")
	}
}

func (r *PayloadReader) ReleasePayloadReader() error {
	st := C.ReleasePayloadReader(r.payloadReaderPtr)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return errors.New(msg)
	}
	return nil
}

func (r *PayloadReader) GetBoolFromPayload() ([]bool, error) {
	if r.colType != schemapb.DataType_Bool {
		return nil, errors.New("incorrect data type")
	}

	var cMsg *C.bool
	var cSize C.int

	st := C.GetBoolFromPayload(r.payloadReaderPtr, &cMsg, &cSize)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return nil, errors.New(msg)
	}

	slice := (*[1 << 28]bool)(unsafe.Pointer(cMsg))[:cSize:cSize]
	return slice, nil
}

func (r *PayloadReader) GetInt8FromPayload() ([]int8, error) {
	if r.colType != schemapb.DataType_Int8 {
		return nil, errors.New("incorrect data type")
	}

	var cMsg *C.int8_t
	var cSize C.int

	st := C.GetInt8FromPayload(r.payloadReaderPtr, &cMsg, &cSize)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return nil, errors.New(msg)
	}

	slice := (*[1 << 28]int8)(unsafe.Pointer(cMsg))[:cSize:cSize]
	return slice, nil
}

func (r *PayloadReader) GetInt16FromPayload() ([]int16, error) {
	if r.colType != schemapb.DataType_Int16 {
		return nil, errors.New("incorrect data type")
	}

	var cMsg *C.int16_t
	var cSize C.int

	st := C.GetInt16FromPayload(r.payloadReaderPtr, &cMsg, &cSize)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return nil, errors.New(msg)
	}

	slice := (*[1 << 28]int16)(unsafe.Pointer(cMsg))[:cSize:cSize]
	return slice, nil
}

func (r *PayloadReader) GetInt32FromPayload() ([]int32, error) {
	if r.colType != schemapb.DataType_Int32 {
		return nil, errors.New("incorrect data type")
	}

	var cMsg *C.int32_t
	var cSize C.int

	st := C.GetInt32FromPayload(r.payloadReaderPtr, &cMsg, &cSize)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return nil, errors.New(msg)
	}

	slice := (*[1 << 28]int32)(unsafe.Pointer(cMsg))[:cSize:cSize]
	return slice, nil
}

func (r *PayloadReader) GetInt64FromPayload() ([]int64, error) {
	if r.colType != schemapb.DataType_Int64 {
		return nil, errors.New("incorrect data type")
	}

	var cMsg *C.int64_t
	var cSize C.int

	st := C.GetInt64FromPayload(r.payloadReaderPtr, &cMsg, &cSize)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return nil, errors.New(msg)
	}

	slice := (*[1 << 28]int64)(unsafe.Pointer(cMsg))[:cSize:cSize]
	return slice, nil
}

func (r *PayloadReader) GetFloatFromPayload() ([]float32, error) {
	if r.colType != schemapb.DataType_Float {
		return nil, errors.New("incorrect data type")
	}

	var cMsg *C.float
	var cSize C.int

	st := C.GetFloatFromPayload(r.payloadReaderPtr, &cMsg, &cSize)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return nil, errors.New(msg)
	}

	slice := (*[1 << 28]float32)(unsafe.Pointer(cMsg))[:cSize:cSize]
	return slice, nil
}

func (r *PayloadReader) GetDoubleFromPayload() ([]float64, error) {
	if r.colType != schemapb.DataType_Double {
		return nil, errors.New("incorrect data type")
	}

	var cMsg *C.double
	var cSize C.int

	st := C.GetDoubleFromPayload(r.payloadReaderPtr, &cMsg, &cSize)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return nil, errors.New(msg)
	}

	slice := (*[1 << 28]float64)(unsafe.Pointer(cMsg))[:cSize:cSize]
	return slice, nil
}

func (r *PayloadReader) GetOneStringFromPayload(idx int) (string, error) {
	if r.colType != schemapb.DataType_String {
		return "", errors.New("incorrect data type")
	}

	var cStr *C.char
	var cSize C.int

	st := C.GetOneStringFromPayload(r.payloadReaderPtr, C.int(idx), &cStr, &cSize)

	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return "", errors.New(msg)
	}
	return C.GoStringN(cStr, cSize), nil
}

// ,dimension, error
func (r *PayloadReader) GetBinaryVectorFromPayload() ([]byte, int, error) {
	if r.colType != schemapb.DataType_BinaryVector {
		return nil, 0, errors.New("incorrect data type")
	}

	var cMsg *C.uint8_t
	var cDim C.int
	var cLen C.int

	st := C.GetBinaryVectorFromPayload(r.payloadReaderPtr, &cMsg, &cDim, &cLen)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return nil, 0, errors.New(msg)
	}
	length := (cDim / 8) * cLen

	slice := (*[1 << 28]byte)(unsafe.Pointer(cMsg))[:length:length]
	return slice, int(cDim), nil
}

// ,dimension, error
func (r *PayloadReader) GetFloatVectorFromPayload() ([]float32, int, error) {
	if r.colType != schemapb.DataType_FloatVector {
		return nil, 0, errors.New("incorrect data type")
	}

	var cMsg *C.float
	var cDim C.int
	var cLen C.int

	st := C.GetFloatVectorFromPayload(r.payloadReaderPtr, &cMsg, &cDim, &cLen)
	errCode := commonpb.ErrorCode(st.error_code)
	if errCode != commonpb.ErrorCode_Success {
		msg := C.GoString(st.error_msg)
		defer C.free(unsafe.Pointer(st.error_msg))
		return nil, 0, errors.New(msg)
	}
	length := cDim * cLen

	slice := (*[1 << 28]float32)(unsafe.Pointer(cMsg))[:length:length]
	return slice, int(cDim), nil
}

func (r *PayloadReader) GetPayloadLengthFromReader() (int, error) {
	length := C.GetPayloadLengthFromReader(r.payloadReaderPtr)
	return int(length), nil
}

func (r *PayloadReader) Close() error {
	return r.ReleasePayloadReader()
}
