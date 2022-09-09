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

package storage

/*
#cgo CFLAGS: -I${SRCDIR}/cwrapper

#cgo linux LDFLAGS: -L${SRCDIR}/cwrapper/output/lib -L${SRCDIR}/cwrapper/output/lib64 -lwrapper -lparquet -larrow -larrow_bundled_dependencies -lstdc++ -lm
#cgo darwin LDFLAGS: -L${SRCDIR}/cwrapper/output/lib -lwrapper -lparquet -larrow -larrow_bundled_dependencies -lstdc++ -lm
#cgo windows LDFLAGS: -L${SRCDIR}/cwrapper/output/lib -lwrapper -lparquet -larrow -lstdc++
#include <stdlib.h>
#include "ParquetWrapper.h"
*/
import "C"
import (
	"errors"
	"reflect"
	"unsafe"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

// PayloadWriterInterface abstracts PayloadWriter
type PayloadWriterInterface interface {
	AddDataToPayload(msgs interface{}, dim ...int) error
	AddBoolToPayload(msgs []bool) error
	AddByteToPayload(msgs []byte) error
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
	ReleasePayloadWriter()
	Close()
}

// PayloadReaderInterface abstracts PayloadReader
type PayloadReaderInterface interface {
	GetDataFromPayload() (interface{}, int, error)
	GetBoolFromPayload() ([]bool, error)
	GetByteFromPayload() ([]byte, error)
	GetInt8FromPayload() ([]int8, error)
	GetInt16FromPayload() ([]int16, error)
	GetInt32FromPayload() ([]int32, error)
	GetInt64FromPayload() ([]int64, error)
	GetFloatFromPayload() ([]float32, error)
	GetDoubleFromPayload() ([]float64, error)
	GetStringFromPayload() ([]string, error)
	GetBinaryVectorFromPayload() ([]byte, int, error)
	GetFloatVectorFromPayload() ([]float32, int, error)
	GetPayloadLengthFromReader() (int, error)
	ReleasePayloadReader()
	Close()
}

// PayloadWriter writes data into payload
type PayloadWriter struct {
	payloadWriterPtr C.CPayloadWriter
	colType          schemapb.DataType
}

// NewPayloadWriter is constructor of PayloadWriter
func NewPayloadWriter(colType schemapb.DataType) (*PayloadWriter, error) {
	w := C.NewPayloadWriter(C.int(colType))
	if w == nil {
		return nil, errors.New("create Payload writer failed")
	}
	return &PayloadWriter{payloadWriterPtr: w, colType: colType}, nil
}

// AddDataToPayload adds @msgs into payload, if @msgs is vector, dimension should be specified by @dim
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
		case schemapb.DataType_String, schemapb.DataType_VarChar:
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

// AddBoolToPayload adds @msgs into payload
func (w *PayloadWriter) AddBoolToPayload(msgs []bool) error {
	length := len(msgs)
	if length <= 0 {
		return errors.New("can't add empty msgs into payload")
	}

	cMsgs := (*C.bool)(unsafe.Pointer(&msgs[0]))
	cLength := C.int(length)

	status := C.AddBooleanToPayload(w.payloadWriterPtr, cMsgs, cLength)
	return HandleCStatus(&status, "AddBoolToPayload failed")
}

// AddByteToPayload adds @msgs into payload
func (w *PayloadWriter) AddByteToPayload(msgs []byte) error {
	length := len(msgs)
	if length <= 0 {
		return errors.New("can't add empty msgs into payload")
	}
	cMsgs := (*C.int8_t)(unsafe.Pointer(&msgs[0]))
	cLength := C.int(length)

	status := C.AddInt8ToPayload(w.payloadWriterPtr, cMsgs, cLength)
	return HandleCStatus(&status, "AddInt8ToPayload failed")
}

func (w *PayloadWriter) AddInt8ToPayload(msgs []int8) error {
	length := len(msgs)
	if length <= 0 {
		return errors.New("can't add empty msgs into payload")
	}
	cMsgs := (*C.int8_t)(unsafe.Pointer(&msgs[0]))
	cLength := C.int(length)

	status := C.AddInt8ToPayload(w.payloadWriterPtr, cMsgs, cLength)
	return HandleCStatus(&status, "AddInt8ToPayload failed")
}

func (w *PayloadWriter) AddInt16ToPayload(msgs []int16) error {
	length := len(msgs)
	if length <= 0 {
		return errors.New("can't add empty msgs into payload")
	}

	cMsgs := (*C.int16_t)(unsafe.Pointer(&msgs[0]))
	cLength := C.int(length)

	status := C.AddInt16ToPayload(w.payloadWriterPtr, cMsgs, cLength)
	return HandleCStatus(&status, "AddInt16ToPayload failed")
}

func (w *PayloadWriter) AddInt32ToPayload(msgs []int32) error {
	length := len(msgs)
	if length <= 0 {
		return errors.New("can't add empty msgs into payload")
	}

	cMsgs := (*C.int32_t)(unsafe.Pointer(&msgs[0]))
	cLength := C.int(length)

	status := C.AddInt32ToPayload(w.payloadWriterPtr, cMsgs, cLength)
	return HandleCStatus(&status, "AddInt32ToPayload failed")
}

func (w *PayloadWriter) AddInt64ToPayload(msgs []int64) error {
	length := len(msgs)
	if length <= 0 {
		return errors.New("can't add empty msgs into payload")
	}

	cMsgs := (*C.int64_t)(unsafe.Pointer(&msgs[0]))
	cLength := C.int(length)

	status := C.AddInt64ToPayload(w.payloadWriterPtr, cMsgs, cLength)
	return HandleCStatus(&status, "AddInt64ToPayload failed")
}

func (w *PayloadWriter) AddFloatToPayload(msgs []float32) error {
	length := len(msgs)
	if length <= 0 {
		return errors.New("can't add empty msgs into payload")
	}

	cMsgs := (*C.float)(unsafe.Pointer(&msgs[0]))
	cLength := C.int(length)

	status := C.AddFloatToPayload(w.payloadWriterPtr, cMsgs, cLength)
	return HandleCStatus(&status, "AddFloatToPayload failed")
}

func (w *PayloadWriter) AddDoubleToPayload(msgs []float64) error {
	length := len(msgs)
	if length <= 0 {
		return errors.New("can't add empty msgs into payload")
	}

	cMsgs := (*C.double)(unsafe.Pointer(&msgs[0]))
	cLength := C.int(length)

	status := C.AddDoubleToPayload(w.payloadWriterPtr, cMsgs, cLength)
	return HandleCStatus(&status, "AddDoubleToPayload failed")
}

func (w *PayloadWriter) AddOneStringToPayload(msg string) error {
	length := len(msg)
	cmsg := C.CString(msg)
	clength := C.int(length)
	defer C.free(unsafe.Pointer(cmsg))

	// the C.AddOneStringToPayload can handle empty string
	status := C.AddOneStringToPayload(w.payloadWriterPtr, cmsg, clength)
	return HandleCStatus(&status, "AddOneStringToPayload failed")
}

// AddBinaryVectorToPayload dimension > 0 && (%8 == 0)
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

	status := C.AddBinaryVectorToPayload(w.payloadWriterPtr, cBinVec, cDim, cLength)
	return HandleCStatus(&status, "AddBinaryVectorToPayload failed")
}

// AddFloatVectorToPayload dimension > 0 && (%8 == 0)
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

	status := C.AddFloatVectorToPayload(w.payloadWriterPtr, cVec, cDim, cLength)
	return HandleCStatus(&status, "AddFloatVectorToPayload failed")
}

func (w *PayloadWriter) FinishPayloadWriter() error {
	status := C.FinishPayloadWriter(w.payloadWriterPtr)
	return HandleCStatus(&status, "FinishPayloadWriter failed")
}

func (w *PayloadWriter) GetPayloadBufferFromWriter() ([]byte, error) {
	cb := C.GetPayloadBufferFromWriter(w.payloadWriterPtr)
	pointer := uintptr(unsafe.Pointer(cb.data))
	length := int(cb.length)
	if length <= 0 {
		return nil, errors.New("empty buffer")
	}

	var data []byte
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	sh.Data = pointer
	sh.Len = length
	sh.Cap = length

	return data, nil
}

func (w *PayloadWriter) GetPayloadLengthFromWriter() (int, error) {
	length := C.GetPayloadLengthFromWriter(w.payloadWriterPtr)
	return int(length), nil
}

func (w *PayloadWriter) ReleasePayloadWriter() {
	C.ReleasePayloadWriter(w.payloadWriterPtr)
}

func (w *PayloadWriter) Close() {
	w.ReleasePayloadWriter()
}
