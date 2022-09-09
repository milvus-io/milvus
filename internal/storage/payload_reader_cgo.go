package storage

/*
#cgo pkg-config: milvus_storage

#include <stdlib.h>
#include "storage/parquet_c.h"
*/
import "C"
import (
	"errors"
	"fmt"
	"unsafe"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

// PayloadReaderCgo reads data from payload
type PayloadReaderCgo struct {
	payloadReaderPtr C.CPayloadReader
	colType          schemapb.DataType
}

func NewPayloadReaderCgo(colType schemapb.DataType, buf []byte) (*PayloadReaderCgo, error) {
	if len(buf) == 0 {
		return nil, errors.New("create Payload reader failed, buffer is empty")
	}
	r := C.NewPayloadReader(C.int(colType), (*C.uint8_t)(unsafe.Pointer(&buf[0])), C.int64_t(len(buf)))
	if r == nil {
		return nil, errors.New("failed to read parquet from buffer")
	}
	return &PayloadReaderCgo{payloadReaderPtr: r, colType: colType}, nil
}

// GetDataFromPayload returns data,length from payload, returns err if failed
// Params:
//      `idx`: String index
// Return:
//      `interface{}`: all types.
//      `int`: length, only meaningful to FLOAT/BINARY VECTOR type.
//      `error`: error.
func (r *PayloadReaderCgo) GetDataFromPayload() (interface{}, int, error) {
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
	case schemapb.DataType_String:
		val, err := r.GetStringFromPayload()
		return val, 0, err
	default:
		return nil, 0, errors.New("unknown type")
	}
}

// ReleasePayloadReader release payload reader.
func (r *PayloadReaderCgo) ReleasePayloadReader() {
	C.ReleasePayloadReader(r.payloadReaderPtr)
}

// GetBoolFromPayload returns bool slice from payload.
func (r *PayloadReaderCgo) GetBoolFromPayload() ([]bool, error) {
	if r.colType != schemapb.DataType_Bool {
		return nil, errors.New("incorrect data type")
	}

	length, err := r.GetPayloadLengthFromReader()
	if err != nil {
		return nil, err
	}
	slice := make([]bool, length)
	for i := 0; i < length; i++ {
		status := C.GetBoolFromPayload(r.payloadReaderPtr, C.int(i), (*C.bool)(&slice[i]))
		if err := HandleCStatus(&status, "GetBoolFromPayload failed"); err != nil {
			return nil, err
		}
	}

	return slice, nil
}

// GetByteFromPayload returns byte slice from payload
func (r *PayloadReaderCgo) GetByteFromPayload() ([]byte, error) {
	if r.colType != schemapb.DataType_Int8 {
		return nil, errors.New("incorrect data type")
	}

	var cMsg *C.int8_t
	var cSize C.int

	status := C.GetInt8FromPayload(r.payloadReaderPtr, &cMsg, &cSize)
	if err := HandleCStatus(&status, "GetInt8FromPayload failed"); err != nil {
		return nil, err
	}

	slice := (*[1 << 28]byte)(unsafe.Pointer(cMsg))[:cSize:cSize]
	return slice, nil
}

// GetInt8FromPayload returns int8 slice from payload
func (r *PayloadReaderCgo) GetInt8FromPayload() ([]int8, error) {
	if r.colType != schemapb.DataType_Int8 {
		return nil, errors.New("incorrect data type")
	}

	var cMsg *C.int8_t
	var cSize C.int

	status := C.GetInt8FromPayload(r.payloadReaderPtr, &cMsg, &cSize)
	if err := HandleCStatus(&status, "GetInt8FromPayload failed"); err != nil {
		return nil, err
	}

	slice := (*[1 << 28]int8)(unsafe.Pointer(cMsg))[:cSize:cSize]
	return slice, nil
}

func (r *PayloadReaderCgo) GetInt16FromPayload() ([]int16, error) {
	if r.colType != schemapb.DataType_Int16 {
		return nil, errors.New("incorrect data type")
	}

	var cMsg *C.int16_t
	var cSize C.int

	status := C.GetInt16FromPayload(r.payloadReaderPtr, &cMsg, &cSize)
	if err := HandleCStatus(&status, "GetInt16FromPayload failed"); err != nil {
		return nil, err
	}

	slice := (*[1 << 28]int16)(unsafe.Pointer(cMsg))[:cSize:cSize]
	return slice, nil
}

func (r *PayloadReaderCgo) GetInt32FromPayload() ([]int32, error) {
	if r.colType != schemapb.DataType_Int32 {
		return nil, errors.New("incorrect data type")
	}

	var cMsg *C.int32_t
	var cSize C.int

	status := C.GetInt32FromPayload(r.payloadReaderPtr, &cMsg, &cSize)
	if err := HandleCStatus(&status, "GetInt32FromPayload failed"); err != nil {
		return nil, err
	}

	slice := (*[1 << 28]int32)(unsafe.Pointer(cMsg))[:cSize:cSize]
	return slice, nil
}

func (r *PayloadReaderCgo) GetInt64FromPayload() ([]int64, error) {
	if r.colType != schemapb.DataType_Int64 {
		return nil, errors.New("incorrect data type")
	}

	var cMsg *C.int64_t
	var cSize C.int

	status := C.GetInt64FromPayload(r.payloadReaderPtr, &cMsg, &cSize)
	if err := HandleCStatus(&status, "GetInt64FromPayload failed"); err != nil {
		return nil, err
	}

	slice := (*[1 << 28]int64)(unsafe.Pointer(cMsg))[:cSize:cSize]
	return slice, nil
}

func (r *PayloadReaderCgo) GetFloatFromPayload() ([]float32, error) {
	if r.colType != schemapb.DataType_Float {
		return nil, errors.New("incorrect data type")
	}

	var cMsg *C.float
	var cSize C.int

	status := C.GetFloatFromPayload(r.payloadReaderPtr, &cMsg, &cSize)
	if err := HandleCStatus(&status, "GetFloatFromPayload failed"); err != nil {
		return nil, err
	}

	slice := (*[1 << 28]float32)(unsafe.Pointer(cMsg))[:cSize:cSize]
	return slice, nil
}

func (r *PayloadReaderCgo) GetDoubleFromPayload() ([]float64, error) {
	if r.colType != schemapb.DataType_Double {
		return nil, errors.New("incorrect data type")
	}

	var cMsg *C.double
	var cSize C.int

	status := C.GetDoubleFromPayload(r.payloadReaderPtr, &cMsg, &cSize)
	if err := HandleCStatus(&status, "GetDoubleFromPayload failed"); err != nil {
		return nil, err
	}

	slice := (*[1 << 28]float64)(unsafe.Pointer(cMsg))[:cSize:cSize]
	return slice, nil
}

func (r *PayloadReaderCgo) GetStringFromPayload() ([]string, error) {
	length, err := r.GetPayloadLengthFromReader()
	if err != nil {
		return nil, err
	}
	ret := make([]string, length)
	for i := 0; i < length; i++ {
		ret[i], err = r.GetOneStringFromPayload(i)
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
}

func (r *PayloadReaderCgo) GetOneStringFromPayload(idx int) (string, error) {
	if r.colType != schemapb.DataType_String {
		return "", errors.New("incorrect data type")
	}

	var cStr *C.char
	var cSize C.int

	status := C.GetOneStringFromPayload(r.payloadReaderPtr, C.int(idx), &cStr, &cSize)
	if err := HandleCStatus(&status, "GetOneStringFromPayload failed"); err != nil {
		return "", err
	}
	return C.GoStringN(cStr, cSize), nil
}

// GetBinaryVectorFromPayload returns vector, dimension, error
func (r *PayloadReaderCgo) GetBinaryVectorFromPayload() ([]byte, int, error) {
	if r.colType != schemapb.DataType_BinaryVector {
		return nil, 0, errors.New("incorrect data type")
	}

	var cMsg *C.uint8_t
	var cDim C.int
	var cLen C.int

	status := C.GetBinaryVectorFromPayload(r.payloadReaderPtr, &cMsg, &cDim, &cLen)
	if err := HandleCStatus(&status, "GetBinaryVectorFromPayload failed"); err != nil {
		return nil, 0, err
	}
	length := (cDim / 8) * cLen

	slice := (*[1 << 28]byte)(unsafe.Pointer(cMsg))[:length:length]
	return slice, int(cDim), nil
}

// GetFloatVectorFromPayload returns vector, dimension, error
func (r *PayloadReaderCgo) GetFloatVectorFromPayload() ([]float32, int, error) {
	if r.colType != schemapb.DataType_FloatVector {
		return nil, 0, errors.New("incorrect data type")
	}

	var cMsg *C.float
	var cDim C.int
	var cLen C.int

	status := C.GetFloatVectorFromPayload(r.payloadReaderPtr, &cMsg, &cDim, &cLen)
	if err := HandleCStatus(&status, "GetFloatVectorFromPayload failed"); err != nil {
		return nil, 0, err
	}
	length := cDim * cLen

	slice := (*[1 << 28]float32)(unsafe.Pointer(cMsg))[:length:length]
	return slice, int(cDim), nil
}

func (r *PayloadReaderCgo) GetPayloadLengthFromReader() (int, error) {
	length := C.GetPayloadLengthFromReader(r.payloadReaderPtr)
	return int(length), nil
}

// Close closes the payload reader
func (r *PayloadReaderCgo) Close() {
	r.ReleasePayloadReader()
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
