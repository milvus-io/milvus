package indexcgowrapper

/*
#cgo pkg-config: milvus_common milvus_storage

#include <stdlib.h>	// free
#include "common/binary_set_c.h"
#include "storage/storage_c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/log"
)

func GetBinarySetKeys(cBinarySet C.CBinarySet) ([]string, error) {
	size := int(C.GetBinarySetSize(cBinarySet))
	if size == 0 {
		return nil, fmt.Errorf("BinarySet size is zero")
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
		return nil, fmt.Errorf("GetBinarySetValueSize size is zero")
	}
	value := make([]byte, size)
	status := C.CopyBinarySetValue(unsafe.Pointer(&value[0]), cIndexKey, cBinarySet)

	if err := HandleCStatus(&status, "CopyBinarySetValue failed"); err != nil {
		return nil, err
	}

	return value, nil
}

func GetBinarySetSize(cBinarySet C.CBinarySet, key string) (int64, error) {
	cIndexKey := C.CString(key)
	defer C.free(unsafe.Pointer(cIndexKey))
	ret := C.GetBinarySetValueSize(cBinarySet, cIndexKey)
	return int64(ret), nil
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

func GetLocalUsedSize(path string) (int64, error) {
	var availableSize int64
	cSize := (*C.int64_t)(&availableSize)
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	status := C.GetLocalUsedSize(cPath, cSize)
	err := HandleCStatus(&status, "get local used size failed")
	if err != nil {
		return 0, err
	}

	return availableSize, nil
}
