package querynodeimp

/*
#cgo CFLAGS: -I${SRCDIR}/../core/output/include
#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_segcore -Wl,-rpath=${SRCDIR}/../core/output/lib

#include "segcore/load_index_c.h"

*/
import "C"
import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"unsafe"
)

type LoadIndexInfo struct {
	cLoadIndexInfo C.CLoadIndexInfo
}

func newLoadIndexInfo() (*LoadIndexInfo, error) {
	var cLoadIndexInfo C.CLoadIndexInfo
	status := C.NewLoadIndexInfo(&cLoadIndexInfo)
	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return nil, errors.New("NewLoadIndexInfo failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}
	return &LoadIndexInfo{cLoadIndexInfo: cLoadIndexInfo}, nil
}

func deleteLoadIndexInfo(info *LoadIndexInfo) {
	C.DeleteLoadIndexInfo(info.cLoadIndexInfo)
}

func (li *LoadIndexInfo) appendIndexParam(indexKey string, indexValue string) error {
	cIndexKey := C.CString(indexKey)
	cIndexValue := C.CString(indexValue)
	status := C.AppendIndexParam(li.cLoadIndexInfo, cIndexKey, cIndexValue)
	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("AppendIndexParam failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}
	return nil
}

func (li *LoadIndexInfo) appendFieldInfo(fieldName string, fieldID int64) error {
	cFieldName := C.CString(fieldName)
	cFieldID := C.long(fieldID)
	status := C.AppendFieldInfo(li.cLoadIndexInfo, cFieldName, cFieldID)
	errorCode := status.error_code

	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("AppendFieldInfo failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}
	return nil
}

func (li *LoadIndexInfo) appendIndex(bytesIndex [][]byte, indexKeys []string) error {
	var cBinarySet C.CBinarySet
	status := C.NewBinarySet(&cBinarySet)

	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("newBinarySet failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}

	for i, byteIndex := range bytesIndex {
		indexPtr := unsafe.Pointer(&byteIndex[0])
		indexLen := C.long(len(byteIndex))
		binarySetKey := filepath.Base(indexKeys[i])
		fmt.Println("index key = ", binarySetKey)
		indexKey := C.CString(binarySetKey)
		status = C.AppendBinaryIndex(cBinarySet, indexPtr, indexLen, indexKey)
		errorCode = status.error_code
		if errorCode != 0 {
			break
		}
	}
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("AppendBinaryIndex failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}

	status = C.AppendIndex(li.cLoadIndexInfo, cBinarySet)
	errorCode = status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return errors.New("AppendIndex failed, C runtime error detected, error code = " + strconv.Itoa(int(errorCode)) + ", error msg = " + errorMsg)
	}

	return nil
}
