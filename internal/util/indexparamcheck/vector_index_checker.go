package indexparamcheck

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>	// free
#include "segcore/vector_index_c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type vecIndexChecker struct {
	baseChecker
}

// HandleCStatus deals with the error returned from CGO
func HandleCStatus(status *C.CStatus) error {
	if status.error_code == 0 {
		return nil
	}
	errorMsg := C.GoString(status.error_msg)
	defer C.free(unsafe.Pointer(status.error_msg))

	return fmt.Errorf("%s", string(errorMsg))
}

func (c vecIndexChecker) StaticCheck(dataType schemapb.DataType, params map[string]string) error {
	indexType, exist := params[common.IndexTypeKey]

	if !exist {
		return fmt.Errorf("no indexType is specified")
	}

	if !GetVecIndexMgrInstance().IsVecIndex(indexType) {
		return fmt.Errorf("indexType %s is not supported", indexType)
	}

	protoIndexParams := &indexcgopb.IndexParams{
		Params: make([]*commonpb.KeyValuePair, 0),
	}

	for key, value := range params {
		protoIndexParams.Params = append(protoIndexParams.Params, &commonpb.KeyValuePair{Key: key, Value: value})
	}

	indexParamsBlob, err := proto.Marshal(protoIndexParams)
	if err != nil {
		return fmt.Errorf("failed to marshal index params: %s", err)
	}

	var status C.CStatus

	cIndexType := C.CString(indexType)
	cDataType := uint32(dataType)
	status = C.ValidateIndexParams(cIndexType, cDataType, (*C.uint8_t)(unsafe.Pointer(&indexParamsBlob[0])), (C.uint64_t)(len(indexParamsBlob)))
	C.free(unsafe.Pointer(cIndexType))

	return HandleCStatus(&status)
}

func (c vecIndexChecker) CheckTrain(dataType schemapb.DataType, params map[string]string) error {
	if err := c.StaticCheck(dataType, params); err != nil {
		return err
	}
	return c.baseChecker.CheckTrain(dataType, params)
}

func (c vecIndexChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	if !typeutil.IsVectorType(field.GetDataType()) {
		return fmt.Errorf("index %s only supports vector data type", indexType)
	}
	if !GetVecIndexMgrInstance().IsDataTypeSupport(indexType, field.GetDataType()) {
		return fmt.Errorf("index %s do not support data type: %s", indexType, schemapb.DataType_name[int32(field.GetDataType())])
	}
	return nil
}

func (c vecIndexChecker) SetDefaultMetricTypeIfNotExist(dType schemapb.DataType, params map[string]string) {
	if typeutil.IsDenseFloatVectorType(dType) {
		setDefaultIfNotExist(params, common.MetricTypeKey, FloatVectorDefaultMetricType)
	} else if typeutil.IsSparseFloatVectorType(dType) {
		setDefaultIfNotExist(params, common.MetricTypeKey, SparseFloatVectorDefaultMetricType)
	} else if typeutil.IsBinaryVectorType(dType) {
		setDefaultIfNotExist(params, common.MetricTypeKey, BinaryVectorDefaultMetricType)
	}
}

func newVecIndexChecker() IndexChecker {
	return &vecIndexChecker{}
}
