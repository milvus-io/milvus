package indexparamcheck

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>	// free
#include "segcore/vector_index_c.h"
*/
import "C"

import (
	"fmt"
	"math"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

	return fmt.Errorf("%s", errorMsg)
}

func (c vecIndexChecker) StaticCheck(dataType schemapb.DataType, params map[string]string) error {
	if typeutil.IsDenseFloatVectorType(dataType) {
		if !CheckStrByValues(params, Metric, FloatVectorMetrics) {
			return fmt.Errorf("metric type %s not found or not supported, supported: %v", params[Metric], FloatVectorMetrics)
		}
	} else if typeutil.IsSparseFloatVectorType(dataType) {
		if !CheckStrByValues(params, Metric, SparseMetrics) {
			return fmt.Errorf("metric type not found or not supported, supported: %v", SparseMetrics)
		}
	} else if typeutil.IsBinaryVectorType(dataType) {
		if !CheckStrByValues(params, Metric, BinaryVectorMetrics) {
			return fmt.Errorf("metric type %s not found or not supported, supported: %v", params[Metric], BinaryVectorMetrics)
		}
	} else if typeutil.IsIntVectorType(dataType) {
		if !CheckStrByValues(params, Metric, IntVectorMetrics) {
			return fmt.Errorf("metric type %s not found or not supported, supported: %v", params[Metric], IntVectorMetrics)
		}
	}

	indexType, exist := params[common.IndexTypeKey]

	if !exist {
		return fmt.Errorf("no indexType is specified")
	}

	if !vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType) {
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

	if typeutil.IsFixDimVectorType(dataType) {
		if !CheckIntByRange(params, DIM, 1, math.MaxInt) {
			return fmt.Errorf("failed to check vector dimension, should be larger than 0 and smaller than math.MaxInt")
		}
	}

	return c.baseChecker.CheckTrain(dataType, params)
}

func (c vecIndexChecker) CheckValidDataType(indexType IndexType, field *schemapb.FieldSchema) error {
	if !typeutil.IsVectorType(field.GetDataType()) {
		return fmt.Errorf("index %s only supports vector data type", indexType)
	}
	if !vecindexmgr.GetVecIndexMgrInstance().IsDataTypeSupport(indexType, field.GetDataType()) {
		return fmt.Errorf("index %s do not support data type: %s", indexType, schemapb.DataType_name[int32(field.GetDataType())])
	}
	return nil
}

func (c vecIndexChecker) SetDefaultMetricTypeIfNotExist(dType schemapb.DataType, params map[string]string) {
	paramtable.SetDefaultMetricTypeIfNotExist(dType, params)
}

func newVecIndexChecker() IndexChecker {
	return &vecIndexChecker{}
}
