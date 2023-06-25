package indexcgowrapper

/*
#cgo pkg-config: milvus_indexbuilder

#include <stdlib.h>	// free
#include "indexbuilder/index_c.h"
*/
import "C"
import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"unsafe"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

	"github.com/milvus-io/milvus/internal/proto/indexcgopb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
)

type Blob = storage.Blob

type IndexFileInfo struct {
	FileName string
	FileSize int64
}

type CodecIndex interface {
	Build(*Dataset) error
	Serialize() ([]*Blob, error)
	GetIndexFileInfo() ([]*IndexFileInfo, error)
	Load([]*Blob) error
	Delete() error
	CleanLocalData() error
	UpLoad() (map[string]int64, error)
}

var (
	_ CodecIndex = (*CgoIndex)(nil)
)

type CgoIndex struct {
	indexPtr C.CIndex
	close    bool
}

// TODO: use proto.Marshal instead of proto.MarshalTextString for better compatibility.
func NewCgoIndex(dtype schemapb.DataType, typeParams, indexParams map[string]string) (CodecIndex, error) {
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

	var indexPtr C.CIndex
	cintDType := uint32(dtype)
	status := C.CreateIndex(cintDType, typeParamsPointer, indexParamsPointer, &indexPtr)
	if err := HandleCStatus(&status, "failed to create index"); err != nil {
		return nil, err
	}

	index := &CgoIndex{
		indexPtr: indexPtr,
		close:    false,
	}

	runtime.SetFinalizer(index, func(index *CgoIndex) {
		if index != nil && !index.close {
			log.Error("there is leakage in index object, please check.")
		}
	})

	return index, nil
}

func CreateIndex(ctx context.Context, buildIndexInfo *BuildIndexInfo) (CodecIndex, error) {
	var indexPtr C.CIndex
	status := C.CreateIndexV2(&indexPtr, buildIndexInfo.cBuildIndexInfo)
	if err := HandleCStatus(&status, "failed to create index"); err != nil {
		return nil, err
	}

	index := &CgoIndex{
		indexPtr: indexPtr,
		close:    false,
	}

	return index, nil
}

func (index *CgoIndex) Build(dataset *Dataset) error {
	switch dataset.DType {
	case schemapb.DataType_None:
		return fmt.Errorf("build index on supported data type: %s", dataset.DType.String())
	case schemapb.DataType_FloatVector:
		return index.buildFloatVecIndex(dataset)
	case schemapb.DataType_BinaryVector:
		return index.buildBinaryVecIndex(dataset)
	case schemapb.DataType_Bool:
		return index.buildBoolIndex(dataset)
	case schemapb.DataType_Int8:
		return index.buildInt8Index(dataset)
	case schemapb.DataType_Int16:
		return index.buildInt16Index(dataset)
	case schemapb.DataType_Int32:
		return index.buildInt32Index(dataset)
	case schemapb.DataType_Int64:
		return index.buildInt64Index(dataset)
	case schemapb.DataType_Float:
		return index.buildFloatIndex(dataset)
	case schemapb.DataType_Double:
		return index.buildDoubleIndex(dataset)
	case schemapb.DataType_String:
		return index.buildStringIndex(dataset)
	case schemapb.DataType_VarChar:
		return index.buildStringIndex(dataset)
	default:
		return fmt.Errorf("build index on unsupported data type: %s", dataset.DType.String())
	}
}

func (index *CgoIndex) buildFloatVecIndex(dataset *Dataset) error {
	vectors := dataset.Data[keyRawArr].([]float32)
	status := C.BuildFloatVecIndex(index.indexPtr, (C.int64_t)(len(vectors)), (*C.float)(&vectors[0]))
	return HandleCStatus(&status, "failed to build float vector index")
}

func (index *CgoIndex) buildBinaryVecIndex(dataset *Dataset) error {
	vectors := dataset.Data[keyRawArr].([]byte)
	status := C.BuildBinaryVecIndex(index.indexPtr, (C.int64_t)(len(vectors)), (*C.uint8_t)(&vectors[0]))
	return HandleCStatus(&status, "failed to build binary vector index")
}

// TODO: investigate if we can pass an bool array to cgo.
func (index *CgoIndex) buildBoolIndex(dataset *Dataset) error {
	arr := dataset.Data[keyRawArr].([]bool)
	f := &schemapb.BoolArray{
		Data: arr,
	}
	data, err := proto.Marshal(f)
	if err != nil {
		return err
	}
	status := C.BuildScalarIndex(index.indexPtr, (C.int64_t)(len(data)), unsafe.Pointer(&data[0]))
	return HandleCStatus(&status, "failed to build scalar index")
}

// TODO: refactor these duplicated code after generic programming is supported.

func (index *CgoIndex) buildInt8Index(dataset *Dataset) error {
	data := dataset.Data[keyRawArr].([]int8)
	status := C.BuildScalarIndex(index.indexPtr, (C.int64_t)(len(data)), unsafe.Pointer(&data[0]))
	return HandleCStatus(&status, "failed to build scalar index")
}

func (index *CgoIndex) buildInt16Index(dataset *Dataset) error {
	data := dataset.Data[keyRawArr].([]int16)
	status := C.BuildScalarIndex(index.indexPtr, (C.int64_t)(len(data)), unsafe.Pointer(&data[0]))
	return HandleCStatus(&status, "failed to build scalar index")
}

func (index *CgoIndex) buildInt32Index(dataset *Dataset) error {
	data := dataset.Data[keyRawArr].([]int32)
	status := C.BuildScalarIndex(index.indexPtr, (C.int64_t)(len(data)), unsafe.Pointer(&data[0]))
	return HandleCStatus(&status, "failed to build scalar index")
}

func (index *CgoIndex) buildInt64Index(dataset *Dataset) error {
	data := dataset.Data[keyRawArr].([]int64)
	status := C.BuildScalarIndex(index.indexPtr, (C.int64_t)(len(data)), unsafe.Pointer(&data[0]))
	return HandleCStatus(&status, "failed to build scalar index")
}

func (index *CgoIndex) buildFloatIndex(dataset *Dataset) error {
	data := dataset.Data[keyRawArr].([]float32)
	status := C.BuildScalarIndex(index.indexPtr, (C.int64_t)(len(data)), unsafe.Pointer(&data[0]))
	return HandleCStatus(&status, "failed to build scalar index")
}

func (index *CgoIndex) buildDoubleIndex(dataset *Dataset) error {
	data := dataset.Data[keyRawArr].([]float64)
	status := C.BuildScalarIndex(index.indexPtr, (C.int64_t)(len(data)), unsafe.Pointer(&data[0]))
	return HandleCStatus(&status, "failed to build scalar index")
}

func (index *CgoIndex) buildStringIndex(dataset *Dataset) error {
	arr := dataset.Data[keyRawArr].([]string)
	f := &schemapb.StringArray{
		Data: arr,
	}
	data, err := proto.Marshal(f)
	if err != nil {
		return err
	}
	status := C.BuildScalarIndex(index.indexPtr, (C.int64_t)(len(data)), unsafe.Pointer(&data[0]))
	return HandleCStatus(&status, "failed to build scalar index")
}

func (index *CgoIndex) Serialize() ([]*Blob, error) {
	var cBinarySet C.CBinarySet

	status := C.SerializeIndexToBinarySet(index.indexPtr, &cBinarySet)
	defer func() {
		if cBinarySet != nil {
			C.DeleteBinarySet(cBinarySet)
		}
	}()
	if err := HandleCStatus(&status, "failed to serialize index to binary set"); err != nil {
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
		size, err := GetBinarySetSize(cBinarySet, key)
		if err != nil {
			return nil, err
		}
		blob := &Blob{
			Key:   key,
			Value: value,
			Size:  size,
		}
		ret = append(ret, blob)
	}

	return ret, nil
}

func (index *CgoIndex) GetIndexFileInfo() ([]*IndexFileInfo, error) {
	var cBinarySet C.CBinarySet

	status := C.SerializeIndexToBinarySet(index.indexPtr, &cBinarySet)
	defer func() {
		if cBinarySet != nil {
			C.DeleteBinarySet(cBinarySet)
		}
	}()
	if err := HandleCStatus(&status, "failed to serialize index to binary set"); err != nil {
		return nil, err
	}

	keys, err := GetBinarySetKeys(cBinarySet)
	if err != nil {
		return nil, err
	}
	ret := make([]*IndexFileInfo, 0)
	for _, key := range keys {
		size, err := GetBinarySetSize(cBinarySet, key)
		if err != nil {
			return nil, err
		}
		info := &IndexFileInfo{
			FileName: key,
			FileSize: size,
		}
		ret = append(ret, info)
	}

	return ret, nil
}

func (index *CgoIndex) Load(blobs []*Blob) error {
	var cBinarySet C.CBinarySet
	status := C.NewBinarySet(&cBinarySet)
	defer C.DeleteBinarySet(cBinarySet)

	if err := HandleCStatus(&status, "failed to load index"); err != nil {
		return err
	}
	for _, blob := range blobs {
		key := blob.Key
		byteIndex := blob.Value
		indexPtr := unsafe.Pointer(&byteIndex[0])
		indexLen := C.int64_t(len(byteIndex))
		binarySetKey := filepath.Base(key)
		indexKey := C.CString(binarySetKey)
		status = C.AppendIndexBinary(cBinarySet, indexPtr, indexLen, indexKey)
		C.free(unsafe.Pointer(indexKey))
		if err := HandleCStatus(&status, "failed to load index"); err != nil {
			return err
		}
	}
	status = C.LoadIndexFromBinarySet(index.indexPtr, cBinarySet)
	return HandleCStatus(&status, "failed to load index")
}

func (index *CgoIndex) Delete() error {
	if index.close {
		return nil
	}
	status := C.DeleteIndex(index.indexPtr)
	index.close = true
	return HandleCStatus(&status, "failed to delete index")
}

func (index *CgoIndex) CleanLocalData() error {
	status := C.CleanLocalData(index.indexPtr)
	return HandleCStatus(&status, "failed to clean cached data on disk")
}

func (index *CgoIndex) UpLoad() (map[string]int64, error) {
	var cBinarySet C.CBinarySet

	status := C.SerializeIndexAndUpLoad(index.indexPtr, &cBinarySet)
	defer func() {
		if cBinarySet != nil {
			C.DeleteBinarySet(cBinarySet)
		}
	}()
	if err := HandleCStatus(&status, "failed to serialize index and upload index"); err != nil {
		return nil, err
	}

	res := make(map[string]int64)
	indexFilePaths, err := GetBinarySetKeys(cBinarySet)
	if err != nil {
		return nil, err
	}
	for _, path := range indexFilePaths {
		size, err := GetBinarySetSize(cBinarySet, path)
		if err != nil {
			return nil, err
		}
		res[path] = size
	}

	runtime.SetFinalizer(index, func(index *CgoIndex) {
		if index != nil && !index.close {
			log.Error("there is leakage in index object, please check.")
		}
	})

	return res, nil
}
