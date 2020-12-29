package indexbuilder

/*

#cgo CFLAGS: -I${SRCDIR}/../core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../core/output/lib -lmilvus_indexbuilder -Wl,-rpath=${SRCDIR}/../core/output/lib

#include <stdlib.h>	// free
#include "segcore/collection_c.h"
#include "indexbuilder/index_c.h"

*/
import "C"
import (
	"unsafe"

	"github.com/golang/protobuf/proto"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexcgopb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
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
	/*
		char*
		SerializeToSlicedBuffer(CIndex index, int32_t* buffer_size);
	*/
	var bufferSize int32
	var cDumpedSlicedBuffer *C.char = C.SerializeToSlicedBuffer(index.indexPtr, (*C.int32_t)(unsafe.Pointer(&bufferSize)))
	defer C.free(unsafe.Pointer(cDumpedSlicedBuffer))

	dumpedSlicedBuffer := C.GoBytes(unsafe.Pointer(cDumpedSlicedBuffer), (C.int32_t)(bufferSize))
	var blobs indexcgopb.BinarySet
	err := proto.Unmarshal(dumpedSlicedBuffer, &blobs)
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

	datas, err := proto.Marshal(binarySet)
	if err != nil {
		return err
	}

	/*
		void
		LoadFromSlicedBuffer(CIndex index, const char* serialized_sliced_blob_buffer, int32_t size);
	*/
	C.LoadFromSlicedBuffer(index.indexPtr, (*C.char)(unsafe.Pointer(&datas[0])), (C.int32_t)(len(datas)))
	return nil
}

func (index *CIndex) BuildFloatVecIndexWithoutIds(vectors []float32) error {
	/*
		void
		BuildFloatVecIndexWithoutIds(CIndex index, int64_t float_value_num, const float* vectors);
	*/
	C.BuildFloatVecIndexWithoutIds(index.indexPtr, (C.int64_t)(len(vectors)), (*C.float)(&vectors[0]))
	return nil
}

func (index *CIndex) BuildBinaryVecIndexWithoutIds(vectors []byte) error {
	/*
		void
		BuildBinaryVecIndexWithoutIds(CIndex index, int64_t data_size, const uint8_t* vectors);
	*/
	C.BuildBinaryVecIndexWithoutIds(index.indexPtr, (C.int64_t)(len(vectors)), (*C.uint8_t)(&vectors[0]))
	return nil
}

func (index *CIndex) Delete() error {
	/*
		void
		DeleteIndex(CIndex index);
	*/
	C.DeleteIndex(index.indexPtr)
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

	//print := func(param []byte) {
	//	for i, c := range param {
	//		fmt.Print(c)
	//		fmt.Print(", ")
	//		if i % 25 == 0 {
	//			fmt.Println()
	//		}
	//	}
	//	fmt.Println()
	//}
	//print(typeParamsStr)
	//fmt.Println("len(typeParamsStr): ", len(typeParamsStr))
	//print(indexParamsStr)
	//fmt.Println("len(indexParamsStr): ", len(indexParamsStr))

	typeParamsPointer := C.CString(typeParamsStr)
	indexParamsPointer := C.CString(indexParamsStr)

	/*
		CIndex
		CreateIndex(const char* serialized_type_params,
					int64_t type_params_size,
					const char* serialized_index_params
					int64_t index_params_size);
	*/
	return &CIndex{
		indexPtr: C.CreateIndex(typeParamsPointer, indexParamsPointer),
	}, nil
}
