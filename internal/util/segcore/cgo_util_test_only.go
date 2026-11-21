//go:build test
// +build test

package segcore

/*
#cgo pkg-config: milvus_core

#include "common/protobuf_utils_c.h"
#include "segcore/segment_c.h"
*/
import "C"

import (
	"reflect"
	"unsafe"
)

func CreateProtoLayout() *C.ProtoLayout {
	ptr := C.CreateProtoLayout()
	layout := unsafe.Pointer(reflect.ValueOf(ptr).Pointer())
	return (*C.ProtoLayout)(layout)
}

func SetProtoLayout(protoLayout *C.ProtoLayout, slice []byte) {
	protoLayout.blob = unsafe.Pointer(&slice[0])
	protoLayout.size = C.size_t(len(slice))
}

func ReleaseProtoLayout(protoLayout *C.ProtoLayout) {
	C.ReleaseProtoLayout((C.ProtoLayoutInterface)(unsafe.Pointer(reflect.ValueOf(protoLayout).Pointer())))
}

// UpdateVersionCountForTest samples a pinned physical record via the production C ABI.
func UpdateVersionCountForTest(segment CSegment) (uint64, error) {
	var stats C.CUpdateStoreStats
	status := C.GetUpdateStoreStats(C.CSegmentInterface(segment.RawPointer()), &stats)
	return uint64(stats.versions), ConsumeCStatusIntoError(&status)
}
