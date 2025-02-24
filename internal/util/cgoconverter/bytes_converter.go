package cgoconverter

/*
 #include <stdlib.h>
*/
import "C"

import (
	"math"
	"sync/atomic"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const maxByteArrayLen = math.MaxInt32

var globalConverter = NewBytesConverter()

type BytesConverter struct {
	pointers  *typeutil.ConcurrentMap[int32, unsafe.Pointer] // leaseId -> unsafe.Pointer
	nextLease int32
}

func NewBytesConverter() *BytesConverter {
	return &BytesConverter{
		pointers:  typeutil.NewConcurrentMap[int32, unsafe.Pointer](),
		nextLease: 0,
	}
}

func (converter *BytesConverter) add(p unsafe.Pointer) int32 {
	lease := atomic.AddInt32(&converter.nextLease, 1)
	converter.pointers.Insert(lease, p)

	return lease
}

// Return a lease and []byte from C bytes (typically char*)
// which references the same memory of C bytes
// Call Release(lease) after you don't need the returned []byte
func (converter *BytesConverter) UnsafeGoBytes(cbytes *unsafe.Pointer, len int) (int32, []byte) {
	var (
		goBytes []byte
		lease   int32
	)

	if len > maxByteArrayLen {
		// C.GoBytes takes the length as C.int,
		// which is always 32-bit (not depends on platform)
		panic("UnsafeGoBytes: out of length")
	}
	goBytes = (*[maxByteArrayLen]byte)(*cbytes)[:len:len]
	lease = converter.add(*cbytes)
	*cbytes = nil

	return lease, goBytes
}

func (converter *BytesConverter) Release(lease int32) {
	p := converter.Extract(lease)

	C.free(p)
}

func (converter *BytesConverter) Extract(lease int32) unsafe.Pointer {
	p, ok := converter.pointers.GetAndRemove(lease)
	if !ok {
		panic("try to release the resource that doesn't exist")
	}

	return p
}

// Make sure only the caller own the converter
// or this would release someone's memory
func (converter *BytesConverter) ReleaseAll() {
	converter.pointers.Range(func(lease int32, pointer unsafe.Pointer) bool {
		converter.pointers.GetAndRemove(lease)
		C.free(pointer)

		return true
	})
}

func UnsafeGoBytes(cbytes *unsafe.Pointer, len int) (int32, []byte) {
	return globalConverter.UnsafeGoBytes(cbytes, len)
}

func Release(lease int32) {
	globalConverter.Release(lease)
}

func Extract(lease int32) unsafe.Pointer {
	return globalConverter.Extract(lease)
}

// DO NOT provide ReleaseAll() method for global converter
