package cgoconverter

/*
 #include <stdlib.h>
*/
import "C"
import (
	"math"
	"sync"
	"sync/atomic"
	"unsafe"
)

const maxByteArrayLen = math.MaxInt32

var globalConverter = NewBytesConverter()

type BytesConverter struct {
	pointers  sync.Map // leaseId -> unsafe.Pointer
	nextLease int32
}

func NewBytesConverter() *BytesConverter {
	return &BytesConverter{
		pointers:  sync.Map{},
		nextLease: 0,
	}
}

func (converter *BytesConverter) add(p unsafe.Pointer) int32 {
	lease := atomic.AddInt32(&converter.nextLease, 1)
	converter.pointers.Store(lease, p)

	return lease
}

// Return a lease and []byte from C bytes (typically char*)
// which references the same memory of C bytes
// Call Release(lease) after you don't need the returned []byte
func (converter *BytesConverter) UnsafeGoBytes(cbytes *unsafe.Pointer, len int) (int32, []byte) {
	var (
		goBytes []byte = nil
		lease   int32  = 0
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
	pI, ok := converter.pointers.LoadAndDelete(lease)
	if !ok {
		panic("try to release the resource that doesn't exist")
	}

	p, ok := pI.(unsafe.Pointer)
	if !ok {
		panic("incorrect value type")
	}

	C.free(p)
}

// Make sure only the caller own the converter
// or this would release someone's memory
func (converter *BytesConverter) ReleaseAll() {
	converter.pointers.Range(func(key, value interface{}) bool {
		pointer := value.(unsafe.Pointer)

		converter.pointers.Delete(key)
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

// DO NOT provide ReleaseAll() method for global converter
