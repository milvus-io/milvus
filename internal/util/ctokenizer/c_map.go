package ctokenizer

/*
#cgo pkg-config: milvus_core
#include <stdlib.h>	// free
#include "segcore/map_c.h"
*/
import "C"
import "unsafe"

type CMap struct {
	ptr C.CMap
}

func NewCMap() *CMap {
	return &CMap{
		ptr: C.create_cmap(),
	}
}

func (m *CMap) GetPointer() C.CMap {
	return m.ptr
}

func (m *CMap) Set(key string, value string) {
	cKey := C.CString(key)
	defer C.free(unsafe.Pointer(cKey))

	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))

	C.cmap_set(m.ptr, cKey, (C.uint32_t)(len(key)), cValue, (C.uint32_t)(len(value)))
}

func (m *CMap) From(gm map[string]string) {
	for k, v := range gm {
		m.Set(k, v)
	}
}

func (m *CMap) Destroy() {
	if m.ptr != nil {
		C.free_cmap(m.ptr)
	}
}
