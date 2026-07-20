package segcore

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "storage/storage_c.h"
*/
import "C"

import (
	"runtime"
	"unsafe"
)

// LocalFileSystem is an owned handle to a rooted C++ local filesystem.
type LocalFileSystem struct {
	ptr C.CLocalFileSystem
}

func NewLocalFileSystem(root string) (*LocalFileSystem, error) {
	cRoot := C.CString(root)
	defer C.free(unsafe.Pointer(cRoot))

	var ptr C.CLocalFileSystem
	status := C.OpenLocalFileSystem(cRoot, &ptr)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	files := &LocalFileSystem{ptr: ptr}
	runtime.SetFinalizer(files, (*LocalFileSystem).Close)
	return files, nil
}

func (f *LocalFileSystem) Close() {
	if f == nil || f.ptr == nil {
		return
	}
	C.CloseLocalFileSystem(f.ptr)
	f.ptr = nil
	runtime.SetFinalizer(f, nil)
}

func (f *LocalFileSystem) rawPointer() C.CLocalFileSystem {
	if f == nil {
		return nil
	}
	return f.ptr
}

func (f *LocalFileSystem) RawPointer() unsafe.Pointer {
	if f == nil {
		return nil
	}
	return unsafe.Pointer(f.ptr)
}
