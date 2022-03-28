package cgoconverter

/*
#include <stdlib.h>
#include <string.h>
*/
import "C"
import "unsafe"

func copyToCBytes(data []byte) unsafe.Pointer {
	return C.CBytes(data)
}

func mallocCBytes(v byte, len int) unsafe.Pointer {
	p := C.malloc(C.size_t(len))
	C.memset(p, C.int(v), C.size_t(len))

	return p
}
