package cgo

/*
#cgo pkg-config: milvus_futures

#include "futures/future_c.h"
*/
import "C"

type FollyInit struct {
	cFollyInit *C.CFollyInit
}

func NewFollyInit() *FollyInit {
	return &FollyInit{
		cFollyInit: C.folly_init(),
	}
}

func (f *FollyInit) Destroy() {
	C.folly_init_destroy(f.cFollyInit)
}
