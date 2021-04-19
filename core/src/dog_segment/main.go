package main

/*

#cgo CFLAGS: -I./

#cgo LDFLAGS: -L/home/sheep/workspace/milvus/sheep/suvlim/core/cmake-build-debug/src/dog_segment -lmilvus_dog_segment -Wl,-rpath=/home/sheep/workspace/milvus/sheep/suvlim/core/cmake-build-debug/src/dog_segment

#include "cwrap.h"

*/
import "C"
import (
	"fmt"
	"unsafe"
)

func testInsert() {
	const DIM = 4
	const N = 3

	var ids = [N]uint64{1, 2, 3}
	var timestamps = [N]uint64{0, 0, 0}

	var vec = [DIM]float32{1.1, 2.2, 3.3, 4.4}
	var rawData []int8

	for i := 0; i <= N; i++ {
		for _, ele := range vec {
			rawData=append(rawData, int8(ele))
		}
		rawData=append(rawData, int8(i))
	}

	var segment = C.SegmentBaseInit()
	fmt.Println(segment)

	const sizeofPerRow = 4 + DIM * 4
	var res = C.Insert(segment, N, (*C.ulong)(&ids[0]), (*C.ulong)(&timestamps[0]), unsafe.Pointer(&rawData[0]), C.int(sizeofPerRow), C.long(N))
	fmt.Println(res)
}

func main() {
	fmt.Println("Test milvus segment base:")
	testInsert()
}
