// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package main

/*

#cgo CFLAGS: -I${SRCDIR}/../../internal/core/output/include

#cgo LDFLAGS: -L${SRCDIR}/../../internal/core/output/lib -lmilvus_indexbuilder -Wl,-rpath=${SRCDIR}/../../internal/core/output/lib

#include <stdlib.h>
#include "indexbuilder/codec_c.h"
#include "indexbuilder/index_c.h"
*/
import "C"
import (
	"fmt"
	"os"
	"syscall"
	"unsafe"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/proto/indexcgopb"
)

const metaKey = "SLICE_META"

func printIndexFileMeta(filename string) error {
	fd, err := os.OpenFile(filename, os.O_RDONLY, 0400)
	if err != nil {
		return err
	}
	defer fd.Close()

	fileInfo, err := fd.Stat()
	if err != nil {
		return err
	}

	fmt.Printf("file size = %d\n", fileInfo.Size())

	b, err := syscall.Mmap(int(fd.Fd()), 0, int(fileInfo.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil
	}
	defer syscall.Munmap(b)

	fmt.Printf("buf size = %d\n", len(b))

	binarySet := &indexcgopb.BinarySet{
		Datas: []*indexcgopb.Binary{
			{
				Key:   metaKey,
				Value: b,
			},
		},
	}

	datas, err := proto.Marshal(binarySet)
	if err != nil {
		return err
	}

	var cBinary C.CBinary
	status := C.AssembleBinarySet((*C.char)(unsafe.Pointer(&datas[0])), (C.int32_t)(len(datas)), &cBinary)
	defer func() {
		if cBinary != nil {
			C.DeleteCBinary(cBinary)
		}
	}()

	errorCode := status.error_code
	if errorCode != 0 {
		errorMsg := C.GoString(status.error_msg)
		defer C.free(unsafe.Pointer(status.error_msg))
		return fmt.Errorf("failed, C runtime error detected, error code = %d, err msg = %s", errorCode, errorMsg)
	}

	binarySize := C.GetCBinarySize(cBinary)
	binaryData := make([]byte, binarySize)
	C.GetCBinaryData(cBinary, unsafe.Pointer(&binaryData[0]))

	var blobs indexcgopb.BinarySet
	err = proto.Unmarshal(binaryData, &blobs)
	if err != nil {
		return err
	}

	for _, data := range blobs.Datas {
		fmt.Printf("key: %v, value: %v\n", data.Key, string(data.Value))
	}

	return nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("usage: parse_index_file_meta file")
		return
	}

	if err := printIndexFileMeta(os.Args[1]); err != nil {
		fmt.Printf("error: %s\n", err.Error())
	} else {
		fmt.Printf("print binlog complete.\n")
	}
}
