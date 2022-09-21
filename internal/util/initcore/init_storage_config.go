// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package initcore

/*
#cgo pkg-config: milvus_common

#include <stdlib.h>
#include <stdint.h>
#include "common/init_c.h"
*/
import "C"
import (
	"os"
	"path/filepath"
	"strings"
	"unsafe"

	"github.com/milvus-io/milvus/internal/util/paramtable"
)

func InitMinioConfig(params *paramtable.ComponentParam) {
	CMinioAddress := C.CString(params.MinioCfg.Address)
	C.MinioAddressInit(CMinioAddress)
	C.free(unsafe.Pointer(CMinioAddress))

	CMinioAccessKey := C.CString(params.MinioCfg.AccessKeyID)
	C.MinioAccessKeyInit(CMinioAccessKey)
	C.free(unsafe.Pointer(CMinioAccessKey))

	CMinioAccessValue := C.CString(params.MinioCfg.SecretAccessKey)
	C.MinioAccessValueInit(CMinioAccessValue)
	C.free(unsafe.Pointer(CMinioAccessValue))

	CUseSSL := C.bool(params.MinioCfg.UseSSL)
	C.MinioSSLInit(CUseSSL)

	CUseIam := C.bool(params.MinioCfg.UseIAM)
	C.MinioUseIamInit(CUseIam)

	CMinioBucketName := C.CString(strings.TrimLeft(params.MinioCfg.BucketName, "/"))
	C.MinioBucketNameInit(CMinioBucketName)
	C.free(unsafe.Pointer(CMinioBucketName))

	CMinioRootPath := C.CString(params.MinioCfg.RootPath)
	C.MinioRootPathInit(CMinioRootPath)
	C.free(unsafe.Pointer(CMinioRootPath))
}

func InitLocalStorageConfig(params *paramtable.ComponentParam) {
	b, _ := os.Getwd()
	LocalRootPath := filepath.Dir(b) + "/" + filepath.Base(b) + "/" + "data/"
	CLocalRootPath := C.CString(LocalRootPath)
	C.LocalRootPathInit(CLocalRootPath)
	C.free(unsafe.Pointer(CLocalRootPath))
}
