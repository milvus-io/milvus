// Copyright 2026 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package native

/*
#cgo pkg-config: milvus-storage
#include <stdlib.h>
#include "milvus-storage/ffi_c.h"
*/
import "C"

import (
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/storagev2"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// makeProperties uses only the caller's config. It does not read Milvus globals
// or install process-wide credentials. The filesystem namespace remains bucket
// relative (or absolute on local storage), matching manifest and object keys.
func makeProperties(cfg *indexpb.StorageConfig) (*C.LoonProperties, error) {
	if cfg == nil {
		return nil, merr.WrapErrParameterMissingMsg("source storage config is required for native manifests")
	}
	var keys, values []*C.char
	for key, value := range storagev2.StorageProperties(cfg) {
		keys = append(keys, C.CString(key))
		values = append(values, C.CString(value))
	}
	defer func() {
		for i := range keys {
			C.free(unsafe.Pointer(keys[i]))
			C.free(unsafe.Pointer(values[i]))
		}
	}()
	properties := &C.LoonProperties{}
	if err := handleResult(C.loon_properties_create(&keys[0], &values[0], C.size_t(len(keys)), properties)); err != nil {
		return nil, merr.Wrap(err, "create native manifest properties")
	}
	return properties, nil
}

func handleResult(result C.LoonFFIResult) error {
	defer C.loon_ffi_free_result(&result)
	if C.loon_ffi_is_success(&result) != 0 {
		return nil
	}
	message := C.GoString(C.loon_ffi_get_errmsg(&result))
	if C.loon_ffi_is_retryable_errcode(C.int(result.err_code)) != 0 {
		return merr.Wrapf(objectstorage.ErrLoonTransient, "manifest operation failed (code=%d): %s", int(result.err_code), message)
	}
	return merr.Wrapf(objectstorage.ErrLoonPermanent, "manifest operation failed (code=%d): %s", int(result.err_code), message)
}

func openTransaction(base string, version int64, config *indexpb.StorageConfig) (C.LoonTransactionHandle, error) {
	var handle C.LoonTransactionHandle
	properties, err := makeProperties(config)
	if err != nil {
		return handle, err
	}
	defer C.loon_properties_free(properties)
	cBase := C.CString(base)
	defer C.free(unsafe.Pointer(cBase))
	resolver := C.int32_t(C.LOON_TRANSACTION_RESOLVE_FAIL)
	err = handleResult(C.loon_transaction_open(cBase, properties, C.int64_t(version), C.int64_t(-1), resolver, C.uint32_t(1), &handle))
	return handle, err
}

func GetManifestHandle(descriptor string, config *indexpb.StorageConfig) (*C.LoonManifest, error) {
	base, version, err := UnmarshalManifestPath(descriptor)
	if err != nil {
		return nil, err
	}
	handle, err := openTransaction(base, version, config)
	if err != nil {
		return nil, err
	}
	defer C.loon_transaction_destroy(handle)
	var manifest *C.LoonManifest
	if err := handleResult(C.loon_transaction_get_manifest(handle, &manifest)); err != nil {
		return nil, err
	}
	return manifest, nil
}

func UnmarshalManifestPath(descriptor string) (string, int64, error) {
	base, version, err := storagev2.UnmarshalManifestPath(descriptor)
	if err != nil {
		return "", 0, merr.WrapErrDataIntegrity(err, "invalid manifest descriptor")
	}
	return base, version, nil
}
