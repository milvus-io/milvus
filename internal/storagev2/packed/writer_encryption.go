// Copyright 2023 Zilliz
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

package packed

/*
#cgo pkg-config: milvus_core milvus-storage
#include <stdlib.h>
#include "storage/loon_ffi/ffi_writer_c.h"
*/
import "C"

import "unsafe"

// WriterEncryptionProperties prepares a fresh data key using the collection
// context registered by QueryNode's Collection.Ref. The caller must hold that
// reference while preparing and using the writer configuration.
func WriterEncryptionProperties(ezID, collectionID int64) (map[string]string, error) {
	return writerEncryptionProperties(ezID, collectionID, nil)
}

func writerEncryptionProperties(ezID, collectionID int64, encryptionKey *string) (map[string]string, error) {
	var pluginContext C.CPluginContext
	pluginContext.ez_id = C.int64_t(ezID)
	pluginContext.collection_id = C.int64_t(collectionID)
	if encryptionKey != nil {
		pluginContext.key = C.CString(*encryptionKey)
		defer C.free(unsafe.Pointer(pluginContext.key))
	}
	var key, metadata *C.char
	status := C.GetEncParams(&pluginContext, &key, &metadata)
	defer C.free(unsafe.Pointer(key))
	defer C.free(unsafe.Pointer(metadata))
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	return map[string]string{
		PropertyWriterEncEnable: "true",
		// GetEncParams returns Base64 text, decoded by the Parquet writer.
		PropertyWriterEncKey:  C.GoString(key),
		PropertyWriterEncMeta: C.GoString(metadata),
		PropertyWriterEncAlgo: "AES_GCM_V1",
	}, nil
}
