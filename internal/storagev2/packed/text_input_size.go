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

package packed

/*
#cgo pkg-config: milvus_core milvus-storage
#include "milvus-storage/ffi_c.h"
#include "storage/loon_ffi/ffi_reader_c.h"
*/
import "C"

import (
	"context"
	"strconv"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// EstimateManifestTextInputSize estimates an internal TEXT index's input from
// one manifest revision. Footer sizes describe the encoded column group (inline
// values and LOB references), regardless of whether the writer's Go statistics
// counted raw TEXT or references. The entire group's estimated memory and the
// target field's full LOB file sizes are charged conservatively, not as exact
// field bytes or process RSS. No TEXT values or LOB payloads are read here.
func EstimateManifestTextInputSize(ctx context.Context, io *ManifestIOContext, manifestPath string, fieldID int64,
	config *indexpb.StorageConfig, pluginContext *indexcgopb.StoragePluginContext,
) (size int64, found bool, err error) {
	manifest, err := getManifestAsync(ctx, io, manifestPath, config)
	if err != nil {
		return 0, false, err
	}
	defer C.loon_manifest_destroy(manifest)

	for _, file := range manifestLobFiles(manifest) {
		if file.FieldID == fieldID {
			size += file.FileSizeBytes
		}
	}
	column := strconv.FormatInt(fieldID, 10)
	groupID := -1
	for i, group := range unsafe.Slice(manifest.column_groups.column_group_array, int(manifest.column_groups.num_of_column_groups)) {
		for _, name := range unsafe.Slice(group.columns, int(group.num_of_columns)) {
			if C.GoString(name) == column {
				groupID = i
				break
			}
		}
		if groupID >= 0 {
			break
		}
	}
	// A nullable field added after the segment was written has no stored bytes,
	// but the worker synthesizes null rows. Let the caller retain its existing
	// row/segment estimate for that supported schema-evolution case.
	if groupID < 0 {
		return 0, false, nil
	}

	// Footer reads run on the coordinator and share the manifest FFI budget.
	// Acquire after the asynchronous manifest read releases its worker slot.
	if err := acquireManifestFFI(ctx); err != nil {
		return 0, false, err
	}
	defer releaseManifestFFI()

	schema := arrow.NewSchema([]arrow.Field{{Name: column, Type: arrow.BinaryTypes.Binary, Nullable: true}}, nil)
	var cSchema cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cSchema)
	defer cdata.ReleaseCArrowSchema(&cSchema)
	cColumns, numColumns, freeColumns := newCNeededColumns([]string{column})
	defer freeColumns()
	cConfig, freeConfig := newCStorageConfig(config)
	defer freeConfig()
	cPlugin, freePlugin := newCPluginContext(pluginContext)
	defer freePlugin()
	var reader C.CFFIPackedReader
	status := C.NewPackedFFIReaderWithManifest(manifest, (*C.struct_ArrowSchema)(unsafe.Pointer(&cSchema)),
		cColumns, numColumns, &reader, cConfig, cPlugin, 0, nil, nil)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return 0, false, merr.Wrap(err, "open TEXT size reader")
	}
	defer C.CloseFFIReader(reader)
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}

	// CFFIPackedReader and LoonReaderHandle both own an api::Reader pointer.
	// The Milvus constructor above also installs the required CMEK retriever;
	// unlike openFFIPackedReader it does not start a data-reading stream.
	var chunks C.LoonChunkReaderHandle
	if err := HandleLoonFFIResult(C.loon_get_chunk_reader(C.LoonReaderHandle(uintptr(reader)),
		C.int64_t(groupID), cColumns, C.size_t(numColumns), &chunks)); err != nil {
		return 0, false, merr.Wrap(err, "read TEXT column-group footer")
	}
	defer C.loon_chunk_reader_destroy(chunks)
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	var count C.uint64_t
	if err := HandleLoonFFIResult(C.loon_get_number_of_chunks(chunks, &count)); err != nil {
		return 0, false, err
	}
	if count == 0 {
		return size, true, nil
	}
	var metadata C.LoonChunkMetadatas
	if err := HandleLoonFFIResult(C.loon_get_chunk_metadatas(chunks, C.LOON_CHUNK_METADATA_ESTIMATED_MEMORY, &metadata)); err != nil {
		return 0, false, merr.Wrap(err, "estimate encoded TEXT column-group size")
	}
	defer C.loon_free_chunk_metadatas(&metadata)
	// Requesting one metadata type returns one entry; each union value is the
	// requested uint64 estimated_memsz. Projections do not narrow group sizes.
	for _, bytes := range unsafe.Slice((*C.uint64_t)(unsafe.Pointer(metadata.metadatas.data)), int(metadata.metadatas.number_of_chunks)) {
		size += int64(bytes)
	}
	return size, true, nil
}
