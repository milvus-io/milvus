// Copyright 2024 Zilliz
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
#include "milvus-storage/ffi_c.h"
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"
*/
import "C"

import (
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"

	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
)

// TextColumnConfig represents configuration for a TEXT column.
type TextColumnConfig struct {
	FieldID             int64
	LobBasePath         string
	InlineThreshold     int64
	MaxLobFileBytes     int64
	FlushThresholdBytes int64
}

// SegmentWriterConfig represents configuration for SegmentWriter.
type SegmentWriterConfig struct {
	SegmentPath string
	LobBasePath string
	ReadVersion int64
	RetryLimit  uint32
	TextColumns []TextColumnConfig
}

// SegmentWriterResult contains the result of closing a SegmentWriter.
type SegmentWriterResult struct {
	ManifestPath     string
	CommittedVersion int64
	RowsWritten      int64
}

// FFISegmentWriter wraps the C SegmentWriter handle for incremental writes.
type FFISegmentWriter struct {
	handle      C.LoonSegmentWriterHandle
	cProperties *C.LoonProperties
	schema      *arrow.Schema
}

// NewFFISegmentWriter creates a new segment writer via FFI.
func NewFFISegmentWriter(
	schema *arrow.Schema,
	config *SegmentWriterConfig,
	storageConfig *indexpb.StorageConfig,
) (*FFISegmentWriter, error) {
	// export schema to C Arrow format
	var cas cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cas)
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))
	defer cdata.ReleaseCArrowSchema(&cas)

	// create storage config if not provided
	if storageConfig == nil {
		storageConfig = createStorageConfig()
	}

	// create properties
	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, err
	}

	// build C config
	cConfig := buildCSegmentWriterConfig(config)
	defer freeCSegmentWriterConfig(cConfig)

	// create writer
	var writerHandle C.LoonSegmentWriterHandle
	result := C.loon_segment_writer_new(cSchema, cConfig, cProperties, &writerHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		C.loon_properties_free(cProperties)
		return nil, err
	}

	return &FFISegmentWriter{
		handle:      writerHandle,
		cProperties: cProperties,
		schema:      schema,
	}, nil
}

// Write writes a record batch to the segment writer.
func (w *FFISegmentWriter) Write(record arrow.Record) error {
	var caa cdata.CArrowArray
	var cas cdata.CArrowSchema

	cdata.ExportArrowRecordBatch(record, &caa, &cas)
	defer cdata.ReleaseCArrowArray(&caa)
	defer cdata.ReleaseCArrowSchema(&cas)

	cArray := (*C.struct_ArrowArray)(unsafe.Pointer(&caa))

	result := C.loon_segment_writer_write(w.handle, cArray)
	return HandleLoonFFIResult(result)
}

// Flush flushes buffered data to storage.
func (w *FFISegmentWriter) Flush() error {
	result := C.loon_segment_writer_flush(w.handle)
	return HandleLoonFFIResult(result)
}

// Close closes the writer and commits the segment.
func (w *FFISegmentWriter) Close() (*SegmentWriterResult, error) {
	var cResult C.LoonSegmentWriterResult

	result := C.loon_segment_writer_close(w.handle, &cResult)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, err
	}
	defer C.loon_segment_writer_result_free(&cResult)

	return &SegmentWriterResult{
		ManifestPath:     C.GoString(cResult.manifest_path),
		CommittedVersion: int64(cResult.committed_version),
		RowsWritten:      int64(cResult.rows_written),
	}, nil
}

// Abort aborts the write operation without committing.
func (w *FFISegmentWriter) Abort() error {
	result := C.loon_segment_writer_abort(w.handle)
	return HandleLoonFFIResult(result)
}

// Destroy destroys the writer and releases resources.
func (w *FFISegmentWriter) Destroy() {
	if w.handle != 0 {
		C.loon_segment_writer_destroy(w.handle)
		w.handle = 0
	}
	if w.cProperties != nil {
		C.loon_properties_free(w.cProperties)
		w.cProperties = nil
	}
}

// buildCSegmentWriterConfig converts Go config to C config.
func buildCSegmentWriterConfig(config *SegmentWriterConfig) *C.LoonSegmentWriterConfig {
	cConfig := (*C.LoonSegmentWriterConfig)(C.malloc(C.sizeof_LoonSegmentWriterConfig))

	if config.SegmentPath != "" {
		cConfig.segment_path = C.CString(config.SegmentPath)
	} else {
		cConfig.segment_path = nil
	}

	if config.LobBasePath != "" {
		cConfig.lob_base_path = C.CString(config.LobBasePath)
	} else {
		cConfig.lob_base_path = nil
	}

	cConfig.read_version = C.int64_t(config.ReadVersion)
	cConfig.retry_limit = C.uint32_t(config.RetryLimit)

	// build text column configs
	numTextColumns := len(config.TextColumns)
	cConfig.num_text_columns = C.size_t(numTextColumns)
	if numTextColumns > 0 {
		cTextColumns := (*C.LoonTextColumnConfig)(C.malloc(C.size_t(numTextColumns) * C.sizeof_LoonTextColumnConfig))
		textColumnsSlice := unsafe.Slice(cTextColumns, numTextColumns)

		for i, tc := range config.TextColumns {
			textColumnsSlice[i].field_id = C.int64_t(tc.FieldID)
			if tc.LobBasePath != "" {
				textColumnsSlice[i].lob_base_path = C.CString(tc.LobBasePath)
			} else {
				textColumnsSlice[i].lob_base_path = nil
			}
			textColumnsSlice[i].inline_threshold = C.int64_t(tc.InlineThreshold)
			textColumnsSlice[i].max_lob_file_bytes = C.int64_t(tc.MaxLobFileBytes)
			textColumnsSlice[i].flush_threshold_bytes = C.int64_t(tc.FlushThresholdBytes)
		}
		cConfig.text_columns = cTextColumns
	} else {
		cConfig.text_columns = nil
	}

	return cConfig
}

// freeCSegmentWriterConfig frees the C config memory.
func freeCSegmentWriterConfig(cConfig *C.LoonSegmentWriterConfig) {
	if cConfig == nil {
		return
	}

	if cConfig.segment_path != nil {
		C.free(unsafe.Pointer(cConfig.segment_path))
	}
	if cConfig.lob_base_path != nil {
		C.free(unsafe.Pointer(cConfig.lob_base_path))
	}

	// free text column configs
	if cConfig.text_columns != nil {
		textColumnsSlice := unsafe.Slice(cConfig.text_columns, int(cConfig.num_text_columns))
		for i := range textColumnsSlice {
			if textColumnsSlice[i].lob_base_path != nil {
				C.free(unsafe.Pointer(textColumnsSlice[i].lob_base_path))
			}
		}
		C.free(unsafe.Pointer(cConfig.text_columns))
	}

	C.free(unsafe.Pointer(cConfig))
}
