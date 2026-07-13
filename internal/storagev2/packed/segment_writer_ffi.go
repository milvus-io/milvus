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
	"context"
	"strings"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storageprofile"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TextColumnConfig represents configuration for a TEXT column.
type TextColumnConfig struct {
	FieldID             int64
	LobBasePath         string
	InlineThreshold     int64
	MaxLobFileBytes     int64
	FlushThresholdBytes int64
	RewriteMode         bool // true = input is LOB references, decode & rewrite during compaction
}

// SegmentWriterConfig represents configuration for SegmentWriter. The
// writer is concerned only with file output; manifest-level concerns
// (version, retry) live in CommitManifestUpdates.
type SegmentWriterConfig struct {
	SegmentPath        string
	TextColumns        []TextColumnConfig
	ColumnGroups       []storagecommon.ColumnGroup
	WriterFormat       string
	SchemaBasedPattern string
	SchemaBasedFormats []string
}

// FFISegmentWriter wraps the C SegmentWriter handle for incremental writes.
type FFISegmentWriter struct {
	handle      C.LoonSegmentWriterHandle
	cProperties *C.LoonProperties
	schema      *arrow.Schema
	closed      bool
	profileCtx  context.Context
}

// NewFFISegmentWriter creates a new segment writer via FFI.
func NewFFISegmentWriter(
	schema *arrow.Schema,
	config *SegmentWriterConfig,
	storageConfig *indexpb.StorageConfig,
	profileContexts ...context.Context,
) (*FFISegmentWriter, error) {
	profileCtx := packedProfileContext(firstProfileContext(profileContexts), storageConfig)
	// export schema to C Arrow format
	var cas cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cas)
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))
	defer cdata.ReleaseCArrowSchema(&cas)

	if storageConfig == nil {
		return nil, merr.WrapErrStorageMsg("storageConfig must not be nil")
	}

	extra := segmentWriterProperties(schema, config)
	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, extra)
	if err != nil {
		return nil, err
	}

	// build C config
	cConfig := buildCSegmentWriterConfig(config)
	defer freeCSegmentWriterConfig(cConfig)

	// create writer
	var writerHandle C.LoonSegmentWriterHandle
	operation := beginPackedOperation(profileCtx, storageprofile.StorageOperationStat, storageprofile.WorkloadPhaseWriteMetadata, 0, false)
	result := C.loon_segment_writer_new(cSchema, cConfig, cProperties, &writerHandle)
	if err := HandleLoonFFIResult(result); err != nil {
		operation.Finish(storageprofile.OperationResult{Err: err})
		C.loon_properties_free(cProperties)
		return nil, err
	}
	operation.Finish(storageprofile.OperationResult{})

	return &FFISegmentWriter{
		handle:      writerHandle,
		cProperties: cProperties,
		schema:      schema,
		profileCtx:  profileCtx,
	}, nil
}

func segmentWriterProperties(schema *arrow.Schema, config *SegmentWriterConfig) map[string]string {
	extra := map[string]string{
		PropertyWriterFormat: "parquet",
	}
	if config == nil {
		extra[PropertyWriterPolicy] = "single"
		return extra
	}
	if config.WriterFormat != "" {
		extra[PropertyWriterFormat] = config.WriterFormat
	}
	if len(config.SchemaBasedFormats) > 0 {
		extra[PropertyWriterSchemaBasedFormats] = strings.Join(config.SchemaBasedFormats, ",")
	}
	if config.SchemaBasedPattern != "" {
		extra[PropertyWriterPolicy] = "schema_based"
		extra[PropertyWriterSchemaBasedPattern] = config.SchemaBasedPattern
		return extra
	}

	columnGroups := config.ColumnGroups
	if len(columnGroups) == 0 {
		extra[PropertyWriterPolicy] = "single"
		return extra
	}

	pattern := strings.Join(lo.Map(columnGroups, func(columnGroup storagecommon.ColumnGroup, _ int) string {
		return strings.Join(lo.Map(columnGroup.Columns, func(index int, _ int) string {
			return schema.Field(index).Name
		}), "|")
	}), ",")

	extra[PropertyWriterPolicy] = "schema_based"
	extra[PropertyWriterSchemaBasedPattern] = pattern
	return extra
}

// Write writes a record batch to the segment writer.
func (w *FFISegmentWriter) Write(record arrow.Record) error {
	size := arrowRecordBytes(record)
	operation := beginPackedOperation(w.profileCtx, storageprofile.StorageOperationWrite, storageprofile.WorkloadPhaseWriteOutput, size, true)
	var caa cdata.CArrowArray
	var cas cdata.CArrowSchema

	cdata.ExportArrowRecordBatch(record, &caa, &cas)
	defer cdata.ReleaseCArrowArray(&caa)
	defer cdata.ReleaseCArrowSchema(&cas)

	cArray := (*C.struct_ArrowArray)(unsafe.Pointer(&caa))

	result := C.loon_segment_writer_write(w.handle, cArray)
	err := HandleLoonFFIResult(result)
	if err == nil {
		operation.AddCompletedBytes(size)
	}
	operation.Finish(storageprofile.OperationResult{Err: err, SizeKnown: true})
	return err
}

// SyncBuffered syncs buffered data to storage without closing the writer.
// Used for mid-stream flushes; Close detaches output and finalizes the writer.
func (w *FFISegmentWriter) SyncBuffered() error {
	operation := beginPackedOperation(w.profileCtx, storageprofile.StorageOperationWrite, storageprofile.WorkloadPhaseWriteOutput, 0, false)
	result := C.loon_segment_writer_flush(w.handle)
	err := HandleLoonFFIResult(result)
	operation.Finish(storageprofile.OperationResult{Err: err})
	return err
}

// SegmentOutput is the data carrier returned by FFISegmentWriter.Close.
// It holds the column-groups + LOB payload produced by the C writer and
// owns C memory; the caller MUST call Destroy after passing the handle to
// CommitManifestUpdates (success or failure). Destroy is idempotent; a
// nil column_groups pointer indicates the handle has already been released.
type SegmentOutput struct {
	cOutput     C.LoonSegmentWriteOutput
	rowsWritten int64
}

// RowsWritten returns the number of rows the segment writer reported.
func (f *SegmentOutput) RowsWritten() int64 { return f.rowsWritten }

// Destroy releases the C output (LOB file strings + array). Safe to call
// multiple times — the nil column_groups pointer left behind serves as
// the already-released marker.
func (f *SegmentOutput) Destroy() {
	if f == nil || f.cOutput.column_groups == nil {
		return
	}
	C.loon_segment_write_output_free(&f.cOutput)
	f.cOutput.column_groups = nil
	f.cOutput.lob_files = nil
	f.cOutput.num_lob_files = 0
}

// applyTo stages the column-groups + LOB payload onto a loon transaction.
// Column groups (when present) are appended via loon_transaction_append_files,
// and each LOB file is registered via loon_transaction_add_lob_file.
func (f *SegmentOutput) applyTo(handle C.LoonTransactionHandle) error {
	if f == nil {
		return nil
	}
	if f.cOutput.column_groups != nil {
		if err := HandleLoonFFIResult(C.loon_transaction_append_files(handle, f.cOutput.column_groups)); err != nil {
			return merr.Wrap(err, "commit manifest append_files (segment)")
		}
	}
	if f.cOutput.num_lob_files > 0 && f.cOutput.lob_files != nil {
		lob := unsafe.Slice(f.cOutput.lob_files, f.cOutput.num_lob_files)
		for i := range lob {
			if err := HandleLoonFFIResult(C.loon_transaction_add_lob_file(handle, &lob[i])); err != nil {
				return merr.Wrap(err, "commit manifest add_lob_file")
			}
		}
	}
	return nil
}

// Close closes the underlying segment writer and returns the column-groups
// + LOB payload. The writer never touches the manifest — the caller is
// responsible for passing the returned handle to CommitManifestUpdates
// and calling Destroy on the returned WriterOutput when done.
//
// Close releases the writer's C resources (segment-writer handle and
// cProperties) in a defer, so even when loon_segment_writer_close fails
// those resources are reclaimed. After Close the writer is exhausted;
// further Close or Write calls fail.
func (w *FFISegmentWriter) Close() (WriterOutput, error) {
	if w.closed {
		return nil, merr.WrapErrServiceInternal("FFISegmentWriter already closed")
	}
	w.closed = true
	defer func() {
		if w.handle != 0 {
			C.loon_segment_writer_destroy(w.handle)
			w.handle = 0
		}
		if w.cProperties != nil {
			C.loon_properties_free(w.cProperties)
			w.cProperties = nil
		}
	}()
	var cOutput C.LoonSegmentWriteOutput
	operation := beginPackedOperation(w.profileCtx, storageprofile.StorageOperationWrite, storageprofile.WorkloadPhaseWriteOutput, 0, false)
	result := C.loon_segment_writer_close(w.handle, &cOutput)
	if err := HandleLoonFFIResult(result); err != nil {
		operation.Finish(storageprofile.OperationResult{Err: err})
		return nil, err
	}
	operation.Finish(storageprofile.OperationResult{})
	return &SegmentOutput{
		cOutput:     cOutput,
		rowsWritten: int64(cOutput.rows_written),
	}, nil
}

// buildCSegmentWriterConfig converts Go config to C config.
func buildCSegmentWriterConfig(config *SegmentWriterConfig) *C.LoonSegmentWriterConfig {
	cConfig := (*C.LoonSegmentWriterConfig)(C.malloc(C.sizeof_LoonSegmentWriterConfig))

	if config.SegmentPath != "" {
		cConfig.segment_path = C.CString(config.SegmentPath)
	} else {
		cConfig.segment_path = nil
	}

	// build text column configs
	numTextColumns := len(config.TextColumns)
	cConfig.num_lob_columns = C.size_t(numTextColumns)
	if numTextColumns > 0 {
		cTextColumns := (*C.LoonLobColumnConfig)(C.malloc(C.size_t(numTextColumns) * C.sizeof_LoonLobColumnConfig))
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
			textColumnsSlice[i].rewrite_mode = C.bool(tc.RewriteMode)
		}
		cConfig.lob_columns = cTextColumns
	} else {
		cConfig.lob_columns = nil
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

	// free lob column configs
	if cConfig.lob_columns != nil {
		textColumnsSlice := unsafe.Slice(cConfig.lob_columns, int(cConfig.num_lob_columns))
		for i := range textColumnsSlice {
			if textColumnsSlice[i].lob_base_path != nil {
				C.free(unsafe.Pointer(textColumnsSlice[i].lob_base_path))
			}
		}
		C.free(unsafe.Pointer(cConfig.lob_columns))
	}

	C.free(unsafe.Pointer(cConfig))
}
