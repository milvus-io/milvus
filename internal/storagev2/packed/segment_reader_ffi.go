// Copyright 2024 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
*/
import "C"

import (
	"io"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/arrio"
	"github.com/apache/arrow/go/v17/arrow/cdata"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// FFISegmentReader reads a StorageV3 segment through milvus-storage's
// LOB-aware SegmentReader. Unlike FFIPackedReader, TEXT columns are returned
// as UTF-8 values instead of encoded LOB references.
type FFISegmentReader struct {
	handle       C.LoonSegmentReaderHandle
	recordReader arrio.Reader
	schema       *arrow.Schema
}

// NewFFISegmentReader opens a manifest and returns only neededColumns. The
// full schema is exported to milvus-storage so it can resolve the physical
// column groups, while the imported Arrow stream uses the extracted schema.
func NewFFISegmentReader(
	manifestPath string,
	schema *arrow.Schema,
	neededColumns []string,
	textColumns []TextColumnConfig,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
) (*FFISegmentReader, error) {
	if storageConfig == nil {
		return nil, merr.WrapErrServiceInternalMsg("storage config is required for segment reader")
	}
	basePath, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return nil, merr.Wrap(err, "parse segment reader manifest")
	}
	extractedSchema, err := extractArrowSchema(schema, neededColumns)
	if err != nil {
		return nil, err
	}

	var cas cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cas)
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))
	defer cdata.ReleaseCArrowSchema(&cas)

	cNeededColumns, numColumns, freeNeededColumns := newCNeededColumns(neededColumns)
	defer freeNeededColumns()

	cConfig, err := buildCSegmentReaderConfig(textColumns, bufferSize)
	if err != nil {
		return nil, err
	}
	defer freeCSegmentReaderConfig(cConfig)

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, err
	}
	defer C.loon_properties_free(cProperties)

	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	var handle C.LoonSegmentReaderHandle
	result := C.loon_segment_reader_open(
		cBasePath,
		C.int64_t(version),
		cSchema,
		(**C.char)(unsafe.Pointer(cNeededColumns)),
		numColumns,
		cConfig,
		cProperties,
		&handle,
	)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, merr.WrapErrStorage(err, "open LOB-aware segment reader")
	}

	var cStream cdata.CArrowArrayStream
	result = C.loon_segment_reader_get_stream(handle, (*C.struct_ArrowArrayStream)(unsafe.Pointer(&cStream)))
	if err := HandleLoonFFIResult(result); err != nil {
		C.loon_segment_reader_destroy(handle)
		return nil, merr.WrapErrStorage(err, "get LOB-aware segment reader stream")
	}

	recordReader, err := cdata.ImportCRecordReader(&cStream, extractedSchema)
	if err != nil {
		C.loon_segment_reader_destroy(handle)
		return nil, merr.WrapErrStorage(err, "import LOB-aware segment reader stream")
	}

	return &FFISegmentReader{
		handle:       handle,
		recordReader: recordReader,
		schema:       extractedSchema,
	}, nil
}

func extractArrowSchema(schema *arrow.Schema, neededColumns []string) (*arrow.Schema, error) {
	if len(neededColumns) == 0 {
		return schema, nil
	}
	wanted := make(map[string]struct{}, len(neededColumns))
	for _, name := range neededColumns {
		wanted[name] = struct{}{}
	}
	fields := make([]arrow.Field, 0, len(neededColumns))
	for _, field := range schema.Fields() {
		if _, ok := wanted[field.Name]; ok {
			fields = append(fields, field)
			delete(wanted, field.Name)
		}
	}
	if len(wanted) != 0 {
		return nil, merr.WrapErrServiceInternalMsg("segment reader schema is missing %d requested columns", len(wanted))
	}
	return arrow.NewSchema(fields, nil), nil
}

func buildCSegmentReaderConfig(textColumns []TextColumnConfig, bufferSize int64) (*C.LoonSegmentReaderConfig, error) {
	cConfig := (*C.LoonSegmentReaderConfig)(C.calloc(1, C.size_t(C.sizeof_LoonSegmentReaderConfig)))
	if cConfig == nil {
		return nil, merr.Wrap(merr.ErrServiceResourceInsufficient, "allocate LOB-aware segment reader config")
	}
	cConfig.read_buffer_size = C.int64_t(bufferSize)
	cConfig.num_lob_columns = C.size_t(len(textColumns))
	if len(textColumns) == 0 {
		cConfig.lob_columns = nil
		return cConfig, nil
	}

	cTextColumns := (*C.LoonLobColumnConfig)(C.calloc(C.size_t(len(textColumns)), C.size_t(C.sizeof_LoonLobColumnConfig)))
	if cTextColumns == nil {
		freeCSegmentReaderConfig(cConfig)
		return nil, merr.Wrap(merr.ErrServiceResourceInsufficient, "allocate LOB-aware segment reader column config")
	}
	cConfig.lob_columns = cTextColumns
	configs := unsafe.Slice(cTextColumns, len(textColumns))
	for i, textColumn := range textColumns {
		configs[i].field_id = C.int64_t(textColumn.FieldID)
		configs[i].lob_base_path = C.CString(textColumn.LobBasePath)
		if configs[i].lob_base_path == nil {
			freeCSegmentReaderConfig(cConfig)
			return nil, merr.Wrap(merr.ErrServiceResourceInsufficient, "allocate LOB-aware segment reader column path")
		}
		configs[i].inline_threshold = C.int64_t(textColumn.InlineThreshold)
		configs[i].max_lob_file_bytes = C.int64_t(textColumn.MaxLobFileBytes)
		configs[i].flush_threshold_bytes = C.int64_t(textColumn.FlushThresholdBytes)
		configs[i].rewrite_mode = C.bool(textColumn.RewriteMode)
	}
	return cConfig, nil
}

func freeCSegmentReaderConfig(cConfig *C.LoonSegmentReaderConfig) {
	if cConfig == nil {
		return
	}
	if cConfig.lob_columns != nil {
		configs := unsafe.Slice(cConfig.lob_columns, int(cConfig.num_lob_columns))
		for i := range configs {
			if configs[i].lob_base_path != nil {
				C.free(unsafe.Pointer(configs[i].lob_base_path))
			}
		}
		C.free(unsafe.Pointer(cConfig.lob_columns))
	}
	C.free(unsafe.Pointer(cConfig))
}

func (r *FFISegmentReader) ReadNext() (arrow.Record, error) {
	if r == nil || r.recordReader == nil {
		return nil, io.EOF
	}
	record, err := r.recordReader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, merr.WrapErrStorage(err, "read LOB-aware segment batch")
	}
	return record, nil
}

func (r *FFISegmentReader) Schema() *arrow.Schema {
	if r == nil {
		return nil
	}
	return r.schema
}

func (r *FFISegmentReader) Close() error {
	if r == nil || r.handle == 0 {
		return nil
	}
	r.recordReader = nil
	C.loon_segment_reader_destroy(r.handle)
	r.handle = 0
	return nil
}
