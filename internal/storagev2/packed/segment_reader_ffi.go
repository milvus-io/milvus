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
#cgo pkg-config: milvus-storage

#include <stdlib.h>
#include "milvus-storage/ffi_c.h"
#include "arrow/c/abi.h"
*/
import "C"

import (
	"io"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// FFISegmentReader resolves TEXT LOB references through the partition paths in
// textColumns and returns logical UTF8 columns. Compaction uses it when source
// and output manifests belong to different partition namespaces.
type FFISegmentReader struct {
	handle       C.LoonSegmentReaderHandle
	recordReader arrayRecordReader
	schema       *arrow.Schema
}

type arrayRecordReader interface {
	Read() (arrow.Record, error)
}

func NewFFISegmentReader(
	manifestPath string,
	schema *arrow.Schema,
	neededColumns []string,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	textColumns []TextColumnConfig,
) (*FFISegmentReader, error) {
	if storageConfig == nil {
		return nil, merr.WrapErrStorageMsg("storageConfig must not be nil")
	}
	basePath, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return nil, err
	}

	var cas cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cas)
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))
	defer cdata.ReleaseCArrowSchema(&cas)

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, err
	}
	defer C.loon_properties_free(cProperties)

	cConfig, cleanupConfig := buildCSegmentReaderConfig(textColumns, bufferSize)
	defer cleanupConfig()
	cNeededColumns, numColumns, cleanupColumns := buildCSegmentReaderColumns(neededColumns)
	defer cleanupColumns()
	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	var handle C.LoonSegmentReaderHandle
	result := C.loon_segment_reader_open(
		cBasePath,
		C.int64_t(version),
		cSchema,
		cNeededColumns,
		numColumns,
		cConfig,
		cProperties,
		&handle,
	)
	if err := HandleLoonFFIResult(result); err != nil {
		return nil, err
	}

	var stream cdata.CArrowArrayStream
	result = C.loon_segment_reader_get_stream(handle, (*C.struct_ArrowArrayStream)(unsafe.Pointer(&stream)))
	if err := HandleLoonFFIResult(result); err != nil {
		C.loon_segment_reader_destroy(handle)
		return nil, err
	}
	recordReader, err := cdata.ImportCRecordReader(&stream, schema)
	if err != nil {
		C.loon_segment_reader_destroy(handle)
		return nil, merr.WrapErrStorage(err, "failed to import TEXT-decoding segment reader")
	}

	return &FFISegmentReader{handle: handle, recordReader: recordReader, schema: schema}, nil
}

func buildCSegmentReaderConfig(textColumns []TextColumnConfig, bufferSize int64) (*C.LoonSegmentReaderConfig, func()) {
	cConfig := (*C.LoonSegmentReaderConfig)(C.calloc(1, C.sizeof_LoonSegmentReaderConfig))
	cConfig.read_buffer_size = C.int64_t(bufferSize)
	cConfig.num_lob_columns = C.size_t(len(textColumns))
	if len(textColumns) == 0 {
		cConfig.lob_columns = nil
		return cConfig, func() { C.free(unsafe.Pointer(cConfig)) }
	}

	cColumns := (*C.LoonLobColumnConfig)(C.calloc(C.size_t(len(textColumns)), C.sizeof_LoonLobColumnConfig))
	columnSlice := unsafe.Slice(cColumns, len(textColumns))
	for i, column := range textColumns {
		columnSlice[i].field_id = C.int64_t(column.FieldID)
		columnSlice[i].lob_base_path = C.CString(column.LobBasePath)
	}
	cConfig.lob_columns = cColumns
	return cConfig, func() {
		for i := range columnSlice {
			C.free(unsafe.Pointer(columnSlice[i].lob_base_path))
		}
		C.free(unsafe.Pointer(cColumns))
		C.free(unsafe.Pointer(cConfig))
	}
}

func buildCSegmentReaderColumns(columns []string) (**C.char, C.int64_t, func()) {
	cColumns := make([]*C.char, len(columns))
	for i, column := range columns {
		cColumns[i] = C.CString(column)
	}
	cleanup := func() {
		for _, column := range cColumns {
			C.free(unsafe.Pointer(column))
		}
	}
	if len(cColumns) == 0 {
		return nil, 0, cleanup
	}
	return (**C.char)(unsafe.Pointer(&cColumns[0])), C.int64_t(len(cColumns)), cleanup
}

func (r *FFISegmentReader) ReadNext() (arrow.Record, error) {
	if r.recordReader == nil {
		return nil, io.EOF
	}
	record, err := r.recordReader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, merr.WrapErrStorage(err, "failed to read TEXT-decoded segment batch")
	}
	return record, nil
}

func (r *FFISegmentReader) Close() error {
	if r.handle == 0 {
		return nil
	}
	r.recordReader = nil
	C.loon_segment_reader_destroy(r.handle)
	r.handle = 0
	return nil
}

func (r *FFISegmentReader) Schema() *arrow.Schema {
	return r.schema
}
