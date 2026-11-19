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
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "segcore/packed_reader_c.h"
#include "milvus-storage/ffi_c.h"
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"

CStatus NewPackedReaderWithProperties(char** paths,
                                      int64_t num_paths,
                                      struct ArrowSchema* schema,
                                      const int64_t buffer_size,
                                      const bool eager_prebuffer,
                                      const LoonProperties* c_properties,
                                      const char* filesystem_path,
                                      CPackedReader* c_packed_reader,
                                      CPluginContext* c_plugin_context);
*/
import "C"

import (
	"io"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ReaderOption tunes how a PackedReader fetches its files.
type ReaderOption func(*readerOptions)

type readerOptions struct {
	eagerPrebuffer   bool
	importReadKeyset bool
}

// WithImportReadKeyset registers the supplied source keyset for decrypting
// packed files without changing the ordinary write keyset.
func WithImportReadKeyset() ReaderOption {
	return func(o *readerOptions) {
		o.importReadKeyset = true
	}
}

// WithEagerPrebuffer makes every read round fetch all of its byte ranges at
// once instead of one coalesced range at a time. How far adjacent ranges are
// coalesced is left as configured (common.arrow.reader.*), so the requests are
// the ones every other reader issues, only together rather than one after
// another. The raw bytes of a round stay cached until the next round either
// way (arrow's read cache never evicts within a round), so for a given buffer
// size this changes how many requests are in flight, not how much a round
// holds; the buffer size is what bounds that memory.
func WithEagerPrebuffer() ReaderOption {
	return func(o *readerOptions) {
		o.eagerPrebuffer = true
	}
}

func NewPackedReader(filePaths []string, schema *arrow.Schema, bufferSize int64, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext) (*PackedReader, error) {
	return NewPackedReaderWithExtfs(filePaths, schema, bufferSize, storageConfig, storagePluginContext, ExternalReaderContext{})
}

// NewPackedReaderWithExtfs opens packed files and optionally resolves them
// through an external filesystem alias described by extfs.
func NewPackedReaderWithExtfs(
	filePaths []string,
	schema *arrow.Schema,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	extfs ExternalReaderContext,
	opts ...ReaderOption,
) (*PackedReader, error) {
	options := &readerOptions{}
	for _, opt := range opts {
		opt(options)
	}
	var cProperties *C.LoonProperties
	var cFilesystemPath *C.char
	if extfs.Source != "" {
		if storageConfig == nil {
			return nil, merr.WrapErrServiceInternalMsg("storageConfig is required for external packed reader")
		}
		properties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
		if err != nil {
			return nil, merr.Wrap(err, "failed to create properties")
		}
		cProperties = properties
		defer C.loon_properties_free(cProperties)
		if err := injectExternalSpecProperties(cProperties, extfs.CollectionID, extfs.Source, extfs.Spec); err != nil {
			return nil, merr.Wrap(err, "inject extfs")
		}
		var filesystemPath string
		normalizedPaths := make([]string, 0, len(filePaths))
		for _, filePath := range filePaths {
			currentFilesystemPath, normalizedPath, err := normalizeExternalResolvedPathForFilesystem(filePath, cProperties, extfs)
			if err != nil {
				return nil, merr.WrapErrServiceInternalErr(err, "normalize external packed file path %s", filePath)
			}
			if filesystemPath == "" {
				filesystemPath = currentFilesystemPath
			} else if currentFilesystemPath != filesystemPath {
				return nil, merr.WrapErrServiceInternalMsg("external packed reader requires paths from one filesystem, got %s and %s", filesystemPath, currentFilesystemPath)
			}
			normalizedPaths = append(normalizedPaths, normalizedPath)
		}
		filePaths = normalizedPaths
		cFilesystemPath = C.CString(filesystemPath)
		defer C.free(unsafe.Pointer(cFilesystemPath))
	}

	cFilePaths := make([]*C.char, len(filePaths))
	for i, path := range filePaths {
		cFilePaths[i] = C.CString(path)
		defer C.free(unsafe.Pointer(cFilePaths[i]))
	}
	cFilePathsArray := (**C.char)(unsafe.Pointer(&cFilePaths[0]))
	cNumPaths := C.int64_t(len(filePaths))

	var cas cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cas)
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))
	defer cdata.ReleaseCArrowSchema(&cas)

	cBufferSize := C.int64_t(bufferSize)
	cEagerPrebuffer := C.bool(options.eagerPrebuffer)

	var cPackedReader C.CPackedReader
	var status C.CStatus

	pluginContextPtr, releasePluginContext := newCPluginContext(storagePluginContext)
	defer releasePluginContext()
	var cConfig *C.CStorageConfig
	if cProperties == nil && storageConfig != nil {
		config, release := newCStorageConfig(storageConfig)
		defer release()
		cConfig = &config
	}
	if options.importReadKeyset {
		status = C.NewImportPackedReader(cFilePathsArray, cNumPaths, cSchema, cBufferSize, cEagerPrebuffer, cConfig, cProperties, cFilesystemPath, &cPackedReader, pluginContextPtr)
	} else if cProperties != nil {
		status = C.NewPackedReaderWithProperties(cFilePathsArray, cNumPaths, cSchema, cBufferSize, cEagerPrebuffer, cProperties, cFilesystemPath, &cPackedReader, pluginContextPtr)
	} else if cConfig != nil {
		status = C.NewPackedReaderWithStorageConfig(cFilePathsArray, cNumPaths, cSchema, cBufferSize, cEagerPrebuffer, *cConfig, &cPackedReader, pluginContextPtr)
	} else {
		status = C.NewPackedReader(cFilePathsArray, cNumPaths, cSchema, cBufferSize, cEagerPrebuffer, &cPackedReader, pluginContextPtr)
	}
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	return &PackedReader{cPackedReader: cPackedReader, schema: schema}, nil
}

func (pr *PackedReader) ReadNext() (arrow.Record, error) {
	// return EOF if reader is closed
	if pr.cPackedReader == nil {
		return nil, io.EOF
	}

	if pr.currentBatch != nil {
		pr.currentBatch.Release()
		pr.currentBatch = nil
	}
	// The caller owns the Arrow structs; importing transfers their buffers only.
	var cArr C.struct_ArrowArray
	var cSchema C.struct_ArrowSchema
	goCArr := (*cdata.CArrowArray)(unsafe.Pointer(&cArr))
	goCSchema := (*cdata.CArrowSchema)(unsafe.Pointer(&cSchema))
	defer func() {
		cdata.ReleaseCArrowArray(goCArr)
		cdata.ReleaseCArrowSchema(goCSchema)
	}()
	status := C.ReadNext(pr.cPackedReader, &cArr, &cSchema)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}

	if cArr.release == nil {
		return nil, io.EOF // end of stream, no more records to read
	}

	recordBatch, err := cdata.ImportCRecordBatch(goCArr, goCSchema)
	if err != nil {
		return nil, merr.WrapErrStorage(err, "failed to convert ArrowArray to Record")
	}
	pr.currentBatch = recordBatch

	// Return the RecordBatch as an arrow.Record
	return recordBatch, nil
}

func (pr *PackedReader) Close() error {
	if pr.cPackedReader == nil {
		return nil
	}
	if pr.currentBatch != nil {
		pr.currentBatch.Release()
	}
	status := C.CloseReader(pr.cPackedReader)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return err
	}
	pr.cPackedReader = nil
	return nil
}

// GetPackedFileFieldIDs returns, for each packed file, the field IDs of the
// columns it stores, taken from the PARQUET:field_id metadata in its footer.
// Only the footers are read, once per file; the filesystem is resolved as
// NewPackedReader does. A failed read keeps the storage error code the C++
// side attached (transient storage errors stay retriable).
func GetPackedFileFieldIDs(filePaths []string, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext) ([][]int64, error) {
	return getPackedFileFieldIDs(filePaths, storageConfig, storagePluginContext, false)
}

// GetImportPackedFileFieldIDs decrypts each footer using the import read cache.
func GetImportPackedFileFieldIDs(filePaths []string, storageConfig *indexpb.StorageConfig, source *indexcgopb.StoragePluginContext) ([][]int64, error) {
	return getPackedFileFieldIDs(filePaths, storageConfig, source, true)
}

func getPackedFileFieldIDs(filePaths []string, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext, forImport bool) ([][]int64, error) {
	if len(filePaths) == 0 {
		return nil, nil
	}
	cFilePaths := make([]*C.char, len(filePaths))
	for i, path := range filePaths {
		cFilePaths[i] = C.CString(path)
		defer C.free(unsafe.Pointer(cFilePaths[i]))
	}

	var cStorageConfigPtr *C.CStorageConfig
	if storageConfig != nil {
		cStorageConfig, release := newCStorageConfig(storageConfig)
		defer release()
		cStorageConfigPtr = &cStorageConfig
	}
	cPluginContext, releasePluginContext := newCPluginContext(storagePluginContext)
	defer releasePluginContext()

	var cFieldIDs *C.int64_t
	var cFieldCounts *C.int64_t
	paths := (**C.char)(unsafe.Pointer(&cFilePaths[0]))
	var status C.CStatus
	if forImport {
		status = C.GetImportPackedFileFieldIDs(paths, C.int64_t(len(filePaths)), cStorageConfigPtr, cPluginContext, &cFieldIDs, &cFieldCounts)
	} else {
		status = C.GetPackedFileFieldIDs(paths, C.int64_t(len(filePaths)), cStorageConfigPtr, cPluginContext, &cFieldIDs, &cFieldCounts)
	}
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}
	defer C.free(unsafe.Pointer(cFieldIDs))
	defer C.free(unsafe.Pointer(cFieldCounts))

	counts := unsafe.Slice((*int64)(unsafe.Pointer(cFieldCounts)), len(filePaths))
	total := int64(0)
	for _, count := range counts {
		total += count
	}
	ids := unsafe.Slice((*int64)(unsafe.Pointer(cFieldIDs)), total)
	result := make([][]int64, len(filePaths))
	offset := int64(0)
	for i, count := range counts {
		result[i] = append([]int64(nil), ids[offset:offset+count]...)
		offset += count
	}
	return result, nil
}
