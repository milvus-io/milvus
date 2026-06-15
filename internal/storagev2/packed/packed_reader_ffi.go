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
#include "storage/loon_ffi/ffi_reader_c.h"
#include "arrow/c/abi.h"
#include "arrow/c/helpers.h"

CStatus NewPackedFFIReaderWithColumnGroups(
    const LoonColumnGroups* column_groups,
    struct ArrowSchema* schema,
    char** needed_columns,
    int64_t needed_columns_size,
    CFFIPackedReader* c_loon_reader,
    CStorageConfig c_storage_config,
    CPluginContext* c_plugin_context,
    int64_t collection_id,
    const char* external_source,
    const char* external_spec);
*/
import "C"

import (
	"io"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/cdata"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ExternalReaderContext carries per-collection context needed by the FFI
// reader to resolve extfs aliases for external collections. Zero value is
// safe for non-external collections.
type ExternalReaderContext = ExternalSpecContext

// NewFFIPackedReader opens a StorageV3 manifest reader with optional external
// reader context.
func NewFFIPackedReader(manifestPath string, schema *arrow.Schema, neededColumns []string, bufferSize int64, storageConfig *indexpb.StorageConfig, storagePluginContext *indexcgopb.StoragePluginContext, ext ExternalReaderContext) (*FFIPackedReader, error) {
	return NewFFIPackedReaderWithExtfs(
		manifestPath,
		schema,
		neededColumns,
		bufferSize,
		storageConfig,
		storagePluginContext,
		ext,
	)
}

// NewFFIPackedReaderWithExtfs opens a StorageV3 manifest after injecting
// external filesystem properties for source manifests referenced by
// milvus-table external collections.
func NewFFIPackedReaderWithExtfs(
	manifestPath string,
	schema *arrow.Schema,
	neededColumns []string,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	extfs ExternalSpecContext,
) (*FFIPackedReader, error) {
	cLoonManifest, err := GetManifestHandleWithExtfs(manifestPath, storageConfig, extfs)
	if err != nil {
		return nil, merr.Wrap(err, "failed to get manifest")
	}
	defer C.loon_manifest_destroy(cLoonManifest)

	return openFFIPackedReader(schema, neededColumns, bufferSize, storageConfig, storagePluginContext, extfs,
		func(cSchema *C.struct_ArrowSchema,
			cNeededColumnArray **C.char,
			cNumColumns C.int64_t,
			cPackedReader *C.CFFIPackedReader,
			cStorageConfig C.CStorageConfig,
			pluginContextPtr *C.CPluginContext,
			collectionID C.int64_t,
			cExternalSource *C.char,
			cExternalSpec *C.char,
		) C.CStatus {
			return C.NewPackedFFIReaderWithManifest(
				cLoonManifest,
				cSchema,
				cNeededColumnArray,
				cNumColumns,
				cPackedReader,
				cStorageConfig,
				pluginContextPtr,
				collectionID,
				cExternalSource,
				cExternalSpec,
			)
		})
}

// NewFFIPackedReaderWithFragments opens a packed reader from explicit physical
// fragments. It is used for StorageV3 deltalogs where the manifest already
// provides each file's EntriesNum and the caller must preserve those row
// boundaries when constructing column groups.
func NewFFIPackedReaderWithFragments(
	columns []string,
	format string,
	fragments []Fragment,
	schema *arrow.Schema,
	neededColumns []string,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	extfs ExternalSpecContext,
) (*FFIPackedReader, error) {
	readerFragments, err := resolveFFIReaderFragments(fragments, storageConfig, extfs)
	if err != nil {
		return nil, err
	}
	cColumnGroups, err := createColumnGroups(columns, format, readerFragments)
	if err != nil {
		return nil, err
	}
	defer C.loon_column_groups_destroy(cColumnGroups)

	return openFFIPackedReader(schema, neededColumns, bufferSize, storageConfig, storagePluginContext, extfs,
		func(cSchema *C.struct_ArrowSchema,
			cNeededColumnArray **C.char,
			cNumColumns C.int64_t,
			cPackedReader *C.CFFIPackedReader,
			cStorageConfig C.CStorageConfig,
			pluginContextPtr *C.CPluginContext,
			collectionID C.int64_t,
			cExternalSource *C.char,
			cExternalSpec *C.char,
		) C.CStatus {
			return C.NewPackedFFIReaderWithColumnGroups(
				cColumnGroups,
				cSchema,
				cNeededColumnArray,
				cNumColumns,
				cPackedReader,
				cStorageConfig,
				pluginContextPtr,
				collectionID,
				cExternalSource,
				cExternalSpec,
			)
		})
}

type ffiReaderOpenFunc func(
	cSchema *C.struct_ArrowSchema,
	cNeededColumnArray **C.char,
	cNumColumns C.int64_t,
	cPackedReader *C.CFFIPackedReader,
	cStorageConfig C.CStorageConfig,
	pluginContextPtr *C.CPluginContext,
	collectionID C.int64_t,
	cExternalSource *C.char,
	cExternalSpec *C.char,
) C.CStatus

func openFFIPackedReader(
	schema *arrow.Schema,
	neededColumns []string,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	storagePluginContext *indexcgopb.StoragePluginContext,
	extfs ExternalSpecContext,
	open ffiReaderOpenFunc,
) (*FFIPackedReader, error) {
	var cas cdata.CArrowSchema
	cdata.ExportArrowSchema(schema, &cas)
	cSchema := (*C.struct_ArrowSchema)(unsafe.Pointer(&cas))
	defer cdata.ReleaseCArrowSchema(&cas)

	var cPackedReader C.CFFIPackedReader

	pluginContextPtr, cleanupPluginContext := newCPluginContext(storagePluginContext)
	defer cleanupPluginContext()

	if storageConfig == nil {
		return nil, merr.WrapErrServiceInternalMsg("storageConfig is required")
	}
	cStorageConfig, cleanupStorageConfig := newCStorageConfig(storageConfig)
	defer cleanupStorageConfig()

	cNeededColumnArray, cNumColumns, cleanupNeededColumns := newCNeededColumns(neededColumns)
	defer cleanupNeededColumns()

	cExternalSource, cExternalSpec, cleanupExternalContext := newCExternalContext(extfs.Source, extfs.Spec)
	defer cleanupExternalContext()

	status := open(
		cSchema,
		cNeededColumnArray,
		cNumColumns,
		&cPackedReader,
		cStorageConfig,
		pluginContextPtr,
		C.int64_t(extfs.CollectionID),
		cExternalSource,
		cExternalSpec,
	)
	if err := ConsumeCStatusIntoError(&status); err != nil {
		return nil, err
	}

	// Get the ArrowArrayStream
	var cStream cdata.CArrowArrayStream
	status = C.GetFFIReaderStream(cPackedReader, C.int64_t(8196), (*C.struct_ArrowArrayStream)(unsafe.Pointer(&cStream)))
	if err := ConsumeCStatusIntoError(&status); err != nil {
		C.CloseFFIReader(cPackedReader)
		return nil, merr.WrapErrStorage(err, "failed to get reader stream")
	}

	// Import the stream as a RecordReader
	recordReader, err := cdata.ImportCRecordReader(&cStream, schema)
	if err != nil {
		C.CloseFFIReader(cPackedReader)
		return nil, merr.WrapErrStorage(err, "failed to import record reader")
	}

	return &FFIPackedReader{
		cPackedReader: cPackedReader,
		recordReader:  recordReader,
		schema:        schema,
	}, nil
}

func newCPluginContext(storagePluginContext *indexcgopb.StoragePluginContext) (*C.CPluginContext, func()) {
	if storagePluginContext == nil {
		return nil, func() {}
	}
	ckey := C.CString(storagePluginContext.EncryptionKey)
	pluginContext := &C.CPluginContext{
		ez_id:         C.int64_t(storagePluginContext.EncryptionZoneId),
		collection_id: C.int64_t(storagePluginContext.CollectionId),
		key:           ckey,
	}
	return pluginContext, func() {
		C.free(unsafe.Pointer(ckey))
	}
}

func newCStorageConfig(storageConfig *indexpb.StorageConfig) (C.CStorageConfig, func()) {
	cStorageConfig := C.CStorageConfig{
		address:                C.CString(storageConfig.GetAddress()),
		bucket_name:            C.CString(storageConfig.GetBucketName()),
		access_key_id:          C.CString(storageConfig.GetAccessKeyID()),
		access_key_value:       C.CString(storageConfig.GetSecretAccessKey()),
		root_path:              C.CString(storageConfig.GetRootPath()),
		storage_type:           C.CString(storageConfig.GetStorageType()),
		cloud_provider:         C.CString(storageConfig.GetCloudProvider()),
		iam_endpoint:           C.CString(storageConfig.GetIAMEndpoint()),
		log_level:              C.CString("warn"),
		useSSL:                 C.bool(storageConfig.GetUseSSL()),
		sslCACert:              C.CString(storageConfig.GetSslCACert()),
		useIAM:                 C.bool(storageConfig.GetUseIAM()),
		region:                 C.CString(storageConfig.GetRegion()),
		useVirtualHost:         C.bool(storageConfig.GetUseVirtualHost()),
		requestTimeoutMs:       C.int64_t(storageConfig.GetRequestTimeoutMs()),
		gcp_credential_json:    C.CString(storageConfig.GetGcpCredentialJSON()),
		use_custom_part_upload: true,
		max_connections:        C.uint32_t(storageConfig.GetMaxConnections()),
		tls_min_version:        C.CString(tlsMinVersionForStorage(storageConfig.GetSslTlsMinVersion())),
		use_crc32c_checksum:    C.bool(storageConfig.GetUseCrc32CChecksum()),
	}
	return cStorageConfig, func() {
		C.free(unsafe.Pointer(cStorageConfig.address))
		C.free(unsafe.Pointer(cStorageConfig.bucket_name))
		C.free(unsafe.Pointer(cStorageConfig.access_key_id))
		C.free(unsafe.Pointer(cStorageConfig.access_key_value))
		C.free(unsafe.Pointer(cStorageConfig.root_path))
		C.free(unsafe.Pointer(cStorageConfig.storage_type))
		C.free(unsafe.Pointer(cStorageConfig.cloud_provider))
		C.free(unsafe.Pointer(cStorageConfig.iam_endpoint))
		C.free(unsafe.Pointer(cStorageConfig.log_level))
		C.free(unsafe.Pointer(cStorageConfig.sslCACert))
		C.free(unsafe.Pointer(cStorageConfig.region))
		C.free(unsafe.Pointer(cStorageConfig.gcp_credential_json))
		C.free(unsafe.Pointer(cStorageConfig.tls_min_version))
	}
}

func newCNeededColumns(neededColumns []string) (**C.char, C.int64_t, func()) {
	cNeededColumn := make([]*C.char, len(neededColumns))
	for i, columnName := range neededColumns {
		cNeededColumn[i] = C.CString(columnName)
	}
	cleanup := func() {
		for _, columnName := range cNeededColumn {
			C.free(unsafe.Pointer(columnName))
		}
	}
	if len(cNeededColumn) == 0 {
		return nil, 0, cleanup
	}
	return (**C.char)(unsafe.Pointer(&cNeededColumn[0])), C.int64_t(len(cNeededColumn)), cleanup
}

func newCExternalContext(externalSource, externalSpec string) (*C.char, *C.char, func()) {
	if externalSource == "" {
		return nil, nil, func() {}
	}
	cExternalSource := C.CString(externalSource)
	var cExternalSpec *C.char
	if externalSpec != "" {
		cExternalSpec = C.CString(externalSpec)
	}
	return cExternalSource, cExternalSpec, func() {
		C.free(unsafe.Pointer(cExternalSource))
		if cExternalSpec != nil {
			C.free(unsafe.Pointer(cExternalSpec))
		}
	}
}

func resolveFFIReaderFragments(
	fragments []Fragment,
	storageConfig *indexpb.StorageConfig,
	extfs ExternalSpecContext,
) ([]Fragment, error) {
	if extfs.Source == "" {
		return fragments, nil
	}
	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create properties: %w", err)
	}
	defer C.loon_properties_free(cProperties)
	if err := injectExternalSpecProperties(cProperties, extfs.CollectionID, extfs.Source, extfs.Spec); err != nil {
		return nil, fmt.Errorf("inject extfs: %w", err)
	}

	resolvedFragments := make([]Fragment, len(fragments))
	copy(resolvedFragments, fragments)
	for i := range resolvedFragments {
		resolvedPath, err := resolveExternalSourceRelativePath(resolvedFragments[i].FilePath, cProperties, extfs)
		if err != nil {
			return nil, err
		}
		resolvedFragments[i].FilePath = resolvedPath
	}
	return resolvedFragments, nil
}

// ReadNext reads the next record batch from the reader
func (r *FFIPackedReader) ReadNext() (rec arrow.Record, err error) {
	if r.recordReader == nil {
		return nil, io.EOF
	}

	rec, err = r.recordReader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, io.EOF
		}
		return nil, merr.WrapErrStorage(err, "failed to read next record")
	}

	return rec, nil
}

// Close closes the FFI reader
func (r *FFIPackedReader) Close() error {
	if r.cPackedReader == nil {
		return nil
	}

	// no need to manual release current batch
	// stream reader handles it

	if r.recordReader != nil {
		r.recordReader = nil
	}

	status := C.CloseFFIReader(r.cPackedReader)
	r.cPackedReader = nil
	return ConsumeCStatusIntoError(&status)
}

// Schema returns the schema of the reader
func (r *FFIPackedReader) Schema() *arrow.Schema {
	return r.schema
}

// Retain increases the reference count
func (r *FFIPackedReader) Retain() {
}

// Release decreases the reference count
func (r *FFIPackedReader) Release() {
	r.Close()
}

func GetManifestHandle(manifestPath string, storageConfig *indexpb.StorageConfig) (loonManifestHandle *C.LoonManifest, err error) {
	return GetManifestHandleWithExtfs(manifestPath, storageConfig, ExternalSpecContext{})
}

// GetManifestHandleWithExtfs opens a StorageV3 manifest with optional extfs
// properties so external source manifests can be resolved with their own
// storage credentials and endpoint aliases.
func GetManifestHandleWithExtfs(
	manifestPath string,
	storageConfig *indexpb.StorageConfig,
	extfs ExternalSpecContext,
) (loonManifestHandle *C.LoonManifest, err error) {
	var cManifestHandle *C.LoonManifest
	basePath, version, err := UnmarshalManifestPath(manifestPath)
	if err != nil {
		return cManifestHandle, err
	}
	log.Info("GetManifest", zap.String("manifestPath", manifestPath), zap.String("basePath", basePath), zap.Int64("version", version))

	cProperties, err := MakePropertiesFromStorageConfig(storageConfig, nil)
	if err != nil {
		return cManifestHandle, err
	}
	defer C.loon_properties_free(cProperties)
	if err := injectExternalSpecProperties(cProperties, extfs.CollectionID, extfs.Source, extfs.Spec); err != nil {
		return cManifestHandle, fmt.Errorf("inject extfs: %w", err)
	}
	cBasePath := C.CString(basePath)
	defer C.free(unsafe.Pointer(cBasePath))

	var cTransactionHandle C.LoonTransactionHandle
	result := C.loon_transaction_begin(cBasePath, cProperties, C.int64_t(version), C.int32_t(0) /* resolve_id */, C.uint32_t(1) /* retry_limit */, &cTransactionHandle)
	err = HandleLoonFFIResult(result)
	if err != nil {
		return cManifestHandle, err
	}
	defer C.loon_transaction_destroy(cTransactionHandle)

	result = C.loon_transaction_get_manifest(cTransactionHandle, &cManifestHandle)
	err = HandleLoonFFIResult(result)
	if err != nil {
		return cManifestHandle, err
	}

	return cManifestHandle, nil
}

// Ensure FFIPackedReader implements array.RecordReader interface
// var _ array.RecordReader = (*FFIPackedReader)(nil)
