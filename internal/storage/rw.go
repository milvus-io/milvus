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

package storage

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"sort"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	// StorageV1 is milvus 2.0 legacy binlog format
	StorageV1 int64 = 0
	// StorageV2 is milvus-storage packed writer binlog format(parquet)
	StorageV2 int64 = 2
	// StorageV3 is loon manifest format
	StorageV3 int64 = 3
)

type (
	downloaderFn func(ctx context.Context, paths []string) ([][]byte, error)
	uploaderFn   func(ctx context.Context, kvs map[string][]byte) error
)

// rwOp is enum alias for rwOption op field.
type rwOp int32

const (
	OpWrite rwOp = 0
	OpRead  rwOp = 1
)

type rwOptions struct {
	version             int64
	op                  rwOp
	bufferSize          int64
	downloader          downloaderFn
	uploader            uploaderFn
	multiPartUploadSize int64
	columnGroups        []storagecommon.ColumnGroup
	collectionID        int64
	storageConfig       *indexpb.StorageConfig
	neededFields        typeutil.Set[int64]
	useLoonFFI          bool
	pluginContext       *indexcgopb.StoragePluginContext
	textColumnConfigs   []packed.TextColumnConfig // TEXT column configurations for REWRITE_ALL mode
	textRefsAsBinary    bool                      // TEXT columns already contain encoded LOB refs and should be copied as-is
	externalReader      packed.ExternalReaderContext
	writerFormat        string
	presentFields       map[FieldID]struct{} // reader: caller-known physically-present field IDs, skips a manifest re-read
}

func (o *rwOptions) validate() error {
	switch o.version {
	case StorageV1:
		if o.op == OpWrite && o.uploader == nil {
			return merr.WrapErrServiceInternal("uploader is nil for writer")
		}
		if o.op == OpRead && o.downloader == nil {
			return merr.WrapErrServiceInternal("downloader is nil for v1 reader")
		}
	case StorageV2, StorageV3:
		if o.storageConfig == nil {
			return merr.WrapErrServiceInternal("storage config is nil")
		}
	default:
		return merr.WrapErrServiceInternalMsg("unsupported storage version %d", o.version)
	}
	return nil
}

type RwOption func(*rwOptions)

func DefaultWriterOptions() *rwOptions {
	return &rwOptions{
		version:             StorageV1,
		bufferSize:          packed.DefaultWriteBufferSize,
		multiPartUploadSize: packed.DefaultMultiPartUploadSize,
		op:                  OpWrite,
	}
}

func DefaultReaderOptions() *rwOptions {
	return &rwOptions{
		bufferSize: packed.DefaultReadBufferSize,
		op:         OpRead,
	}
}

func WithCollectionID(collID int64) RwOption {
	return func(options *rwOptions) {
		options.collectionID = collID
	}
}

func WithVersion(version int64) RwOption {
	return func(options *rwOptions) {
		options.version = version
	}
}

func WithBufferSize(bufferSize int64) RwOption {
	return func(options *rwOptions) {
		options.bufferSize = bufferSize
	}
}

func WithMultiPartUploadSize(multiPartUploadSize int64) RwOption {
	return func(options *rwOptions) {
		options.multiPartUploadSize = multiPartUploadSize
	}
}

func WithDownloader(downloader func(ctx context.Context, paths []string) ([][]byte, error)) RwOption {
	return func(options *rwOptions) {
		options.downloader = downloader
	}
}

func WithUploader(uploader func(ctx context.Context, kvs map[string][]byte) error) RwOption {
	return func(options *rwOptions) {
		options.uploader = uploader
	}
}

func WithColumnGroups(columnGroups []storagecommon.ColumnGroup) RwOption {
	return func(options *rwOptions) {
		options.columnGroups = columnGroups
	}
}

func WithStorageConfig(storageConfig *indexpb.StorageConfig) RwOption {
	return func(options *rwOptions) {
		options.storageConfig = storageConfig
	}
}

// GetStorageConfig extracts the storage config from the given options.
func GetStorageConfig(option ...RwOption) *indexpb.StorageConfig {
	opts := DefaultReaderOptions()
	for _, opt := range option {
		opt(opts)
	}
	return opts.storageConfig
}

func WithNeededFields(neededFields typeutil.Set[int64]) RwOption {
	return func(options *rwOptions) {
		options.neededFields = neededFields
	}
}

func WithExternalReaderContext(externalReader packed.ExternalReaderContext) RwOption {
	return func(options *rwOptions) {
		options.externalReader = externalReader
	}
}

// WithPresentFields lets a caller that already knows a segment's physically-present
// field IDs hand them to NewManifestRecordReader, so it skips re-deriving them via
// packed.GetManifestFieldIDs (a redundant manifest open on the compaction path).
func WithPresentFields(present map[FieldID]struct{}) RwOption {
	return func(options *rwOptions) {
		options.presentFields = present
	}
}

func WithUseLoonFFI(useLoonFFI bool) RwOption {
	return func(options *rwOptions) {
		options.useLoonFFI = useLoonFFI
	}
}

func WithPluginContext(pluginContext *indexcgopb.StoragePluginContext) RwOption {
	return func(options *rwOptions) {
		options.pluginContext = pluginContext
	}
}

// WithTextColumnConfigs sets TEXT column configurations for REWRITE_ALL mode during compaction.
// when TEXT columns need to be rewritten (hole ratio >= threshold), this option enables
// the writer to expand TEXT LOB references and write to new LOB files.
func WithTextColumnConfigs(configs []packed.TextColumnConfig) RwOption {
	return func(options *rwOptions) {
		options.textColumnConfigs = configs
	}
}

// WithTextRefsAsBinary is for manifest compaction paths that preserve existing
// TEXT LOB references. The input TEXT columns are already encoded binary refs,
// so the plain packed writer should use the physical binary schema.
func WithTextRefsAsBinary() RwOption {
	return func(options *rwOptions) {
		options.textRefsAsBinary = true
	}
}

func WithWriterFormat(format string) RwOption {
	return func(options *rwOptions) {
		options.writerFormat = format
	}
}

func makeBlobsReader(ctx context.Context, binlogs []*datapb.FieldBinlog, downloader downloaderFn) (ChunkedBlobsReader, error) {
	if len(binlogs) == 0 {
		return func() ([]*Blob, error) {
			return nil, io.EOF
		}, nil
	}
	sort.Slice(binlogs, func(i, j int) bool {
		return binlogs[i].FieldID < binlogs[j].FieldID
	})
	for _, binlog := range binlogs {
		sort.Slice(binlog.Binlogs, func(i, j int) bool {
			return binlog.Binlogs[i].LogID < binlog.Binlogs[j].LogID
		})
	}
	nChunks := len(binlogs[0].Binlogs)
	chunks := make([][]string, nChunks) // i is chunkid, j is fieldid
	missingChunks := lo.Map(binlogs, func(binlog *datapb.FieldBinlog, _ int) int {
		return nChunks - len(binlog.Binlogs)
	})
	for i := range nChunks {
		chunks[i] = make([]string, 0, len(binlogs))
		for j, binlog := range binlogs {
			if i >= missingChunks[j] {
				idx := i - missingChunks[j]
				chunks[i] = append(chunks[i], binlog.Binlogs[idx].LogPath)
			}
		}
	}
	// verify if the chunks order is correct.
	// the zig-zag order should have a (strict) increasing order on logids.
	// lastLogID := int64(-1)
	// for _, paths := range chunks {
	// 	lastFieldID := int64(-1)
	// 	for _, path := range paths {
	// 		_, _, _, fieldID, logID, ok := metautil.ParseInsertLogPath(path)
	// 		if !ok {
	// 			return nil, merr.WrapErrIoFailedReason(fmt.Sprintf("malformed log path %s", path))
	// 		}
	// 		if fieldID < lastFieldID {
	// 			return nil, merr.WrapErrIoFailedReason(fmt.Sprintf("unaligned log path %s, fieldID %d less than lastFieldID %d", path, fieldID, lastFieldID))
	// 		}
	// 		if logID < lastLogID {
	// 			return nil, merr.WrapErrIoFailedReason(fmt.Sprintf("unaligned log path %s, logID %d less than lastLogID %d", path, logID, lastLogID))
	// 		}
	// 		lastLogID = logID
	// 		lastFieldID = fieldID
	// 	}
	// }

	chunkPos := 0
	return func() ([]*Blob, error) {
		if chunkPos >= nChunks {
			return nil, io.EOF
		}

		vals, err := downloader(ctx, chunks[chunkPos])
		if err != nil {
			return nil, err
		}
		blobs := make([]*Blob, 0, len(vals))
		for i := range vals {
			blobs = append(blobs, &Blob{
				Key:   chunks[chunkPos][i],
				Value: vals[i],
			})
		}
		chunkPos++
		return blobs, nil
	}, nil
}

func NewBinlogRecordReader(ctx context.Context, binlogs []*datapb.FieldBinlog, schema *schemapb.CollectionSchema, option ...RwOption) (rr RecordReader, err error) {
	rwOptions := DefaultReaderOptions()
	for _, opt := range option {
		opt(rwOptions)
	}
	if err := rwOptions.validate(); err != nil {
		return nil, err
	}

	binlogReaderOpts := []BinlogReaderOption{}
	var pluginContext *indexcgopb.StoragePluginContext
	if hookutil.IsClusterEncryptionEnabled() {
		// Reader pluginContext from import tasks
		if rwOptions.pluginContext != nil {
			pluginContext = rwOptions.pluginContext
		} else {
			ez := hookutil.GetEzByCollProperties(schema.GetProperties(), rwOptions.collectionID)
			if ez != nil {
				binlogReaderOpts = append(binlogReaderOpts, WithReaderDecryptionContext(ez.EzID, ez.CollectionID))

				unsafe := hookutil.GetCipher().GetUnsafeKey(ez.EzID, ez.CollectionID)
				if len(unsafe) > 0 {
					pluginContext = &indexcgopb.StoragePluginContext{
						EncryptionZoneId: ez.EzID,
						CollectionId:     ez.CollectionID,
						EncryptionKey:    base64.StdEncoding.EncodeToString(unsafe),
					}
				}
			}
		}
	}
	switch rwOptions.version {
	case StorageV1:
		var blobsReader ChunkedBlobsReader
		blobsReader, err = makeBlobsReader(ctx, binlogs, rwOptions.downloader)
		if err != nil {
			return nil, err
		}
		rr = newIterativeCompositeBinlogRecordReader(schema, rwOptions.neededFields, blobsReader, binlogReaderOpts...)
	case StorageV2, StorageV3:
		if len(binlogs) <= 0 {
			return nil, io.EOF
		}
		sort.Slice(binlogs, func(i, j int) bool {
			return binlogs[i].GetFieldID() < binlogs[j].GetFieldID()
		})

		binlogLists := lo.Map(binlogs, func(fieldBinlog *datapb.FieldBinlog, _ int) []*datapb.Binlog {
			return fieldBinlog.GetBinlogs()
		})
		paths := make([][]string, len(binlogLists[0]))
		for _, binlogs := range binlogLists {
			for j, binlog := range binlogs {
				logPath := binlog.GetLogPath()
				paths[j] = append(paths[j], logPath)
			}
		}
		// StorageV2/V3 binlog: present declared defaults for absent fields like V1,
		// sourcing physical presence from FieldBinlog.ChildFields (set by the
		// flush/compaction writers, reliable for those segments). Import-reconstructed
		// binlogs carry no ChildFields (FieldID is a column-group ID), so presence is
		// not derivable there; pass the full schema through unchanged (the engine
		// null-fills absent columns). Default-fill for the import path is a documented
		// follow-up (issue #52771).
		present, reliable := binlogFieldIDSet(binlogs)
		if reliable {
			readSchema, ferr := filterSchemaToPresentFields(schema, present)
			if ferr != nil {
				return nil, ferr
			}
			rr = newIterativePackedRecordReader(paths, readSchema, rwOptions.bufferSize, rwOptions.storageConfig, pluginContext, rwOptions.externalReader)
			rr = NewAbsentFieldFillRecordReader(rr, schema, present)
		} else {
			rr = newIterativePackedRecordReader(paths, schema, rwOptions.bufferSize, rwOptions.storageConfig, pluginContext, rwOptions.externalReader)
		}
	default:
		return nil, merr.WrapErrServiceInternalMsg("unsupported storage version %d", rwOptions.version)
	}
	if err != nil {
		return nil, err
	}
	return rr, nil
}

func NewManifestRecordReader(ctx context.Context, manifestPath string, neededSchema *schemapb.CollectionSchema, option ...RwOption) (rr RecordReader, err error) {
	rwOptions := DefaultReaderOptions()
	for _, opt := range option {
		opt(rwOptions)
	}
	if err := rwOptions.validate(); err != nil {
		return nil, err
	}

	var pluginContext *indexcgopb.StoragePluginContext
	if hookutil.IsClusterEncryptionEnabled() {
		// Reader pluginContext from import tasks
		if rwOptions.pluginContext != nil {
			pluginContext = rwOptions.pluginContext
		} else {
			ez := hookutil.GetEzByCollProperties(neededSchema.GetProperties(), rwOptions.collectionID)
			if ez != nil {
				unsafe := hookutil.GetCipher().GetUnsafeKey(ez.EzID, ez.CollectionID)
				if len(unsafe) > 0 {
					pluginContext = &indexcgopb.StoragePluginContext{
						EncryptionZoneId: ez.EzID,
						CollectionId:     ez.CollectionID,
						EncryptionKey:    base64.StdEncoding.EncodeToString(unsafe),
					}
				}
			}
		}
	}
	// External-source reads (e.g. parquet) are not milvus manifests carrying
	// default-aware internal field IDs and are out of scope for #52771; keep the
	// original pass-through. An internal manifest reads only its physically-present
	// columns, then fills every absent read-schema field with its schema default
	// (or null) so declared defaults are not presented as NULL, matching V1.
	if rwOptions.externalReader.Source != "" {
		return NewRecordReaderFromManifest(manifestPath, neededSchema, rwOptions.bufferSize,
			rwOptions.storageConfig, pluginContext, option...)
	}
	present := rwOptions.presentFields
	if present == nil {
		present, err = packed.GetManifestFieldIDs(manifestPath, rwOptions.storageConfig)
		if err != nil {
			return nil, err
		}
	}
	presentSchema, err := filterSchemaToPresentFields(neededSchema, present)
	if err != nil {
		return nil, err
	}
	inner, err := NewRecordReaderFromManifest(manifestPath, presentSchema,
		rwOptions.bufferSize, rwOptions.storageConfig, pluginContext, option...)
	if err != nil {
		return nil, err
	}
	return NewAbsentFieldFillRecordReader(inner, neededSchema, present), nil
}

// filterSchemaToPresentFields returns a copy of schema keeping only the fields
// (and struct sub-fields) whose FieldID is physically present, so the packed
// reader is asked to read only stored columns; absent fields are then filled by
// NewAbsentFieldFillRecordReader rather than null-synthesized by the engine.
// A struct array field is physically all-or-nothing (add/drop/update act on the
// whole struct), so it is kept whole or dropped whole; a partially-present struct
// is a data-integrity violation that must never occur and is surfaced as an error.
func filterSchemaToPresentFields(schema *schemapb.CollectionSchema, present map[FieldID]struct{}) (*schemapb.CollectionSchema, error) {
	out := proto.Clone(schema).(*schemapb.CollectionSchema)
	fields := out.Fields[:0]
	for _, f := range out.Fields {
		if _, ok := present[f.GetFieldID()]; ok {
			fields = append(fields, f)
		}
	}
	out.Fields = fields
	structs := out.StructArrayFields[:0]
	for _, st := range out.StructArrayFields {
		presentChildren := 0
		for _, f := range st.GetFields() {
			if _, ok := present[f.GetFieldID()]; ok {
				presentChildren++
			}
		}
		switch presentChildren {
		case 0:
			// whole struct absent: drop it here, the wrapper fills its children.
		case len(st.GetFields()):
			structs = append(structs, st)
		default:
			return nil, merr.WrapErrServiceInternalMsg(
				"struct array field %q partially present (%d of %d children): a struct array is physically all-or-nothing",
				st.GetName(), presentChildren, len(st.GetFields()))
		}
	}
	out.StructArrayFields = structs
	return out, nil
}

// binlogFieldIDSet returns the physically-present field IDs of a StorageV2/V3
// binlog segment, taken from each FieldBinlog's ChildFields (the column group's
// member field IDs, written by the flush/compaction writers). Presence is reliable
// ONLY when every FieldBinlog carries ChildFields; import-reconstructed binlogs key
// FieldID by column-group ID and set no ChildFields, so presence is not derivable —
// the second return value is then false and the caller must read unfiltered.
func binlogFieldIDSet(binlogs []*datapb.FieldBinlog) (map[FieldID]struct{}, bool) {
	present := make(map[FieldID]struct{}, len(binlogs))
	for _, fb := range binlogs {
		children := fb.GetChildFields()
		if len(children) == 0 {
			return nil, false
		}
		for _, child := range children {
			present[child] = struct{}{}
		}
	}
	return present, true
}

func NewBinlogRecordWriter(ctx context.Context, collectionID, partitionID, segmentID UniqueID,
	schema *schemapb.CollectionSchema, allocator allocator.Interface, chunkSize uint64, maxRowNum int64,
	option ...RwOption,
) (BinlogRecordWriter, error) {
	rwOptions := DefaultWriterOptions()
	option = append(option, WithCollectionID(collectionID))
	for _, opt := range option {
		opt(rwOptions)
	}

	if err := rwOptions.validate(); err != nil {
		return nil, err
	}
	if rwOptions.version == StorageV1 {
		if err := ValidateStorageV1InsertWritableSchema(schema); err != nil {
			return nil, err
		}
	}

	blobsWriter := func(blobs []*Blob) error {
		kvs := make(map[string][]byte, len(blobs))
		for _, blob := range blobs {
			kvs[blob.Key] = blob.Value
		}
		return rwOptions.uploader(ctx, kvs)
	}

	opts := []StreamWriterOption{}
	var pluginContext *indexcgopb.StoragePluginContext
	if hookutil.IsClusterEncryptionEnabled() {
		ez := hookutil.GetEzByCollProperties(schema.GetProperties(), collectionID)
		if ez != nil {
			encryptor, edek, err := hookutil.GetCipher().GetEncryptor(ez.EzID, ez.CollectionID)
			if err != nil {
				return nil, err
			}
			opts = append(opts, GetEncryptionOptions(ez.EzID, edek, encryptor)...)

			unsafe := hookutil.GetCipher().GetUnsafeKey(ez.EzID, ez.CollectionID)
			if len(unsafe) > 0 {
				pluginContext = &indexcgopb.StoragePluginContext{
					EncryptionZoneId: ez.EzID,
					CollectionId:     ez.CollectionID,
					EncryptionKey:    base64.StdEncoding.EncodeToString(unsafe),
				}
			}
		}
	}

	switch rwOptions.version {
	case StorageV1:
		rootPath := rwOptions.storageConfig.GetRootPath()
		return newCompositeBinlogRecordWriter(collectionID, partitionID, segmentID, schema,
			blobsWriter, allocator, chunkSize, rootPath, maxRowNum, opts...)
	case StorageV2:
		return newPackedBinlogRecordWriter(collectionID, partitionID, segmentID, schema,
			blobsWriter, allocator, maxRowNum,
			rwOptions.bufferSize, rwOptions.multiPartUploadSize, rwOptions.columnGroups,
			rwOptions.storageConfig,
			pluginContext,
			rwOptions.writerFormat,
		)
	case StorageV3:
		// if TEXT column configs are provided, use the text writer with TEXT column support
		if len(rwOptions.textColumnConfigs) > 0 {
			return NewPackedTextManifestRecordWriter(collectionID, partitionID, segmentID, schema,
				blobsWriter, allocator, maxRowNum,
				rwOptions.bufferSize, rwOptions.multiPartUploadSize, rwOptions.columnGroups,
				rwOptions.storageConfig,
				rwOptions.textColumnConfigs,
				rwOptions.writerFormat,
			)
		}
		return newPackedManifestRecordWriter(collectionID, partitionID, segmentID, schema,
			blobsWriter, allocator, maxRowNum,
			rwOptions.bufferSize, rwOptions.multiPartUploadSize, rwOptions.columnGroups,
			rwOptions.storageConfig,
			pluginContext,
			rwOptions.textRefsAsBinary,
			rwOptions.writerFormat,
		)
	}
	return nil, merr.WrapErrServiceInternalMsg("unsupported storage version %d", rwOptions.version)
}

func NewDeltalogWriter(
	ctx context.Context,
	collectionID, partitionID, segmentID, logID UniqueID,
	pkType schemapb.DataType,
	path string,
	option ...RwOption,
) (RecordWriter, error) {
	rwOptions := DefaultWriterOptions()
	for _, opt := range option {
		opt(rwOptions)
	}
	if err := rwOptions.validate(); err != nil {
		return nil, err
	}
	switch rwOptions.version {
	case StorageV1:
		return NewLegacyDeltalogWriter(collectionID, partitionID, segmentID, logID, pkType, rwOptions.uploader, path)
	case StorageV2:
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      0,
					DataType:     pkType,
					IsPrimaryKey: true,
				},
				{
					FieldID:  1,
					DataType: schemapb.DataType_Int64,
				},
			},
		}
		bucketName := rwOptions.storageConfig.BucketName
		return NewPackedRecordWriter(bucketName, []string{path}, schema,
			rwOptions.bufferSize, rwOptions.multiPartUploadSize,
			[]storagecommon.ColumnGroup{{GroupID: 0, Columns: []int{0, 1}, Fields: []int64{0, common.TimeStampField}}},
			rwOptions.storageConfig, nil)
	default:
		return nil, merr.WrapErrServiceInternalMsg("unsupported storage version %d", rwOptions.version)
	}
}

func NewDeltalogReader(
	pkType schemapb.DataType,
	paths []string,
	option ...RwOption,
) (RecordReader, error) {
	rwOptions := DefaultReaderOptions()
	for _, opt := range option {
		opt(rwOptions)
	}
	if err := rwOptions.validate(); err != nil {
		return nil, err
	}

	pkField := &schemapb.FieldSchema{
		FieldID:      0,
		DataType:     pkType,
		IsPrimaryKey: true,
	}

	switch rwOptions.version {
	case StorageV1:
		return NewLegacyDeltalogReader(pkField, rwOptions.downloader, paths)
	case StorageV2, StorageV3:
		pathPos := 0
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				pkField,
				{
					FieldID:  common.TimeStampField,
					Name:     "ts",
					DataType: schemapb.DataType_Int64,
				},
			},
		}
		return &IterativeRecordReader{
			iterate: func() (RecordReader, error) {
				if pathPos >= len(paths) {
					return nil, io.EOF
				}
				path := paths[pathPos]
				pathPos++
				return newPackedRecordReader([]string{path}, schema, rwOptions.bufferSize, rwOptions.storageConfig, nil, rwOptions.externalReader)
			},
		}, nil
	default:
		return nil, merr.WrapErrServiceInternalMsg("unsupported storage version %d", rwOptions.version)
	}
}

// NewDeltalogReaderFromBinlogs opens StorageV2/V3 deltalogs while preserving
// each binlog's EntriesNum. StorageV3 FFI readers need those row counts to
// build column groups with stable start/end row offsets.
func NewDeltalogReaderFromBinlogs(
	pkType schemapb.DataType,
	binlogs []*datapb.Binlog,
	option ...RwOption,
) (RecordReader, error) {
	rwOptions := DefaultReaderOptions()
	for _, opt := range option {
		opt(rwOptions)
	}
	if err := rwOptions.validate(); err != nil {
		return nil, err
	}

	pkField := &schemapb.FieldSchema{
		FieldID:      0,
		DataType:     pkType,
		IsPrimaryKey: true,
	}

	switch rwOptions.version {
	case StorageV1:
		paths := make([]string, 0, len(binlogs))
		for _, binlog := range binlogs {
			if binlog == nil || binlog.GetLogPath() == "" {
				continue
			}
			paths = append(paths, binlog.GetLogPath())
		}
		return NewLegacyDeltalogReader(pkField, rwOptions.downloader, paths)
	case StorageV2, StorageV3:
		fragments, err := buildDeltalogFragmentsFromBinlogs(binlogs)
		if err != nil {
			return nil, err
		}
		if len(fragments) == 0 {
			return &IterativeRecordReader{
				iterate: func() (RecordReader, error) {
					return nil, io.EOF
				},
			}, nil
		}
		schema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				pkField,
				{
					FieldID:  common.TimeStampField,
					Name:     "ts",
					DataType: schemapb.DataType_Int64,
				},
			},
		}
		return newFFIPackedRecordReaderFromFragments(
			fragments,
			"parquet",
			schema,
			rwOptions.bufferSize,
			rwOptions.storageConfig,
			nil,
			rwOptions.externalReader,
		)
	default:
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("unsupported storage version %d", rwOptions.version))
	}
}

func buildDeltalogFragmentsFromBinlogs(binlogs []*datapb.Binlog) ([]packed.Fragment, error) {
	fragments := make([]packed.Fragment, 0, len(binlogs))
	var offset int64
	for _, binlog := range binlogs {
		if binlog == nil {
			continue
		}
		path := binlog.GetLogPath()
		if path == "" {
			return nil, merr.WrapErrServiceInternal("deltalog binlog path is empty")
		}
		entriesNum := binlog.GetEntriesNum()
		if entriesNum <= 0 {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("deltalog %s has non-positive entries num %d", path, entriesNum))
		}
		end := offset + entriesNum
		fragments = append(fragments, packed.Fragment{
			FilePath: path,
			StartRow: offset,
			EndRow:   end,
			RowCount: entriesNum,
		})
		offset = end
	}
	return fragments, nil
}
