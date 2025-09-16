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
	"fmt"
	sio "io"
	"path"
	"sort"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	StorageV1 int64 = 0
	StorageV2 int64 = 2
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
}

func (o *rwOptions) validate() error {
	if o.storageConfig == nil {
		return merr.WrapErrServiceInternal("storage config is nil")
	}
	if o.collectionID == 0 {
		log.Warn("storage config collection id is empty when init BinlogReader")
		// return merr.WrapErrServiceInternal("storage config collection id is empty")
	}
	if o.op == OpWrite && o.uploader == nil {
		return merr.WrapErrServiceInternal("uploader is nil for writer")
	}
	switch o.version {
	case StorageV1:
		if o.op == OpRead && o.downloader == nil {
			return merr.WrapErrServiceInternal("downloader is nil for v1 reader")
		}
	case StorageV2:
	default:
		return merr.WrapErrServiceInternal(fmt.Sprintf("unsupported storage version %d", o.version))
	}
	return nil
}

type RwOption func(*rwOptions)

func DefaultWriterOptions() *rwOptions {
	return &rwOptions{
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

func WithNeededFields(neededFields typeutil.Set[int64]) RwOption {
	return func(options *rwOptions) {
		options.neededFields = neededFields
	}
}

func makeBlobsReader(ctx context.Context, binlogs []*datapb.FieldBinlog, downloader downloaderFn) (ChunkedBlobsReader, error) {
	if len(binlogs) == 0 {
		return func() ([]*Blob, error) {
			return nil, sio.EOF
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
			return nil, sio.EOF
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
	if hookutil.IsClusterEncyptionEnabled() {
		if ez := hookutil.GetEzByCollProperties(schema.GetProperties(), rwOptions.collectionID); ez != nil {
			binlogReaderOpts = append(binlogReaderOpts, WithReaderDecryptionContext(ez.EzID, ez.CollectionID))

			unsafe := hookutil.GetCipher().GetUnsafeKey(ez.EzID, ez.CollectionID)
			if len(unsafe) > 0 {
				pluginContext = &indexcgopb.StoragePluginContext{
					EncryptionZoneId: ez.EzID,
					CollectionId:     ez.CollectionID,
					EncryptionKey:    string(unsafe),
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
		rr, err = newCompositeBinlogRecordReader(schema, blobsReader, binlogReaderOpts...)
	case StorageV2:
		if len(binlogs) <= 0 {
			return nil, sio.EOF
		}
		sort.Slice(binlogs, func(i, j int) bool {
			return binlogs[i].GetFieldID() < binlogs[j].GetFieldID()
		})
		binlogLists := lo.Map(binlogs, func(fieldBinlog *datapb.FieldBinlog, _ int) []*datapb.Binlog {
			return fieldBinlog.GetBinlogs()
		})
		bucketName := rwOptions.storageConfig.BucketName
		paths := make([][]string, len(binlogLists[0]))
		for _, binlogs := range binlogLists {
			for j, binlog := range binlogs {
				logPath := binlog.GetLogPath()
				if rwOptions.storageConfig.StorageType != "local" {
					logPath = path.Join(bucketName, logPath)
				}
				paths[j] = append(paths[j], logPath)
			}
		}
		rr, err = newPackedRecordReader(paths, schema, rwOptions.bufferSize, rwOptions.storageConfig, pluginContext)
	default:
		return nil, merr.WrapErrServiceInternal(fmt.Sprintf("unsupported storage version %d", rwOptions.version))
	}
	if err != nil {
		return nil, err
	}
	if rwOptions.neededFields != nil {
		rr.SetNeededFields(rwOptions.neededFields)
	}
	return rr, nil
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

	blobsWriter := func(blobs []*Blob) error {
		kvs := make(map[string][]byte, len(blobs))
		for _, blob := range blobs {
			kvs[blob.Key] = blob.Value
		}
		return rwOptions.uploader(ctx, kvs)
	}

	opts := []StreamWriterOption{}
	var pluginContext *indexcgopb.StoragePluginContext
	if hookutil.IsClusterEncyptionEnabled() {
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
					EncryptionKey:    string(unsafe),
				}
			}
		}
	}

	switch rwOptions.version {
	case StorageV1:
		rootPath := rwOptions.storageConfig.GetRootPath()
		return newCompositeBinlogRecordWriter(collectionID, partitionID, segmentID, schema,
			blobsWriter, allocator, chunkSize, rootPath, maxRowNum, opts...,
		)
	case StorageV2:
		return newPackedBinlogRecordWriter(collectionID, partitionID, segmentID, schema,
			blobsWriter, allocator, maxRowNum,
			rwOptions.bufferSize, rwOptions.multiPartUploadSize, rwOptions.columnGroups,
			rwOptions.storageConfig,
			pluginContext,
		)
	}
	return nil, merr.WrapErrServiceInternal(fmt.Sprintf("unsupported storage version %d", rwOptions.version))
}
