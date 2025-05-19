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
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	StorageV1 int64 = 0
	StorageV2 int64 = 2
)

type (
	downloaderFn func(ctx context.Context, paths []string) ([][]byte, error)
	uploaderFn   func(ctx context.Context, kvs map[string][]byte) error
)

type rwOptions struct {
	version             int64
	bufferSize          int64
	downloader          downloaderFn
	uploader            uploaderFn
	multiPartUploadSize int64
	columnGroups        []storagecommon.ColumnGroup
	bucketName          string
}

type RwOption func(*rwOptions)

func DefaultRwOptions() *rwOptions {
	return &rwOptions{
		bufferSize:          packed.DefaultWriteBufferSize,
		multiPartUploadSize: packed.DefaultMultiPartUploadSize,
	}
}

func WithVersion(version int64) RwOption {
	return func(options *rwOptions) {
		options.version = version
	}
}

func WithBucketName(bucketName string) RwOption {
	return func(options *rwOptions) {
		options.bucketName = bucketName
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

func GuessStorageVersion(binlogs []*datapb.FieldBinlog, schema *schemapb.CollectionSchema) int64 {
	if len(binlogs) == len(schema.Fields) {
		return StorageV1
	}
	return StorageV2
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

func NewBinlogRecordReader(ctx context.Context, binlogs []*datapb.FieldBinlog, schema *schemapb.CollectionSchema, option ...RwOption) (RecordReader, error) {
	rwOptions := DefaultRwOptions()
	for _, opt := range option {
		opt(rwOptions)
	}
	switch rwOptions.version {
	case StorageV1:
		blobsReader, err := makeBlobsReader(ctx, binlogs, rwOptions.downloader)
		if err != nil {
			return nil, err
		}
		return newCompositeBinlogRecordReader(schema, blobsReader)
	case StorageV2:
		if len(binlogs) <= 0 {
			return nil, sio.EOF
		}
		binlogLists := lo.Map(binlogs, func(fieldBinlog *datapb.FieldBinlog, _ int) []*datapb.Binlog {
			return fieldBinlog.GetBinlogs()
		})
		paths := make([][]string, len(binlogLists[0]))
		for _, binlogs := range binlogLists {
			for j, binlog := range binlogs {
				logPath := binlog.GetLogPath()
				if paramtable.Get().CommonCfg.StorageType.GetValue() != "local" {
					logPath = path.Join(rwOptions.bucketName, logPath)
				}
				paths[j] = append(paths[j], logPath)
			}
		}
		return newPackedRecordReader(paths, schema, rwOptions.bufferSize)
	}
	return nil, merr.WrapErrServiceInternal(fmt.Sprintf("unsupported storage version %d", rwOptions.version))
}

func NewBinlogRecordWriter(ctx context.Context, collectionID, partitionID, segmentID UniqueID,
	schema *schemapb.CollectionSchema, allocator allocator.Interface, chunkSize uint64, bucketName, rootPath string, maxRowNum int64,
	option ...RwOption,
) (BinlogRecordWriter, error) {
	rwOptions := DefaultRwOptions()
	for _, opt := range option {
		opt(rwOptions)
	}
	blobsWriter := func(blobs []*Blob) error {
		kvs := make(map[string][]byte, len(blobs))
		for _, blob := range blobs {
			kvs[blob.Key] = blob.Value
		}
		return rwOptions.uploader(ctx, kvs)
	}
	switch rwOptions.version {
	case StorageV1:
		return newCompositeBinlogRecordWriter(collectionID, partitionID, segmentID, schema,
			blobsWriter, allocator, chunkSize, rootPath, maxRowNum,
		)
	case StorageV2:
		return newPackedBinlogRecordWriter(collectionID, partitionID, segmentID, schema,
			blobsWriter, allocator, chunkSize, bucketName, rootPath, maxRowNum,
			rwOptions.bufferSize, rwOptions.multiPartUploadSize, rwOptions.columnGroups,
		)
	}
	return nil, merr.WrapErrServiceInternal(fmt.Sprintf("unsupported storage version %d", rwOptions.version))
}
