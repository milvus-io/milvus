// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package syncmgr

import (
	"context"
	"fmt"
	"path"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// manifestRecordWriter is the common interface for both packedRecordManifestWriter
// and packedTextManifestWriter used for V3 storage writes.
type manifestRecordWriter interface {
	storage.RecordWriter
	GetColumnGroupWrittenCompressed(columnGroup typeutil.UniqueID) uint64
	GetColumnGroupWrittenUncompressed(columnGroup typeutil.UniqueID) uint64
	GetWrittenPaths(columnGroup typeutil.UniqueID) string
	GetWrittenManifest() string
	GetWrittenRowNum() int64
}

type fieldBlobWriteInput struct {
	RootPath     string
	LogRoot      string
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	FieldID      int64
	LogID        int64
	TsFrom       typeutil.Timestamp
	TsTo         typeutil.Timestamp
	Blob         *storage.Blob
	ChunkManager storage.ChunkManager
	RetryOptions []retry.Option
}

func writeFieldBlob(ctx context.Context, input fieldBlobWriteInput) (*datapb.Binlog, int64, error) {
	if input.Blob == nil {
		return nil, 0, nil
	}
	p := metautil.JoinIDPath(input.CollectionID, input.PartitionID, input.SegmentID, input.FieldID, input.LogID)
	key := path.Join(input.RootPath, input.LogRoot, p)
	err := retry.Handle(ctx, func() (bool, error) {
		err := input.ChunkManager.Write(ctx, key, input.Blob.Value)
		if err == nil {
			return false, nil
		}
		err = storage.ToMilvusIoError(key, err)
		if merr.IsNonRetryableErr(err) {
			return false, err
		}
		return true, err
	}, input.RetryOptions...)
	if err != nil {
		return nil, 0, err
	}
	size := int64(len(input.Blob.GetValue()))
	return &datapb.Binlog{
		EntriesNum:    input.Blob.RowNum,
		TimestampFrom: input.TsFrom,
		TimestampTo:   input.TsTo,
		LogPath:       key,
		LogSize:       size,
		MemorySize:    input.Blob.MemorySize,
	}, size, nil
}

type DeltaWriteInput struct {
	CollectionID  int64
	PartitionID   int64
	SegmentID     int64
	LogID         int64
	PKType        schemapb.DataType
	Path          string
	DeleteData    *storage.DeleteData
	Version       int64
	StorageConfig *indexpb.StorageConfig
	Uploader      func(context.Context, map[string][]byte) error
}

func WriteDelta(ctx context.Context, input DeltaWriteInput) (*datapb.Binlog, int64, error) {
	writer, err := storage.NewDeltalogWriter(
		ctx, input.CollectionID, input.PartitionID, input.SegmentID, input.LogID, input.PKType, input.Path,
		storage.WithVersion(input.Version),
		storage.WithStorageConfig(input.StorageConfig),
		storage.WithUploader(input.Uploader),
	)
	if err != nil {
		return nil, 0, err
	}
	record, tsFrom, tsTo, err := storage.BuildDeleteRecord(input.DeleteData.Pks, input.DeleteData.Tss)
	if err != nil {
		return nil, 0, err
	}
	defer record.Release()
	if err := writer.Write(record); err != nil {
		return nil, 0, err
	}
	if err := writer.Close(); err != nil {
		return nil, 0, err
	}
	memorySize := input.DeleteData.Size()
	logSize := input.DeleteData.Size() / 4
	if input.Version == storage.StorageV2 {
		logSize = 0
		if written := writer.GetWrittenUncompressed(); written != 0 {
			memorySize = int64(written)
		}
	}
	binlog := &datapb.Binlog{
		EntriesNum:    input.DeleteData.RowCount,
		TimestampFrom: tsFrom,
		TimestampTo:   tsTo,
		LogPath:       input.Path,
		LogSize:       logSize,
		MemorySize:    memorySize,
	}
	if input.Version == storage.StorageV2 {
		binlog.LogID = input.LogID
	}
	return binlog, logSize, nil
}

func fieldNullCounts(record storage.Record, columnGroup storagecommon.ColumnGroup) map[int64]int64 {
	result := make(map[int64]int64, len(columnGroup.Fields))
	for _, fieldID := range columnGroup.Fields {
		if col := record.Column(fieldID); col != nil {
			result[fieldID] = int64(col.NullN())
		}
	}
	return result
}

func manifestBasePath(rootPath string, collectionID, partitionID, segmentID int64) string {
	return path.Join(rootPath, common.SegmentInsertLogPath, metautil.JoinIDPath(collectionID, partitionID, segmentID))
}

func ManifestBaseAndVersion(rootPath string, collectionID, partitionID, segmentID int64, manifestPath string) (string, int64, error) {
	if manifestPath != "" {
		return packed.UnmarshalManifestPath(manifestPath)
	}
	return manifestBasePath(rootPath, collectionID, partitionID, segmentID), packed.ManifestEarliest, nil
}

func FieldBinlogValues(binlogs map[int64]*datapb.FieldBinlog) []*datapb.FieldBinlog {
	values := make([]*datapb.FieldBinlog, 0, len(binlogs))
	for _, binlog := range binlogs {
		if binlog != nil {
			values = append(values, binlog)
		}
	}
	return values
}

func ColumnGroupsFromRecord(schema *schemapb.CollectionSchema, record storage.Record) []storagecommon.ColumnGroup {
	allFields := typeutil.GetAllFieldSchemas(schema)
	stats := make(map[int64]storagecommon.ColumnStats, len(allFields))
	for _, field := range allFields {
		arr := record.Column(field.GetFieldID())
		if arr == nil || arr.Len() == 0 {
			continue
		}
		stats[field.GetFieldID()] = storagecommon.ColumnStats{
			AvgSize: int64(arr.Data().SizeInBytes()) / int64(arr.Len()),
		}
	}
	return storagecommon.SplitColumns(allFields, stats, storagecommon.DefaultPolicies()...)
}

func deltaSummary(logID int64, entriesNum int64, memorySize int64) *datapb.FieldBinlog {
	return &datapb.FieldBinlog{
		Binlogs: []*datapb.Binlog{{
			LogID:      logID,
			EntriesNum: entriesNum,
			MemorySize: memorySize,
		}},
	}
}

func newUnsupportedStorageVersionError(version int64) error {
	return fmt.Errorf("unsupported storage version %d", version)
}
