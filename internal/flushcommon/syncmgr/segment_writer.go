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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SegmentWriteContext contains the identifiers and dependencies shared by all
// writes for one sync pack.
type SegmentWriteContext struct {
	CollectionID int64
	PartitionID  int64
	SegmentID    int64
	TsFrom       typeutil.Timestamp
	TsTo         typeutil.Timestamp

	Schema       *schemapb.CollectionSchema
	ChunkManager storage.ChunkManager
	Allocator    allocator.Interface
	RetryOptions []retry.Option
}

// SegmentWriter turns the shared segment context into small write methods.
type SegmentWriter struct {
	SegmentWriteContext
}

func (w *SegmentWriter) WithTimeRange(tsFrom, tsTo typeutil.Timestamp) *SegmentWriter {
	next := *w
	next.TsFrom = tsFrom
	next.TsTo = tsTo
	return &next
}

func (w *SegmentWriter) WriteV1InsertBlobs(ctx context.Context, blobs map[int64]*storage.Blob) (map[int64]*datapb.FieldBinlog, int64, error) {
	logs := make(map[int64]*datapb.FieldBinlog)
	var written int64
	for fieldID, blob := range blobs {
		id, err := w.Allocator.AllocOne()
		if err != nil {
			return nil, 0, err
		}
		binlog, size, err := writeFieldBlob(ctx, fieldBlobWriteInput{
			RootPath:     w.ChunkManager.RootPath(),
			LogRoot:      common.SegmentInsertLogPath,
			CollectionID: w.CollectionID,
			PartitionID:  w.PartitionID,
			SegmentID:    w.SegmentID,
			FieldID:      fieldID,
			LogID:        id,
			TsFrom:       w.TsFrom,
			TsTo:         w.TsTo,
			Blob:         blob,
			ChunkManager: w.ChunkManager,
			RetryOptions: w.RetryOptions,
		})
		if err != nil {
			return nil, 0, err
		}
		written += size
		logs[fieldID] = &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{binlog},
		}
	}
	return logs, written, nil
}

type V1StatsBlobsInput struct {
	FieldID    int64
	BatchBlob  *storage.Blob
	MergedBlob *storage.Blob
}

func (w *SegmentWriter) WriteV1StatsBlobs(ctx context.Context, input V1StatsBlobsInput) (map[int64]*datapb.FieldBinlog, int64, error) {
	binlogs := make([]*datapb.Binlog, 0, 2)
	var written int64
	if input.BatchBlob != nil {
		id, err := w.Allocator.AllocOne()
		if err != nil {
			return nil, 0, err
		}
		binlog, size, err := writeFieldBlob(ctx, fieldBlobWriteInput{
			RootPath:     w.ChunkManager.RootPath(),
			LogRoot:      common.SegmentStatslogPath,
			CollectionID: w.CollectionID,
			PartitionID:  w.PartitionID,
			SegmentID:    w.SegmentID,
			FieldID:      input.FieldID,
			LogID:        id,
			TsFrom:       w.TsFrom,
			TsTo:         w.TsTo,
			Blob:         input.BatchBlob,
			ChunkManager: w.ChunkManager,
			RetryOptions: w.RetryOptions,
		})
		if err != nil {
			return nil, 0, err
		}
		written += size
		binlogs = append(binlogs, binlog)
	}
	if input.MergedBlob != nil {
		binlog, size, err := writeFieldBlob(ctx, fieldBlobWriteInput{
			RootPath:     w.ChunkManager.RootPath(),
			LogRoot:      common.SegmentStatslogPath,
			CollectionID: w.CollectionID,
			PartitionID:  w.PartitionID,
			SegmentID:    w.SegmentID,
			FieldID:      input.FieldID,
			LogID:        int64(storage.CompoundStatsType),
			TsFrom:       w.TsFrom,
			TsTo:         w.TsTo,
			Blob:         input.MergedBlob,
			ChunkManager: w.ChunkManager,
			RetryOptions: w.RetryOptions,
		})
		if err != nil {
			return nil, 0, err
		}
		written += size
		binlogs = append(binlogs, binlog)
	}
	return map[int64]*datapb.FieldBinlog{
		input.FieldID: {
			FieldID: input.FieldID,
			Binlogs: binlogs,
		},
	}, written, nil
}

type V1BM25BlobsInput struct {
	BatchBlobs  map[int64]*storage.Blob
	MergedBlobs map[int64]*storage.Blob
}

func (w *SegmentWriter) WriteV1BM25Blobs(ctx context.Context, input V1BM25BlobsInput) (map[int64]*datapb.FieldBinlog, int64, error) {
	logs := make(map[int64]*datapb.FieldBinlog)
	var written int64
	for fieldID, blob := range input.BatchBlobs {
		id, err := w.Allocator.AllocOne()
		if err != nil {
			return nil, 0, err
		}
		binlog, size, err := writeFieldBlob(ctx, fieldBlobWriteInput{
			RootPath:     w.ChunkManager.RootPath(),
			LogRoot:      common.SegmentBm25LogPath,
			CollectionID: w.CollectionID,
			PartitionID:  w.PartitionID,
			SegmentID:    w.SegmentID,
			FieldID:      fieldID,
			LogID:        id,
			TsFrom:       w.TsFrom,
			TsTo:         w.TsTo,
			Blob:         blob,
			ChunkManager: w.ChunkManager,
			RetryOptions: w.RetryOptions,
		})
		if err != nil {
			return nil, 0, err
		}
		written += size
		logs[fieldID] = &datapb.FieldBinlog{FieldID: fieldID, Binlogs: []*datapb.Binlog{binlog}}
	}
	for fieldID, blob := range input.MergedBlobs {
		binlog, size, err := writeFieldBlob(ctx, fieldBlobWriteInput{
			RootPath:     w.ChunkManager.RootPath(),
			LogRoot:      common.SegmentBm25LogPath,
			CollectionID: w.CollectionID,
			PartitionID:  w.PartitionID,
			SegmentID:    w.SegmentID,
			FieldID:      fieldID,
			LogID:        int64(storage.CompoundStatsType),
			TsFrom:       w.TsFrom,
			TsTo:         w.TsTo,
			Blob:         blob,
			ChunkManager: w.ChunkManager,
			RetryOptions: w.RetryOptions,
		})
		if err != nil {
			return nil, 0, err
		}
		written += size
		fieldBinlog := logs[fieldID]
		if fieldBinlog == nil {
			fieldBinlog = &datapb.FieldBinlog{FieldID: fieldID}
			logs[fieldID] = fieldBinlog
		}
		fieldBinlog.Binlogs = append(fieldBinlog.Binlogs, binlog)
	}
	return logs, written, nil
}

type DeltaWriteParams struct {
	LogID         int64
	PKType        schemapb.DataType
	Path          string
	DeleteData    *storage.DeleteData
	Version       int64
	StorageConfig *indexpb.StorageConfig
	Uploader      func(context.Context, map[string][]byte) error
}

func (w *SegmentWriter) WriteDelta(ctx context.Context, input DeltaWriteParams) (*datapb.Binlog, int64, error) {
	writer, err := storage.NewDeltalogWriter(
		ctx, w.CollectionID, w.PartitionID, w.SegmentID, input.LogID, input.PKType, input.Path,
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
