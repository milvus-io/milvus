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

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// PackedSegmentWriter adds storage-v2 packed-record parameters to a segment writer.
type PackedSegmentWriter struct {
	*SegmentWriter

	StorageConfig       *indexpb.StorageConfig
	ColumnGroups        []storagecommon.ColumnGroup
	BufferSize          int64
	MultiPartUploadSize int64
	PluginContext       *indexcgopb.StoragePluginContext
}

func (w *PackedSegmentWriter) WritePackedInsert(ctx context.Context, record storage.Record) (map[int64]*datapb.FieldBinlog, string, error) {
	rootPath := w.ChunkManager.RootPath()
	bucketName := paramtable.Get().MinioCfg.BucketName.GetValue()
	if w.StorageConfig != nil {
		rootPath = w.StorageConfig.GetRootPath()
		bucketName = w.StorageConfig.GetBucketName()
	}
	multiPartUploadSize := w.MultiPartUploadSize
	if multiPartUploadSize == 0 {
		multiPartUploadSize = packed.DefaultMultiPartUploadSize
	}
	logs := make(map[int64]*datapb.FieldBinlog)
	paths := make([]string, 0, len(w.ColumnGroups))
	for _, columnGroup := range w.ColumnGroups {
		id, err := w.Allocator.AllocOne()
		if err != nil {
			return nil, "", err
		}
		p := metautil.BuildInsertLogPath(rootPath, w.CollectionID, w.PartitionID, w.SegmentID, columnGroup.GroupID, id)
		paths = append(paths, p)
	}
	writer, err := storage.NewPackedRecordWriter(bucketName, paths, w.Schema, w.BufferSize, multiPartUploadSize, w.ColumnGroups, w.StorageConfig, w.PluginContext)
	if err != nil {
		return nil, "", err
	}
	if err := writer.Write(record); err != nil {
		_ = writer.Close()
		return nil, "", err
	}
	if err := writer.Close(); err != nil {
		return nil, "", err
	}
	for _, columnGroup := range w.ColumnGroups {
		columnGroupID := columnGroup.GroupID
		logs[columnGroupID] = &datapb.FieldBinlog{
			FieldID:     columnGroupID,
			ChildFields: columnGroup.Fields,
			Binlogs: []*datapb.Binlog{{
				LogSize:         int64(writer.GetColumnGroupWrittenCompressed(columnGroup.GroupID)),
				MemorySize:      int64(writer.GetColumnGroupWrittenUncompressed(columnGroup.GroupID)),
				LogPath:         writer.GetWrittenPaths(columnGroupID),
				EntriesNum:      writer.GetWrittenRowNum(),
				TimestampFrom:   w.TsFrom,
				TimestampTo:     w.TsTo,
				FieldNullCounts: fieldNullCounts(record, columnGroup),
			}},
		}
	}
	return logs, "", nil
}
