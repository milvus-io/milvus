// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package gsegment

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type insertChunkTaskV3 struct {
	chunk         *InsertChunk
	schema        *schemapb.CollectionSchema
	collectionID  int64
	partitionID   int64
	segmentID     int64
	manifestPath  string
	flush         bool
	segmentRows   int64
	columnGroups  []storagecommon.ColumnGroup
	storageConfig *indexpb.StorageConfig
	segmentWriter *syncmgr.SegmentWriter
	onDone        func(*InsertChunkTaskResult, error)

	state      taskState
	cpuBounded bool

	record       storage.Record
	statsPayload *syncmgr.CurrentStatsPayload
	mergedStats  *syncmgr.MergedStatsBlobs

	fieldBinlog map[int64]*datapb.FieldBinlog
}

func newInsertChunkTaskV3(
	chunk *InsertChunk,
	schema *schemapb.CollectionSchema,
	collectionID, partitionID, segmentID int64,
	manifestPath string,
	flush bool,
	segmentRows int64,
	columnGroups []storagecommon.ColumnGroup,
	cm storage.ChunkManager,
	alloc allocator.Interface,
	storageConfig *indexpb.StorageConfig,
	onDone func(*InsertChunkTaskResult, error),
) *insertChunkTaskV3 {
	return &insertChunkTaskV3{
		chunk:         chunk,
		schema:        schema,
		collectionID:  collectionID,
		partitionID:   partitionID,
		segmentID:     segmentID,
		manifestPath:  manifestPath,
		flush:         flush,
		segmentRows:   segmentRows,
		columnGroups:  columnGroups,
		storageConfig: storageConfig,
		segmentWriter: &syncmgr.SegmentWriter{SegmentWriteContext: syncmgr.SegmentWriteContext{
			CollectionID: collectionID,
			PartitionID:  partitionID,
			SegmentID:    segmentID,
			TsFrom:       chunk.startFromTimeTick,
			TsTo:         chunk.endToTimeTick,
			Schema:       schema,
			ChunkManager: cm,
			Allocator:    alloc,
		}},
		onDone:     onDone,
		state:      taskStateSerializing,
		cpuBounded: true,
	}
}

func (t *insertChunkTaskV3) Key() string { return insertTaskKey(t.segmentID, t.chunk) }

func (t *insertChunkTaskV3) CPUBound() bool { return t.cpuBounded }

func (t *insertChunkTaskV3) Poll(ctx context.Context) error {
	if ctx.Err() != nil {
		t.complete(syncutil.ErrStagedSchedulerClosed)
		return nil
	}
	switch t.state {
	case taskStateSerializing:
		if err := t.serialize(ctx); err != nil {
			err = fmt.Errorf("serialize insert data: %w", err)
			t.complete(err)
			return err
		}
		t.state = taskStateUploading
		t.cpuBounded = false
		return syncutil.ErrContinue
	case taskStateUploading:
		if err := t.upload(ctx); err != nil {
			if ctx.Err() != nil {
				t.complete(syncutil.ErrStagedSchedulerClosed)
				return nil
			}
			return syncutil.NewRetryableError(fmt.Errorf("upload insert blobs: %w", err))
		}
		t.state = taskStateDone
		t.complete(nil)
		return nil
	case taskStateDone:
		return nil
	default:
		err := fmt.Errorf("insert v3 task in unknown state: %v", t.state)
		t.complete(err)
		return err
	}
}

func (t *insertChunkTaskV3) complete(err error) {
	defer t.releaseRecord()
	if t.onDone == nil {
		return
	}
	if err != nil {
		t.onDone(nil, err)
		return
	}
	t.onDone(&InsertChunkTaskResult{
		Binlog:       buildInsertTaskBinlog(t.chunk, t.fieldBinlog, nil, nil),
		ManifestPath: t.manifestPath,
	}, nil)
}

func (t *insertChunkTaskV3) serialize(ctx context.Context) error {
	msgs := t.chunk.Drain()
	data, err := prepareInsertData(t.schema, msgs)
	if err != nil {
		return fmt.Errorf("prepare insert data: %w", err)
	}
	record, err := buildInsertRecord(t.schema, data)
	if err != nil {
		return err
	}
	if t.storageConfig == nil {
		t.storageConfig = packed.CreateStorageConfig()
	}
	if len(t.columnGroups) == 0 {
		t.columnGroups = syncmgr.ColumnGroupsFromRecord(t.schema, record)
	}
	statsPayload, err := syncmgr.PrepareCurrentStats(t.collectionID, t.schema, data, record, totalRows(data))
	if err != nil {
		record.Release()
		return err
	}
	var mergedStats *syncmgr.MergedStatsBlobs
	if t.flush {
		mergedStats, err = syncmgr.PrepareMergedManifestStatsBlobs(ctx, t.segmentWriter.ChunkManager, t.collectionID, t.schema, t.storageConfig, t.manifestPath, t.segmentRows, statsPayload)
		if err != nil {
			record.Release()
			return err
		}
	}
	t.record = record
	t.statsPayload = statsPayload
	t.mergedStats = mergedStats
	return nil
}

func (t *insertChunkTaskV3) upload(ctx context.Context) error {
	if t.segmentWriter.Allocator == nil {
		return fmt.Errorf("log id allocator is nil")
	}
	if t.record == nil {
		return fmt.Errorf("insert record is nil")
	}
	var err error
	writer := &syncmgr.ManifestSegmentWriter{
		PackedSegmentWriter: &syncmgr.PackedSegmentWriter{
			SegmentWriter:       t.segmentWriter,
			StorageConfig:       t.storageConfig,
			ColumnGroups:        t.columnGroups,
			BufferSize:          0,
			MultiPartUploadSize: packed.DefaultMultiPartUploadSize,
		},
		ManifestPath: t.manifestPath,
	}
	t.fieldBinlog, t.manifestPath, err = writer.WriteManifestInsert(ctx, t.record)
	if err != nil {
		return err
	}
	_, _, err = writer.WritePKStatsBlobs(ctx, syncmgr.ManifestPKStatsInput{
		FieldID:    t.statsPayload.PKFieldID(),
		BatchBlob:  t.statsPayload.StatsBlob(),
		MergedBlob: mergedStatsBlob(t.mergedStats),
	})
	if err != nil {
		return err
	}
	_, _, err = writer.WriteBM25StatsBlobs(ctx, syncmgr.ManifestBM25StatsInput{
		BatchBlobs:  t.statsPayload.BM25Blobs(),
		MergedBlobs: mergedBM25Blobs(t.mergedStats),
	})
	if err != nil {
		return err
	}
	t.manifestPath = writer.ManifestPath
	return nil
}

func (t *insertChunkTaskV3) releaseRecord() {
	if t.record != nil {
		t.record.Release()
		t.record = nil
	}
}
