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
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type insertChunkTaskV2 struct {
	chunk          *InsertChunk
	schema         *schemapb.CollectionSchema
	collectionID   int64
	partitionID    int64
	segmentID      int64
	flush          bool
	segmentRows    int64
	columnGroups   []storagecommon.ColumnGroup
	previousBinlog []*streamingpb.L1SegmentBinLogs
	storageConfig  *indexpb.StorageConfig
	segmentWriter  *syncmgr.SegmentWriter
	onDone         func(*InsertChunkTaskResult, error)

	state      taskState
	cpuBounded bool

	record       storage.Record
	statsPayload *syncmgr.CurrentStatsPayload
	mergedStats  *syncmgr.MergedStatsBlobs

	fieldBinlog       map[int64]*datapb.FieldBinlog
	statsBinlog       map[int64]*datapb.FieldBinlog
	bm25Binlog        map[int64]*datapb.FieldBinlog
	mergedStatsBinlog *datapb.FieldBinlog
}

func newInsertChunkTaskV2(
	chunk *InsertChunk,
	schema *schemapb.CollectionSchema,
	collectionID, partitionID, segmentID int64,
	flush bool,
	segmentRows int64,
	columnGroups []storagecommon.ColumnGroup,
	previousBinlog []*streamingpb.L1SegmentBinLogs,
	cm storage.ChunkManager,
	alloc allocator.Interface,
	storageConfig *indexpb.StorageConfig,
	onDone func(*InsertChunkTaskResult, error),
) *insertChunkTaskV2 {
	return &insertChunkTaskV2{
		chunk:          chunk,
		schema:         schema,
		collectionID:   collectionID,
		partitionID:    partitionID,
		segmentID:      segmentID,
		flush:          flush,
		segmentRows:    segmentRows,
		columnGroups:   columnGroups,
		previousBinlog: previousBinlog,
		storageConfig:  storageConfig,
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

func (t *insertChunkTaskV2) Key() string { return insertTaskKey(t.segmentID, t.chunk) }

func (t *insertChunkTaskV2) CPUBound() bool { return t.cpuBounded }

func (t *insertChunkTaskV2) Poll(ctx context.Context) error {
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
		err := fmt.Errorf("insert v2 task in unknown state: %v", t.state)
		t.complete(err)
		return err
	}
}

func (t *insertChunkTaskV2) complete(err error) {
	defer t.releaseRecord()
	if t.onDone == nil {
		return
	}
	if err != nil {
		t.onDone(nil, err)
		return
	}
	t.onDone(&InsertChunkTaskResult{
		Binlog:            buildInsertTaskBinlog(t.chunk, t.fieldBinlog, t.statsBinlog, t.bm25Binlog),
		MergedStatsBinlog: t.mergedStatsBinlog,
	}, nil)
}

func (t *insertChunkTaskV2) serialize(ctx context.Context) error {
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
		mergedStats, err = syncmgr.PrepareMergedFieldStatsBlobs(ctx, t.segmentWriter.ChunkManager, t.collectionID, t.schema, syncmgr.MergedStatsBlobsInput{
			SegmentRows:     t.segmentRows,
			PreviousBinlogs: t.previousBinlog,
			Current:         statsPayload,
		})
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

func (t *insertChunkTaskV2) upload(ctx context.Context) error {
	if t.segmentWriter.Allocator == nil {
		return fmt.Errorf("log id allocator is nil")
	}
	if t.record == nil {
		return fmt.Errorf("insert record is nil")
	}
	var err error
	packedWriter := &syncmgr.PackedSegmentWriter{
		SegmentWriter:       t.segmentWriter,
		StorageConfig:       t.storageConfig,
		ColumnGroups:        t.columnGroups,
		BufferSize:          0,
		MultiPartUploadSize: packed.DefaultMultiPartUploadSize,
	}
	t.fieldBinlog, _, err = packedWriter.WritePackedInsert(ctx, t.record)
	if err != nil {
		return err
	}
	stats, _, err := t.segmentWriter.WriteV1StatsBlobs(ctx, syncmgr.V1StatsBlobsInput{
		FieldID:    t.statsPayload.PKFieldID(),
		BatchBlob:  t.statsPayload.StatsBlob(),
		MergedBlob: mergedStatsBlob(t.mergedStats),
	})
	if err != nil {
		return err
	}
	t.statsBinlog, t.mergedStatsBinlog = splitStatsBinlog(t.statsPayload.PKFieldID(), stats)
	t.bm25Binlog, _, err = t.segmentWriter.WriteV1BM25Blobs(ctx, syncmgr.V1BM25BlobsInput{
		BatchBlobs:  t.statsPayload.BM25Blobs(),
		MergedBlobs: mergedBM25Blobs(t.mergedStats),
	})
	if err != nil {
		return err
	}
	return nil
}

func (t *insertChunkTaskV2) releaseRecord() {
	if t.record != nil {
		t.record.Release()
		t.record = nil
	}
}
