// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package goplog

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// DeleteChunkTaskResult is the delta-binlog output of a completed delete task.
type DeleteChunkTaskResult struct {
	Binlog *datapb.FieldBinlog
}

// DeleteChunkTask serializes a DeleteChunk's PKs into a delta log and uploads it.
//
// Stages: Init (CPU, extract PKs into DeleteData) → Serialize (CPU, codec) →
// Upload (IO) → Done. See StagedTask.
type DeleteChunkTask struct {
	chunk         *DeleteChunk
	collectionID  int64
	partitionID   int64
	segmentID     int64
	schema        *schemapb.CollectionSchema
	chunkManager  storage.ChunkManager
	allocator     allocator.Interface
	storageConfig *indexpb.StorageConfig
	onDone        func(*DeleteChunkTaskResult, error)

	state      taskState
	cpuBounded bool
	deleteData *storage.DeleteData
	binlog     *datapb.FieldBinlog
}

// NewDeleteChunkTask creates a delete sync task.
func NewDeleteChunkTask(
	chunk *DeleteChunk,
	collectionID, partitionID, segmentID int64,
	schema *schemapb.CollectionSchema,
	cm storage.ChunkManager,
	alloc allocator.Interface,
	storageConfig *indexpb.StorageConfig,
	onDone func(*DeleteChunkTaskResult, error),
) *DeleteChunkTask {
	return &DeleteChunkTask{
		chunk:         chunk,
		collectionID:  collectionID,
		partitionID:   partitionID,
		segmentID:     segmentID,
		schema:        schema,
		chunkManager:  cm,
		allocator:     alloc,
		storageConfig: storageConfig,
		onDone:        onDone,
		state:         taskStateInit,
		cpuBounded:    true,
	}
}

// Key implements StagedTask.
func (t *DeleteChunkTask) Key() string {
	return fmt.Sprintf("delete/buf=%d/tt=%d-%d", t.segmentID, t.chunk.startFromTimeTick, t.chunk.endToTimeTick)
}

// CPUBound implements StagedTask.
func (t *DeleteChunkTask) CPUBound() bool { return t.cpuBounded }

func (t *DeleteChunkTask) complete(err error) {
	if t.onDone == nil {
		return
	}
	if err != nil {
		t.onDone(nil, err)
		return
	}
	t.onDone(&DeleteChunkTaskResult{Binlog: t.binlog}, nil)
}

// Poll implements StagedTask.
func (t *DeleteChunkTask) Poll(ctx context.Context) error {
	if ctx.Err() != nil {
		t.complete(syncutil.ErrStagedSchedulerClosed)
		return nil
	}
	switch t.state {
	case taskStateInit:
		dd, err := buildDeleteData(t.chunk)
		if err != nil {
			err = fmt.Errorf("build delete data: %w", err)
			t.complete(err)
			return err
		}
		t.deleteData = dd
		t.state = taskStateSerializing
		t.cpuBounded = true
		return syncutil.ErrContinue

	case taskStateSerializing:
		t.state = taskStateUploading
		t.cpuBounded = false
		return syncutil.ErrContinue

	case taskStateUploading:
		if t.binlog == nil {
			pkField, err := typeutil.GetPrimaryFieldSchema(t.schema)
			if err != nil {
				err = fmt.Errorf("primary key field not found: %w", err)
				t.complete(err)
				return err
			}
			logID, err := t.allocator.AllocOne()
			if err != nil {
				t.complete(err)
				return err
			}
			deltaPath := metautil.BuildDeltaLogPath(t.storageConfig.GetRootPath(), t.collectionID, t.partitionID, t.segmentID, logID)
			binlog, _, err := syncmgr.WriteDelta(ctx, syncmgr.DeltaWriteInput{
				CollectionID: t.collectionID,
				PartitionID:  t.partitionID,
				SegmentID:    t.segmentID,
				LogID:        logID,
				PKType:       pkField.GetDataType(),
				Path:         deltaPath,
				DeleteData:   t.deleteData,
				Version:      storage.StorageV1,
				Uploader: func(ctx context.Context, kvs map[string][]byte) error {
					for k, blob := range kvs {
						return t.chunkManager.Write(ctx, k, blob)
					}
					return nil
				},
			})
			if err != nil {
				if ctx.Err() != nil {
					t.complete(syncutil.ErrStagedSchedulerClosed)
					return nil
				}
				return syncutil.NewRetryableError(fmt.Errorf("upload delete blob: %w", err))
			}
			t.binlog = &datapb.FieldBinlog{
				FieldID: pkField.GetFieldID(),
				Binlogs: []*datapb.Binlog{binlog},
			}
		}
		t.state = taskStateDone
		t.complete(nil)
		return nil

	case taskStateDone:
		return nil

	default:
		err := fmt.Errorf("delete task in unknown state: %v", t.state)
		t.complete(err)
		return err
	}
}

// buildDeleteData extracts PKs + timestamps from the chunk's delete messages.
func buildDeleteData(chunk *DeleteChunk) (*storage.DeleteData, error) {
	pks := make([]storage.PrimaryKey, 0)
	tss := make([]uint64, 0)
	for _, msg := range chunk.msgs {
		body, err := msg.Body()
		if err != nil {
			return nil, fmt.Errorf("read delete body: %w", err)
		}
		if body == nil || body.PrimaryKeys == nil {
			continue
		}
		ts := msg.TimeTick()
		switch ids := body.PrimaryKeys.GetIdField().(type) {
		case *schemapb.IDs_IntId:
			for _, id := range ids.IntId.GetData() {
				pks = append(pks, storage.NewInt64PrimaryKey(id))
				tss = append(tss, ts)
			}
		case *schemapb.IDs_StrId:
			for _, id := range ids.StrId.GetData() {
				pks = append(pks, storage.NewVarCharPrimaryKey(id))
				tss = append(tss, ts)
			}
		}
	}
	dd := storage.NewDeleteData(pks, tss)
	dd.RowCount = int64(len(pks))
	return dd, nil
}
