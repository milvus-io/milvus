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
	"path"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// InsertChunkTaskResult is the binlog output of a completed insert sync task.
type InsertChunkTaskResult struct {
	Binlog            *streamingpb.L1SegmentBinLogs
	ManifestPath      string
	MergedStatsBinlog *datapb.FieldBinlog
}

// NewInsertChunkTask creates an insert sync task. onDone is invoked once when
// the task reaches a terminal state; result is non-nil only on success.
func NewInsertChunkTask(
	chunk *InsertChunk,
	schema *schemapb.CollectionSchema,
	collectionID, partitionID, segmentID int64,
	storageVersion int64,
	manifestPath string,
	flush bool,
	segmentRows int64,
	columnGroups []storagecommon.ColumnGroup,
	previousBinlog []*streamingpb.L1SegmentBinLogs,
	cm storage.ChunkManager,
	alloc allocator.Interface,
	storageConfig *indexpb.StorageConfig,
	onDone func(*InsertChunkTaskResult, error),
) syncutil.StagedTask {
	switch storageVersion {
	case storage.StorageV1:
		return newInsertChunkTaskV1(chunk, schema, collectionID, partitionID, segmentID, flush, segmentRows, previousBinlog, cm, alloc, onDone)
	case storage.StorageV2:
		return newInsertChunkTaskV2(chunk, schema, collectionID, partitionID, segmentID, flush, segmentRows, columnGroups, previousBinlog, cm, alloc, storageConfig, onDone)
	case storage.StorageV3:
		return newInsertChunkTaskV3(chunk, schema, collectionID, partitionID, segmentID, manifestPath, flush, segmentRows, columnGroups, cm, alloc, storageConfig, onDone)
	default:
		return &unsupportedInsertChunkTask{
			chunk:          chunk,
			segmentID:      segmentID,
			storageVersion: storageVersion,
			onDone:         onDone,
			cpuBounded:     true,
		}
	}
}

type unsupportedInsertChunkTask struct {
	chunk          *InsertChunk
	segmentID      int64
	storageVersion int64
	onDone         func(*InsertChunkTaskResult, error)
	cpuBounded     bool
}

func (t *unsupportedInsertChunkTask) Key() string {
	return insertTaskKey(t.segmentID, t.chunk)
}

func (t *unsupportedInsertChunkTask) CPUBound() bool { return t.cpuBounded }

func (t *unsupportedInsertChunkTask) Poll(ctx context.Context) error {
	if ctx.Err() != nil {
		if t.onDone != nil {
			t.onDone(nil, syncutil.ErrStagedSchedulerClosed)
		}
		return nil
	}
	err := fmt.Errorf("unsupported storage version %d", t.storageVersion)
	if t.onDone != nil {
		t.onDone(nil, err)
	}
	return err
}

func insertTaskKey(segmentID int64, chunk *InsertChunk) string {
	return fmt.Sprintf("insert/seg=%d/tt=%d-%d", segmentID, chunk.startFromTimeTick, chunk.endToTimeTick)
}

func buildInsertTaskBinlog(chunk *InsertChunk, fieldBinlog, statsBinlog, bm25Binlog map[int64]*datapb.FieldBinlog) *streamingpb.L1SegmentBinLogs {
	return &streamingpb.L1SegmentBinLogs{
		FieldBinlog:  syncmgr.FieldBinlogValues(fieldBinlog),
		StatsBinlog:  syncmgr.FieldBinlogValues(statsBinlog),
		Bm25Binlog:   syncmgr.FieldBinlogValues(bm25Binlog),
		FromTimeTick: chunk.startFromTimeTick,
		ToTimeTick:   chunk.endToTimeTick,
	}
}

func mergedStatsBlob(merged *syncmgr.MergedStatsBlobs) *storage.Blob {
	if merged == nil {
		return nil
	}
	return merged.StatsBlob
}

func mergedBM25Blobs(merged *syncmgr.MergedStatsBlobs) map[int64]*storage.Blob {
	if merged == nil {
		return nil
	}
	return merged.BM25Blobs
}

func splitStatsBinlog(fieldID int64, stats map[int64]*datapb.FieldBinlog) (map[int64]*datapb.FieldBinlog, *datapb.FieldBinlog) {
	fieldStats := stats[fieldID]
	if fieldStats == nil {
		return stats, nil
	}
	batchStats := &datapb.FieldBinlog{FieldID: fieldID}
	mergedStats := &datapb.FieldBinlog{FieldID: fieldID}
	for _, binlog := range fieldStats.GetBinlogs() {
		if path.Base(binlog.GetLogPath()) == storage.CompoundStatsType.LogIdx() {
			mergedStats.Binlogs = append(mergedStats.Binlogs, binlog)
		} else {
			batchStats.Binlogs = append(batchStats.Binlogs, binlog)
		}
	}
	if len(batchStats.GetBinlogs()) == len(fieldStats.GetBinlogs()) {
		return stats, nil
	}
	batchOnly := make(map[int64]*datapb.FieldBinlog, len(stats))
	for id, binlog := range stats {
		if id != fieldID {
			batchOnly[id] = binlog
		}
	}
	if len(batchStats.GetBinlogs()) > 0 {
		batchOnly[fieldID] = batchStats
	}
	if len(mergedStats.GetBinlogs()) == 0 {
		return batchOnly, nil
	}
	return batchOnly, mergedStats
}

func buildInsertRecord(schema *schemapb.CollectionSchema, data []*storage.InsertData) (storage.Record, error) {
	arrowSchema, err := storage.ConvertToArrowSchema(schema, true)
	if err != nil {
		return nil, err
	}
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()

	for _, chunk := range data {
		if err := storage.BuildRecord(builder, chunk, schema); err != nil {
			return nil, err
		}
	}

	rec := builder.NewRecord()
	allFields := typeutil.GetAllFieldSchemas(schema)
	field2Col := make(map[storage.FieldID]int, len(allFields))
	for c, field := range allFields {
		field2Col[field.GetFieldID()] = c
	}
	return storage.NewSimpleArrowRecord(rec, field2Col), nil
}

// prepareInsertData converts insert messages into []*storage.InsertData.
func prepareInsertData(schema *schemapb.CollectionSchema, msgs []message.ImmutableInsertMessageV1) ([]*storage.InsertData, error) {
	out := make([]*storage.InsertData, 0, len(msgs))
	for _, msg := range msgs {
		request := msg.MustBody()
		timetick := msg.TimeTick()
		request.Timestamps = make([]uint64, request.NumRows)
		for i := range request.NumRows {
			request.Timestamps[i] = timetick
		}
		data, err := storage.ColumnBasedInsertMsgToInsertData(&msgstream.InsertMsg{InsertRequest: request}, schema)
		if err != nil {
			return nil, err
		}
		ensureSystemFields(data, request.GetRowIDs(), request.GetTimestamps())
		out = append(out, data)
	}
	if err := applyFunctionOutputs(schema, out); err != nil {
		return nil, err
	}
	return out, nil
}

func applyFunctionOutputs(schema *schemapb.CollectionSchema, data []*storage.InsertData) error {
	for _, functionSchema := range schema.GetFunctions() {
		runner, err := function.NewFunctionRunner(schema, functionSchema)
		if err != nil {
			return err
		}
		if runner == nil {
			continue
		}
		defer runner.Close()
		for _, insertData := range data {
			switch functionSchema.GetType() {
			case schemapb.FunctionType_BM25:
				if err := applyBM25Function(runner, insertData); err != nil {
					return err
				}
			case schemapb.FunctionType_MinHash:
				if err := applyMinHashFunction(runner, insertData); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unknown function type in gsegment insert task: %s", functionSchema.GetType().String())
			}
		}
	}
	return nil
}

func applyBM25Function(runner function.FunctionRunner, data *storage.InsertData) error {
	inputs := make([]any, 0, len(runner.GetInputFields()))
	for _, inputField := range runner.GetInputFields() {
		fieldData, ok := data.Data[inputField.GetFieldID()]
		if !ok {
			return fmt.Errorf("BM25 function input field %d not found", inputField.GetFieldID())
		}
		inputs = append(inputs, fieldData.GetDataRows())
	}
	output, err := runner.BatchRun(inputs...)
	if err != nil {
		return err
	}
	if len(output) == 0 {
		return fmt.Errorf("BM25 function returned empty output")
	}
	sparseArray, ok := output[0].(*schemapb.SparseFloatArray)
	if !ok {
		return fmt.Errorf("BM25 function output is not sparse float array")
	}
	outputFieldID := runner.GetOutputFields()[0].GetFieldID()
	data.Data[outputFieldID] = &storage.SparseFloatVectorFieldData{
		SparseFloatArray: schemapb.SparseFloatArray{
			Contents: sparseArray.GetContents(),
		},
	}
	return nil
}

func applyMinHashFunction(runner function.FunctionRunner, data *storage.InsertData) error {
	inputs := make([]any, 0, len(runner.GetInputFields()))
	for _, inputField := range runner.GetInputFields() {
		fieldData, ok := data.Data[inputField.GetFieldID()]
		if !ok {
			return fmt.Errorf("MinHash function input field %d not found", inputField.GetFieldID())
		}
		inputs = append(inputs, fieldData.GetDataRows())
	}
	output, err := runner.BatchRun(inputs...)
	if err != nil {
		return err
	}
	if len(output) == 0 {
		return fmt.Errorf("MinHash function returned empty output")
	}
	fieldData, ok := output[0].(*schemapb.FieldData)
	if !ok {
		return fmt.Errorf("MinHash function output is not field data")
	}
	vectorField := fieldData.GetVectors()
	if vectorField == nil || vectorField.GetBinaryVector() == nil {
		return fmt.Errorf("MinHash function output is not binary vector")
	}
	outputFieldID := runner.GetOutputFields()[0].GetFieldID()
	data.Data[outputFieldID] = &storage.BinaryVectorFieldData{
		Data: vectorField.GetBinaryVector(),
		Dim:  int(vectorField.GetDim()),
	}
	return nil
}

func ensureSystemFields(data *storage.InsertData, rowIDs []int64, timestamps []uint64) {
	rowNum := data.GetRowNum()
	if _, ok := data.Data[common.RowIDField]; !ok {
		rows := make([]int64, rowNum)
		copy(rows, rowIDs)
		data.Data[common.RowIDField] = &storage.Int64FieldData{Data: rows}
	}
	if _, ok := data.Data[common.TimeStampField]; !ok {
		tss := make([]int64, rowNum)
		for i := range tss {
			if i < len(timestamps) {
				tss[i] = int64(timestamps[i])
			}
		}
		data.Data[common.TimeStampField] = &storage.Int64FieldData{Data: tss}
	}
}

// totalRows sums row counts across a slice of InsertData.
func totalRows(data []*storage.InsertData) int64 {
	var total int64
	for _, d := range data {
		for _, fd := range d.Data {
			total += int64(fd.RowNum())
			break
		}
	}
	return total
}
