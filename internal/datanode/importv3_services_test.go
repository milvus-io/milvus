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

package datanode

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TestNormalizeReshardBatchKeepsFunctionOutput pins that normalization leaves
// function output columns untouched: the reshard reader (full schema) reads a
// user-supplied column and runReshardFunctions fills or overwrites the rest,
// so fragments stay uniform without normalize knowing about functions.
func TestNormalizeReshardBatchKeepsFunctionOutput(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "embedding_out", DataType: schemapb.DataType_Int64, IsFunctionOutput: true},
		},
	}
	data, err := storage.NewInsertDataWithFunctionOutputField(schema)
	require.NoError(t, err)
	data.Data[100] = &storage.Int64FieldData{Data: []int64{1, 2}}
	data.Data[101] = &storage.Int64FieldData{Data: []int64{10, 20}}

	var offset int64
	source := &datapb.SourceFileSpec{
		File: &internalpb.ImportFile{Id: 1, PreAllocatedAutoIds: &commonpb.IDRange{Begin: 100, End: 102}},
	}
	require.NoError(t, normalizeReshardBatch(source, schema, data, 2, &offset))

	_, has := data.Data[101]
	require.True(t, has)
	require.Equal(t, []int64{10, 20}, data.Data[101].(*storage.Int64FieldData).Data)
	require.Equal(t, []int64{1, 2}, data.Data[100].(*storage.Int64FieldData).Data)
	require.Equal(t, []int64{100, 101}, data.Data[common.RowIDField].(*storage.Int64FieldData).Data)
}

// TestNormalizeReshardBatchKeepsBackupFunctionOutput pins the backup branch:
// backup fragments carry function outputs per the source binlog reader, so they
// must survive normalization untouched.
func TestNormalizeReshardBatchKeepsBackupFunctionOutput(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "embedding_out", DataType: schemapb.DataType_Int64, IsFunctionOutput: true},
		},
	}
	data, err := storage.NewInsertDataWithFunctionOutputField(schema)
	require.NoError(t, err)
	data.Data[100] = &storage.Int64FieldData{Data: []int64{1, 2}}
	data.Data[101] = &storage.Int64FieldData{Data: []int64{10, 20}}

	source := &datapb.SourceFileSpec{
		FileType: datapb.ImportFileType_BackupBinlog,
		File:     &internalpb.ImportFile{Id: 1},
	}
	require.NoError(t, normalizeReshardBatch(source, schema, data, 2, nil))

	_, has := data.Data[101]
	require.True(t, has)
	require.Equal(t, []int64{10, 20}, data.Data[101].(*storage.Int64FieldData).Data)
}

// TestRunReshardFunctionsGeneratesBM25AndMinHash pins the V2-equivalent
// placement: reshard runs every function over the batch, so missing BM25 and
// MinHash output columns are generated before hash routing and fragment write.
func TestRunReshardFunctionsGeneratesBM25AndMinHash(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			{
				FieldID: 103, Name: "mh", DataType: schemapb.DataType_BinaryVector, IsFunctionOutput: true,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "64"}},
			},
		},
		Functions: []*schemapb.FunctionSchema{
			{Name: "bm", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}},
			{Name: "mh", Type: schemapb.FunctionType_MinHash, InputFieldIds: []int64{101}, OutputFieldIds: []int64{103}},
		},
	}
	data, err := storage.NewInsertDataWithFunctionOutputField(schema)
	require.NoError(t, err)
	data.Data[100] = &storage.Int64FieldData{Data: []int64{1, 2}}
	data.Data[101] = &storage.StringFieldData{Data: []string{"milvus vector database", "milvus again"}}

	require.NoError(t, runReshardFunctions(context.Background(), schema, data))

	sparse, ok := data.Data[102].(*storage.SparseFloatVectorFieldData)
	require.True(t, ok)
	require.Equal(t, 2, sparse.RowNum())
	mh, ok := data.Data[103].(*storage.BinaryVectorFieldData)
	require.True(t, ok)
	require.Equal(t, 2, mh.RowNum())
	require.Equal(t, 64, mh.Dim)
}

// TestRunReshardFunctionsOverwritesUserMinHash pins the V2 alignment for a
// user-supplied MinHash column: V2's RunAll unconditionally recomputes MinHash,
// so reshard must overwrite the user column with the deterministic recomputation
// instead of preserving or dropping it.
func TestRunReshardFunctionsOverwritesUserMinHash(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Properties: []*commonpb.KeyValuePair{
			{Key: common.CollectionAllowInsertNonBM25FunctionOutputs, Value: "true"},
		},
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 103, Name: "mh", DataType: schemapb.DataType_BinaryVector, IsFunctionOutput: true,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "32"}},
			},
		},
		Functions: []*schemapb.FunctionSchema{
			{Name: "mh", Type: schemapb.FunctionType_MinHash, InputFieldIds: []int64{101}, OutputFieldIds: []int64{103}},
		},
	}
	data, err := storage.NewInsertDataWithFunctionOutputField(schema)
	require.NoError(t, err)
	data.Data[100] = &storage.Int64FieldData{Data: []int64{1, 2}}
	data.Data[101] = &storage.StringFieldData{Data: []string{"milvus vector database", "milvus again"}}
	// user-supplied junk signatures: all-zero bytes
	data.Data[103] = &storage.BinaryVectorFieldData{Data: make([]byte, 8), Dim: 32}

	require.NoError(t, runReshardFunctions(context.Background(), schema, data))

	mh, ok := data.Data[103].(*storage.BinaryVectorFieldData)
	require.True(t, ok)
	require.Equal(t, 2, mh.RowNum())
	require.Equal(t, 32, mh.Dim)
	require.NotEqual(t, make([]byte, 8), mh.Data[:8])
}

type captureRecordWriter struct {
	records []storage.Record
}

func (w *captureRecordWriter) Write(r storage.Record) error {
	r.Retain()
	w.records = append(w.records, r)
	return nil
}

func (w *captureRecordWriter) GetWrittenUncompressed() uint64 { return 0 }

func (w *captureRecordWriter) Close() error { return nil }

// TestImportV3FinalWriterPassesFunctionOutputs pins that the final transform
// runs no functions: every function output column is already in the fragments,
// so the writer only materializes timestamps and forwards the record.
func TestImportV3FinalWriterPassesFunctionOutputs(t *testing.T) {
	targetSchema := typeutil.AppendSystemFields(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{Name: "bm", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}},
		},
	})
	// ordinary-import temp schema: target user fields + RowID (what
	// buildImportV3TempSchema produces for a non-backup job)
	tempSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
		},
		Functions: []*schemapb.FunctionSchema{
			{Name: "bm", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}},
		},
	}

	// fragment batch: pk/text/sparse/RowID, no timestamp
	fragmentData := map[int64]storage.FieldData{
		100:               &storage.Int64FieldData{Data: []int64{1, 2}},
		101:               &storage.StringFieldData{Data: []string{"milvus vector database", "milvus again"}},
		102:               &storage.SparseFloatVectorFieldData{SparseFloatArray: schemapb.SparseFloatArray{Dim: 8, Contents: [][]byte{{1, 0, 0, 0, 2, 0, 0, 0}, {1, 0, 0, 0, 3, 0, 0, 0}}}},
		common.RowIDField: &storage.Int64FieldData{Data: []int64{7, 8}},
	}
	recordData := &storage.InsertData{Data: fragmentData}
	reader, err := storage.NewInsertDataRecordReader(recordData, tempSchema)
	require.NoError(t, err)
	record, err := reader.Next()
	require.NoError(t, err)

	output := &captureRecordWriter{}
	writer := newImportV3FinalWriter(context.Background(), output, tempSchema, targetSchema, 12345, false)
	require.NoError(t, writer.Write(record))
	require.NoError(t, reader.Close())
	require.Len(t, output.records, 1)

	final := output.records[0]
	require.Equal(t, 2, final.Len())
	tsCol := final.Column(common.TimeStampField)
	require.Equal(t, 2, tsCol.Len())
	require.Equal(t, int64(12345), tsCol.(*array.Int64).Value(0))
	sparseCol := final.Column(102)
	require.Equal(t, 2, sparseCol.Len())
}

// TestImportV3FinalWriterBackupKeepsSourceTimestamps pins the backup branch:
// fragments carry the source binlog timestamps, so the final merge must pass
// them through untouched instead of overwriting them with the import data ts.
func TestImportV3FinalWriterBackupKeepsSourceTimestamps(t *testing.T) {
	targetSchema := typeutil.AppendSystemFields(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	})
	tempSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
		},
	}

	fragmentData := map[int64]storage.FieldData{
		100:                   &storage.Int64FieldData{Data: []int64{1, 2}},
		common.RowIDField:     &storage.Int64FieldData{Data: []int64{7, 8}},
		common.TimeStampField: &storage.Int64FieldData{Data: []int64{111, 222}},
	}
	recordData := &storage.InsertData{Data: fragmentData}
	reader, err := storage.NewInsertDataRecordReader(recordData, tempSchema)
	require.NoError(t, err)
	record, err := reader.Next()
	require.NoError(t, err)

	output := &captureRecordWriter{}
	writer := newImportV3FinalWriter(context.Background(), output, tempSchema, targetSchema, 12345, true)
	require.NoError(t, writer.Write(record))
	require.NoError(t, reader.Close())
	require.Len(t, output.records, 1)

	tsCol := output.records[0].Column(common.TimeStampField)
	require.Equal(t, 2, tsCol.Len())
	require.Equal(t, int64(111), tsCol.(*array.Int64).Value(0))
	require.Equal(t, int64(222), tsCol.(*array.Int64).Value(1))
}

// TestImportV3FinalWriterBackupRejectsBadTimestampColumn covers the two backup
// validation branches. Both are defensive: a well-formed backup plan carries
// timestamps in its fragments, so the triggers are constructed by violating
// the schema/fragment contract the way a corrupt plan would.
func TestImportV3FinalWriterBackupRejectsBadTimestampColumn(t *testing.T) {
	t.Run("timestamp field absent from target schema", func(t *testing.T) {
		// RecordToInsertData only initializes fields listed in the target
		// schema, so a target schema without the timestamp system field is the
		// one shape that leaves data.Data[TimeStampField] nil.
		targetSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			},
		}
		tempSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			},
		}
		recordData := &storage.InsertData{Data: map[int64]storage.FieldData{
			100: &storage.Int64FieldData{Data: []int64{1}},
		}}
		reader, err := storage.NewInsertDataRecordReader(recordData, tempSchema)
		require.NoError(t, err)
		record, err := reader.Next()
		require.NoError(t, err)

		writer := newImportV3FinalWriter(context.Background(), &captureRecordWriter{}, tempSchema, targetSchema, 12345, true)
		err = writer.Write(record)
		require.NoError(t, reader.Close())
		require.ErrorContains(t, err, "backup timestamp is missing")
	})

	t.Run("fragment without timestamp column", func(t *testing.T) {
		// The target schema carries the timestamp field but the fragment does
		// not, so RecordToInsertData leaves it empty: RowNum 0 against N data
		// rows trips the mismatch check.
		targetSchema := typeutil.AppendSystemFields(&schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			},
		})
		tempSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			},
		}
		recordData := &storage.InsertData{Data: map[int64]storage.FieldData{
			100:               &storage.Int64FieldData{Data: []int64{1, 2}},
			common.RowIDField: &storage.Int64FieldData{Data: []int64{7, 8}},
		}}
		reader, err := storage.NewInsertDataRecordReader(recordData, tempSchema)
		require.NoError(t, err)
		record, err := reader.Next()
		require.NoError(t, err)

		writer := newImportV3FinalWriter(context.Background(), &captureRecordWriter{}, tempSchema, targetSchema, 12345, true)
		err = writer.Write(record)
		require.NoError(t, reader.Close())
		require.ErrorContains(t, err, "backup timestamp rows mismatch")
	})
}

// TestExecuteReshardPlanSpillsAtMemoryCheckpoint pins the dynamic spill path
// end to end: with a roomy slot budget (static ceiling far above the data)
// but a process pinned near the high-water mark, every source-batch
// checkpoint spills the largest bucket, and the end-of-input flush replays
// all chunks with rows intact.
func TestExecuteReshardPlanSpillsAtMemoryCheckpoint(t *testing.T) {
	initReshardPipelineParams(t)
	const mib = int64(1024 * 1024)
	totalMock := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1024 * mib)).Build()
	defer totalMock.UnPatch()
	usedMock := mockey.Mock(hardware.GetUsedMemoryCount).Return(uint64(1000 * mib)).Build()
	defer usedMock.UnPatch()

	fix := newReshardPipelineFixture()
	readers := map[int64]reshardReaderBuilder{
		1: staticReshardReader(&scriptReader{batches: []*storage.InsertData{
			fix.wideBatch(1), fix.wideBatch(3000), fix.wideBatch(6000),
		}, size: 100}),
	}
	// slot=3 x default 160MiB per slot: the static ceiling is the full
	// 480MiB budget, never hit by the ~18.6MiB of test data -- only the
	// dynamic free-memory checkpoint can spill (free=24MiB is far below the
	// flush spike plus the 20% reserve).
	plan := fix.plan([]*datapb.SourceFileSpec{fix.source(1)}, 30*mib)
	recorder := &reshardCallRecorder{}
	mockReshardBoundaries(t, readers, recorder)

	require.NoError(t, executeReshardPlan(context.Background(), nil, fix.request(plan, 3), plan, nil))
	require.Equal(t, int64(1), recorder.publishCalled.Load())
	require.Len(t, recorder.published, 1)
	// One spill per source-batch checkpoint; the single end-of-input flush
	// (18.6MiB <= 30MiB sort input) replays all three chunks as one group.
	require.Equal(t, []int{3}, recorder.spillChunks,
		"every source-batch checkpoint must have spilled under memory pressure")
	require.Equal(t, []int64{0}, recorder.seqs)
	var totalRows int64
	for _, fragment := range recorder.published[0].GetFragments() {
		totalRows += fragment.GetRows()
	}
	require.Equal(t, int64(6000), totalRows, "spilled bytes must survive the spill and come back through flush")
}

// sentinelPackedWriter is a fake importV3PackedRecordWriter whose
// GetWrittenUncompressed returns a caller-chosen sentinel, so a test can tell
// whether the descriptor carried the writer metric or the decoded metric.
type sentinelPackedWriter struct {
	rowNum       int64
	path         string
	uncompressed uint64
}

func (w *sentinelPackedWriter) Write(storage.Record) error     { return nil }
func (w *sentinelPackedWriter) GetWrittenUncompressed() uint64 { return w.uncompressed }
func (w *sentinelPackedWriter) Close() error                   { return nil }
func (w *sentinelPackedWriter) GetWrittenRowNum() int64        { return w.rowNum }
func (w *sentinelPackedWriter) GetWrittenPaths(typeutil.UniqueID) string {
	return w.path
}

// TestWriteReshardFragmentReportsDecodedLogicalBytes pins the packing metric:
// FragmentDescriptor.logical_bytes carries the fragment input's normalized
// decoded bytes (group.logicalBytes, the same value the fragment target and
// the sort split accumulate), not the packed writer's arrow-columnar
// uncompressed count. The fake writer reports a sentinel 999 so any
// regression to the writer metric fails the assertion.
func TestWriteReshardFragmentReportsDecodedLogicalBytes(t *testing.T) {
	tempSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "name", DataType: schemapb.DataType_VarChar},
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
		},
	}
	batch1 := &storage.InsertData{Data: map[int64]storage.FieldData{
		100:               &storage.Int64FieldData{Data: []int64{1, 2}},
		101:               &storage.StringFieldData{Data: []string{"aaa", "bb"}},
		common.RowIDField: &storage.Int64FieldData{Data: []int64{7, 8}},
	}}
	batch2 := &storage.InsertData{Data: map[int64]storage.FieldData{
		100:               &storage.Int64FieldData{Data: []int64{3}},
		101:               &storage.StringFieldData{Data: []string{"c"}},
		common.RowIDField: &storage.Int64FieldData{Data: []int64{9}},
	}}
	const rows = int64(3)
	logicalBytes := int64(batch1.GetMemorySize()) + int64(batch2.GetMemorySize())
	group := reshardFragmentGroup{
		batches:      []*storage.InsertData{batch1, batch2},
		rows:         rows,
		logicalBytes: logicalBytes,
	}

	fake := &sentinelPackedWriter{rowNum: rows, path: "fragments/0/10/3_7.parquet", uncompressed: 999}
	writerMock := mockey.Mock(newImportV3PackedRecordWriter).To(
		func(string, []string, *schemapb.CollectionSchema, int64, *indexpb.StorageConfig, *indexcgopb.StoragePluginContext) (importV3PackedRecordWriter, error) {
			return fake, nil
		}).Build()
	defer writerMock.UnPatch()
	sortMock := mockey.Mock(storage.Sort).To(
		func(uint64, *schemapb.CollectionSchema, []storage.RecordReader, storage.RecordWriter, func(storage.Record, int, int) bool, []int64) (int, *storage.SortTimings, error) {
			return int(rows), &storage.SortTimings{}, nil
		}).Build()
	defer sortMock.UnPatch()

	req := &datapb.ReshardTaskRequest{
		JobId: 1, TaskId: 2, RunId: 3,
		StorageConfig: &indexpb.StorageConfig{RootPath: "root", BucketName: "bucket"},
	}
	plan := &datapb.ReshardTaskPlan{Partitions: []int64{10}}
	// The sentinel must stay distinct from the decoded byte count, otherwise
	// the two assertions below cannot tell the metrics apart.
	require.NotEqual(t, int64(999), logicalBytes)
	outcome, err := writeReshardFragment(context.Background(), req, plan, tempSchema, 0, 0, group, 7, 16*1024*1024, nil, nil)
	require.NoError(t, err)
	require.Equal(t, logicalBytes, outcome.descriptor.GetLogicalBytes())
	require.NotEqual(t, int64(999), outcome.descriptor.GetLogicalBytes())
	require.Equal(t, rows, outcome.descriptor.GetRows())
	require.Equal(t, int32(0), outcome.descriptor.GetChannelIndex())
	require.Equal(t, int32(0), outcome.descriptor.GetPartitionIndex())
	require.Equal(t, int64(7), outcome.descriptor.GetSeq())
	// The writer metric travels alongside the descriptor for the run summary
	// log.
	require.Equal(t, uint64(999), outcome.writtenUncompressed)
}

// TestSplitReshardBucketForSortAccountsDecodedBytes pins that the sort split
// preserves the bucket's decoded-byte accounting: every group carries the
// bytes of exactly its own items, and the group sums reproduce the bucket
// totals, so the published descriptor bytes match the fragment-target metric.
func TestSplitReshardBucketForSortAccountsDecodedBytes(t *testing.T) {
	batch1 := &storage.InsertData{Data: map[int64]storage.FieldData{
		100: &storage.Int64FieldData{Data: []int64{1, 2, 3}},
	}}
	batch2 := &storage.InsertData{Data: map[int64]storage.FieldData{
		100: &storage.Int64FieldData{Data: []int64{4}},
	}}
	size1 := int64(batch1.GetMemorySize())
	size2 := int64(batch2.GetMemorySize())
	bucket := &reshardBucket{
		batches:         []reshardBatch{{data: batch1, bytes: size1}, {data: batch2, bytes: size2}},
		spillChunks:     []string{"chunk-0"},
		spillChunkBytes: []int64{100},
		spillChunkRows:  []int64{5},
	}
	totalBytes := int64(100) + size1 + size2
	totalRows := int64(5 + 3 + 1)
	sumGroups := func(groups []reshardFragmentGroup) (int64, int64) {
		var sumBytes, sumRows int64
		for _, g := range groups {
			sumBytes += g.logicalBytes
			sumRows += g.rows
		}
		return sumBytes, sumRows
	}

	// A limit covering the whole bucket keeps a single group.
	groups := splitReshardBucketForSort(bucket, totalBytes)
	require.Len(t, groups, 1)
	require.Equal(t, totalBytes, groups[0].logicalBytes)
	require.Equal(t, totalRows, groups[0].rows)

	// A limit fitting the spill chunk plus the first batch packs those two
	// and leaves the second batch alone.
	groups = splitReshardBucketForSort(bucket, 100+size1)
	require.Len(t, groups, 2)
	require.Equal(t, int64(100)+size1, groups[0].logicalBytes)
	require.Equal(t, size2, groups[1].logicalBytes)
	sumBytes, sumRows := sumGroups(groups)
	require.Equal(t, totalBytes, sumBytes)
	require.Equal(t, totalRows, sumRows)

	// A limit below every item still isolates each item instead of dropping
	// or merging bytes across items.
	groups = splitReshardBucketForSort(bucket, 1)
	require.Len(t, groups, 3)
	sumBytes, sumRows = sumGroups(groups)
	require.Equal(t, totalBytes, sumBytes)
	require.Equal(t, totalRows, sumRows)

	// A non-positive limit degrades to one group per item.
	groups = splitReshardBucketForSort(bucket, 0)
	require.Len(t, groups, 3)
	for _, g := range groups {
		require.Equal(t, 1, len(g.spillChunks)+len(g.batches))
	}
	sumBytes, sumRows = sumGroups(groups)
	require.Equal(t, totalBytes, sumBytes)
	require.Equal(t, totalRows, sumRows)
}

// scriptReader is a fake importutilv2.Reader that replays canned batches. It
// counts every Read call so overlap tests can observe whether the reader kept
// advancing while a flush was in flight.
type scriptReader struct {
	batches []*storage.InsertData
	pos     int
	reads   atomic.Int64
	size    int64
}

func (r *scriptReader) Size() (int64, error) { return r.size, nil }

func (r *scriptReader) Read() (*storage.InsertData, error) {
	r.reads.Add(1)
	if r.pos >= len(r.batches) {
		return nil, io.EOF
	}
	batch := r.batches[r.pos]
	r.pos++
	return batch, nil
}

func (r *scriptReader) Close() {}

// reshardPipelineFixture builds the minimum valid reshard input: one PK field
// plus the RowID the temporary schema carries, one vchannel, one partition.
// Every source gets a generous preallocated ID range so normalization can
// materialize RowIDs without touching any storage.
type reshardPipelineFixture struct {
	schema     *schemapb.CollectionSchema
	tempSchema *schemapb.CollectionSchema
	sort       *datapb.SortSpec
}

func newReshardPipelineFixture() *reshardPipelineFixture {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "note", DataType: schemapb.DataType_VarChar, Nullable: true},
		},
	}
	tempSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "note", DataType: schemapb.DataType_VarChar, Nullable: true},
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
		},
	}
	return &reshardPipelineFixture{
		schema:     schema,
		tempSchema: tempSchema,
		sort: &datapb.SortSpec{Fields: []*datapb.SortFieldSpec{
			{FieldId: 100, DataType: schemapb.DataType_Int64},
		}},
	}
}

func (f *reshardPipelineFixture) batch(ids ...int64) *storage.InsertData {
	return &storage.InsertData{Data: map[int64]storage.FieldData{
		100: &storage.Int64FieldData{Data: append([]int64(nil), ids...)},
	}}
}

// wideBatch builds a ~6MiB batch (2000 rows with ~3KiB strings) for budget
// tests. The varchar field is part of the fixture schema, so hash routing
// accepts the extra column.
func (f *reshardPipelineFixture) wideBatch(start int64) *storage.InsertData {
	const rows = 2000
	ids := make([]int64, rows)
	payload := make([]string, rows)
	blob := string(make([]byte, 3000))
	for i := range ids {
		ids[i] = start + int64(i)
		payload[i] = blob
	}
	return &storage.InsertData{Data: map[int64]storage.FieldData{
		100: &storage.Int64FieldData{Data: ids},
		101: &storage.StringFieldData{Data: payload},
	}}
}

func (f *reshardPipelineFixture) plan(sources []*datapb.SourceFileSpec, fragmentSize int64) *datapb.ReshardTaskPlan {
	return &datapb.ReshardTaskPlan{
		Schema: f.schema, TempSchema: f.tempSchema,
		Vchannels: []string{"ch0"}, Partitions: []int64{10},
		Sources: sources, Sort: f.sort, FragmentSize: fragmentSize,
	}
}

func (f *reshardPipelineFixture) source(id int64) *datapb.SourceFileSpec {
	return &datapb.SourceFileSpec{
		File: &internalpb.ImportFile{
			Id: id, PreAllocatedAutoIds: &commonpb.IDRange{Begin: 1000, End: 1000000},
		},
	}
}

func (f *reshardPipelineFixture) request(plan *datapb.ReshardTaskPlan, slot int64) *datapb.ReshardTaskRequest {
	return &datapb.ReshardTaskRequest{
		JobId: 1, TaskId: 2, RunId: 3, Slot: slot,
		StorageConfig: &indexpb.StorageConfig{RootPath: "root", BucketName: "bucket"},
		Plan:          plan,
	}
}

// mockReshardBoundaries replaces the three storage-touching boundaries of
// executeReshardPlan: source opening, fragment writing, and manifest publish.
// The fake writer builds descriptors straight from the group it receives and
// records every call, so tests observe exactly what the pipeline enqueued.
type reshardCallRecorder struct {
	mu            sync.Mutex
	seqs          []int64
	rows          []int64
	logicalBytes  []int64
	spillChunks   []int
	paths         []string
	onWrite       func(call int, group reshardFragmentGroup) error
	published     []*datapb.ReshardManifest
	publishCalled atomic.Int64
}

// reshardReaderBuilder lets tests build readers that observe the context the
// pipeline constructed them with — real readers bind their IO to that ctx
// (see readReshardSource), and the prepare-interruption test relies on it.
type reshardReaderBuilder func(context.Context) importutilv2.Reader

func staticReshardReader(reader importutilv2.Reader) reshardReaderBuilder {
	return func(context.Context) importutilv2.Reader { return reader }
}

func mockReshardBoundaries(t *testing.T, readers map[int64]reshardReaderBuilder, recorder *reshardCallRecorder) {
	t.Helper()
	readerMock := mockey.Mock(newReshardSourceReader).To(
		func(ctx context.Context, _ storage.ChunkManager, _ *schemapb.CollectionSchema, source *datapb.SourceFileSpec, _ int64, _ *indexpb.StorageConfig, _ *indexcgopb.StoragePluginContext) (importutilv2.Reader, error) {
			build, ok := readers[source.GetFile().GetId()]
			require.True(t, ok, "unexpected reshard source %d", source.GetFile().GetId())
			return build(ctx), nil
		}).Build()
	t.Cleanup(func() { readerMock.UnPatch() })
	writerMock := mockey.Mock(writeReshardFragment).To(
		func(_ context.Context, _ *datapb.ReshardTaskRequest, _ *datapb.ReshardTaskPlan, _ *schemapb.CollectionSchema, vchannelOrdinal, partitionOrdinal int, group reshardFragmentGroup, seq, _ int64, _ []int64, _ *indexcgopb.StoragePluginContext) (*reshardFragmentOutcome, error) {
			recorder.mu.Lock()
			call := len(recorder.seqs)
			recorder.seqs = append(recorder.seqs, seq)
			recorder.rows = append(recorder.rows, group.rows)
			recorder.logicalBytes = append(recorder.logicalBytes, group.logicalBytes)
			recorder.spillChunks = append(recorder.spillChunks, len(group.spillChunks))
			recorder.mu.Unlock()
			if recorder.onWrite != nil {
				if err := recorder.onWrite(call, group); err != nil {
					return nil, err
				}
			}
			descriptor := &datapb.FragmentDescriptor{
				ChannelIndex: int32(vchannelOrdinal), PartitionIndex: int32(partitionOrdinal), Seq: seq,
				Path: fmt.Sprintf("frag-%d-%d-%d.parquet", vchannelOrdinal, partitionOrdinal, seq),
				Rows: group.rows, LogicalBytes: group.logicalBytes,
			}
			recorder.mu.Lock()
			recorder.paths = append(recorder.paths, descriptor.GetPath())
			recorder.mu.Unlock()
			return &reshardFragmentOutcome{
				descriptor:          descriptor,
				timings:             &storage.SortTimings{},
				writtenUncompressed: uint64(group.logicalBytes),
			}, nil
		}).Build()
	t.Cleanup(func() { writerMock.UnPatch() })
	publishMock := mockey.Mock(publishReshardManifest).To(
		func(_ context.Context, _ storage.ChunkManager, _ *datapb.ReshardTaskRequest, manifest *datapb.ReshardManifest) error {
			recorder.publishCalled.Add(1)
			recorder.mu.Lock()
			recorder.published = append(recorder.published, manifest)
			recorder.mu.Unlock()
			return nil
		}).Build()
	t.Cleanup(func() { publishMock.UnPatch() })
}

func initReshardPipelineParams(t *testing.T) {
	t.Helper()
	paramtable.Init()
	t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key) })
	t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().DataCoordCfg.ImportMemoryLimitPerSlot.Key) })
	t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().DataNodeCfg.ImportBaseBufferSize.Key) })
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, t.TempDir())
	// The memory-budget tests below compute thresholds from this buffer size.
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.ImportBaseBufferSize.Key, "16")
}

// TestExecuteReshardPlanKeepsManifestDeterministic pins the run's ordering
// contract: sequence numbers are assigned in flush order, so the published
// manifest carries contiguous seqs whose rows and bytes match the input
// batches in read order, including a zero-row batch that must be skipped
// without disturbing the sequence.
func TestExecuteReshardPlanKeepsManifestDeterministic(t *testing.T) {
	initReshardPipelineParams(t)
	fix := newReshardPipelineFixture()
	b11 := fix.batch(1, 2, 3)
	b12 := fix.batch(4, 5)
	b13 := &storage.InsertData{Data: map[int64]storage.FieldData{}}
	b21 := fix.batch(6, 7, 8, 9)
	b22 := fix.batch(10)
	readers := map[int64]reshardReaderBuilder{
		1: staticReshardReader(&scriptReader{batches: []*storage.InsertData{b11, b12, b13}, size: 100}),
		2: staticReshardReader(&scriptReader{batches: []*storage.InsertData{b21, b22}, size: 100}),
	}
	plan := fix.plan([]*datapb.SourceFileSpec{fix.source(1), fix.source(2)}, 1)
	recorder := &reshardCallRecorder{}
	mockReshardBoundaries(t, readers, recorder)

	require.NoError(t, executeReshardPlan(context.Background(), nil, fix.request(plan, 3), plan, nil))
	require.Equal(t, int64(1), recorder.publishCalled.Load())
	require.Len(t, recorder.published, 1)
	manifest := recorder.published[0]

	// One flush per non-empty batch: the zero-row batch contributes no job
	// and no sequence number.
	require.Len(t, manifest.GetFragments(), 4)
	require.Len(t, recorder.seqs, 4)
	for i, fragment := range manifest.GetFragments() {
		require.Equal(t, int64(i), fragment.GetSeq(), "manifest must be contiguous from 0 in flush order")
		require.Equal(t, int32(0), fragment.GetChannelIndex())
		require.Equal(t, int32(0), fragment.GetPartitionIndex())
	}
	require.Equal(t, []int64{0, 1, 2, 3}, recorder.seqs)
	require.Equal(t, []int64{3, 2, 4, 1}, recorder.rows)
	var totalRows int64
	seen := make(map[string]struct{})
	for _, fragment := range manifest.GetFragments() {
		totalRows += fragment.GetRows()
		_, dup := seen[fragment.GetPath()]
		require.False(t, dup, "fragment paths must be unique")
		seen[fragment.GetPath()] = struct{}{}
	}
	require.Equal(t, int64(10), totalRows)
	require.Len(t, recorder.logicalBytes, 4)
	for i, logicalBytes := range recorder.logicalBytes {
		require.Equal(t, logicalBytes, manifest.GetFragments()[i].GetLogicalBytes())
		require.Greater(t, logicalBytes, int64(0), "every non-empty group carries RowID bytes at minimum")
	}
}

// TestExecuteReshardPlanPrefetchesReadDuringFlush proves with a counter, not
// a clock, that the source prepare stage advances reads while a flush is in
// flight:
// the first flush sleeps with the remaining reads gated open behind it, so a
// strictly larger read count at flush end means the reads overlapped the
// flush instead of stalling behind it. It also pins the run-ahead bound: the
// reads may advance by at most one buffered batch plus one in-flight batch
// (reshardPrepareDepth+1), so the stage can never turn into unbounded
// read-ahead memory.
func TestExecuteReshardPlanPrefetchesReadDuringFlush(t *testing.T) {
	initReshardPipelineParams(t)
	fix := newReshardPipelineFixture()
	inner := &scriptReader{batches: []*storage.InsertData{
		fix.batch(1, 2), fix.batch(3, 4), fix.batch(5, 6),
		fix.batch(7, 8), fix.batch(9, 10), fix.batch(11, 12),
	}, size: 100}
	release := make(chan struct{})
	var releaseOnce sync.Once
	readers := map[int64]reshardReaderBuilder{
		1: staticReshardReader(&blockingReader{inner: inner, release: release}),
	}
	plan := fix.plan([]*datapb.SourceFileSpec{fix.source(1)}, 1)
	recorder := &reshardCallRecorder{}

	var entryReads, exitReads atomic.Int64
	var firstCall atomic.Bool
	recorder.onWrite = func(_ int, _ reshardFragmentGroup) error {
		if firstCall.CompareAndSwap(false, true) {
			// The second read is still gated at this point, so entryReads
			// is exactly 1; release the gate and sleep so the remaining
			// reads must land inside the flush span.
			entryReads.Store(inner.reads.Load())
			releaseOnce.Do(func() { close(release) })
			time.Sleep(200 * time.Millisecond)
			exitReads.Store(inner.reads.Load())
		}
		return nil
	}
	mockReshardBoundaries(t, readers, recorder)

	require.NoError(t, executeReshardPlan(context.Background(), nil, fix.request(plan, 3), plan, nil))
	require.Greater(t, exitReads.Load(), entryReads.Load(),
		"reads must advance while the first flush is in flight (entry=%d exit=%d)", entryReads.Load(), exitReads.Load())
	require.LessOrEqual(t, exitReads.Load()-entryReads.Load(), int64(reshardPrepareDepth+1),
		"prepare run-ahead is bounded by one buffered batch plus one in-flight batch (entry=%d exit=%d)", entryReads.Load(), exitReads.Load())
	require.Equal(t, int64(len(inner.batches)+1), inner.reads.Load(), "every batch plus the terminal EOF must be read")
}

// blockingReader holds back every Read after the first batch until release is
// closed, so the run-ahead test controls exactly which reads can proceed
// during the first flush.
type blockingReader struct {
	inner   *scriptReader
	release <-chan struct{}
}

func (r *blockingReader) Size() (int64, error) { return r.inner.Size() }

func (r *blockingReader) Read() (*storage.InsertData, error) {
	if r.inner.pos > 0 {
		<-r.release
	}
	return r.inner.Read()
}

func (r *blockingReader) Close() {}

// ctxBlockingReader blocks every Read after the first until the ctx it was
// constructed with is canceled, then fails with the ctx error — the same
// contract a real reader has, since its IO is bound to its construction ctx.
type ctxBlockingReader struct {
	inner *scriptReader
	ctx   context.Context
}

func (r *ctxBlockingReader) Size() (int64, error) { return r.inner.Size() }

func (r *ctxBlockingReader) Read() (*storage.InsertData, error) {
	if r.inner.pos > 0 {
		<-r.ctx.Done()
		return nil, r.ctx.Err()
	}
	return r.inner.Read()
}

func (r *ctxBlockingReader) Close() {}

// TestExecuteReshardPlanFlushErrorInterruptsBlockedRead pins the failure
// contract: when a flush fails while the prepare stage's next Read is stuck, the
// deferred cancel of the source context interrupts that read, so the run
// returns promptly instead of holding its slot until the read times out or
// DataCoord drops the task.
func TestExecuteReshardPlanFlushErrorInterruptsBlockedRead(t *testing.T) {
	initReshardPipelineParams(t)
	fix := newReshardPipelineFixture()
	script := &scriptReader{batches: []*storage.InsertData{fix.batch(1, 2), fix.batch(3, 4)}, size: 100}
	readers := map[int64]reshardReaderBuilder{
		1: func(ctx context.Context) importutilv2.Reader {
			return &ctxBlockingReader{inner: script, ctx: ctx}
		},
	}
	plan := fix.plan([]*datapb.SourceFileSpec{fix.source(1)}, 1)
	recorder := &reshardCallRecorder{}
	recorder.onWrite = func(_ int, _ reshardFragmentGroup) error {
		return merr.WrapErrImportSysFailedMsg("injected flush failure")
	}
	mockReshardBoundaries(t, readers, recorder)

	done := make(chan error, 1)
	go func() {
		done <- executeReshardPlan(context.Background(), nil, fix.request(plan, 3), plan, nil)
	}()
	select {
	case err := <-done:
		require.Error(t, err)
		require.ErrorContains(t, err, "injected flush failure")
		require.Equal(t, int64(0), recorder.publishCalled.Load(), "a failed run must not publish a manifest")
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return while a source read was blocked: the deferred source-context cancel must interrupt the in-flight read")
	}
}

// TestExecuteReshardPlanFlushErrorFailsRun pins the failure contract: a
// failing flush aborts the run with the flush error before any manifest is
// published, and the run shuts down without leaking its prepare-stage goroutine.
func TestExecuteReshardPlanFlushErrorFailsRun(t *testing.T) {
	initReshardPipelineParams(t)
	fix := newReshardPipelineFixture()
	readers := map[int64]reshardReaderBuilder{
		1: staticReshardReader(&scriptReader{batches: []*storage.InsertData{fix.batch(1, 2), fix.batch(3, 4), fix.batch(5, 6)}, size: 100}),
	}
	plan := fix.plan([]*datapb.SourceFileSpec{fix.source(1)}, 1)
	recorder := &reshardCallRecorder{}
	injected := merr.WrapErrImportSysFailedMsg("injected flush failure")
	var calls atomic.Int64
	recorder.onWrite = func(_ int, _ reshardFragmentGroup) error {
		if calls.Add(1) == 2 {
			return injected
		}
		return nil
	}
	mockReshardBoundaries(t, readers, recorder)

	err := executeReshardPlan(context.Background(), nil, fix.request(plan, 3), plan, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "injected flush failure")
	require.Equal(t, int64(0), recorder.publishCalled.Load(), "a failed run must not publish a manifest")
	// No goroutine-count assertion here on purpose: executeReshardPlan only
	// returns after the deferred stopPrepare has joined the source prepare
	// stage, so a leaked stage would hang the call above and fail
	// the test by timeout. A global NumGoroutine check would instead catch
	// unrelated background drift (for example paramtable's config refresher
	// starting lazily).
}

// TestStartReshardPrepareKeepsOrderAndSkipsEmpty pins the stage contract:
// batches flow to the routing side in read order, and a nil prepare result
// (an empty batch after normalization) is skipped without sending.
func TestStartReshardPrepareKeepsOrderAndSkipsEmpty(t *testing.T) {
	mkBatch := func(id int64) *storage.InsertData {
		return &storage.InsertData{Data: map[int64]storage.FieldData{
			100: &storage.Int64FieldData{Data: []int64{id}},
		}}
	}
	reader := &scriptReader{batches: []*storage.InsertData{mkBatch(1), mkBatch(2), mkBatch(3)}, size: 10}
	var mu sync.Mutex
	var preparedOrder []int64
	prepare := func(batch *storage.InsertData) (*storage.InsertData, error) {
		id := batch.Data[100].(*storage.Int64FieldData).Data[0]
		mu.Lock()
		preparedOrder = append(preparedOrder, id)
		mu.Unlock()
		if id == 2 {
			return nil, nil
		}
		return batch, nil
	}
	results, stop := startReshardPrepare(context.Background(), reader, prepare)
	defer stop()

	var got []int64
	for r := range results {
		require.NoError(t, r.err)
		got = append(got, r.batch.Data[100].(*storage.Int64FieldData).Data[0])
	}
	require.Equal(t, []int64{1, 3}, got)
	require.Equal(t, []int64{1, 2, 3}, preparedOrder)
}

// TestStartReshardPrepareForwardsError pins that a prepare failure is
// delivered exactly once and then closes the channel.
func TestStartReshardPrepareForwardsError(t *testing.T) {
	mkBatch := func(id int64) *storage.InsertData {
		return &storage.InsertData{Data: map[int64]storage.FieldData{
			100: &storage.Int64FieldData{Data: []int64{id}},
		}}
	}
	reader := &scriptReader{batches: []*storage.InsertData{mkBatch(1), mkBatch(2)}, size: 10}
	injected := merr.WrapErrImportSysFailedMsg("injected prepare failure")
	prepare := func(batch *storage.InsertData) (*storage.InsertData, error) {
		if batch.Data[100].(*storage.Int64FieldData).Data[0] == 2 {
			return nil, injected
		}
		return batch, nil
	}
	results, stop := startReshardPrepare(context.Background(), reader, prepare)
	defer stop()

	first, ok := <-results
	require.True(t, ok)
	require.NoError(t, first.err)
	require.Equal(t, int64(1), first.batch.Data[100].(*storage.Int64FieldData).Data[0])
	second, ok := <-results
	require.True(t, ok)
	require.ErrorContains(t, second.err, "injected prepare failure")
	_, ok = <-results
	require.False(t, ok, "channel must close after the error result")
}

// TestStartReshardPrepareCancelInterruptsBlockedSend pins the join contract:
// with nobody draining, the stage parks on a blocked send; canceling the
// parent ctx must unblock the join promptly instead of hanging on the reader.
func TestStartReshardPrepareCancelInterruptsBlockedSend(t *testing.T) {
	mkBatch := func(id int64) *storage.InsertData {
		return &storage.InsertData{Data: map[int64]storage.FieldData{
			100: &storage.Int64FieldData{Data: []int64{id}},
		}}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := &scriptReader{batches: []*storage.InsertData{mkBatch(1), mkBatch(2), mkBatch(3)}, size: 10}
	identity := func(batch *storage.InsertData) (*storage.InsertData, error) { return batch, nil }
	results, stop := startReshardPrepare(ctx, reader, identity)

	first, ok := <-results
	require.True(t, ok)
	require.NoError(t, first.err)
	// The channel (depth reshardPrepareDepth) now fills with the second batch
	// and the stage parks on its third send with nobody draining; wait until
	// the third Read has happened so the park is established.
	deadline := time.Now().Add(10 * time.Second)
	for reader.reads.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatal("prepare stage did not advance to a blocked send")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("stop did not return after cancel: the stage must not wait out a blocked send")
	}
}

// TestExecuteReshardPlanRunsFunctionsInPrepareStage pins the pipeline
// placement: function execution (here BM25, which needs no external model)
// happens inside the prepare stage, so every batch the fragment writer
// receives already carries the generated output column.
func TestExecuteReshardPlanRunsFunctionsInPrepareStage(t *testing.T) {
	initReshardPipelineParams(t)
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
		Functions: []*schemapb.FunctionSchema{
			{Name: "bm", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}},
		},
	}
	tempSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
		},
		Functions: []*schemapb.FunctionSchema{
			{Name: "bm", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{101}, OutputFieldIds: []int64{102}},
		},
	}
	mkBatch := func(ids []int64, texts []string) *storage.InsertData {
		// Same shape real readers produce: the function output column starts
		// as an empty placeholder the pipeline fills in.
		data, err := storage.NewInsertDataWithFunctionOutputField(schema)
		require.NoError(t, err)
		data.Data[100] = &storage.Int64FieldData{Data: ids}
		data.Data[101] = &storage.StringFieldData{Data: texts}
		return data
	}
	fix := newReshardPipelineFixture()
	readers := map[int64]reshardReaderBuilder{
		1: staticReshardReader(&scriptReader{batches: []*storage.InsertData{
			mkBatch([]int64{1, 2}, []string{"milvus vector database", "milvus again"}),
			mkBatch([]int64{3}, []string{"hello world"}),
		}, size: 100}),
	}
	plan := &datapb.ReshardTaskPlan{
		Schema: schema, TempSchema: tempSchema,
		Vchannels: []string{"ch0"}, Partitions: []int64{10},
		Sources: []*datapb.SourceFileSpec{fix.source(1)}, Sort: fix.sort, FragmentSize: 1,
	}
	recorder := &reshardCallRecorder{}
	type sparseSeen struct {
		ok   bool
		rows int
	}
	var mu sync.Mutex
	var seen []sparseSeen
	recorder.onWrite = func(_ int, group reshardFragmentGroup) error {
		mu.Lock()
		defer mu.Unlock()
		for _, b := range group.batches {
			s, ok := b.Data[102].(*storage.SparseFloatVectorFieldData)
			rows := 0
			if ok {
				rows = s.RowNum()
			}
			seen = append(seen, sparseSeen{ok: ok, rows: rows})
		}
		return nil
	}
	mockReshardBoundaries(t, readers, recorder)

	require.NoError(t, executeReshardPlan(context.Background(), nil, fix.request(plan, 3), plan, nil))
	require.Equal(t, int64(1), recorder.publishCalled.Load())
	require.Len(t, recorder.published, 1)
	// Two input batches flush separately; both must arrive with BM25 output.
	require.Len(t, recorder.published[0].GetFragments(), 2)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, 2)
	totalRows := 0
	for _, s := range seen {
		require.True(t, s.ok, "every routed batch must carry the BM25 output column")
		require.Greater(t, s.rows, 0)
		totalRows += s.rows
	}
	require.Equal(t, 3, totalRows)
}

// TestExecuteReshardPlanSpillsAndReplaysThroughFlush pins the static spill
// ceiling end to end: with a 16MiB budget (1 slot x 16MiB per slot) and
// ~18.6MiB of routed data, resident bytes cross the budget during the third
// batch and the whole bucket spills to one real local Arrow IPC file; the
// degenerate budget also clamps the sort input to 1 byte, so the
// end-of-input flush replays the chunk as its own group with all rows
// intact, and the flush-side removal plus the run-end directory cleanup
// leave no spill file behind. The memory mocks pin the dynamic checkpoint
// open (unlimited free memory) so only the static ceiling can spill.
// The 30MiB target exceeds the three batches' combined logical size (~18.6MiB):
// fragmentTarget gates on logicalBytes, which accumulates across spills, so a
// smaller target would flush mid-run instead of spilling.
func TestExecuteReshardPlanSpillsAndReplaysThroughFlush(t *testing.T) {
	initReshardPipelineParams(t)
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.ImportMemoryLimitPerSlot.Key, "16")
	totalMock := mockey.Mock(hardware.GetMemoryCount).Return(uint64(1) << 40).Build()
	defer totalMock.UnPatch()
	usedMock := mockey.Mock(hardware.GetUsedMemoryCount).Return(uint64(0)).Build()
	defer usedMock.UnPatch()
	fix := newReshardPipelineFixture()
	readers := map[int64]reshardReaderBuilder{
		1: staticReshardReader(&scriptReader{batches: []*storage.InsertData{
			fix.wideBatch(1), fix.wideBatch(3000), fix.wideBatch(6000),
		}, size: 100}),
	}
	const mib = int64(1024 * 1024)
	plan := fix.plan([]*datapb.SourceFileSpec{fix.source(1)}, 30*mib)
	recorder := &reshardCallRecorder{}
	mockReshardBoundaries(t, readers, recorder)

	require.NoError(t, executeReshardPlan(context.Background(), nil, fix.request(plan, 1), plan, nil))
	require.Equal(t, int64(1), recorder.publishCalled.Load())
	require.Len(t, recorder.published, 1)
	// One spill event once resident crossed the 16MiB budget: the single
	// bucket's three batches land in one chunk, and the clamped sort input
	// (1 byte) keeps that chunk alone in its group.
	require.Equal(t, []int{1}, recorder.spillChunks,
		"resident crossing the budget must have spilled the bucket before the final flush")
	require.Equal(t, []int64{0}, recorder.seqs)
	var totalRows int64
	for _, fragment := range recorder.published[0].GetFragments() {
		totalRows += fragment.GetRows()
	}
	require.Equal(t, int64(6000), totalRows, "spilled bytes must survive the spill and come back through flush")
	_, err := os.ReadDir(paramtable.Get().LocalStorageCfg.Path.GetValue() + "/" + importV3SpillRootDir + "/1/2/3")
	require.True(t, os.IsNotExist(err), "the run spill directory must be cleaned up, got: %v", err)
}

// TestCleanImportV3PrefixesRemovesSpillOnly pins the startup contract: the
// spill root is garbage after a restart and must be removed, while the
// import_v3 prefix holds durable reshard output under local storage and must
// survive the cleanup (see importV3SpillRootDir).
func TestCleanImportV3PrefixesRemovesSpillOnly(t *testing.T) {
	paramtable.Init()
	t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().LocalStorageCfg.Path.Key) })
	root := t.TempDir()
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, root)

	spill := path.Join(root, importV3SpillRootDir, "1", "2", "3", "chunk.bin")
	require.NoError(t, os.MkdirAll(path.Dir(spill), 0o755))
	require.NoError(t, os.WriteFile(spill, []byte("leftover"), 0o600))

	fragment := path.Join(root, metautil.BuildImportReshardOutputPath(100, 200), "fragments", "0", "4", "1_0.parquet")
	require.NoError(t, os.MkdirAll(path.Dir(fragment), 0o755))
	require.NoError(t, os.WriteFile(fragment, []byte("durable"), 0o600))

	cleanImportV3Prefixes()

	_, err := os.Stat(path.Join(root, importV3SpillRootDir))
	require.True(t, os.IsNotExist(err), "spill root must be removed, got: %v", err)
	got, err := os.ReadFile(fragment)
	require.NoError(t, err, "durable reshard output must survive the startup cleanup")
	require.Equal(t, "durable", string(got))
}
