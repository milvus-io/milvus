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

package compactor

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	binlogio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/clusteringpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Observe the Record interface while retaining the real binlog writer below it.
type clusteringRecordObserver struct {
	storage.BinlogRecordWriter
	rows          []int
	bytes         []uint64
	values        []*storage.Value
	captureValues bool
	writeError    error
	blobs         map[string][]byte
}

func (w *clusteringRecordObserver) Write(r storage.Record) error {
	if w.writeError != nil {
		return w.writeError
	}
	if w.captureValues {
		values := make([]*storage.Value, r.Len())
		if err := storage.ValueDeserializerWithSchema(r, values, w.Schema(), true); err != nil {
			return err
		}
		w.values = append(w.values, values...)
	}
	before := w.GetWrittenUncompressed()
	if err := w.BinlogRecordWriter.Write(r); err != nil {
		return err
	}
	w.rows = append(w.rows, r.Len())
	w.bytes = append(w.bytes, w.GetWrittenUncompressed()-before)
	return nil
}

func (s *ClusteringCompactionTaskSuite) TestMappingAccumulatesRecords() {
	s.checkMappingRecords(false, -1)
}

func (s *ClusteringCompactionTaskSuite) TestMappingVectorOffsetsAfterDelete() {
	s.checkMappingRecords(true, -1)
}

func (s *ClusteringCompactionTaskSuite) TestMappingRecordTTL() {
	s.checkMappingRecords(false, 511)
}

type clusteringOffsetReader struct {
	binlogio.BinlogIO
	offsets []byte
}

func (r *clusteringOffsetReader) Download(ctx context.Context, paths []string) ([][]byte, error) {
	if len(paths) == 1 && paths[0] == "offsets" {
		return [][]byte{r.offsets}, nil
	}
	return r.BinlogIO.Download(ctx, paths)
}

func (s *ClusteringCompactionTaskSuite) checkMappingRecords(vector bool, ttlCutoff int64) {
	s.preparScalarCompactionNormalTask()
	s.Require().NoError(s.task.init())
	defer s.task.cleanUp(context.Background())
	writer, err := NewMultiSegmentWriter(context.Background(), s.mockBinlogIO,
		NewCompactionAllocator(s.task.segIDAlloc, s.task.logIDAlloc),
		s.plan.MaxSize, s.plan.Schema, s.task.compactionParams, s.plan.MaxSegmentRows,
		PartitionID, CollectionID, "", 100, storage.WithStorageConfig(s.task.compactionParams.StorageConfig))
	s.Require().NoError(err)
	s.Require().NoError(writer.rotateWriter())
	observer := &clusteringRecordObserver{BinlogRecordWriter: writer.writer.BinlogRecordWriter, captureValues: true}
	writer.writer = storage.NewBinlogValueWriter(observer, 100)
	buffer := newClusterBuffer(0, writer, nil)
	s.task.clusterBuffers = []*ClusterBuffer{buffer}
	s.task.keyToBufferFunc = func(any) *ClusterBuffer { return buffer }
	s.task.memoryLimit = 1 << 30
	if ttlCutoff >= 0 {
		// Reuse the fixture's int64 PK column as expiration timestamps. Rows
		// through ttlCutoff expire, while the existing delete still filters 100.
		s.task.ttlFieldID = 100
		s.task.currentTime = time.UnixMicro(ttlCutoff)
	}
	if vector {
		ids := make([]uint32, 10240)
		for i := range ids {
			ids[i] = uint32(i % 7)
		}
		encoded, err := proto.Marshal(&clusteringpb.ClusteringCentroidIdMappingStats{CentroidIdMapping: ids})
		s.Require().NoError(err)
		s.task.binlogIO = &clusteringOffsetReader{BinlogIO: s.mockBinlogIO, offsets: encoded}
		s.task.isVectorClusteringKey = true
		s.task.segmentIDOffsetMapping = map[int64]string{s.plan.SegmentBinlogs[0].SegmentID: "offsets"}
		nextOffset := int64(0)
		s.task.offsetToBufferFunc = func(offset int64, mapping []uint32) *ClusterBuffer {
			if nextOffset == 100 {
				nextOffset++
			}
			s.Equal(nextOffset, offset)
			s.Equal(uint32(offset%7), mapping[offset])
			nextOffset++
			return buffer
		}
	}
	defer buffer.Close()

	s.Require().NoError(s.task.mappingSegment(context.Background(), s.plan.SegmentBinlogs[0]))
	s.Empty(observer.rows, "input Record boundaries must not submit underfilled output batches")
	s.Require().NoError(buffer.Close())
	expectedRows := 10239
	if ttlCutoff >= 100 {
		expectedRows = 10240 - int(ttlCutoff) - 1
	}
	s.Equal([]int{expectedRows}, observer.rows)
	s.Require().Len(observer.values, expectedRows)
	for i, v := range observer.values {
		id := int64(i)
		if ttlCutoff >= 100 {
			id += ttlCutoff + 1
		}
		if ttlCutoff < 100 && id >= 100 {
			id++
		} // deleted by the fixture's deltalog
		s.Equal(id, v.PK.GetValue())
		s.Equal(genRow(id), v.Value)
	}
}

// Use both a wide vector and variable-length nullable columns: row count alone
// must not determine when a bucket submits a Record.
func clusteringWideSchema() *schemapb.CollectionSchema {
	schema := genCollectionSchema()
	schema.Fields[4].Nullable = true
	schema.Fields[5].TypeParams = []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2048"}}
	for i := int64(104); i < 112; i++ {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID: i, Name: fmt.Sprint(i), DataType: schemapb.DataType_VarChar,
			Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "1024"}},
		})
	}
	return schema
}

func clusteringTestRecord(t *testing.T, schema *schemapb.CollectionSchema, first, rows int) storage.Record {
	t.Helper()
	arrowSchema, err := storage.ConvertToArrowSchema(schema, false)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	field2Col := make(map[int64]int)
	for i, field := range schema.Fields {
		field2Col[field.FieldID] = i
		for row := first; row < first+rows; row++ {
			b := builder.Field(i)
			if field.Nullable && row%3 == 0 {
				b.AppendNull()
				continue
			}
			switch b := b.(type) {
			case *array.Int64Builder:
				b.Append(int64(row))
			case *array.Int32Builder:
				b.Append(int32(row))
			case *array.Float64Builder:
				b.Append(float64(row) + 0.25)
			case *array.StringBuilder:
				b.Append(strings.Repeat("x", 128+row%17) + fmt.Sprint(row))
			case *array.FixedSizeBinaryBuilder:
				vector := make([]byte, b.Type().(*arrow.FixedSizeBinaryType).ByteWidth)
				for dim := 0; dim < len(vector)/4; dim++ {
					binary.LittleEndian.PutUint32(vector[dim*4:], math.Float32bits(float32(row+dim)))
				}
				b.Append(vector)
			default:
				t.Fatalf("unsupported fixture field %v", field)
			}
		}
	}
	return storage.NewSimpleArrowRecord(builder.NewRecord(), field2Col)
}

func newClusteringTestBuffer(t *testing.T, schema *schemapb.CollectionSchema, batchBytes uint64, version int64) (*ClusterBuffer, *clusteringRecordObserver) {
	t.Helper()
	paramtable.Init()
	params := compaction.GenParams()
	params.BinLogMaxSize = batchBytes
	params.StorageVersion = version
	params.StorageConfig = &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
	binlogIO := mock_util.NewMockBinlogIO(t)
	blobs := make(map[string][]byte)
	binlogIO.EXPECT().Upload(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, data map[string][]byte) error {
		for key, value := range data {
			blobs[key] = append([]byte(nil), value...)
		}
		return nil
	}).Maybe()
	writer, err := NewMultiSegmentWriter(context.Background(), binlogIO,
		NewCompactionAllocator(allocator.NewLocalAllocator(1, 100), allocator.NewLocalAllocator(100, 10000)),
		1<<30, schema, params, 10000, PartitionID, CollectionID, "", 100,
		storage.WithStorageConfig(params.StorageConfig), storage.WithBufferSize(8<<20),
		storage.WithColumnGroups([]storagecommon.ColumnGroup{
			{GroupID: 0, Fields: []int64{0, 1, 100, 101, 102}, Columns: []int{0, 1, 2, 3, 4}},
			{GroupID: 103, Fields: []int64{103}, Columns: []int{5}},
			{GroupID: 104, Fields: []int64{104, 105, 106, 107, 108, 109, 110, 111}, Columns: []int{6, 7, 8, 9, 10, 11, 12, 13}},
		}))
	require.NoError(t, err)
	require.NoError(t, writer.rotateWriter())
	observer := &clusteringRecordObserver{BinlogRecordWriter: writer.writer.BinlogRecordWriter, blobs: blobs}
	writer.writer = storage.NewBinlogValueWriter(observer, 100)
	buffer := newClusterBuffer(0, writer, nil)
	t.Cleanup(func() { require.NoError(t, buffer.Close()) })
	return buffer, observer
}

func TestClusterBufferRecordBatches(t *testing.T) {
	for _, version := range []int64{storage.StorageV1, storage.StorageV2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			schema := clusteringWideSchema()
			// One row is larger than 8 KiB. This flushes well before 1024 rows.
			buffer, observer := newClusteringTestBuffer(t, schema, 16<<10, version)
			observer.captureValues = true
			for i := 0; i < 5; i++ {
				record := clusteringTestRecord(t, schema, i, 1)
				require.NoError(t, buffer.WriteRecord(record, 0))
				record.Release() // bucket must own copies across input lifetimes
				if i == 0 {
					require.Empty(t, observer.rows)
				}
			}
			require.Equal(t, []int{2, 2}, observer.rows)
			require.GreaterOrEqual(t, buffer.GetBufferSize(), uint64(8192))
			require.NoError(t, buffer.Close())
			require.Equal(t, []int{2, 2, 1}, observer.rows)
			require.Len(t, observer.values, 5)
			for i, value := range observer.values {
				require.Equal(t, int64(i), value.PK.GetValue())
				row := value.Value.(map[int64]interface{})
				vector := row[103].([]float32)
				require.Len(t, vector, 2048)
				for dim, v := range vector {
					require.Equal(t, float32(i+dim), v)
				}
				if i%3 == 0 {
					require.Nil(t, row[102])
				} else {
					require.Equal(t, strings.Repeat("x", 128+i%17)+fmt.Sprint(i), row[102])
				}
			}
		})
	}
}

func TestClusterBufferOversizedRowAndWriteError(t *testing.T) {
	buffer, observer := newClusteringTestBuffer(t, clusteringWideSchema(), 1024, storage.StorageV1)
	record := clusteringTestRecord(t, clusteringWideSchema(), 1, 2)
	defer record.Release()
	require.NoError(t, buffer.WriteRecord(record, 0))
	require.Equal(t, []int{1}, observer.rows)
	want := errors.New("write record failed")
	observer.writeError = want
	require.ErrorIs(t, buffer.WriteRecord(record, 1), want)
	observer.writeError = nil
	require.NoError(t, buffer.Close())
	require.Nil(t, buffer.builder)
}

func TestClusterBufferConcurrentRecords(t *testing.T) {
	buffer, observer := newClusteringTestBuffer(t, clusteringWideSchema(), 64<<10, storage.StorageV1)
	observer.captureValues = true
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		record := clusteringTestRecord(t, clusteringWideSchema(), worker*10, 10)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer record.Release()
			for row := 0; row < record.Len(); row++ {
				if err := buffer.WriteRecord(record, row); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	require.NoError(t, buffer.Close())
	seen := make(map[int64]bool)
	for _, value := range observer.values {
		key := value.PK.GetValue().(int64)
		require.False(t, seen[key])
		seen[key] = true
	}
	require.Len(t, seen, 40)
}

// This characterizes the remaining phase-two gap. Real native packed writers
// retain buffers after Record submission, and the current FlushChunk is a no-op.
func TestClusteringRecordBuilderStillExceedsGlobalLimit(t *testing.T) {
	schema := clusteringWideSchema()
	task := &clusteringCompactionTask{memoryLimit: 1 << 20, flushPool: conc.NewPool[any](1)}
	defer task.flushPool.Release()
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	for i := 0; i < 4; i++ {
		buffer, _ := newClusteringTestBuffer(t, schema, 64<<10, storage.StorageV2)
		buffer.id = i
		// Install tracking only while constructing the output builders. They
		// retain the allocator; input records and other writers are untracked.
		original := memory.DefaultAllocator
		memory.DefaultAllocator = alloc
		buffer.builder = storage.NewRecordBuilder(schema)
		memory.DefaultAllocator = original
		task.clusterBuffers = append(task.clusterBuffers, buffer)
	}
	for bucket, buffer := range task.clusterBuffers {
		record := clusteringTestRecord(t, schema, bucket*64, 64)
		for row := 0; row < record.Len(); row++ {
			require.NoError(t, buffer.WriteRecord(record, row))
		}
		record.Release()
		require.NoError(t, buffer.Flush())
		require.Zero(t, buffer.builder.GetRowNum())
	}
	before := alloc.CurrentAlloc()
	require.Greater(t, before, int(task.memoryLimit), "tracked output buffers alone exceed the task budget")
	require.NoError(t, task.flushLargestBuffers(context.Background()))
	require.Equal(t, before, alloc.CurrentAlloc(), "V2 FlushChunk has not freed the submitted Arrow buffers")
	t.Logf("phase-one gap: limit=%d retained Arrow bytes=%d buckets=%d batchBytes=%d writerBytes=%d",
		task.memoryLimit, before, len(task.clusterBuffers), 64<<10, 8<<20)
	for bucket, buffer := range task.clusterBuffers {
		require.NoError(t, buffer.Close())
		segments := buffer.GetCompactionSegments()
		require.Len(t, segments, 1)
		require.EqualValues(t, 64, segments[0].NumOfRows)
		reader, err := storage.NewBinlogRecordReader(context.Background(), segments[0].InsertLogs, schema,
			storage.WithVersion(storage.StorageV2), storage.WithStorageConfig(buffer.writer.params.StorageConfig))
		require.NoError(t, err)
		rowID := bucket * 64
		for {
			record, err := reader.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			for row := 0; row < record.Len(); row++ {
				require.EqualValues(t, rowID, record.Column(100).(*array.Int64).Value(row))
				vector := record.Column(103).(*array.FixedSizeBinary).Value(row)
				for dim := 0; dim < 2048; dim++ {
					require.Equal(t, float32(rowID+dim), math.Float32frombits(binary.LittleEndian.Uint32(vector[dim*4:])))
				}
				rowID++
			}
		}
		reader.Close()
		require.Equal(t, (bucket+1)*64, rowID)
	}
	alloc.AssertSize(t, 0)
}

func TestClusteringScalarNullAndDefault(t *testing.T) {
	for _, dataType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		t.Run(dataType.String(), func(t *testing.T) {
			field := &schemapb.FieldSchema{DataType: dataType, Nullable: true}
			var builder array.Builder
			var expected any
			if dataType == schemapb.DataType_Int64 {
				b := array.NewInt64Builder(memory.DefaultAllocator)
				b.Append(42)
				builder, expected = b, int64(42)
			} else {
				b := array.NewStringBuilder(memory.DefaultAllocator)
				b.Append("key")
				builder, expected = b, "key"
			}
			defer builder.Release()
			builder.AppendNull()
			values := builder.NewArray()
			defer values.Release()
			value, err := clusteringScalarValue(values, field, 0)
			require.NoError(t, err)
			require.Equal(t, expected, value)
			value, err = clusteringScalarValue(values, field, 1)
			require.NoError(t, err)
			require.Nil(t, value)
			if dataType == schemapb.DataType_Int64 {
				field.DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}}
			} else {
				field.DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: "key"}}
			}
			value, err = clusteringScalarValue(values, field, 1)
			require.NoError(t, err)
			require.Equal(t, expected, value)
		})
	}
}

func TestClusterBufferFlushChunkAndCleanup(t *testing.T) {
	buffer, observer := newClusteringTestBuffer(t, clusteringWideSchema(), 64<<20, storage.StorageV1)
	record := clusteringTestRecord(t, clusteringWideSchema(), 0, 1)
	defer record.Release()
	require.NoError(t, buffer.WriteRecord(record, 0))
	require.Empty(t, observer.rows)
	require.NoError(t, buffer.FlushChunk())
	require.Equal(t, []int{1}, observer.rows)
	require.Zero(t, buffer.GetBufferSize(), "V1 pressure flush must include the pending Builder")
	require.NoError(t, buffer.WriteRecord(record, 0))
	task := &clusteringCompactionTask{clusterBuffers: []*ClusterBuffer{buffer}}
	task.cleanUp(context.Background())
	require.Nil(t, buffer.builder, "cleanup discards an unfinished batch on task failure")
	require.Equal(t, []int{1}, observer.rows)
}
