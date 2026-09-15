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
	"io"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A clustering watermark flush must release the packed writer without ending
// the output segment. Both chunks must remain readable and share segment stats.
func TestPackedBinlogRecordWriterFlushChunks(t *testing.T) {
	schema := genCollectionSchemaWithTTLField()
	schema.Fields = append(schema.Fields,
		&schemapb.FieldSchema{FieldID: 102, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "128"}}},
		&schemapb.FieldSchema{FieldID: 103, Name: "bm25", DataType: schemapb.DataType_SparseFloatVector})
	schema.Functions = []*schemapb.FunctionSchema{{Name: "bm25", Type: schemapb.FunctionType_BM25, InputFieldIds: []int64{102}, OutputFieldIds: []int64{103}}}
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
	groups := []storagecommon.ColumnGroup{
		{GroupID: 0, Columns: []int{0, 1, 2}, Fields: []int64{0, 1, 100}},
		{GroupID: 101, Columns: []int{3, 4, 5}, Fields: []int64{101, 102, 103}},
	}
	var statsBlobs []*Blob
	w, err := newPackedBinlogRecordWriter(1, 2, 3, schema, func(blobs []*Blob) error {
		statsBlobs = append(statsBlobs, blobs...)
		return nil
	}, allocator.NewLocalAllocator(1000, 2000), 10, 64<<20, 10<<20, groups, cfg, nil, "")
	require.NoError(t, err)
	defer w.Close()

	var previousWritten uint64
	for chunk := 0; chunk < 2; chunk++ {
		values := genTTLValues()
		for i, v := range values {
			fields := v.Value.(map[FieldID]any)
			fields[100] = int64(chunk*5 + i)
			fields[102] = "test"
			fields[103] = typeutil.CreateSparseFloatRow([]uint32{1}, []float32{float32(chunk + 1)})
			fields[common.TimeStampField] = int64((chunk+1)*100 + i)
		}
		record, err := ValueSerializer(values, schema)
		require.NoError(t, err)
		require.NoError(t, w.Write(record))
		record.Release()
		require.Greater(t, w.GetWrittenUncompressed(), previousWritten)
		previousWritten = w.GetWrittenUncompressed()
		require.Positive(t, w.GetBufferUncompressed())
		require.NoError(t, w.FlushChunk())
		require.Zero(t, w.GetBufferUncompressed(), "flushing must release the current packed chunk")
		require.Equal(t, previousWritten, w.GetWrittenUncompressed())
		require.Empty(t, statsBlobs, "chunk flush must not finalize segment stats")
		require.NoError(t, w.FlushChunk(), "empty flush is idempotent")

		logs, _, _, _, _ := w.GetLogs()
		require.Len(t, logs, 2)
		var binlogs []*datapb.FieldBinlog
		for _, group := range groups {
			field := logs[group.GroupID]
			require.Len(t, field.Binlogs, chunk+1)
			last := field.Binlogs[chunk]
			require.EqualValues(t, 5, last.EntriesNum)
			require.EqualValues(t, (chunk+1)*100, last.TimestampFrom)
			require.EqualValues(t, (chunk+1)*100+4, last.TimestampTo)
			require.Positive(t, last.LogSize)
			require.Positive(t, last.MemorySize)
			if group.GroupID == 101 {
				require.EqualValues(t, 1, last.FieldNullCounts[101])
			}
			binlogs = append(binlogs, field)
		}
		// Read back before Close: this also verifies that native Parquet buffers
		// and file footers were actually flushed, independently of the counter.
		reader, err := NewBinlogRecordReader(context.Background(), binlogs, schema,
			WithVersion(StorageV2), WithStorageConfig(cfg), WithUseLoonFFI(false))
		require.NoError(t, err)
		seen := 0
		for {
			record, err := reader.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			keys := record.Column(100).(*array.Int64)
			for i := 0; i < record.Len(); i++ {
				require.EqualValues(t, seen, keys.Value(i))
				seen++
			}
		}
		require.NoError(t, reader.Close())
		require.Equal(t, (chunk+1)*5, seen)
	}
	require.NoError(t, w.Close())
	require.EqualValues(t, 10, w.GetRowNum())
	_, stats, bm25Logs, _, quantiles := w.GetLogs()
	require.Len(t, stats.Binlogs, 1)
	require.EqualValues(t, 10, stats.Binlogs[0].EntriesNum)
	require.Equal(t, []int64{100, 200, neverExpireTTL, neverExpireTTL, neverExpireTTL}, quantiles)
	require.Len(t, statsBlobs, 2)
	require.Len(t, bm25Logs[103].Binlogs, 1)
	require.EqualValues(t, 10, bm25Logs[103].Binlogs[0].EntriesNum)
	bm25, err := NewBM25StatsWithBytes(statsBlobs[1].Value)
	require.NoError(t, err)
	require.EqualValues(t, 10, bm25.NumRow())
	require.EqualValues(t, 15, bm25.NumToken())
	require.NoError(t, w.Close(), "Close must not append statistics again")
	require.Len(t, statsBlobs, 2)
	pkStats, err := DeserializeStats(statsBlobs[:1])
	require.NoError(t, err)
	require.Len(t, pkStats, 1)
	require.Equal(t, NewInt64PrimaryKey(0), pkStats[0].MinPk)
	require.Equal(t, NewInt64PrimaryKey(9), pkStats[0].MaxPk)
}

func TestPackedBinlogRecordWriterFlushLogIDExhaustion(t *testing.T) {
	schema := genCollectionSchemaWithTTLField()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
	w, err := newPackedBinlogRecordWriter(1, 2, 3, schema, func([]*Blob) error { return nil },
		allocator.NewLocalAllocator(1000, 1001), 10, 64<<20, 0,
		[]storagecommon.ColumnGroup{{GroupID: 0, Columns: []int{0, 1, 2, 3}, Fields: []int64{0, 1, 100, 101}}}, cfg, nil, "")
	require.NoError(t, err)
	defer w.Close()
	record, err := ValueSerializer(genTTLValues(), schema)
	require.NoError(t, err)
	defer record.Release()
	require.NoError(t, w.Write(record))
	require.NoError(t, w.FlushChunk())
	err = w.Write(record)
	require.Error(t, err)
	require.True(t, allocator.IsIDExhausted(err))
	require.EqualValues(t, 5, w.GetRowNum())
	require.Zero(t, w.GetBufferUncompressed())
}
