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
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

func TestPkStatsCollector(t *testing.T) {
	collectionID := int64(1)
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
		},
	}

	t.Run("collect and digest int64 pk", func(t *testing.T) {
		collector, err := NewPkStatsCollector(collectionID, schema, 100)
		require.NoError(t, err)
		require.NotNil(t, collector)

		// Create test record
		fields := []arrow.Field{
			{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		}
		arrowSchema := arrow.NewSchema(fields, nil)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer builder.Release()

		pkBuilder := builder.Field(0).(*array.Int64Builder)
		for i := 0; i < 10; i++ {
			pkBuilder.Append(int64(i))
		}

		rec := builder.NewRecord()
		field2Col := map[FieldID]int{100: 0}
		record := NewSimpleArrowRecord(rec, field2Col)

		// Collect stats
		err = collector.Collect(record)
		assert.NoError(t, err)

		// Digest stats
		alloc := allocator.NewLocalAllocator(1, 100)
		writer := func(blobs []*Blob) error { return nil }

		resultMap, err := collector.Digest(collectionID, 1, 2, "/tmp", 10, alloc, writer)
		assert.NoError(t, err)
		assert.NotNil(t, resultMap)
		assert.Len(t, resultMap, 1)
	})

	t.Run("varchar pk", func(t *testing.T) {
		varcharSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64},
				{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
				{
					FieldID:      100,
					Name:         "pk",
					DataType:     schemapb.DataType_VarChar,
					IsPrimaryKey: true,
				},
			},
		}

		collector, err := NewPkStatsCollector(collectionID, varcharSchema, 100)
		require.NoError(t, err)

		// Create test record with varchar pk
		fields := []arrow.Field{
			{Name: "pk", Type: arrow.BinaryTypes.String},
		}
		arrowSchema := arrow.NewSchema(fields, nil)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer builder.Release()

		pkBuilder := builder.Field(0).(*array.StringBuilder)
		for i := 0; i < 10; i++ {
			pkBuilder.Append(fmt.Sprintf("key_%d", i))
		}

		rec := builder.NewRecord()
		field2Col := map[FieldID]int{100: 0}
		record := NewSimpleArrowRecord(rec, field2Col)

		err = collector.Collect(record)
		assert.NoError(t, err)
	})
}

func TestBm25StatsCollector(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
			{
				FieldID:  100,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
			},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:             "bm25_function",
				Type:             schemapb.FunctionType_BM25,
				InputFieldIds:    []int64{100},
				OutputFieldIds:   []int64{101},
				OutputFieldNames: []string{"bm25_field"},
			},
		},
	}

	t.Run("collect bm25 stats", func(t *testing.T) {
		collector := NewBm25StatsCollector(schema)
		assert.NotNil(t, collector)
		assert.NotNil(t, collector.bm25Stats)
	})

	t.Run("digest with empty stats", func(t *testing.T) {
		collector := NewBm25StatsCollector(schema)

		alloc := allocator.NewLocalAllocator(1, 100)
		writer := func(blobs []*Blob) error { return nil }

		_, err := collector.Digest(1, 1, 2, "/tmp", 10, alloc, writer)
		assert.NoError(t, err)
	})
}

func TestNewPkStatsCollector_NoPkField(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, DataType: schemapb.DataType_Int64},
		},
	}

	collector, err := NewPkStatsCollector(1, schema, 100)
	assert.Error(t, err)
	assert.Nil(t, collector)
}

func TestPkStatsCollector_DigestEndToEnd(t *testing.T) {
	collectionID := int64(1)
	partitionID := int64(2)
	segmentID := int64(3)
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
		},
	}

	collector, err := NewPkStatsCollector(collectionID, schema, 100)
	require.NoError(t, err)

	// Create test record
	fields := []arrow.Field{
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
	}
	arrowSchema := arrow.NewSchema(fields, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()

	pkBuilder := builder.Field(0).(*array.Int64Builder)
	for i := 0; i < 10; i++ {
		pkBuilder.Append(int64(i))
	}

	rec := builder.NewRecord()
	field2Col := map[FieldID]int{100: 0}
	record := NewSimpleArrowRecord(rec, field2Col)

	err = collector.Collect(record)
	require.NoError(t, err)

	alloc := allocator.NewLocalAllocator(1, 100)
	var writtenBlobs []*Blob
	writer := func(blobs []*Blob) error {
		writtenBlobs = blobs
		return nil
	}

	// Test Digest which includes writing
	binlogMap, err := collector.Digest(collectionID, partitionID, segmentID,
		"/tmp", 10, alloc, writer)
	assert.NoError(t, err)
	assert.NotNil(t, binlogMap)
	assert.Len(t, binlogMap, 1)

	binlog := binlogMap[100]
	assert.NotNil(t, binlog)
	assert.Equal(t, int64(100), binlog.FieldID)
	assert.Len(t, binlog.Binlogs, 1)
	assert.Contains(t, binlog.Binlogs[0].LogPath, "stats_log")
	assert.NotNil(t, writtenBlobs)
	assert.Len(t, writtenBlobs, 1)
}

func TestBm25StatsCollector_DigestEndToEnd(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:             "bm25_function",
				Type:             schemapb.FunctionType_BM25,
				InputFieldIds:    []int64{100},
				OutputFieldIds:   []int64{101},
				OutputFieldNames: []string{"bm25_field"},
			},
		},
	}

	collector := NewBm25StatsCollector(schema)

	alloc := allocator.NewLocalAllocator(1, 100)
	writer := func(blobs []*Blob) error { return nil }

	// Test with empty stats
	_, err := collector.Digest(1, 2, 3, "/tmp", 10, alloc, writer)
	assert.NoError(t, err)
}
