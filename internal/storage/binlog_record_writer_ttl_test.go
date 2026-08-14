// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package storage

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

const neverExpireTTL = int64(^uint64(0) >> 1)

func genCollectionSchemaWithTTLField() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "ttl_schema",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: "Timestamp", DataType: schemapb.DataType_Int64},
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "expire_at", DataType: schemapb.DataType_Timestamptz, Nullable: true},
		},
		Properties: []*commonpb.KeyValuePair{
			{Key: common.CollectionTTLFieldKey, Value: "expire_at"},
		},
	}
}

// genTTLValues builds 5 rows whose ttl values are [100, -5, 0, null, 200].
// Only 100 and 200 are collectable; -5/0/null all mean "never expire".
func genTTLValues() []*Value {
	ttls := []any{int64(100), int64(-5), int64(0), nil, int64(200)}
	values := make([]*Value, 0, len(ttls))
	for i, ttl := range ttls {
		ts := int64(tsoutil.ComposeTSByTime(getMilvusBirthday()))
		values = append(values, &Value{
			PK:        NewInt64PrimaryKey(int64(i)),
			Timestamp: ts,
			Value: map[FieldID]any{
				common.RowIDField:     int64(i),
				common.TimeStampField: ts,
				100:                   int64(i),
				101:                   ttl,
			},
		})
	}
	return values
}

func Test_packedBinlogRecordWriterBase_collectTTLValues(t *testing.T) {
	newRecord := func(ttlValues []int64, valid []bool) Record {
		tsBuilder := array.NewInt64Builder(memory.DefaultAllocator)
		tsBuilder.AppendValues(make([]int64, len(ttlValues)), nil)
		tsArr := tsBuilder.NewArray()
		tsBuilder.Release()

		ttlBuilder := array.NewInt64Builder(memory.DefaultAllocator)
		ttlBuilder.AppendValues(ttlValues, valid)
		ttlArr := ttlBuilder.NewArray()
		ttlBuilder.Release()

		ar := array.NewRecord(
			arrow.NewSchema([]arrow.Field{
				{Name: "ts", Type: arrow.PrimitiveTypes.Int64},
				{Name: "ttl", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			}, nil),
			[]arrow.Array{tsArr, ttlArr},
			int64(len(ttlValues)),
		)
		return NewSimpleArrowRecord(ar, map[FieldID]int{
			common.TimeStampField: 0,
			FieldID(101):          1,
		})
	}

	t.Run("collects positive values, skips null and non-positive", func(t *testing.T) {
		pw := &packedBinlogRecordWriterBase{ttlFieldID: 101, ttlFieldValues: make([]int64, 0)}
		r := newRecord([]int64{10, -1, 0, 30, 20}, []bool{true, true, true, false, true})
		defer r.Release()

		require.NoError(t, pw.collectTTLValues(r))
		assert.ElementsMatch(t, []int64{10, 20}, pw.ttlFieldValues)
	})

	t.Run("no-op when ttl field not enabled", func(t *testing.T) {
		pw := &packedBinlogRecordWriterBase{ttlFieldID: -1}
		r := newRecord([]int64{10, 20}, nil)
		defer r.Release()

		require.NoError(t, pw.collectTTLValues(r))
		assert.Empty(t, pw.ttlFieldValues)
	})

	t.Run("error when ttl column is not int64", func(t *testing.T) {
		pw := &packedBinlogRecordWriterBase{ttlFieldID: 101}

		ttlBuilder := array.NewFloat64Builder(memory.DefaultAllocator)
		ttlBuilder.AppendValues([]float64{1.0}, nil)
		ttlArr := ttlBuilder.NewArray()
		ttlBuilder.Release()

		ar := array.NewRecord(
			arrow.NewSchema([]arrow.Field{
				{Name: "ttl", Type: arrow.PrimitiveTypes.Float64},
			}, nil),
			[]arrow.Array{ttlArr},
			1,
		)
		r := NewSimpleArrowRecord(ar, map[FieldID]int{FieldID(101): 0})
		defer r.Release()

		assert.Error(t, pw.collectTTLValues(r))
	})
}

// TestPackedRecordWriters_TTLExpirQuantiles verifies that every packed
// writer (storage V2 and both V3 variants) collects TTL field values during
// Write so that GetLogs reports correct ExpirQuantiles. Regression test for
// the V3 writers silently reporting all-neverExpire quantiles.
func TestPackedRecordWriters_TTLExpirQuantiles(t *testing.T) {
	schema := genCollectionSchemaWithTTLField()

	newWriter := map[string]func(t *testing.T, cfg *indexpb.StorageConfig) BinlogRecordWriter{
		"v2_packed": func(t *testing.T, cfg *indexpb.StorageConfig) BinlogRecordWriter {
			w, err := newPackedBinlogRecordWriter(1, 2, 3, schema,
				ChunkedBlobsWriter(func(_ []*Blob) error { return nil }),
				allocator.NewLocalAllocator(1000, 1<<20),
				1024, 1024, 0, nil, cfg, nil, "")
			require.NoError(t, err)
			return w
		},
		"v3_manifest": func(t *testing.T, cfg *indexpb.StorageConfig) BinlogRecordWriter {
			w, err := newPackedManifestRecordWriter(1, 2, 3, schema,
				ChunkedBlobsWriter(func(_ []*Blob) error { return nil }),
				allocator.NewLocalAllocator(1000, 1<<20),
				1024, 1024, 0, nil, cfg, nil, false, "")
			require.NoError(t, err)
			return w
		},
		"v3_text_manifest": func(t *testing.T, cfg *indexpb.StorageConfig) BinlogRecordWriter {
			w, err := NewPackedTextManifestRecordWriter(1, 2, 3, schema,
				ChunkedBlobsWriter(func(_ []*Blob) error { return nil }),
				allocator.NewLocalAllocator(1000, 1<<20),
				1024, 1024, 0, nil, cfg, nil, "")
			require.NoError(t, err)
			return w
		},
	}

	for name, construct := range newWriter {
		t.Run(name, func(t *testing.T) {
			cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
			w := construct(t, cfg)

			rec, err := ValueSerializer(genTTLValues(), schema)
			require.NoError(t, err)
			require.NoError(t, w.Write(rec))
			require.NoError(t, w.Close())

			_, _, _, _, expirQuantiles := w.GetLogs()
			// 5 rows, collectable ttls sorted = [100, 200]:
			// 20% -> 100, 40% -> 200, 60%/80%/100% -> neverExpire.
			assert.Equal(t, []int64{100, 200, neverExpireTTL, neverExpireTTL, neverExpireTTL},
				expirQuantiles)
		})
	}
}
