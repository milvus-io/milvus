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
	"os"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestClusteringMappingExternalSort(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "payload", DataType: schemapb.DataType_VarChar, Nullable: true},
	}}
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "payload", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil))
	defer b.Release()
	for i := 0; i < 300; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		if i%3 == 0 {
			b.Field(1).AppendNull()
		} else {
			b.Field(1).(*array.StringBuilder).Append("payload")
		}
	}
	r := storage.NewSimpleArrowRecord(b.NewRecord(), map[int64]int{100: 0, 101: 1})
	defer r.Release()
	var reference []int64
	for _, budget := range []uint64{1, 1024, 1 << 20} {
		s, err := newClusteringMappingSorter(context.Background(), schema, t.TempDir(), budget)
		require.NoError(t, err)
		for i := 299; i >= 0; i-- {
			err := s.Append(r, i, clusteringSortKey{group: uint32(i % 2), cluster: uint32(i % 4), distance: float32(i % 5), source: uint64(i % 3), row: uint64(i)})
			require.NoError(t, err)
		}
		var ids []int64
		var previous *clusteringSortKey
		err = s.Finish(func(record storage.Record, row int, key clusteringSortKey) error {
			if previous != nil {
				require.LessOrEqual(t, previous.compare(key), 0)
			}
			id := record.Column(100).(*array.Int64).Value(row)
			require.Equal(t, uint64(id), key.row)
			require.Equal(t, id%3 == 0, record.Column(101).IsNull(row))
			if id%3 != 0 {
				require.Equal(t, "payload", record.Column(101).(*array.String).Value(row))
			}
			ids = append(ids, id)
			previous = &key
			return nil
		})
		require.NoError(t, err)
		require.Len(t, ids, 300)
		require.LessOrEqual(t, s.peakOpenRuns, clusteringMergeFanIn)
		if reference == nil {
			reference = ids
		} else {
			require.Equal(t, reference, ids, "spill boundaries cannot change final order")
		}
		require.NoError(t, s.Close())
		_, err = os.Stat(s.dir)
		require.True(t, os.IsNotExist(err))
	}
}

func TestClusteringMappingSortFailureCleanup(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, DataType: schemapb.DataType_Int64}}}
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.Append(7)
	a := b.NewArray()
	b.Release()
	defer a.Release()
	r := storage.NewSimpleArrowRecord(array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "id", Type: a.DataType()}}, nil), []arrow.Array{a}, 1), map[int64]int{100: 0})
	defer r.Release()
	for _, kind := range []string{"cancel", "emit", "corrupt", "empty"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s, err := newClusteringMappingSorter(ctx, schema, t.TempDir(), 1)
			require.NoError(t, err)
			if kind != "empty" {
				require.NoError(t, s.Append(r, 0, clusteringSortKey{}))
			}
			switch kind {
			case "cancel":
				cancel()
			case "corrupt":
				require.NoError(t, os.WriteFile(s.levels[0][0], []byte("broken"), 0o600))
			}
			err = s.Finish(func(storage.Record, int, clusteringSortKey) error { return merr.ErrServiceUnavailable })
			switch kind {
			case "empty":
				require.NoError(t, err)
			case "cancel":
				require.ErrorIs(t, err, context.Canceled)
			case "emit":
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			case "corrupt":
				require.ErrorIs(t, err, merr.ErrDataIntegrity)
			}
			require.NoError(t, s.Close())
			_, err = os.Stat(s.dir)
			require.True(t, os.IsNotExist(err))
		})
	}
}

func TestClusteringMappingTextNullSpills(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "text", DataType: schemapb.DataType_Text, Nullable: true}}}
	sorter, err := newClusteringMappingSorter(context.Background(), schema, t.TempDir(), 1)
	require.NoError(t, err)
	defer sorter.Close()
	for i := 0; i < 40; i++ {
		var col arrow.Array
		if i%2 == 0 {
			b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
			b.AppendNull()
			col = b.NewArray()
			b.Release()
		} else {
			b := array.NewStringBuilder(memory.DefaultAllocator)
			b.Append("decoded text")
			col = b.NewArray()
			b.Release()
		}
		a := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "text", Type: col.DataType(), Nullable: true}}, nil), []arrow.Array{col}, 1)
		col.Release()
		record := storage.NewSimpleArrowRecord(a, map[int64]int{100: 0})
		require.NoError(t, sorter.Append(record, 0, clusteringSortKey{row: uint64(i)}))
		record.Release()
	}
	seen := 0
	require.NoError(t, sorter.Finish(func(record storage.Record, row int, key clusteringSortKey) error {
		require.Equal(t, key.row%2 == 0, record.Column(100).IsNull(row))
		if key.row%2 != 0 {
			require.Equal(t, "decoded text", record.Column(100).(*array.String).Value(row))
		}
		seen++
		return nil
	}))
	require.Equal(t, 40, seen)
}
