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
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/routing"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	recordPKField  = int64(100)
	recordI32Field = int64(101)
)

// recordSchema is a minimal int64-pk collection.
func recordSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "split",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: "Timestamp", DataType: schemapb.DataType_Int64},
			{FieldID: recordPKField, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: recordI32Field, Name: "i32", DataType: schemapb.DataType_Int32},
		},
	}
}

// pkRecord builds a record of recordSchema rows; row i carries timestamp i+1.
func pkRecord(t *testing.T, pks []int64) storage.Record {
	t.Helper()
	mem := memory.DefaultAllocator
	rowID, ts, pk, i32 := array.NewInt64Builder(mem), array.NewInt64Builder(mem), array.NewInt64Builder(mem), array.NewInt32Builder(mem)
	for i, v := range pks {
		rowID.Append(v)
		ts.Append(int64(i + 1))
		pk.Append(v)
		i32.Append(int32(v))
	}
	fields := []arrow.Field{
		{Name: "row_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "Timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "i32", Type: arrow.PrimitiveTypes.Int32},
	}
	cols := []arrow.Array{rowID.NewArray(), ts.NewArray(), pk.NewArray(), i32.NewArray()}
	rec := array.NewRecord(arrow.NewSchema(fields, nil), cols, int64(len(pks)))
	return storage.NewSimpleArrowRecord(rec, map[storage.FieldID]int{
		common.RowIDField: 0, common.TimeStampField: 1, recordPKField: 2, recordI32Field: 3,
	})
}

// capturingSink records what the rewrite would have written to one target.
type capturingSink struct {
	records []storage.Record
}

func (c *capturingSink) Write(r storage.Record) error {
	// The rewrite releases what it wrote once Write returns; keep a reference.
	r.Retain()
	c.records = append(c.records, r)
	return nil
}

type failingSink struct{}

func (failingSink) Write(storage.Record) error { return merr.WrapErrIoFailedReason("sink down") }

func newCapturingSinks(n int) ([]*capturingSink, []hashSplitSink) {
	sinks := make([]*capturingSink, n)
	outs := make([]hashSplitSink, n)
	for i := range sinks {
		sinks[i] = &capturingSink{}
		outs[i] = sinks[i]
	}
	return sinks, outs
}

// recordTask builds a task over recordSchema, adjusted by mutate, for the
// doubling of the only shard at modulus 2.
func recordTask(t *testing.T, mutate func(*schemapb.CollectionSchema)) (*hashSplitCompactionTask, *hashSplitPartitioner, *schemapb.FieldSchema) {
	t.Helper()
	schema := recordSchema()
	if mutate != nil {
		mutate(schema)
	}
	targets := doublingTargets(0, 1)
	plan := &datapb.CompactionPlan{Schema: schema, HashSplitTargets: targets, HashSplitModulus: 2}
	task := NewHashSplitCompactionTask(context.Background(), nil, plan, compaction.GenParams())
	partitioner, err := newHashSplitPartitioner(2, targets)
	require.NoError(t, err)
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	require.NoError(t, err)
	return task, partitioner, pkField
}

func capturedPKs(sink *capturingSink) []int64 {
	var out []int64
	for _, rec := range sink.records {
		col := rec.Column(recordPKField).(*array.Int64)
		for i := 0; i < rec.Len(); i++ {
			out = append(out, col.Value(i))
		}
	}
	return out
}

func TestHashSplitRouteRecordPlacesEveryRowWhereTheWritePathDoes(t *testing.T) {
	task, partitioner, pkField := recordTask(t, nil)
	pks := make([]int64, 0, 1000)
	for i := int64(0); i < 1000; i++ {
		pks = append(pks, i)
	}
	sinks, outs := newCapturingSinks(2)
	rows := make([]int64, 2)
	filter := compaction.NewEntityFilter(nil, 0, time.Now(), 0)
	require.NoError(t, task.routeRecord(pkRecord(t, pks), pkField, 0, filter, partitioner, outs, rows))

	table, err := routing.TableFromMeta([]string{"target-a", "target-b"},
		[]*schemapb.CollectionShardInfo{hashInfo("target-a", 0), hashInfo("target-b", 1)}, 2)
	require.NoError(t, err)
	total := 0
	for i, sink := range sinks {
		got := capturedPKs(sink)
		assert.EqualValues(t, len(got), rows[i])
		assert.NotEmpty(t, got)
		for _, pk := range got {
			want, err := table.VChannelOfPK(pk)
			require.NoError(t, err)
			assert.Equal(t, want, partitioner.TargetVChannel(i), "pk %d", pk)
		}
		total += len(got)
	}
	assert.Equal(t, len(pks), total, "every row lands exactly once")
}

func TestHashSplitRouteRecordSurfacesASinkFailure(t *testing.T) {
	task, partitioner, pkField := recordTask(t, nil)
	filter := compaction.NewEntityFilter(nil, 0, time.Now(), 0)
	err := task.routeRecord(pkRecord(t, []int64{1}), pkField, 0, filter, partitioner,
		[]hashSplitSink{failingSink{}, failingSink{}}, make([]int64, 2))
	assert.ErrorIs(t, err, merr.ErrIoFailed)
}

func TestHashSplitRouteRecordRefusesAnUnsupportedPrimaryKeyType(t *testing.T) {
	task, partitioner, _ := recordTask(t, nil)
	pkField := &schemapb.FieldSchema{FieldID: recordI32Field, DataType: schemapb.DataType_Int32}
	filter := compaction.NewEntityFilter(nil, 0, time.Now(), 0)
	err := task.routeRecord(pkRecord(t, []int64{1}), pkField, 0, filter, partitioner, []hashSplitSink{failingSink{}, failingSink{}}, make([]int64, 2))
	assert.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestHashSplitStampsTheInputCommitTimestampOnEveryRow(t *testing.T) {
	// An import segment's binlog row timestamps predate its commit. Datacoord
	// publishes rewrite outputs with commit_timestamp 0, so the rows must carry
	// the commit timestamp themselves, as a mix compaction's outputs do.
	const commitTs = uint64(449000000000000000)
	pks := make([]int64, 0, 64)
	for i := int64(0); i < 64; i++ {
		pks = append(pks, i)
	}
	for _, tc := range []struct {
		name     string
		commitTs uint64
	}{
		{name: "import segment", commitTs: commitTs},
		{name: "ordinary segment keeps its row timestamps", commitTs: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task, partitioner, pkField := recordTask(t, nil)
			sinks, outs := newCapturingSinks(2)
			filter := compaction.NewEntityFilter(nil, 0, time.Now(), tc.commitTs)
			require.NoError(t, task.routeRecord(pkRecord(t, pks), pkField, tc.commitTs, filter, partitioner, outs, make([]int64, 2)))

			var got []int64
			for _, sink := range sinks {
				for _, rec := range sink.records {
					col := rec.Column(common.TimeStampField).(*array.Int64)
					for i := 0; i < rec.Len(); i++ {
						got = append(got, col.Value(i))
					}
				}
			}
			require.Len(t, got, len(pks))
			for i, ts := range got {
				if tc.commitTs != 0 {
					assert.Equal(t, int64(tc.commitTs), ts, "row %d", i)
				} else {
					assert.Greater(t, ts, int64(0))
					assert.LessOrEqual(t, ts, int64(len(pks)))
				}
			}
		})
	}
}

func TestHashSplitFailsCleanlyWhenAppendFails(t *testing.T) {
	appendMock := mockey.Mock((*storage.RecordBuilder).Append).Return(merr.WrapErrServiceInternalMsg("injected append failure")).Build()
	defer appendMock.UnPatch()

	task, partitioner, pkField := recordTask(t, nil)
	sinks, outs := newCapturingSinks(2)
	filter := compaction.NewEntityFilter(nil, 0, time.Now(), 0)
	rows := make([]int64, 2)
	err := task.routeRecord(pkRecord(t, []int64{1, 2, 3}), pkField, 0, filter, partitioner, outs, rows)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "injected append failure")
	for i, sink := range sinks {
		assert.Empty(t, sink.records, "no partial batch reaches target %d", i)
	}
	assert.Equal(t, []int64{0, 0}, rows, "no row is counted for a failed batch")
}

// withColumn adds one column to a record.
type withColumn struct {
	storage.Record
	fieldID storage.FieldID
	col     arrow.Array
}

func (r withColumn) Column(i storage.FieldID) arrow.Array {
	if i == r.fieldID {
		return r.col
	}
	return r.Record.Column(i)
}

func TestHashSplitDropsRowsExpiredByTheTTLField(t *testing.T) {
	const ttlField = int64(103)
	withTTL := func(schema *schemapb.CollectionSchema) {
		schema.Fields = append(schema.Fields, &schemapb.FieldSchema{
			FieldID: ttlField, Name: "expire_at", DataType: schemapb.DataType_Timestamptz, Nullable: true,
		})
		schema.Properties = append(schema.Properties, &commonpb.KeyValuePair{Key: common.CollectionTTLFieldKey, Value: "expire_at"})
	}

	t.Run("expired rows are dropped", func(t *testing.T) {
		task, partitioner, pkField := recordTask(t, withTTL)
		now := time.Now()
		past := now.Add(-time.Hour).UnixMicro()
		future := now.Add(time.Hour).UnixMicro()
		// pk 1 expired by its TTL field, pk 2 not yet, pk 3 carries no expiry.
		ttlBuilder := array.NewInt64Builder(memory.DefaultAllocator)
		ttlBuilder.Append(past)
		ttlBuilder.Append(future)
		ttlBuilder.AppendNull()
		rec := withColumn{Record: pkRecord(t, []int64{1, 2, 3}), fieldID: ttlField, col: ttlBuilder.NewArray()}

		sinks, outs := newCapturingSinks(2)
		filter := compaction.NewEntityFilter(nil, 0, now, 0)
		require.NoError(t, task.routeRecord(rec, pkField, 0, filter, partitioner, outs, make([]int64, 2)))

		kept := map[int64]bool{}
		for _, sink := range sinks {
			for _, pk := range capturedPKs(sink) {
				kept[pk] = true
			}
		}
		assert.Equal(t, map[int64]bool{2: true, 3: true}, kept)
		assert.Equal(t, 1, filter.GetExpiredCount())
	})

	t.Run("a record without the TTL column is refused", func(t *testing.T) {
		task, partitioner, pkField := recordTask(t, withTTL)
		filter := compaction.NewEntityFilter(nil, 0, time.Now(), 0)
		// A binlog record without the field answers nil for its column.
		rec := withColumn{Record: pkRecord(t, []int64{1}), fieldID: ttlField}
		err := task.routeRecord(rec, pkField, 0, filter, partitioner, []hashSplitSink{failingSink{}, failingSink{}}, make([]int64, 2))
		assert.ErrorIs(t, err, merr.ErrServiceInternal)
	})
}

func TestHashSplitDropsDeletedRowsWithoutBreakingTheirRun(t *testing.T) {
	// A deleted row in the middle of a run ends the run: the rows around it are
	// still written, the deleted one is not.
	task, partitioner, pkField := recordTask(t, nil)
	pks := []int64{10, 11, 12, 13, 14, 15}
	deleted := map[any]typeutil.Timestamp{int64(12): 100, int64(13): 100}
	sinks, outs := newCapturingSinks(2)
	filter := compaction.NewEntityFilter(deleted, 0, time.Now(), 0)
	require.NoError(t, task.routeRecord(pkRecord(t, pks), pkField, 0, filter, partitioner, outs, make([]int64, 2)))

	var kept []int64
	for _, sink := range sinks {
		kept = append(kept, capturedPKs(sink)...)
	}
	assert.ElementsMatch(t, []int64{10, 11, 14, 15}, kept)
	assert.Equal(t, 2, filter.GetDeletedCount())
}
