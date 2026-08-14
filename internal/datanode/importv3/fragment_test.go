// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestSortFieldsValidatesPersistedTypes(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64},
		{FieldID: 101, DataType: schemapb.DataType_VarChar},
	}}
	fields, err := SortFields(&datapb.SortSpec{FormatVersion: 1, Fields: []*datapb.SortFieldSpec{
		{FieldId: 101, KeyType: datapb.SortKeyType_SORT_KEY_TYPE_STRING},
		{FieldId: 100, KeyType: datapb.SortKeyType_SORT_KEY_TYPE_INT64},
	}}, schema)
	require.NoError(t, err)
	require.Equal(t, []int64{101, 100}, fields)

	_, err = SortFields(&datapb.SortSpec{FormatVersion: 1, Fields: []*datapb.SortFieldSpec{{
		FieldId: 101, KeyType: datapb.SortKeyType_SORT_KEY_TYPE_INT64,
	}}}, schema)
	require.Error(t, err)
}

func TestResultSortFlagsRequireFrozenContract(t *testing.T) {
	ordinary := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
	}}}
	pkSpec := &datapb.SortSpec{FormatVersion: 1, Fields: []*datapb.SortFieldSpec{{
		FieldId: 100, KeyType: datapb.SortKeyType_SORT_KEY_TYPE_INT64,
	}}}
	isSorted, namespaceSorted, err := ResultSortFlags(pkSpec, ordinary)
	require.NoError(t, err)
	require.True(t, isSorted)
	require.False(t, namespaceSorted)

	namespace := &schemapb.CollectionSchema{EnableNamespace: true, Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, DataType: schemapb.DataType_VarChar, IsPartitionKey: true},
	}}
	namespaceSpec := &datapb.SortSpec{FormatVersion: 1, Fields: []*datapb.SortFieldSpec{
		{FieldId: 101, KeyType: datapb.SortKeyType_SORT_KEY_TYPE_STRING},
		{FieldId: 100, KeyType: datapb.SortKeyType_SORT_KEY_TYPE_INT64},
	}}
	isSorted, namespaceSorted, err = ResultSortFlags(namespaceSpec, namespace)
	require.NoError(t, err)
	require.False(t, isSorted)
	require.True(t, namespaceSorted)

	_, _, err = ResultSortFlags(pkSpec, namespace)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	_, _, err = ResultSortFlags(&datapb.SortSpec{FormatVersion: 1, Fields: []*datapb.SortFieldSpec{
		{FieldId: 100, KeyType: datapb.SortKeyType_SORT_KEY_TYPE_INT64},
		{FieldId: 101, KeyType: datapb.SortKeyType_SORT_KEY_TYPE_STRING},
	}}, namespace)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func ttlRecord(expirationMicros ...int64) storage.Record {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.AppendValues(expirationMicros, nil)
	column := builder.NewArray()
	builder.Release()
	record := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "ttl", Type: arrow.PrimitiveTypes.Int64}}, nil),
		[]arrow.Array{column}, int64(len(expirationMicros)))
	column.Release()
	return storage.NewSimpleArrowRecord(record, map[int64]int{101: 0})
}

func ttlRecordWithTimestamp(timestamp uint64) storage.Record {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.Append(int64(timestamp))
	column := builder.NewArray()
	builder.Release()
	record := array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "ts", Type: arrow.PrimitiveTypes.Int64}}, nil),
		[]arrow.Array{column}, 1)
	column.Release()
	return storage.NewSimpleArrowRecord(record, map[int64]int{common.TimeStampField: 0})
}

func TestTTLOnlyPredicateUsesOneClockPerBatch(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	clockCalls := 0
	schema := &schemapb.CollectionSchema{
		Fields:     []*schemapb.FieldSchema{{FieldID: 101, Name: "expires", DataType: schemapb.DataType_Timestamptz}},
		Properties: []*commonpb.KeyValuePair{{Key: common.CollectionTTLFieldKey, Value: "expires"}},
	}
	predicate := newTTLOnlyPredicate(schema, 0, 0, func() time.Time {
		clockCalls++
		return now
	})
	record := ttlRecord(now.Add(-time.Second).UnixMicro(), now.Add(time.Second).UnixMicro())
	defer record.Release()
	require.False(t, predicate(record, 0, 0))
	require.True(t, predicate(record, 0, 1))
	require.Equal(t, 1, clockCalls)

	record2 := ttlRecord(now.Add(time.Second).UnixMicro())
	defer record2.Release()
	require.True(t, predicate(record2, 0, 0))
	require.Equal(t, 2, clockCalls)
}

func TestTTLOnlyPredicateUsesDataTSWithoutDeletes(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	dataTS := tsoutil.ComposeTSByTime(now.Add(-2 * time.Hour))
	predicate := newTTLOnlyPredicate(&schemapb.CollectionSchema{}, int64(time.Hour), dataTS, func() time.Time { return now })
	record := ttlRecord(-1)
	defer record.Release()
	require.False(t, predicate(record, 0, 0), "the row is expired by collection TTL")
}

func TestTTLOnlyPredicateKeepsBackupTimestamp(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	dataTS := tsoutil.ComposeTSByTime(now)
	sourceTS := tsoutil.ComposeTSByTime(now.Add(-2 * time.Hour))
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: common.TimeStampField, Name: "ts", DataType: schemapb.DataType_Int64}}}
	predicate := newTTLOnlyPredicate(schema, int64(time.Hour), dataTS, func() time.Time { return now })
	record := ttlRecordWithTimestamp(sourceTS)
	defer record.Release()
	require.False(t, predicate(record, 0, 0), "backup rows use their source timestamp for collection TTL")
}
