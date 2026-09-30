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
	"fmt"
	"io"
	"math"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	w6bCompactionPK     = int64(100)
	w6bCompactionStruct = int64(200)
	w6bCompactionArray  = int64(201)
	w6bCompactionVector = int64(202)
	w14LegacyArray      = int64(103)
	w14FloatVector      = int64(104)
	w14JSON             = int64(105)
	w14TTL              = int64(106)
)

func w6bCompactionSchema() *schemapb.CollectionSchema {
	leaf := &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_LeafType{LeafType: schemapb.DataType_VarChar}}
	inner := &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: leaf}}
	return &schemapb.CollectionSchema{
		Name: "native_compaction",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: w6bCompactionPK, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: w6bCompactionStruct, Name: "s", Nullable: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: w6bCompactionArray, Name: "s[nested]", DataType: schemapb.DataType_Array,
					ElementType: schemapb.DataType_Array, Nullable: true, ElementNullable: true,
					TypeSchema: &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: inner}}},
				{FieldID: w6bCompactionVector, Name: "s[vectors]", DataType: schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector, Nullable: true, ElementNullable: true,
					TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
			},
		}},
	}
}

func w6bCompactionArrayRow(pk int64) *schemapb.ScalarField {
	switch pk {
	case 1:
		return &schemapb.ScalarField{ValidData: []bool{true, false, true},
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
				ElementType: schemapb.DataType_VarChar,
				Data: []*schemapb.ScalarField{
					{ValidData: []bool{true, false}, Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a", ""}}}},
					{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{}}},
					{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{}}},
				},
			}}}
	case 2:
		return nil
	case 3:
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{
			ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_VarChar}}}
	default:
		return &schemapb.ScalarField{ValidData: []bool{true}, Data: &schemapb.ScalarField_ArrayData{
			ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_VarChar,
				Data: []*schemapb.ScalarField{{ValidData: []bool{true}, Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"z"}}}}}}}}
	}
}

func w6bCompactionVectorRow(pk int64) *schemapb.VectorField {
	switch pk {
	case 1:
		return &schemapb.VectorField{Dim: 2, ValidData: []bool{true, false, true},
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3, 4}}}}
	case 2:
		return nil
	case 3:
		return &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{}}}
	default:
		return &schemapb.VectorField{Dim: 2, ValidData: []bool{true},
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{5, 6}}}}
	}
}

func w6bWriteCompactionSource(t *testing.T, cfg *indexpb.StorageConfig, schema *schemapb.CollectionSchema,
	segmentID int64, pks []int64, sorted bool,
) *datapb.CompactionSegmentBinlogs {
	t.Helper()
	ts := int64(tsoutil.ComposeTSByTime(getMilvusBirthday()))
	values := make([]*storage.Value, 0, len(pks))
	for _, pk := range pks {
		row := map[storage.FieldID]any{
			common.RowIDField:     pk + 10,
			common.TimeStampField: ts,
			w6bCompactionPK:       pk,
			w6bCompactionArray:    w6bCompactionArrayRow(pk),
		}
		if typeutil.GetField(schema, w6bCompactionVector) != nil {
			row[w6bCompactionVector] = w6bCompactionVectorRow(pk)
		}
		values = append(values, &storage.Value{Value: row})
	}
	record, err := storage.ValueSerializer(values, schema)
	require.NoError(t, err)
	defer record.Release()
	writer, err := storage.NewBinlogRecordWriter(context.Background(), CollectionID, PartitionID, segmentID,
		schema, allocator.NewLocalAllocator(100000+segmentID*100, math.MaxInt64), 1<<20, int64(len(pks)),
		storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(cfg), storage.WithWriterFormat("parquet"))
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	logs, _, _, manifest, _ := writer.GetLogs()
	require.NotEmpty(t, manifest)
	return &datapb.CompactionSegmentBinlogs{
		CollectionID: CollectionID, PartitionID: PartitionID, SegmentID: segmentID,
		FieldBinlogs: storage.SortFieldBinlogs(logs), StorageVersion: storage.StorageV3,
		Manifest: manifest, IsSorted: sorted,
	}
}

func w6bPartialStructSchemas() (*schemapb.CollectionSchema, *schemapb.CollectionSchema) {
	target := w6bCompactionSchema()
	source := proto.Clone(target).(*schemapb.CollectionSchema)
	source.StructArrayFields[0].Fields = source.StructArrayFields[0].Fields[:1]
	return source, target
}

func w6bAssertBackfilledVector(t *testing.T, cfg *indexpb.StorageConfig, schema *schemapb.CollectionSchema, segments []*datapb.CompactionSegment) {
	t.Helper()
	wantCounts := map[int64]int64{1: 3, 2: 0, 3: 0, 4: 1}
	seen := make(map[int64]bool)
	for _, segment := range segments {
		present, err := packed.GetManifestFieldIDs(segment.GetManifest(), cfg)
		require.NoError(t, err)
		require.Contains(t, present, w6bCompactionVector, "backfilled child must be physically written")
		rr, err := storage.NewManifestRecordReader(context.Background(), segment.GetManifest(), schema,
			storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(cfg))
		require.NoError(t, err)
		for {
			record, err := rr.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			pkCol := record.Column(w6bCompactionPK).(*array.Int64)
			vector := record.Column(w6bCompactionVector).(*array.List)
			for i := 0; i < record.Len(); i++ {
				pk := pkCol.Value(i)
				require.False(t, seen[pk])
				seen[pk] = true
				start, end := vector.ValueOffsets(i)
				require.Equal(t, wantCounts[pk], end-start)
				if pk == 2 {
					require.True(t, vector.IsNull(i))
				} else {
					require.False(t, vector.IsNull(i))
				}
				for j := start; j < end; j++ {
					require.True(t, vector.ListValues().IsNull(int(j)))
				}
			}
		}
		require.NoError(t, rr.Close())
	}
	require.Len(t, seen, 4)
}

func w14ArrowCompactionRecord(t *testing.T, schema *schemapb.CollectionSchema) storage.Record {
	t.Helper()
	arrowSchema, err := storage.ConvertToArrowSchema(schema, false)
	require.NoError(t, err)
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer b.Release()
	fields := typeutil.GetAllFieldSchemas(schema)
	builders := make(map[int64]array.Builder, len(fields))
	fieldToColumn := make(map[int64]int, len(fields))
	for i, field := range fields {
		builders[field.FieldID] = b.Field(i)
		fieldToColumn[field.FieldID] = i
	}
	ts := int64(tsoutil.ComposeTSByTime(getMilvusBirthday()))
	for _, pk := range []int64{1, 2, 3, 4} {
		builders[common.RowIDField].(*array.Int64Builder).Append(pk + 10)
		builders[common.TimeStampField].(*array.Int64Builder).Append(ts)
		builders[w6bCompactionPK].(*array.Int64Builder).Append(pk)

		outer := builders[w6bCompactionArray].(*array.ListBuilder)
		inner := outer.ValueBuilder().(*array.ListBuilder)
		leaf := inner.ValueBuilder().(*array.StringBuilder)
		switch pk {
		case 1:
			outer.Append(true)
			inner.Append(true)
			leaf.Append("a")
			leaf.AppendNull()
			inner.AppendNull()
			inner.Append(true)
		case 2:
			outer.AppendNull()
		case 3:
			outer.Append(true)
		case 4:
			outer.Append(true)
			inner.Append(true)
			leaf.Append("z")
		}

		vectors := builders[w6bCompactionVector].(*array.ListBuilder)
		vector := vectors.ValueBuilder().(*array.BinaryBuilder)
		switch pk {
		case 1:
			vectors.Append(true)
			vector.Append(arrow.Float32Traits.CastToBytes([]float32{1, 2}))
			vector.AppendNull()
			vector.Append(arrow.Float32Traits.CastToBytes([]float32{3, 4}))
		case 2:
			vectors.AppendNull()
		case 3:
			vectors.Append(true)
		case 4:
			vectors.Append(true)
			vector.Append(arrow.Float32Traits.CastToBytes([]float32{5, 6}))
		}

		if legacy, ok := builders[w14LegacyArray]; ok {
			payload, err := proto.Marshal(&schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{
				LongData: &schemapb.LongArray{Data: []int64{pk, pk + 10}},
			}})
			require.NoError(t, err)
			legacy.(*array.BinaryBuilder).Append(payload)
			builders[w14FloatVector].(*array.FixedSizeBinaryBuilder).Append(arrow.Float32Traits.CastToBytes([]float32{float32(pk), float32(-pk)}))
			json := builders[w14JSON].(*array.BinaryBuilder)
			if pk == 2 {
				json.AppendNull()
			} else {
				json.Append([]byte(fmt.Sprintf(`{"pk":%d}`, pk)))
			}
			ttl := builders[w14TTL].(*array.Int64Builder)
			if pk == 2 {
				ttl.AppendNull()
			} else if pk == 4 {
				ttl.Append(time.Now().Add(-time.Hour).UnixMicro())
			} else {
				ttl.Append(time.Now().Add(time.Hour).UnixMicro())
			}
		}
	}
	return storage.NewSimpleArrowRecord(b.NewRecord(), fieldToColumn)
}

func w14WriteArrowCompactionSource(t *testing.T, cfg *indexpb.StorageConfig, schema *schemapb.CollectionSchema,
	segmentID int64, record storage.Record,
) *datapb.CompactionSegmentBinlogs {
	t.Helper()
	writer, err := storage.NewBinlogRecordWriter(context.Background(), CollectionID, PartitionID, segmentID,
		schema, allocator.NewLocalAllocator(100000+segmentID*100, math.MaxInt64), 1<<20, int64(record.Len()),
		storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(cfg), storage.WithWriterFormat("parquet"))
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	logs, _, _, manifest, _ := writer.GetLogs()
	require.NotEmpty(t, manifest)
	return &datapb.CompactionSegmentBinlogs{
		CollectionID: CollectionID, PartitionID: PartitionID, SegmentID: segmentID,
		FieldBinlogs: storage.SortFieldBinlogs(logs), StorageVersion: storage.StorageV3, Manifest: manifest,
	}
}

func w14AssertArrowCompactionRows(t *testing.T, cfg *indexpb.StorageConfig, schema *schemapb.CollectionSchema,
	segments []*datapb.CompactionSegment, source storage.Record, wantPKs []int64,
) {
	t.Helper()
	want := make(map[int64]int, source.Len())
	for i := 0; i < source.Len(); i++ {
		want[source.Column(w6bCompactionPK).(*array.Int64).Value(i)] = i
	}
	seen := make(map[int64]bool, len(wantPKs))
	for _, segment := range segments {
		require.NotEmpty(t, segment.GetManifest())
		rr, err := storage.NewManifestRecordReader(context.Background(), segment.GetManifest(), schema,
			storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(cfg))
		require.NoError(t, err)
		for {
			record, err := rr.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			for row := 0; row < record.Len(); row++ {
				pk := record.Column(w6bCompactionPK).(*array.Int64).Value(row)
				sourceRow, ok := want[pk]
				require.True(t, ok, "unexpected pk %d", pk)
				require.False(t, seen[pk], "duplicate pk %d", pk)
				seen[pk] = true
				for _, field := range typeutil.GetAllFieldSchemas(schema) {
					before := array.NewSlice(source.Column(field.FieldID), int64(sourceRow), int64(sourceRow+1))
					after := array.NewSlice(record.Column(field.FieldID), int64(row), int64(row+1))
					require.True(t, array.Equal(before, after), "pk=%d field=%d", pk, field.FieldID)
					before.Release()
					after.Release()
				}
			}
		}
		require.NoError(t, rr.Close())
	}
	require.Len(t, seen, len(wantPKs))
	for _, pk := range wantPKs {
		require.True(t, seen[pk], "missing pk %d", pk)
	}
}

func w6bWriteCompactionDelete(t *testing.T, cfg *indexpb.StorageConfig, segment *datapb.CompactionSegmentBinlogs, pk int64) {
	t.Helper()
	const logID = int64(777001)
	basePath, _, err := packed.UnmarshalManifestPath(segment.GetManifest())
	require.NoError(t, err)
	deltaPath := metautil.BuildDeltaLogPathV3(basePath, logID)
	writer, err := storage.NewDeltalogWriter(context.Background(), segment.GetCollectionID(),
		segment.GetPartitionID(), segment.GetSegmentID(), logID, schemapb.DataType_Int64, deltaPath,
		storage.WithVersion(storage.StorageV2), storage.WithStorageConfig(cfg))
	require.NoError(t, err)
	record, _, _, err := storage.BuildDeleteRecord(
		[]storage.PrimaryKey{storage.NewInt64PrimaryKey(pk)},
		[]uint64{tsoutil.ComposeTSByTime(getMilvusBirthday().Add(time.Second))})
	require.NoError(t, err)
	defer record.Release()
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	segment.Manifest, err = packed.AddDeltaLogsToManifest(segment.GetManifest(), cfg,
		[]packed.DeltaLogEntry{{Path: deltaPath, NumEntries: 1}})
	require.NoError(t, err)
}

func w6bAssertCompactionOutput(t *testing.T, cfg *indexpb.StorageConfig, schema *schemapb.CollectionSchema,
	segments []*datapb.CompactionSegment, wantPKs []int64, requireSorted bool,
) {
	t.Helper()
	want := make(map[int64]bool, len(wantPKs))
	for _, pk := range wantPKs {
		want[pk] = true
	}
	seen := make(map[int64]bool, len(wantPKs))
	var ordered []int64
	for _, segment := range segments {
		require.NotEmpty(t, segment.GetManifest())
		rr, err := storage.NewManifestRecordReader(context.Background(), segment.GetManifest(), schema,
			storage.WithVersion(storage.StorageV3), storage.WithStorageConfig(cfg))
		require.NoError(t, err)
		for {
			record, err := rr.Next()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			pkColumn := record.Column(w6bCompactionPK).(*array.Int64)
			rows, err := storage.RecordToInsertData(record, schema, nil)
			require.NoError(t, err)
			for i := 0; i < record.Len(); i++ {
				pk := pkColumn.Value(i)
				require.True(t, want[pk], "unexpected pk %d", pk)
				require.False(t, seen[pk], "duplicate pk %d", pk)
				seen[pk] = true
				ordered = append(ordered, pk)
				gotArray, _ := rows.Data[w6bCompactionArray].GetRow(i).(*schemapb.ScalarField)
				gotVector, _ := rows.Data[w6bCompactionVector].GetRow(i).(*schemapb.VectorField)
				require.True(t, proto.Equal(w6bCompactionArrayRow(pk), gotArray), "nested array pk=%d", pk)
				require.True(t, proto.Equal(w6bCompactionVectorRow(pk), gotVector), "vector array pk=%d", pk)
			}
		}
		require.NoError(t, rr.Close())
	}
	require.Len(t, seen, len(want))
	if requireSorted {
		for i := 1; i < len(ordered); i++ {
			require.Less(t, ordered[i-1], ordered[i])
		}
	}
}

func (s *MixCompactionTaskStorageV3Suite) nativeArrayCompaction(withDelete, mergeSort bool) {
	schema := w6bCompactionSchema()
	s.task.plan.Schema = schema
	s.task.plan.MaxSize = 1 << 30
	s.task.sortByFieldIDs = []int64{w6bCompactionPK}
	s.task.plan.SegmentBinlogs = nil
	cfg := s.task.compactionParams.StorageConfig
	if mergeSort {
		paramtable.Get().Save(paramtable.Get().DataNodeCfg.UseMergeSort.Key, "true")
		defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.UseMergeSort.Key)
		s.task.compactionParams = compaction.GenParams()
		cfg = s.task.compactionParams.StorageConfig
		s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
			w6bWriteCompactionSource(s.T(), cfg, schema, 10, []int64{1, 3}, true),
			w6bWriteCompactionSource(s.T(), cfg, schema, 11, []int64{2, 4}, true),
		}
		if withDelete {
			w6bWriteCompactionDelete(s.T(), cfg, s.task.plan.SegmentBinlogs[0], 3)
		}
	} else {
		source := w6bWriteCompactionSource(s.T(), cfg, schema, 10, []int64{1, 2, 3, 4}, false)
		if withDelete {
			w6bWriteCompactionDelete(s.T(), cfg, source, 3)
		}
		s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{source}
	}
	s.task.plan.TotalRows = 4
	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().NotNil(result)
	s.Require().NotEmpty(result.GetSegments())
	want := []int64{1, 2, 3, 4}
	if withDelete {
		want = []int64{1, 2, 4}
	}
	w6bAssertCompactionOutput(s.T(), cfg, schema, result.GetSegments(), want, mergeSort)
}

func (s *MixCompactionTaskStorageV3Suite) TestNativeArrayMergeSplitWithoutDelete() {
	s.nativeArrayCompaction(false, false)
}

func (s *MixCompactionTaskStorageV3Suite) TestNativeArrayMergeSplitWithDelete() {
	s.nativeArrayCompaction(true, false)
}

func (s *MixCompactionTaskStorageV3Suite) TestNativeArrayMergeSort() {
	s.nativeArrayCompaction(false, true)
}

func (s *MixCompactionTaskStorageV3Suite) TestNativeArrayMergeSortWithDelete() {
	s.nativeArrayCompaction(true, true)
}

func (s *MixCompactionTaskStorageV3Suite) TestPartialStructChildBackfill() {
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.StorageFormat.Key, "parquet")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.StorageFormat.Key)
	sourceSchema, targetSchema := w6bPartialStructSchemas()
	s.task.plan.Schema = targetSchema
	s.task.plan.MaxSize = 1 << 30
	s.task.sortByFieldIDs = []int64{w6bCompactionPK}
	cfg := s.task.compactionParams.StorageConfig
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		w6bWriteCompactionSource(s.T(), cfg, sourceSchema, 10, []int64{1, 2, 3, 4}, false),
	}
	s.task.plan.TotalRows = 4
	result, err := s.task.Compact()
	s.Require().NoError(err)
	w6bAssertBackfilledVector(s.T(), cfg, targetSchema, result.GetSegments())
}

func (s *SortCompactionTaskSuite) nativeArraySort(withDelete bool) {
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	initcore.CleanArrowFileSystem()
	s.Require().NoError(initcore.InitLocalArrowFileSystem(rootPath))
	s.task.compactionParams = compaction.GenParams()
	cfg := s.task.compactionParams.StorageConfig
	schema := w6bCompactionSchema()
	s.task.plan.Schema = schema
	s.task.sortByFieldIDs = []int64{w6bCompactionPK}
	source := w6bWriteCompactionSource(s.T(), cfg, schema, 10, []int64{4, 2, 1, 3}, false)
	if withDelete {
		w6bWriteCompactionDelete(s.T(), cfg, source, 3)
	}
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{source}
	s.task.plan.TotalRows = 4
	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().Equal(datapb.CompactionTaskState_completed, result.GetState())
	want := []int64{1, 2, 3, 4}
	if withDelete {
		want = []int64{1, 2, 4}
	}
	w6bAssertCompactionOutput(s.T(), cfg, schema, result.GetSegments(), want, true)
}

func (s *SortCompactionTaskSuite) TestNativeArraySortWithoutDelete() {
	s.nativeArraySort(false)
}

func (s *SortCompactionTaskSuite) TestNativeArraySortWithDelete() {
	s.nativeArraySort(true)
}

func (s *SortCompactionTaskSuite) TestPartialStructChildBackfill() {
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.StorageFormat.Key, "parquet")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.StorageFormat.Key)
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	initcore.CleanArrowFileSystem()
	s.Require().NoError(initcore.InitLocalArrowFileSystem(rootPath))
	s.task.compactionParams = compaction.GenParams()
	cfg := s.task.compactionParams.StorageConfig
	sourceSchema, targetSchema := w6bPartialStructSchemas()
	s.task.plan.Schema = targetSchema
	s.task.sortByFieldIDs = []int64{w6bCompactionPK}
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		w6bWriteCompactionSource(s.T(), cfg, sourceSchema, 10, []int64{4, 2, 1, 3}, false),
	}
	s.task.plan.TotalRows = 4
	result, err := s.task.Compact()
	s.Require().NoError(err)
	w6bAssertBackfilledVector(s.T(), cfg, targetSchema, result.GetSegments())
}

func (s *ClusteringCompactionTaskStorageV3Suite) nativeArrayArrowCompaction(mixed, withDelete bool) {
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	initcore.CleanArrowFileSystem()
	s.Require().NoError(initcore.InitLocalArrowFileSystem(rootPath))
	s.task.compactionParams = compaction.GenParams()
	cfg := s.task.compactionParams.StorageConfig
	schema := w6bCompactionSchema()
	s.task.plan.Schema = schema
	s.task.plan.ClusteringKeyField = w6bCompactionPK
	s.task.plan.PreferSegmentRows = 10
	s.task.plan.MaxSegmentRows = 10
	s.task.plan.MaxSize = 1 << 30
	s.task.plan.PreAllocatedSegmentIDs = &datapb.IDRange{Begin: 3000, End: 4000}
	s.task.plan.PreAllocatedLogIDs = &datapb.IDRange{Begin: 5000, End: 6000}
	if mixed {
		schema.Fields = append(schema.Fields,
			&schemapb.FieldSchema{FieldID: w14LegacyArray, Name: "legacy_array", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
			&schemapb.FieldSchema{FieldID: w14FloatVector, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
			&schemapb.FieldSchema{FieldID: w14JSON, Name: "json", DataType: schemapb.DataType_JSON, Nullable: true},
			&schemapb.FieldSchema{FieldID: w14TTL, Name: "expire_at", DataType: schemapb.DataType_Timestamptz, Nullable: true},
		)
		schema.Properties = append(schema.Properties, &commonpb.KeyValuePair{Key: common.CollectionTTLFieldKey, Value: "expire_at"})
	}
	source := w14ArrowCompactionRecord(s.T(), schema)
	defer source.Release()
	segment := w14WriteArrowCompactionSource(s.T(), cfg, schema, 10, source)
	if withDelete {
		w6bWriteCompactionDelete(s.T(), cfg, segment, 3)
	}
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{segment}
	s.task.plan.TotalRows = 4
	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().NotEmpty(result.GetSegments())
	want := []int64{1, 2, 3, 4}
	if withDelete {
		want = []int64{1, 2}
	}
	w14AssertArrowCompactionRows(s.T(), cfg, schema, result.GetSegments(), source, want)
}

func (s *ClusteringCompactionTaskStorageV3Suite) TestNativeArrayArrowCopyThroughCompaction() {
	s.nativeArrayArrowCompaction(false, false)
}

func (s *ClusteringCompactionTaskStorageV3Suite) TestMixedArrowColumnsThroughClusteringCompaction() {
	s.nativeArrayArrowCompaction(true, true)
}

func (s *ClusteringCompactionTaskStorageV3Suite) TestPartialStructChildBackfill() {
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.StorageFormat.Key, "parquet")
	defer paramtable.Get().Reset(paramtable.Get().DataNodeCfg.StorageFormat.Key)
	rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	initcore.CleanArrowFileSystem()
	s.Require().NoError(initcore.InitLocalArrowFileSystem(rootPath))
	s.task.compactionParams = compaction.GenParams()
	cfg := s.task.compactionParams.StorageConfig
	sourceSchema, targetSchema := w6bPartialStructSchemas()
	s.task.plan.Schema = targetSchema
	s.task.plan.ClusteringKeyField = w6bCompactionPK
	s.task.plan.PreferSegmentRows = 10
	s.task.plan.MaxSegmentRows = 10
	s.task.plan.MaxSize = 1 << 30
	s.task.plan.PreAllocatedSegmentIDs = &datapb.IDRange{Begin: 3000, End: 4000}
	s.task.plan.PreAllocatedLogIDs = &datapb.IDRange{Begin: 5000, End: 6000}
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		w6bWriteCompactionSource(s.T(), cfg, sourceSchema, 10, []int64{4, 2, 1, 3}, false),
	}
	s.task.plan.TotalRows = 4
	result, err := s.task.Compact()
	s.Require().NoError(err)
	w6bAssertBackfilledVector(s.T(), cfg, targetSchema, result.GetSegments())
}
