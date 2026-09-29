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
	"io"
	"math"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"
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
)

const (
	w6bCompactionPK     = int64(100)
	w6bCompactionStruct = int64(200)
	w6bCompactionArray  = int64(201)
	w6bCompactionVector = int64(202)
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
		values = append(values, &storage.Value{Value: map[storage.FieldID]any{
			common.RowIDField:     pk + 10,
			common.TimeStampField: ts,
			w6bCompactionPK:       pk,
			w6bCompactionArray:    w6bCompactionArrayRow(pk),
			w6bCompactionVector:   w6bCompactionVectorRow(pk),
		}})
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

func (s *ClusteringCompactionTaskStorageV3Suite) TestNativeArrayValueSerdeThroughCompaction() {
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
	s.task.plan.SegmentBinlogs = []*datapb.CompactionSegmentBinlogs{
		w6bWriteCompactionSource(s.T(), cfg, schema, 10, []int64{1, 2, 3, 4}, false),
	}
	s.task.plan.TotalRows = 4
	result, err := s.task.Compact()
	s.Require().NoError(err)
	s.Require().NotEmpty(result.GetSegments())
	w6bAssertCompactionOutput(s.T(), cfg, schema, result.GetSegments(), []int64{1, 2, 3, 4}, false)
}
