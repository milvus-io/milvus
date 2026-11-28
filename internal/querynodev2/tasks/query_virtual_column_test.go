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

package tasks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A Query whose plan carries $seg_id and $seg_offset projection items, run
// through the real QueryNode task on growing segments. The cases cover one
// segment and two, a finite limit and none, and both transports of the
// fetched fields (protobuf and Arrow): each combination takes a different
// path through the retrieve pipeline, and every one must return the two
// columns under their request-local IDs.
func TestQueryTaskVirtualColumns(t *testing.T) {
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitLocalChunkManager(t.TempDir())
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	initcore.InitTieredStorage(paramtable.Get())
	const (
		segIDColumn     int64 = -1000
		segOffsetColumn int64 = -1001
	)
	ctx := context.Background()
	manager := segments.NewManager()
	schema := &schemapb.CollectionSchema{Name: "virtual", Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64},
	}}
	require.NoError(t, manager.Collection.PutOrRef(100, schema, &segcorepb.CollectionIndexMeta{}, &querypb.LoadMetaInfo{LoadType: querypb.LoadType_LoadCollection, CollectionID: 100, PartitionIDs: []int64{10}}))
	collection := manager.Collection.Get(100)
	defer segments.DeleteCollection(collection)
	// pk -> (segment, offset)
	rows := map[int64][2]int64{}
	for i, pks := range [][]int64{{1, 40, 30}, {2, 50, 20}} {
		segmentID := int64(100 + i)
		seg, err := segments.NewSegment(ctx, collection, manager.Segment, segments.SegmentTypeGrowing, 0, &querypb.SegmentLoadInfo{SegmentID: segmentID, CollectionID: 100, PartitionID: 10, InsertChannel: "by-dev-rootcoord-dml_0_100v0", Level: datapb.SegmentLevel_Legacy})
		require.NoError(t, err)
		defer seg.Release(ctx)
		values := make([]int64, len(pks))
		for offset, pk := range pks {
			rows[pk] = [2]int64{segmentID, int64(offset)}
			values[offset] = pk * 10
		}
		require.NoError(t, seg.Insert(ctx, []int64{0, 1, 2}, []uint64{1, 1, 1}, &segcorepb.InsertRecord{NumRows: 3, FieldsData: []*schemapb.FieldData{
			{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}}}}},
			{FieldId: 101, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}}}}},
		}}))
		manager.Segment.Put(ctx, segments.SegmentTypeGrowing, seg)
	}

	run := func(t *testing.T, limit int64, segmentIDs []int64) *internalpb.RetrieveResults {
		t.Helper()
		// 1 is the timestamp system field; the merge across segments reads it.
		outputs := []int64{100, 101, 1}
		data, err := proto.Marshal(&planpb.PlanNode{
			Node:           &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Limit: limit}},
			OutputFieldIds: outputs,
			ProjectionItems: []*planpb.ProjectionItem{
				{OutputId: segIDColumn, DataType: schemapb.DataType_Int64, VirtualColumn: planpb.VirtualColumn_VirtualColumnSegmentID},
				{OutputId: segOffsetColumn, DataType: schemapb.DataType_Int64, VirtualColumn: planpb.VirtualColumn_VirtualColumnSegmentOffset},
			},
		})
		require.NoError(t, err)
		req := &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{Base: &commonpb.MsgBase{}, CollectionID: 100, PartitionIDs: []int64{10}, SerializedExprPlan: data, MvccTimestamp: typeutil.MaxTimestamp, Limit: limit, OutputFieldsId: outputs}, SegmentIDs: segmentIDs, Scope: querypb.DataScope_Streaming}
		task := NewQueryTask(ctx, collection, manager, req)
		require.NoError(t, task.PreExecute())
		require.NoError(t, task.Execute())
		return task.Result()
	}
	column := func(t *testing.T, result *internalpb.RetrieveResults, id int64) []int64 {
		t.Helper()
		for _, field := range result.GetFieldsData() {
			if field.GetFieldId() == id {
				require.Equal(t, schemapb.DataType_Int64, field.GetType())
				return field.GetScalars().GetLongData().GetData()
			}
		}
		t.Fatalf("column %d is missing", id)
		return nil
	}

	for _, arrowTransport := range []string{"false", "true"} {
		t.Run("transport_arrow="+arrowTransport, func(t *testing.T) {
			item := &paramtable.Get().CommonCfg.InterfaceZeroCopyEnabled
			old := item.GetValue()
			defer paramtable.Get().Save(item.Key, old)
			require.NoError(t, paramtable.Get().Save(item.Key, arrowTransport))

			for _, tc := range []struct {
				name     string
				limit    int64
				segments []int64
				pks      []int64
			}{
				{"one segment, limit", 2, []int64{101}, []int64{2, 20}},
				{"one segment, no limit", -1, []int64{101}, []int64{2, 20, 50}},
				{"two segments, limit", 3, []int64{100, 101}, []int64{1, 2, 20}},
				{"two segments, no limit", -1, []int64{100, 101}, []int64{1, 2, 20, 30, 40, 50}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					result := run(t, tc.limit, tc.segments)
					require.Equal(t, tc.pks, result.GetIds().GetIntId().GetData())
					wantSegments := make([]int64, len(tc.pks))
					wantOffsets := make([]int64, len(tc.pks))
					wantValues := make([]int64, len(tc.pks))
					for i, pk := range tc.pks {
						wantSegments[i], wantOffsets[i], wantValues[i] = rows[pk][0], rows[pk][1], pk*10
					}
					require.Equal(t, wantValues, column(t, result, 101))
					require.Equal(t, wantSegments, column(t, result, segIDColumn))
					require.Equal(t, wantOffsets, column(t, result, segOffsetColumn))
				})
			}
		})
	}
}
