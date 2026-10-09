// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dml

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/dql"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestPartialUpsertOmittedTimestamptz(t *testing.T) {
	const defaultTimestamp = int64(1600000000123456)
	for _, mode := range []string{"nullable", "default", "nullable_default"} {
		for _, withRLS := range []bool{false, true} {
			for _, scenario := range []struct {
				name         string
				oldValid     []bool
				newRows      int
				nullableOnly bool
			}{
				{name: "only_new", newRows: 2},
				{name: "one_existing", oldValid: []bool{true}, newRows: 1},
				{name: "more_existing", oldValid: []bool{true, true}, newRows: 1},
				{name: "more_new", oldValid: []bool{true}, newRows: 3},
				{name: "null_then_value", oldValid: []bool{false, true}, newRows: 1, nullableOnly: true},
				{name: "value_then_null", oldValid: []bool{true, false}, newRows: 1, nullableOnly: true},
				{name: "all_null", oldValid: []bool{false, false}, newRows: 1, nullableOnly: true},
			} {
				// Stored default-valued fields contain values, including when nullable.
				if mode != "nullable" && scenario.nullableOnly {
					continue
				}
				t.Run(fmt.Sprintf("%s/rls=%t/%s", mode, withRLS, scenario.name), func(t *testing.T) {
					task := partialUpdateAutoIDInsertTestTask(t, false)
					timestampSchema := &schemapb.FieldSchema{
						FieldID: 101, Name: "ts", DataType: schemapb.DataType_Timestamptz, Nullable: mode != "default",
					}
					if mode != "nullable" {
						timestampSchema.DefaultValue = &schemapb.ValueField{Data: &schemapb.ValueField_TimestamptzData{TimestamptzData: defaultTimestamp}}
					}
					task.schema = mustNewSchemaInfo(&schemapb.CollectionSchema{
						Name: task.req.GetCollectionName(),
						Fields: []*schemapb.FieldSchema{
							{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
							timestampSchema,
						},
						Properties: []*commonpb.KeyValuePair{{Key: common.TimezoneKey, Value: "Asia/Shanghai"}},
					})
					numRows := len(scenario.oldValid) + scenario.newRows
					// Request order differs from query order and places new PKs first.
					requestIDs := make([]int64, 0, numRows)
					for i := 0; i < scenario.newRows; i++ {
						requestIDs = append(requestIDs, int64(len(scenario.oldValid)+i+1))
					}
					oldIDs := make([]int64, len(scenario.oldValid))
					oldTimestamps := make([]int64, len(scenario.oldValid))
					for i, valid := range scenario.oldValid {
						oldIDs[i] = int64(i + 1)
						requestIDs = append(requestIDs, int64(len(scenario.oldValid)-i))
						if valid {
							oldTimestamps[i] = defaultTimestamp + int64(i+1)*1234567
						}
					}
					task.req.FieldsData = []*schemapb.FieldData{partialUpdateCASPKFieldData(requestIDs)}
					task.req.NumRows = uint32(numRows)
					task.upsertMsg.InsertMsg.FieldsData = task.req.FieldsData
					task.upsertMsg.InsertMsg.NumRows = uint64(numRows)
					task.partialUpdateOriginalFields = cloneFieldDataList(task.req.FieldsData)
					task.rlsEnabled = withRLS
					if withRLS {
						task.rlsUsingPredicate = &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
							ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64},
							Op:         planpb.OpType_NotEqual, Value: &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 0}},
						}}}
					}

					fakeWAL := newPartialUpdateCASTestWAL(t, 9)
					oldWAL := streaming.WAL()
					streaming.SetWALForTest(fakeWAL)
					defer streaming.SetWALForTest(oldWAL)
					read := mockey.Mock(retrieveByPKs).To(func(ctx context.Context, task *UpsertTask, ids *schemapb.IDs, fields []string) (*milvuspb.QueryResults, segcore.StorageCost, error) {
						require.Equal(t, requestIDs, ids.GetIntId().GetData())
						require.Equal(t, []string{"*"}, fields)
						for _, proof := range task.partialUpdateCASGroups {
							proof.ReadTs = 1001
						}
						ts := &schemapb.FieldData{
							FieldId: 101, FieldName: "ts", Type: schemapb.DataType_Timestamptz,
							Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_TimestamptzData{
								TimestamptzData: &schemapb.TimestamptzArray{Data: oldTimestamps},
							}}},
						}
						if timestampSchema.GetNullable() {
							typeutil.SetFieldDataValidData(ts, scenario.oldValid)
						}
						result := &milvuspb.QueryResults{Status: merr.Success(), FieldsData: []*schemapb.FieldData{partialUpdateCASPKFieldData(oldIDs), ts}}
						// Ordinary queries format before returning. RLS USING needs raw
						// timestamps first; queryPreExecute formats them after authorization.
						if !withRLS {
							require.NoError(t, dql.FormatTimestamptzFields(result.FieldsData, task.schema.SchemaHelper.GetTimezone()))
						}
						return result, segcore.StorageCost{}, nil
					}).Build()
					defer read.UnPatch()
					alloc := mockey.Mock((*allocator.IDAllocator).Alloc).Return(int64(1000), int64(1000+numRows), nil).Build()
					defer alloc.UnPatch()

					var prepareErr error
					require.NotPanics(t, func() { prepareErr = task.prepareUpsert(task.ctx) })
					require.NoError(t, prepareErr)
					require.NoError(t, task.insertPreExecute(task.ctx))
					require.ElementsMatch(t, oldIDs, task.deletePKs.GetIntId().GetData())
					mergedIDs := task.result.GetIDs().GetIntId().GetData()
					require.ElementsMatch(t, requestIDs, mergedIDs)
					ts := task.upsertMsg.InsertMsg.FieldsData[1]
					require.Equal(t, "ts", ts.GetFieldName())
					require.NotNil(t, ts.GetScalars().GetTimestamptzData())
					expectedTimestamps := make([]int64, numRows)
					expectedValid := make([]bool, numRows)
					for i, id := range mergedIDs {
						if id <= int64(len(oldIDs)) {
							expectedTimestamps[i] = oldTimestamps[id-1]
							expectedValid[i] = scenario.oldValid[id-1]
						} else if mode != "nullable" {
							expectedTimestamps[i] = defaultTimestamp
							expectedValid[i] = true
						}
					}
					require.Equal(t, expectedTimestamps, ts.GetScalars().GetTimestamptzData().GetData())
					if timestampSchema.GetNullable() {
						require.Equal(t, expectedValid, typeutil.GetFieldDataValidData(ts))
					} else {
						require.Empty(t, typeutil.GetFieldDataValidData(ts))
					}
				})
			}
		}
	}
}
