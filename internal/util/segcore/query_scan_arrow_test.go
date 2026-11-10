// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package segcore_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryScanArrowTransport(t *testing.T) {
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitLocalChunkManager(filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole))
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	initcore.InitTieredStorage(paramtable.Get())
	for _, stringPK := range []bool{false, true} {
		t.Run(fmt.Sprint(stringPK), func(t *testing.T) {
			ctx := context.Background()
			pkType := schemapb.DataType_Int64
			if stringPK {
				pkType = schemapb.DataType_VarChar
			}
			schema := &schemapb.CollectionSchema{Name: "identity_arrow", Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: pkType, IsPrimaryKey: true, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "65535"}}}}}
			collection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{CollectionID: 123, Schema: schema})
			require.NoError(t, err)
			defer collection.Release()
			segment, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{Collection: collection, SegmentID: 123, SegmentType: segcore.SegmentTypeGrowing})
			require.NoError(t, err)
			defer segment.Release()
			expr, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Limit: 1}}, OutputFieldIds: []int64{100, 1}})
			require.NoError(t, err)
			plan, err := segcore.NewRetrievePlan(collection, expr, typeutil.MaxTimestamp, 0, 0, 0, 0)
			require.NoError(t, err)
			defer plan.Delete()
			require.NoError(t, plan.EnableCandidateMode(0))
			empty, err := segment.RetrieveAsArrow(ctx, plan)
			require.NoError(t, err)
			header, record, err := empty.GetResult()
			require.NoError(t, err)
			empty.Release()
			require.Empty(t, header.Offset)
			require.Zero(t, record.NumRows())
			record.Release()
			fd := &schemapb.FieldData{FieldId: 100, Type: pkType}
			if stringPK {
				fd.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"z", "a", "m"}}}}}
			} else {
				fd.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{30, 10, 20}}}}}
			}
			_, err = segment.Insert(ctx, &segcore.InsertRequest{RowIDs: []int64{0, 1, 2}, Timestamps: []uint64{1, 2, 3}, Record: &segcorepb.InsertRecord{NumRows: 3, FieldsData: []*schemapb.FieldData{fd}}})
			require.NoError(t, err)
			for _, cap := range []int64{0, 2} {
				require.NoError(t, plan.EnableCandidateMode(cap))
				ref, err := segment.Retrieve(ctx, plan)
				require.NoError(t, err)
				expected, err := ref.GetResult()
				require.NoError(t, err)
				ref.Release()
				result, err := segment.RetrieveAsArrow(ctx, plan)
				require.NoError(t, err)
				header, record, err := result.GetResult()
				require.NoError(t, err)
				_, _, err = result.GetResult()
				require.Error(t, err)
				result.Release()
				require.Empty(t, header.Offset)
				require.Empty(t, header.FieldsData)
				require.Nil(t, header.Ids)
				require.Equal(t, expected.AllRetrieveCount, header.AllRetrieveCount)
				require.Equal(t, expected.HasMoreResult, header.HasMoreResult)
				require.EqualValues(t, 1, record.NumCols())
				require.Equal(t, "$seg_offset", record.Schema().Field(0).Name)
				require.Equal(t, expected.Offset, record.Column(0).(*array.Int64).Int64Values())
				record.Release()
			}
			offsets := []int64{2, 0}
			for _, allowed := range []bool{false, true} {
				result, err := segcore.RetrieveIdentitiesByOffsets(ctx, segment, &segcore.RetrievePlanWithOffsets{RetrievePlan: plan, Offsets: offsets}, allowed)
				require.NoError(t, err)
				header, record, err := result.GetResult()
				require.NoError(t, err)
				result.Release() // Imported columns must outlive the native wrapper.
				require.Equal(t, "$id", record.Schema().Field(1).Name)
				require.Empty(t, header.Offset)
				require.Empty(t, header.FieldsData)
				require.Nil(t, header.Ids)
				require.Equal(t, offsets, record.Column(0).(*array.Int64).Int64Values())
				if stringPK {
					require.Equal(t, "m", record.Column(1).(*array.String).Value(0))
					require.Equal(t, "z", record.Column(1).(*array.String).Value(1))
				} else {
					require.Equal(t, []int64{20, 30}, record.Column(1).(*array.Int64).Int64Values())
				}
				require.Equal(t, []int64{3, 1}, record.Column(2).(*array.Int64).Int64Values())
				record.Release()
			}
			t.Run("materialization budget propagates as input error", func(t *testing.T) {
				record, err := segcore.FillRetrieveFieldsOrdered(ctx, []segcore.CSegment{segment}, plan, []int32{0}, []int64{0}, 0)
				require.Nil(t, record)
				require.Error(t, err)
				require.Equal(t, merr.InputError, merr.GetErrorType(err))
				require.Contains(t, err.Error(), "remaining output byte budget")
				require.Equal(t, merr.InputError, merr.GetErrorType(merr.Error(merr.Status(err))))
				record, err = segcore.FillRetrieveFieldsOrdered(ctx, []segcore.CSegment{segment}, plan, []int32{0}, []int64{0}, 1048576)
				require.NoError(t, err)
				require.EqualValues(t, 1, record.NumRows())
				record.Release()
			})
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			result, err := segcore.RetrieveIdentitiesByOffsets(canceled, segment, &segcore.RetrievePlanWithOffsets{RetrievePlan: plan, Offsets: offsets}, true)
			// Async cancellation races a short native read. Preserve that
			// contract: either no result, or a valid result we must release.
			if err != nil {
				require.Nil(t, result)
			} else {
				_, record, importErr := result.GetResult()
				result.Release()
				require.NoError(t, importErr)
				require.Equal(t, offsets, record.Column(0).(*array.Int64).Int64Values())
				record.Release()
			}
		})
	}
}
