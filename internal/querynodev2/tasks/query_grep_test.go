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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// A Query whose plan carries the grep predicate and a GREP_MATCHES item, run
// through the real QueryNode task on two growing segments: limit-bounded
// evaluation in each segment, the ordinary reduce across them, and the JSON
// column travelling in fields_data under its request-local ID.
func TestQueryTaskGrepPredicateAndMatches(t *testing.T) {
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitLocalChunkManager(t.TempDir())
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	initcore.InitTieredStorage(paramtable.Get())
	require.NoError(t, initcore.InitGrepLimits(paramtable.Get()))
	const (
		textFieldID int64 = 123
		matchesID   int64 = -1000
	)
	ctx := context.Background()
	manager := segments.NewManager()
	textField := &schemapb.FieldSchema{FieldID: textFieldID, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "65535"}}}
	schema := &schemapb.CollectionSchema{Name: "grep", Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		textField,
	}}
	require.NoError(t, manager.Collection.PutOrRef(100, schema, &segcorepb.CollectionIndexMeta{}, &querypb.LoadMetaInfo{LoadType: querypb.LoadType_LoadCollection, CollectionID: 100, PartitionIDs: []int64{10}}))
	collection := manager.Collection.Get(100)
	defer segments.DeleteCollection(collection)
	for i := 0; i < 2; i++ {
		seg, err := segments.NewSegment(ctx, collection, manager.Segment, segments.SegmentTypeGrowing, 0, &querypb.SegmentLoadInfo{SegmentID: int64(100 + i), CollectionID: 100, PartitionID: 10, InsertChannel: "by-dev-rootcoord-dml_0_100v0", Level: datapb.SegmentLevel_Legacy})
		require.NoError(t, err)
		defer seg.Release(ctx)
		pks := []int64{1, 40, 30}
		texts := []string{"skip", "first\ntimeout forty", "timeout thirty"}
		if i == 1 {
			pks = []int64{2, 50, 20}
			texts = []string{"skip", "timeout fifty", "timeout twenty"}
		}
		require.NoError(t, seg.Insert(ctx, []int64{0, 1, 2}, []uint64{1, 1, 1}, &segcorepb.InsertRecord{NumRows: 3, FieldsData: []*schemapb.FieldData{
			{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}}}}},
			{FieldId: textFieldID, Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: texts}}}}},
		}}))
		manager.Segment.Put(ctx, segments.SegmentTypeGrowing, seg)
	}

	type request struct {
		predicate string // grep arguments of the filter; empty for no filter
		item      string // grep arguments of the output item; empty for none
		limit     int64
		segments  []int64
	}
	run := func(r request) (*internalpb.RetrieveResults, error) {
		query := &planpb.QueryPlanNode{Limit: r.limit}
		p := &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: query}, OutputFieldIds: []int64{100, textFieldID, 1}}
		if r.predicate != "" {
			spec, err := planparserv2.NewGrepMatches(textField, r.predicate)
			require.NoError(t, err)
			query.Predicates = planparserv2.AndGrepPredicate(nil, spec.GetSpec())
		}
		if r.item != "" {
			matches, err := planparserv2.NewGrepMatches(textField, r.item)
			require.NoError(t, err)
			p.ProjectionItems = []*planpb.ProjectionItem{{OutputId: matchesID, DataType: schemapb.DataType_JSON, GrepMatches: matches}}
		}
		data, err := proto.Marshal(p)
		require.NoError(t, err)
		req := &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{Base: &commonpb.MsgBase{}, CollectionID: 100, PartitionIDs: []int64{10}, SerializedExprPlan: data, MvccTimestamp: typeutil.MaxTimestamp, Limit: r.limit, OutputFieldsId: []int64{100, textFieldID, 1}}, SegmentIDs: r.segments, Scope: querypb.DataScope_Streaming}
		task := NewQueryTask(ctx, collection, manager, req)
		if err = task.PreExecute(); err != nil {
			task.scheduleSpan.End()
			return nil, err
		}
		err = task.Execute()
		return task.Result(), err
	}
	column := func(t *testing.T, result *internalpb.RetrieveResults, id int64) *schemapb.FieldData {
		t.Helper()
		for _, field := range result.GetFieldsData() {
			if field.GetFieldId() == id {
				return field
			}
		}
		t.Fatalf("column %d is missing", id)
		return nil
	}
	both := []int64{100, 101}

	for _, arrowPath := range []string{"false", "true"} {
		t.Run("transport_arrow="+arrowPath, func(t *testing.T) {
			item := &paramtable.Get().CommonCfg.InterfaceZeroCopyEnabled
			old := item.GetValue()
			defer paramtable.Get().Save(item.Key, old)
			require.NoError(t, paramtable.Get().Save(item.Key, arrowPath))

			for _, ids := range [][]int64{{101}, both} {
				result, err := run(request{predicate: "-e timeout", item: "-e timeout", limit: 1, segments: ids})
				require.NoError(t, err)
				require.Equal(t, []int64{20}, result.GetIds().GetIntId().GetData())
				require.Equal(t, []string{"timeout twenty"}, column(t, result, textFieldID).GetScalars().GetStringData().GetData())
				matches := column(t, result, matchesID)
				require.Equal(t, schemapb.DataType_JSON, matches.GetType())
				require.Equal(t, []bool{true}, matches.GetValidData())
				details := matches.GetScalars().GetJsonData().GetData()
				require.Len(t, details, 1)
				require.JSONEq(t, `[{"type":"match","content":"timeout twenty","line_number":1,"line_start_offset":0,"starts":[0],"ends":[7]}]`, string(details[0]))
			}

			// The Query limit bounds the matches of all segments together.
			for _, tc := range []struct {
				limit int64
				ids   []int64
			}{{2, []int64{20, 30}}, {3, []int64{20, 30, 40}}, {-1, []int64{20, 30, 40, 50}}, {10001, []int64{20, 30, 40, 50}}} {
				result, err := run(request{predicate: "-e timeout", item: "-e timeout -B 1", limit: tc.limit, segments: both})
				require.NoError(t, err)
				require.Equal(t, tc.ids, result.GetIds().GetIntId().GetData())
				matches := column(t, result, matchesID)
				require.Len(t, matches.GetScalars().GetJsonData().GetData(), len(tc.ids))
				for i, pk := range tc.ids {
					require.True(t, matches.GetValidData()[i])
					want := 1
					if pk == 40 {
						want = 2 // one context line before the match
					}
					var lines []map[string]any
					require.NoError(t, json.Unmarshal(matches.GetScalars().GetJsonData().GetData()[i], &lines))
					require.Len(t, lines, want, "pk %d", pk)
				}
			}

			// The item without the predicate: rows without a match are NULL.
			result, err := run(request{item: "-e timeout", limit: -1, segments: both})
			require.NoError(t, err)
			require.Equal(t, []int64{1, 2, 20, 30, 40, 50}, result.GetIds().GetIntId().GetData())
			require.Equal(t, []bool{false, false, true, true, true, true}, column(t, result, matchesID).GetValidData())

			// The predicate without the item.
			result, err = run(request{predicate: "-e timeout", limit: 1, segments: both})
			require.NoError(t, err)
			require.Equal(t, []int64{20}, result.GetIds().GetIntId().GetData())
			for _, field := range result.GetFieldsData() {
				require.NotEqual(t, matchesID, field.GetFieldId())
			}

			// No matching row: the column is present, typed and empty.
			result, err = run(request{predicate: "-e missing", item: "-e timeout", limit: -1, segments: both})
			require.NoError(t, err)
			require.Zero(t, typeutil.GetSizeOfIDs(result.GetIds()))
		})
	}

	t.Run("regex budget", func(t *testing.T) {
		regexItem := &paramtable.Get().FunctionCfg.GrepMaxRegexBytes
		old := regexItem.GetValue()
		defer func() {
			paramtable.Get().Save(regexItem.Key, old)
			require.NoError(t, initcore.InitGrepLimits(paramtable.Get()))
		}()
		require.NoError(t, paramtable.Get().Save(regexItem.Key, "1"))
		require.NoError(t, initcore.InitGrepLimits(paramtable.Get()))
		_, err := run(request{predicate: "-e '[a-z]+'", limit: 1, segments: both})
		require.Error(t, err)
		require.Equal(t, merr.InputError, merr.GetErrorType(err))
		_, err = run(request{item: "-e '[a-z]+'", limit: 1, segments: both})
		require.Error(t, err)
		require.Equal(t, merr.InputError, merr.GetErrorType(err))
	})

	t.Run("result budget", func(t *testing.T) {
		item := &paramtable.Get().QuotaConfig.MaxOutputSize
		old := item.GetValue()
		defer func() {
			paramtable.Get().Save(item.Key, old)
			require.NoError(t, initcore.InitGrepLimits(paramtable.Get()))
		}()
		require.NoError(t, paramtable.Get().Save(item.Key, "64"))
		require.NoError(t, initcore.InitGrepLimits(paramtable.Get()))
		_, err := run(request{predicate: "-e timeout", item: "-e timeout", limit: -1, segments: both})
		require.Error(t, err)
	})
}

// A Query plan that still carries a function chain comes from a proxy that
// predates the rewrite of QueryScan Grep chains; the QueryNode rejects it.
func TestQueryTaskRejectsQueryFunctionChains(t *testing.T) {
	paramtable.Init()
	plan := &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}},
		QuerynodeFunctionChains: []*schemapb.FunctionChain{{
			Stage: schemapb.FunctionChainStage_FunctionChainStageQueryScan,
			Ops:   []*schemapb.FunctionChainOp{{Op: "grep", Inputs: []string{"message"}, Outputs: []string{"matches"}}},
		}},
	}
	run := func(p *planpb.PlanNode) error {
		data, err := proto.Marshal(p)
		require.NoError(t, err)
		req := &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{SerializedExprPlan: data}}
		task := NewQueryTask(context.Background(), &segments.Collection{}, nil, req)
		defer task.scheduleSpan.End()
		return task.PreExecute()
	}
	require.ErrorIs(t, run(plan), merr.ErrParameterInvalid)
	plan.QuerynodeFunctionChains = nil
	require.NoError(t, run(plan))
}
