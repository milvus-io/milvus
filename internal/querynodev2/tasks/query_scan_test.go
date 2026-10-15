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

package tasks

import (
	"context"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryTaskRealQueryScan(t *testing.T)  { testRealQueryTask(t, true) }
func TestQueryTaskRealPlainQuery(t *testing.T) { testRealQueryTask(t, false) }

func testRealQueryTask(t *testing.T, enableScan bool) {
	t.Helper()
	paramtable.Init()
	initcore.InitExecExpressionFunctionFactory()
	initcore.InitLocalChunkManager(t.TempDir())
	require.NoError(t, initcore.InitMmapManager(paramtable.Get(), 1))
	initcore.InitTieredStorage(paramtable.Get())
	const textFieldID int64 = 123 // Exercise the field ID resolved by Query preparation.
	ctx := context.Background()
	manager := segments.NewManager()
	schema := &schemapb.CollectionSchema{Name: "grep", Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: textFieldID, Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "65535"}}},
	}}
	require.NoError(t, manager.Collection.PutOrRef(100, schema, &segcorepb.CollectionIndexMeta{}, &querypb.LoadMetaInfo{LoadType: querypb.LoadType_LoadCollection, CollectionID: 100, PartitionIDs: []int64{10}}))
	collection := manager.Collection.Get(100)
	defer segments.DeleteCollection(collection)
	for i := 0; i < 2; i++ {
		seg, err := segments.NewSegment(ctx, collection, manager.Segment, segments.SegmentTypeGrowing, 0, &querypb.SegmentLoadInfo{SegmentID: int64(100 + i), CollectionID: 100, PartitionID: 10, InsertChannel: "by-dev-rootcoord-dml_0_100v0", Level: datapb.SegmentLevel_Legacy})
		require.NoError(t, err)
		defer seg.Release(ctx)
		pks := []int64{1, 40, 30}
		texts := []string{"skip", "timeout forty", "timeout thirty"}
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
	segmentIDs := []int64{100, 101}
	chainName := "scan"
	run := func(arguments string, withChain bool) (*internalpb.RetrieveResults, error) {
		query := &planpb.QueryPlanNode{Limit: 1}
		p := &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: query}, OutputFieldIds: []int64{100, textFieldID, 1}}
		if withChain {
			p.QuerynodeFunctionChains = []*schemapb.FunctionChain{{Name: chainName, Stage: schemapb.FunctionChainStage_FunctionChainStageQueryScan, OutputColumns: []string{"matches"}, Ops: []*schemapb.FunctionChainOp{{Op: "grep", Inputs: []string{"text"}, Outputs: []string{"matches"}, Params: map[string]*schemapb.FunctionParamValue{
				"arguments": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: arguments}}, "limit": {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 2}},
			}}}}}
		}
		data, err := proto.Marshal(p)
		require.NoError(t, err)
		req := &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{Base: &commonpb.MsgBase{}, CollectionID: 100, PartitionIDs: []int64{10}, SerializedExprPlan: data, MvccTimestamp: typeutil.MaxTimestamp, Limit: 1, OutputFieldsId: []int64{100, textFieldID, 1}}, SegmentIDs: segmentIDs, Scope: querypb.DataScope_Streaming}
		task := NewQueryTask(ctx, collection, manager, req)
		if err = task.PreExecute(); err != nil {
			task.scheduleSpan.End()
			return nil, err
		}
		err = task.Execute()
		return task.Result(), err
	}
	assertClientStatus := func(t *testing.T, err error, code int32, fragments ...string) {
		t.Helper()
		require.Error(t, err)
		worker := merr.Status(err)
		leader := merr.Status(merr.Wrap(merr.Error(worker), "worker query failed"))
		client := merr.Status(merr.Wrap(merr.Error(leader), "failed to query"))
		require.Equal(t, code, client.GetCode())
		require.Equal(t, worker.GetExtraInfo(), client.GetExtraInfo())
		require.Equal(t, worker.GetRetriable(), client.GetRetriable())
		for _, fragment := range fragments {
			require.Contains(t, client.GetReason(), fragment)
		}
	}
	assertText := func(t *testing.T, result *internalpb.RetrieveResults, want string) {
		t.Helper()
		for _, field := range result.GetFieldsData() {
			if field.GetFieldId() == textFieldID {
				require.Equal(t, []string{want}, field.GetScalars().GetStringData().GetData())
				return
			}
		}
		t.Fatal("text output field is missing")
	}
	if !enableScan {
		for _, arrowPath := range []string{"false", "true"} {
			t.Run("transport_arrow="+arrowPath, func(t *testing.T) {
				item := &paramtable.Get().CommonCfg.InterfaceZeroCopyEnabled
				old := item.GetValue()
				defer paramtable.Get().Save(item.Key, old)
				require.NoError(t, paramtable.Get().Save(item.Key, arrowPath))
				for _, tc := range []struct {
					name string
					ids  []int64
				}{
					{"single_segment", []int64{100}},
					{"multiple_segments", []int64{100, 101}},
				} {
					t.Run(tc.name, func(t *testing.T) {
						segmentIDs = tc.ids
						result, err := run("", false)
						require.NoError(t, err)
						require.Equal(t, []int64{1}, result.GetIds().GetIntId().GetData())
						require.Nil(t, result.ExtraResults)
						assertText(t, result, "skip")
					})
				}
			})
		}
		return
	}

	for _, arrowPath := range []string{"false", "true"} {
		t.Run("materialize_arrow="+arrowPath, func(t *testing.T) {
			item := &paramtable.Get().CommonCfg.InterfaceZeroCopyEnabled
			old := item.GetValue()
			defer paramtable.Get().Save(item.Key, old)
			require.NoError(t, paramtable.Get().Save(item.Key, arrowPath))
			result, err := run("-e timeout", true)
			require.NoError(t, err)
			require.Equal(t, []int64{20}, result.GetIds().GetIntId().GetData())
			assertText(t, result, "timeout twenty")
			delegated, err := segments.RunDelegatorQueryPipeline(ctx, &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{Limit: 1, OutputFieldsId: []int64{100, textFieldID, 1}}}, schema, []*internalpb.RetrieveResults{result})
			require.NoError(t, err)
			require.True(t, proto.Equal(result.ExtraResults, delegated.ExtraResults))
			details := result.GetExtraResults().GetFields()["scan"].GetObjectValue().GetFields()["matches"].GetListValue().GetValues()
			require.Len(t, details, 1)
			require.Equal(t, "timeout twenty", details[0].GetListValue().GetValues()[0].GetObjectValue().GetFields()["content"].GetStringValue())
		})
	}
	ordinary, err := run("", false)
	require.NoError(t, err)
	require.Equal(t, []int64{1}, ordinary.GetIds().GetIntId().GetData())
	require.Nil(t, ordinary.ExtraResults)
	for _, tc := range []struct {
		threshold string
		allowed   bool
	}{
		{"1", false}, // Query limit is 1, but each segment has two Grep matches.
		{"2", true},
		{"0", true}, // No take row-count limit.
	} {
		t.Run("identity take threshold="+tc.threshold, func(t *testing.T) {
			item := &paramtable.Get().QueryNodeCfg.TakeForOutputResultCountLimit
			previous := item.GetValue()
			require.NoError(t, paramtable.Get().Save(item.Key, tc.threshold))
			defer paramtable.Get().Save(item.Key, previous)
			type decision struct {
				rows    int
				allowed bool
			}
			decisions := make(chan decision, 2)
			var original func(context.Context, segcore.CSegment, *segcore.RetrievePlanWithOffsets, bool) (*segcore.RetrieveResult, error)
			patch := mockey.Mock(segcore.RetrieveIdentitiesByOffsets).To(
				func(ctx context.Context, segment segcore.CSegment, plan *segcore.RetrievePlanWithOffsets, allowed bool) (*segcore.RetrieveResult, error) {
					decisions <- decision{len(plan.Offsets), allowed}
					return original(ctx, segment, plan, allowed)
				}).Origin(&original).Build()
			defer patch.UnPatch()
			result, err := run("-e timeout", true)
			require.NoError(t, err)
			require.Equal(t, []int64{20}, result.GetIds().GetIntId().GetData())
			require.Len(t, decisions, 2)
			for i := 0; i < 2; i++ {
				d := <-decisions
				require.Equal(t, 2, d.rows)
				require.Equal(t, tc.allowed, d.allowed)
			}
		})
	}
	segmentIDs = []int64{100}
	t.Run("configurable grep preparation", func(t *testing.T) {
		argumentItem := &paramtable.Get().FunctionCfg.GrepMaxArgumentBytes
		regexItem := &paramtable.Get().FunctionCfg.GrepMaxRegexBytes
		oldArguments, oldRegex := argumentItem.GetValue(), regexItem.GetValue()
		defer paramtable.Get().Save(argumentItem.Key, oldArguments)
		defer paramtable.Get().Save(regexItem.Key, oldRegex)
		require.NoError(t, paramtable.Get().Save(argumentItem.Key, "65536"))
		result, err := run(strings.Repeat("-e timeout ", 600), true)
		require.NoError(t, err)
		require.Equal(t, []int64{30}, result.GetIds().GetIntId().GetData())
		require.NoError(t, paramtable.Get().Save(argumentItem.Key, "4"))
		_, err = run("-e timeout", true)
		require.ErrorIs(t, err, merr.ErrParameterTooLarge)
		assertClientStatus(t, err, merr.Code(merr.ErrParameterTooLarge), "function.grep.maxArgumentBytes", "actual=10 bytes", "limit=4 bytes")
		require.NoError(t, paramtable.Get().Save(argumentItem.Key, "65536"))
		require.NoError(t, paramtable.Get().Save(regexItem.Key, "1"))
		_, err = run("-e '[a-z]+'", true)
		require.Error(t, err)
		require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		assertClientStatus(t, err, int32(merr.CodeUnexpectedError), "function.grep.maxRegexBytes=1 bytes", "function.grep.maxDFACacheBytes=", "compiled regex exceeds size limit of 1")
		require.NoError(t, paramtable.Get().Save(regexItem.Key, oldRegex))
		result, err = run("-e timeout", true)
		require.NoError(t, err)
		require.Equal(t, []int64{30}, result.GetIds().GetIntId().GetData())
	})
	t.Run("grep result budget diagnostics", func(t *testing.T) {
		item := &paramtable.Get().QuotaConfig.MaxOutputSize
		previous := item.GetValue()
		defer paramtable.Get().Save(item.Key, previous)
		require.NoError(t, paramtable.Get().Save(item.Key, "128"))
		_, err := run("-e timeout", true)
		assertClientStatus(t, err, int32(merr.CodeOutOfRange), "quotaAndLimits.limits.maxOutputSize=128 bytes", "resource limit exceeded: ResultBytes")

		// An empty match set fits the native budget, but its exported chain
		// name makes the final protobuf exceed the same result-size ceiling.
		require.NoError(t, paramtable.Get().Save(item.Key, "512"))
		chainName = strings.Repeat("s", 1024)
		defer func() { chainName = "scan" }()
		_, err = run("-e missing", true)
		assertClientStatus(t, err, merr.Code(merr.ErrParameterTooLarge), "quotaAndLimits.limits.maxOutputSize", "actual=", "limit=512 bytes")
	})
	single, err := run("-e timeout", true)
	require.NoError(t, err)
	require.Equal(t, []int64{30}, single.GetIds().GetIntId().GetData())
	segmentIDs = []int64{100, 101}
	empty, err := run("-e missing", true)
	require.NoError(t, err)
	require.Zero(t, typeutil.GetSizeOfIDs(empty.GetIds()))
	require.NotNil(t, empty.ExtraResults.GetFields()["scan"].GetObjectValue().GetFields()["matches"].GetListValue())
	_, err = run("-e '['", true)
	require.Error(t, err)
	// Failed execution must release pins and leave subsequent requests usable.
	_, err = run("-e timeout", true)
	require.NoError(t, err)
	capItem := &paramtable.Get().FunctionCfg.GrepMaxScanRows
	oldCap := capItem.GetValue()
	defer paramtable.Get().Save(capItem.Key, oldCap)
	require.NoError(t, paramtable.Get().Save(capItem.Key, "1"))
	capped, err := run("-e timeout", true)
	require.NoError(t, err)
	require.Zero(t, typeutil.GetSizeOfIDs(capped.GetIds()))
	require.True(t, capped.GetHasMoreResult())
	for _, value := range []string{"bad", "-1", "9223372036854775808", ""} {
		t.Run("invalid maxScanRows="+value, func(t *testing.T) {
			require.NoError(t, paramtable.Get().Save(capItem.Key, value))
			result, err := run("-e timeout", true)
			require.NoError(t, err)
			require.Equal(t, []int64{20}, result.GetIds().GetIntId().GetData())
		})
	}
	require.NoError(t, paramtable.Get().Save(capItem.Key, oldCap))
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	ctx = cancelled
	_, err = run("-e timeout", true)
	require.ErrorIs(t, err, context.Canceled)

}
