package queryresource

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func pruningFixture(t *testing.T) (globalOptimizer, *viewpb.QueryViewOfShard) {
	paramtable.Init()
	cfg := paramtable.Get()
	require.NoError(t, cfg.Save(cfg.QueryNodeCfg.EnableSegmentPrune.Key, "true"))
	t.Cleanup(func() { cfg.Reset(cfg.QueryNodeCfg.EnableSegmentPrune.Key) })
	stats := func(lo, hi int64) storage.SegmentStats {
		return storage.SegmentStats{NumRows: 10, FieldStats: []storage.FieldStats{{FieldID: 101, Type: schemapb.DataType_Int64, Min: storage.NewInt64FieldValue(lo), Max: storage.NewInt64FieldValue(hi)}}}
	}
	o := globalOptimizer{schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 101, DataType: schemapb.DataType_Int64, IsClusteringKey: true}}}, partitionStats: map[int64]*storage.PartitionStatsSnapshot{2: {Version: 7, SegmentStats: map[int64]storage.SegmentStats{10: stats(0, 10), 11: stats(100, 110), 99: stats(0, 10)}}}}
	view := &viewpb.QueryViewOfShard{QueryNode: []*viewpb.QueryViewOfQueryNode{
		{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 2, SegmentIds: []int64{10}}}},
		{NodeId: 2, Partitions: []*viewpb.QueryViewOfPartition{{PartitionId: 2, SegmentIds: []int64{11}}}},
	}}
	return o, view
}

func pruningPredicate(t *testing.T, value int64) []byte {
	expr := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{ColumnInfo: &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Int64}, Op: planpb.OpType_Equal, Value: &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: value}}}}}
	data, err := proto.Marshal(&planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{Predicates: expr}}})
	require.NoError(t, err)
	return data
}

func pruningWorkers() []*viewpb.QueryPlanWorkNode {
	return []*viewpb.QueryPlanWorkNode{
		{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 1}}},
		{Node: &viewpb.QueryPlanWorkNode_QueryNode{QueryNode: &viewpb.QueryWorkNode{NodeId: 2}}},
		{Node: &viewpb.QueryPlanWorkNode_StreamingNode{StreamingNode: &viewpb.StreamingWorkNode{Pchannel: "p"}}},
	}
}

func TestGlobalPruningRemovesWorkersButKeepsGrowing(t *testing.T) {
	o, view := pruningFixture(t)
	plan := &viewpb.QueryPlan{WorkNodes: pruningWorkers(), Request: &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{PartitionIDs: []int64{2}, SerializedExprPlan: pruningPredicate(t, 5)}}}
	summary, err := o.OptimizePlan(t.Context(), plan, view)
	require.NoError(t, err)
	require.Equal(t, uint32(1<<viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering), summary.GetFlags())
	require.Len(t, plan.WorkNodes, 2)
	require.Equal(t, int64(1), plan.WorkNodes[0].GetQueryNode().NodeId)
	require.NotNil(t, plan.WorkNodes[1].GetStreamingNode())
	// A newer unrepresented segment cannot be excluded using another segment's stats.
	view.QueryNode[1].Partitions[0].SegmentIds = append(view.QueryNode[1].Partitions[0].SegmentIds, 12)
	plan.WorkNodes = pruningWorkers()
	_, err = o.OptimizePlan(t.Context(), plan, view)
	require.NoError(t, err)
	require.Len(t, plan.WorkNodes, 3)
	accepts := qviews.SegmentSelector(plan.WorkNodes[1].SegmentSelection)
	require.False(t, accepts(11))
	require.True(t, accepts(12))
	require.Len(t, o.partitionStats[2].SegmentStats, 3)
}

func TestGlobalPruningHybridSelectionsRemainIndependent(t *testing.T) {
	o, view := pruningFixture(t)
	plan := &viewpb.QueryPlan{WorkNodes: pruningWorkers(), Request: &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: &internalpb.SearchRequest{IsAdvanced: true, SubReqs: []*internalpb.SubSearchRequest{
		{PartitionIDs: []int64{2}, SerializedExprPlan: pruningPredicate(t, 5)},
		{PartitionIDs: []int64{2}, SerializedExprPlan: pruningPredicate(t, 105)},
	}}}}
	summary, err := o.OptimizePlan(t.Context(), plan, view)
	require.NoError(t, err)
	require.Equal(t, uint32(1<<viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering), summary.GetFlags())
	require.Len(t, plan.WorkNodes, 3)
	for i, id := range []int64{10, 11} {
		worker := plan.WorkNodes[i]
		require.Len(t, worker.SubSelections, 2)
		require.True(t, qviews.SegmentSelector(worker.SubSelections[i])(id))
		require.False(t, qviews.SegmentSelector(worker.SubSelections[1-i])(id))
		require.True(t, qviews.SegmentSelector(worker.SegmentSelection)(id))
	}
}

func TestPartitionSelectionDoesNotReportSegmentPruning(t *testing.T) {
	o, view := pruningFixture(t)
	view.QueryNode = view.QueryNode[:1]
	view.QueryNode[0].Partitions = append(view.QueryNode[0].Partitions,
		&viewpb.QueryViewOfPartition{PartitionId: 3, SegmentIds: []int64{11}})
	plan := &viewpb.QueryPlan{
		WorkNodes: pruningWorkers()[:1],
		Request:   &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{PartitionIDs: []int64{2}}},
	}
	summary, err := o.OptimizePlan(t.Context(), plan, view)
	require.NoError(t, err)
	require.Nil(t, summary)
	require.Len(t, plan.WorkNodes, 1)
	selection := qviews.SegmentSelector(plan.WorkNodes[0].GetSegmentSelection())
	require.True(t, selection(10))
	require.False(t, selection(11))
}

func TestGlobalOptimizerSkipsStatisticsWithoutRecheckingPartitionScope(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("disabled=%t", disabled), func(t *testing.T) {
			o, view := pruningFixture(t)
			if disabled {
				cfg := paramtable.Get()
				require.NoError(t, cfg.Save(cfg.QueryNodeCfg.EnableSegmentPrune.Key, "false"))
			} else {
				o.partitionStats = nil
			}
			view.QueryNode[1].Partitions[0].PartitionId = 3
			plan := &viewpb.QueryPlan{WorkNodes: pruningWorkers(), Request: &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{PartitionIDs: []int64{2}}}}
			summary, err := o.OptimizePlan(t.Context(), plan, view)
			require.NoError(t, err)
			require.Nil(t, summary)
			require.Equal(t, pruningWorkers(), plan.WorkNodes, "diagnostics preserve the existing partition-scoped execution plan")
			for _, partitions := range [][]int64{nil, {2, 3}} {
				plan.WorkNodes = pruningWorkers()
				plan.GetLegacyRetrieveRequest().PartitionIDs = partitions
				summary, err = o.OptimizePlan(t.Context(), plan, view)
				require.NoError(t, err)
				require.Nil(t, summary)
				require.Len(t, plan.WorkNodes, 3)
			}
			for _, request := range []*internalpb.SearchRequest{
				{PartitionIDs: []int64{2}},
				{PartitionIDs: []int64{2, 3}},
				{IsAdvanced: true, SubReqs: []*internalpb.SubSearchRequest{{PartitionIDs: []int64{2}}, {PartitionIDs: []int64{3}}}},
				{IsAdvanced: true, SubReqs: []*internalpb.SubSearchRequest{{PartitionIDs: []int64{2}, Skip: true}, {}}},
			} {
				plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: request}
				before := proto.Clone(plan)
				summary, err = o.OptimizePlan(t.Context(), plan, view)
				require.NoError(t, err)
				require.Nil(t, summary)
				require.True(t, proto.Equal(before, plan), "summary alone must not mutate the execution plan")
			}
		})
	}
}

func TestGlobalOptimizerSummaryReportsActualExclusions(t *testing.T) {
	o, view := pruningFixture(t)
	plan := &viewpb.QueryPlan{WorkNodes: pruningWorkers(), Request: &viewpb.QueryPlan_LegacyRetrieveRequest{LegacyRetrieveRequest: &internalpb.RetrieveRequest{PartitionIDs: []int64{2}}}}
	// Neither available statistics nor a partition list proves an exclusion.
	summary, err := o.OptimizePlan(t.Context(), plan, view)
	require.NoError(t, err)
	require.Nil(t, summary)
	view.QueryNode[0].Partitions = append(view.QueryNode[0].Partitions, &viewpb.QueryViewOfPartition{PartitionId: 3, SegmentIds: []int64{12}})
	plan.GetLegacyRetrieveRequest().SerializedExprPlan = pruningPredicate(t, 5)
	plan.WorkNodes = pruningWorkers()
	summary, err = o.OptimizePlan(t.Context(), plan, view)
	require.NoError(t, err)
	require.Equal(t, uint32(1<<viewpb.GlobalOptimizerExecutionSummary_PrunedByScalarFiltering), summary.GetFlags())
	// An already-empty view contributes no pruning diagnostic.
	view.QueryNode = nil
	plan.WorkNodes = nil
	summary, err = o.OptimizePlan(t.Context(), plan, view)
	require.NoError(t, err)
	require.Nil(t, summary)
}

func TestGlobalOptimizerSummaryVectorPruning(t *testing.T) {
	o, view := pruningFixture(t)
	cfg := paramtable.Get()
	require.NoError(t, cfg.Save(cfg.CommonCfg.EnableVectorClusteringKey.Key, "true"))
	require.NoError(t, cfg.Save(cfg.QueryNodeCfg.DefaultSegmentFilterRatio.Key, "0.5"))
	t.Cleanup(func() {
		cfg.Reset(cfg.CommonCfg.EnableVectorClusteringKey.Key)
		cfg.Reset(cfg.QueryNodeCfg.DefaultSegmentFilterRatio.Key)
	})
	o.schema.Fields[0] = &schemapb.FieldSchema{FieldID: 101, IsClusteringKey: true, DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}
	for id, value := range map[int64]float32{10: 0, 11: 100} {
		o.partitionStats[2].SegmentStats[id] = storage.SegmentStats{NumRows: 100, FieldStats: []storage.FieldStats{{FieldID: 101, Type: schemapb.DataType_FloatVector, Centroids: []storage.VectorFieldValue{&storage.FloatVectorFieldValue{Value: []float32{value, value}}}}}}
	}
	holder, err := proto.Marshal(&commonpb.PlaceholderGroup{Placeholders: []*commonpb.PlaceholderValue{{Type: commonpb.PlaceholderType_FloatVector, Values: [][]byte{make([]byte, 8)}}}})
	require.NoError(t, err)
	plan := &viewpb.QueryPlan{WorkNodes: pruningWorkers(), Request: &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: &internalpb.SearchRequest{FieldId: 101, MetricType: "L2", Topk: 1, PlaceholderGroup: holder}}}
	summary, err := o.OptimizePlan(t.Context(), plan, view)
	require.NoError(t, err)
	require.Equal(t, uint32(1<<viewpb.GlobalOptimizerExecutionSummary_PrunedByVectorFiltering), summary.GetFlags())
	require.Len(t, plan.WorkNodes, 2)
	require.Equal(t, int64(1), plan.WorkNodes[0].GetQueryNode().GetNodeId())
	plan.GetLegacySearchRequest().FieldId = 102
	plan.WorkNodes = pruningWorkers()
	summary, err = o.OptimizePlan(t.Context(), plan, view)
	require.NoError(t, err)
	require.Nil(t, summary)
	require.Len(t, plan.WorkNodes, 3)
}

func TestHybridSelectionsEncodeWithinPartitionScope(t *testing.T) {
	for _, unrestricted := range []bool{false, true} {
		t.Run(fmt.Sprintf("unrestricted=%t", unrestricted), func(t *testing.T) {
			o, view := pruningFixture(t)
			view.QueryNode = []*viewpb.QueryViewOfQueryNode{{NodeId: 1, Partitions: []*viewpb.QueryViewOfPartition{
				{PartitionId: 2, SegmentIds: []int64{10, 11}},
				{PartitionId: 3, SegmentIds: []int64{20, 21}},
				{PartitionId: 4, SegmentIds: []int64{30}},
			}}}
			requests := []*internalpb.SubSearchRequest{
				{PartitionIDs: []int64{3}}, // No pruning: no segment IDs need to be sent.
				{PartitionIDs: []int64{2}, SerializedExprPlan: pruningPredicate(t, 5)},
				{PartitionIDs: []int64{2, 3}}, // Overlapping scope.
				{PartitionIDs: []int64{4}, Skip: true},
				{PartitionIDs: []int64{99}}, // No members on this node: explicit none.
			}
			expected := [][]int64{{20, 21}, {10}, {10, 11, 20, 21}, {}, {}}
			union := []int64{10, 11, 20, 21}
			if unrestricted {
				requests = append(requests, &internalpb.SubSearchRequest{})
				expected = append(expected, []int64{10, 11, 20, 21, 30})
				union = append(union, 30)
			}
			plan := &viewpb.QueryPlan{
				WorkNodes: pruningWorkers()[:1],
				Request: &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: &internalpb.SearchRequest{
					IsAdvanced: true, PartitionIDs: []int64{2, 3, 4, 99}, SubReqs: requests,
				}},
			}
			_, err := o.OptimizePlan(t.Context(), plan, view)
			require.NoError(t, err)
			require.Len(t, plan.WorkNodes, 1)
			worker := plan.WorkNodes[0]
			require.Len(t, worker.SubSelections, len(requests))
			for i, request := range requests {
				scoped := &viewpb.QueryViewOfQueryNode{NodeId: 1}
				for _, partition := range view.QueryNode[0].Partitions {
					if len(request.PartitionIDs) == 0 || slices.Contains(request.PartitionIDs, partition.PartitionId) {
						scoped.Partitions = append(scoped.Partitions, partition)
					}
				}
				selected := qviews.FilterSelectedSegments(scoped, worker.SubSelections[i])
				var actual []int64
				for _, partition := range selected.Partitions {
					actual = append(actual, partition.SegmentIds...)
				}
				require.ElementsMatch(t, expected[i], actual, "slot %d", i)
			}
			require.True(t, worker.SubSelections[0].Exclude)
			require.Empty(t, worker.SubSelections[0].SegmentIds)
			require.False(t, worker.SubSelections[3].Exclude, "skipped slot is none")
			require.Empty(t, worker.SubSelections[3].SegmentIds)
			require.False(t, worker.SubSelections[4].Exclude, "empty local scope is none")
			require.Empty(t, worker.SubSelections[4].SegmentIds)
			// Requery may carry a broader outer scope. Its union must still exclude
			// the skipped subquery's partition unless another active subquery selected it.
			retained := qviews.FilterSelectedSegments(view.QueryNode[0], worker.SegmentSelection)
			var actual []int64
			for _, partition := range retained.Partitions {
				actual = append(actual, partition.SegmentIds...)
			}
			require.ElementsMatch(t, union, actual)
		})
	}
}
