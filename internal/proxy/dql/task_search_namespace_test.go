package dql

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSearchTask_PlanNamespace_AfterPreExecute(t *testing.T) {
	mockey.PatchConvey("TestSearchTask_PlanNamespace_AfterPreExecute", t, func() {
		cache := newTestCache()
		schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
			},
			EnableNamespace: true,
		})
		mockTest(t, (*metacache.MetaCache).GetCollectionID, int64(1001), nil)
		mockTest(t, (*metacache.MetaCache).GetCollectionInfo, &collectionInfo{Schema: schema, UpdateTimestamp: 12345, ConsistencyLevel: commonpb.ConsistencyLevel_Strong}, nil)
		mockTest(t, isPartitionKeyMode, false, nil)
		mockTest(t, isIgnoreGrowing, false, nil)

		// Schema with namespace enabled and a vector field
		mockTest(t, (*metacache.MetaCache).GetCollectionSchema, schema, nil)

		// Patch checkNq to bypass placeholder parsing
		mockTest(t, (*SearchTask).checkNq, int64(1), nil)

		// Capture plan to verify namespace by mocking tryGeneratePlan
		var capturedPlan *planpb.PlanNode
		mockTestTo(t, (*SearchTask).tryGeneratePlan, func(_ *SearchTask, _ []*commonpb.KeyValuePair, _ string, _ map[string]*schemapb.TemplateValue, _ *planparserv2.MembershipPreflightBudget, _ bool) (*planpb.PlanNode, *planpb.QueryInfo, int64, bool, []OrderByField, internalpb.SearchType, error) {
			capturedPlan = &planpb.PlanNode{}
			qi := &planpb.QueryInfo{Topk: 10, MetricType: "L2", QueryFieldId: 101, GroupByFieldId: -1}
			return capturedPlan, qi, 0, false, nil, internalpb.SearchType_DEFAULT, nil
		})

		// Build task
		task := &SearchTask{
			baseTask:      baseTask{MetaCache: cache},
			Condition:     NewTaskCondition(context.Background()),
			SearchRequest: &internalpb.SearchRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Search}},
			ctx:           context.Background(),
			request:       &milvuspb.SearchRequest{CollectionName: "test_collection"},
			result:        &milvuspb.SearchResults{Status: merr.Success()},
		}
		ns := "ns-1"
		task.request.Namespace = &ns

		err := task.PreExecute(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, capturedPlan)
		assert.NotNil(t, capturedPlan.Namespace)
		assert.Equal(t, *task.request.Namespace, *capturedPlan.Namespace)
	})
}

func TestSearchTask_NamespaceSetsPartitionIDs(t *testing.T) {
	mockey.PatchConvey("TestSearchTask_NamespaceSetsPartitionIDs", t, func() {
		cache := newTestCache()

		partitionNames := []string{"_default_0", "_default_1"}
		partitionIDs := map[string]int64{"_default_0": 101, "_default_1": 102}
		namespaces := []string{"ns-0", "ns-1", "ns-2", "ns-3", "ns-4", "ns-5", "ns-6", "ns-7"}
		schema := namespaceEnabledSchema(
			&schemapb.FieldSchema{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			&schemapb.FieldSchema{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
			&schemapb.FieldSchema{FieldID: 102, Name: "value", DataType: schemapb.DataType_Int64},
		)

		mockTest(t, (*metacache.MetaCache).GetCollectionID, int64(1001), nil)
		schemaInfo := mustNewSchemaInfo(schema)
		mockTest(t, (*metacache.MetaCache).GetCollectionInfo, &collectionInfo{Schema: schemaInfo, UpdateTimestamp: 12345, ConsistencyLevel: commonpb.ConsistencyLevel_Strong}, nil)
		mockTest(t, (*metacache.MetaCache).GetCollectionSchema, schemaInfo, nil)
		mockTest(t, (*metacache.MetaCache).GetPartitionsIndex, partitionNames, nil)
		mockTest(t, (*metacache.MetaCache).GetPartitions, partitionIDs, nil)
		mockTest(t, isIgnoreGrowing, false, nil)
		mockTest(t, (*SearchTask).checkNq, int64(1), nil)
		mockTestTo(t, (*SearchTask).tryGeneratePlan, func(_ *SearchTask, _ []*commonpb.KeyValuePair, _ string, _ map[string]*schemapb.TemplateValue, _ *planparserv2.MembershipPreflightBudget, _ bool) (*planpb.PlanNode, *planpb.QueryInfo, int64, bool, []OrderByField, internalpb.SearchType, error) {
			plan := &planpb.PlanNode{
				Node: &planpb.PlanNode_VectorAnns{
					VectorAnns: &planpb.VectorANNS{
						Predicates: nonPartitionKeyPredicate(102, schemapb.DataType_Int64),
					},
				},
			}
			qi := &planpb.QueryInfo{Topk: 10, MetricType: "L2", QueryFieldId: 101, GroupByFieldId: -1}
			return plan, qi, 0, false, nil, internalpb.SearchType_DEFAULT, nil
		})

		for _, ns := range namespaces {
			namespace := ns
			task := &SearchTask{
				baseTask:      baseTask{MetaCache: cache},
				Condition:     NewTaskCondition(context.Background()),
				SearchRequest: &internalpb.SearchRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Search}},
				ctx:           context.Background(),
				request:       &milvuspb.SearchRequest{CollectionName: "test_collection", Namespace: &namespace},
				result:        &milvuspb.SearchResults{Status: merr.Success()},
			}

			err := task.PreExecute(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, []int64{expectedNamespacePartitionID(namespace, partitionNames, partitionIDs)}, task.GetPartitionIDs())
		}
	})
}

func TestSearchTask_RequeryPlanNamespace(t *testing.T) {
	mockey.PatchConvey("TestSearchTask_RequeryPlanNamespace", t, func() {
		// Minimal SearchTask with schema and namespace
		schema := &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
			},
			EnableNamespace: true,
		}
		tsk := &SearchTask{
			Condition:     NewTaskCondition(context.Background()),
			ctx:           context.Background(),
			schema:        mustNewSchemaInfo(schema),
			request:       &milvuspb.SearchRequest{CollectionName: "test_collection"},
			SearchRequest: &internalpb.SearchRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Search}},
			node:          &namespaceRequeryMockNode{},
		}
		ns := "ns-1"
		tsk.request.Namespace = &ns

		// Capture plan created in requery
		var capturedPlan *planpb.PlanNode
		mockTestTo(t, planparserv2.CreateRequeryPlan, func(_ *schemapb.FieldSchema, _ *schemapb.IDs) *planpb.PlanNode {
			capturedPlan = &planpb.PlanNode{}
			return capturedPlan
		})

		// Capture qt.plan to ensure request namespace is wired
		mockTestTo(t, (*namespaceRequeryMockNode).ExecuteQuery, func(_ *namespaceRequeryMockNode, _ context.Context, qt taskmodel.Task, _ trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
			queryTask := qt.(*QueryTask)
			if queryTask.plan == nil || queryTask.plan.Namespace == nil || *queryTask.plan.Namespace != *tsk.request.Namespace {
				t.Fatalf("requery plan namespace mismatch, got=%v want=%v", queryTask.plan.Namespace, *tsk.request.Namespace)
			}
			return &milvuspb.QueryResults{Status: merr.Success()}, segcore.StorageCost{}, nil
		})

		op, err := newRequeryOperator(tsk, nil)
		assert.NoError(t, err)

		ids := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}}
		_, runErr := op.run(context.Background(), nil, ids, segcore.StorageCost{})
		assert.NoError(t, runErr)
		assert.NotNil(t, capturedPlan)
		assert.NotNil(t, capturedPlan.Namespace)
		assert.Equal(t, *tsk.request.Namespace, *capturedPlan.Namespace)
	})
}

// namespaceRequeryMockNode is a taskmodel.TaskNode + QueryRunner stub used to
// capture the requery plan namespace in tests.
type namespaceRequeryMockNode struct{}

func (n *namespaceRequeryMockNode) GetMetaCache() metacache.Cache        { return newTestCache() }
func (n *namespaceRequeryMockNode) MixCoord() types.MixCoordClient       { return nil }
func (n *namespaceRequeryMockNode) LBPolicy() shardclient.LBPolicy       { return nil }
func (n *namespaceRequeryMockNode) ShardMgr() shardclient.ShardClientMgr { return nil }
func (n *namespaceRequeryMockNode) ChMgr() channelmgr.ChannelsMgr        { return nil }
func (n *namespaceRequeryMockNode) TsoAllocator() taskmodel.TsoAllocator { return &mockTsoAllocator{} }
func (n *namespaceRequeryMockNode) ResolveRLSEnforcement(_ context.Context, _ metacache.Cache, rlsEnabled, _, _ bool, _, _, _ string) (bool, error) {
	return rlsEnabled, nil
}
func (n *namespaceRequeryMockNode) ExecuteQuery(ctx context.Context, qt taskmodel.Task, sp trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
	panic("ExecuteQuery must be patched by mockey")
}

var (
	_ taskmodel.TaskNode    = (*namespaceRequeryMockNode)(nil)
	_ taskmodel.QueryRunner = (*namespaceRequeryMockNode)(nil)
)
