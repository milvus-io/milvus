package proxy

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/trace"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestSearchTask_PlanNamespace_AfterPreExecute(t *testing.T) {
	mockey.PatchConvey("TestSearchTask_PlanNamespace_AfterPreExecute", t, func() {
		// Setup global meta cache and common mocks
		globalMetaCache = &MetaCache{}
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()
		mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{updateTimestamp: 12345, consistencyLevel: commonpb.ConsistencyLevel_Strong}, nil).Build()
		mockey.Mock(isPartitionKeyMode).Return(false, nil).Build()
		mockey.Mock(isIgnoreGrowing).Return(false, nil).Build()

		// Schema with namespace enabled and a vector field
		mockey.Mock((*MetaCache).GetCollectionSchema).To(func(_ *MetaCache, _ context.Context, _ string, _ string) (*schemaInfo, error) {
			schema := &schemapb.CollectionSchema{
				Name: "test_collection",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
					{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
				},
				Properties: []*commonpb.KeyValuePair{{Key: common.NamespaceEnabledKey, Value: "true"}},
			}
			return newSchemaInfo(schema), nil
		}).Build()

		// Patch checkNq to bypass placeholder parsing
		mockey.Mock((*searchTask).checkNq).Return(int64(1), nil).Build()

		// Capture plan to verify namespace by mocking tryGeneratePlan
		var capturedPlan *planpb.PlanNode
		mockey.Mock((*searchTask).tryGeneratePlan).To(func(_ *searchTask, _ []*commonpb.KeyValuePair, _ string, _ map[string]*schemapb.TemplateValue) (*planpb.PlanNode, *planpb.QueryInfo, int64, bool, error) {
			capturedPlan = &planpb.PlanNode{}
			qi := &planpb.QueryInfo{Topk: 10, MetricType: "L2", QueryFieldId: 101, GroupByFieldId: -1}
			return capturedPlan, qi, 0, false, nil
		}).Build()

		// Build task
		task := &searchTask{
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

func TestSearchTask_RequeryPlanNamespace(t *testing.T) {
	mockey.PatchConvey("TestSearchTask_RequeryPlanNamespace", t, func() {
		// Minimal searchTask with schema and namespace
		schema := &schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
			},
			Properties: []*commonpb.KeyValuePair{{Key: common.NamespaceEnabledKey, Value: "true"}},
		}
		tsk := &searchTask{
			Condition:     NewTaskCondition(context.Background()),
			ctx:           context.Background(),
			schema:        newSchemaInfo(schema),
			request:       &milvuspb.SearchRequest{CollectionName: "test_collection"},
			SearchRequest: &internalpb.SearchRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Search}},
			node:          &Proxy{},
		}
		ns := "ns-1"
		tsk.request.Namespace = &ns

		// Capture plan created in requery
		var capturedPlan *planpb.PlanNode
		mockey.Mock(planparserv2.CreateRequeryPlan).To(func(_ *schemapb.FieldSchema, _ *schemapb.IDs) *planpb.PlanNode {
			capturedPlan = &planpb.PlanNode{}
			return capturedPlan
		}).Build()

		// Capture qt.request as well to ensure request namespace is wired
		mockey.Mock((*Proxy).query).To(func(_ *Proxy, _ context.Context, qt *queryTask, _ trace.Span) (*milvuspb.QueryResults, segcore.StorageCost, error) {
			if qt.plan == nil || qt.plan.Namespace == nil || *qt.plan.Namespace != *tsk.request.Namespace {
				t.Fatalf("requery plan namespace mismatch, got=%v want=%v", qt.plan.Namespace, *tsk.request.Namespace)
			}
			return &milvuspb.QueryResults{Status: merr.Success()}, segcore.StorageCost{}, nil
		}).Build()

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
