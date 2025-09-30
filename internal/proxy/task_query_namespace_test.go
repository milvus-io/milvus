package proxy

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestQueryTask_PlanNamespace_AfterPreExecute(t *testing.T) {
	mockey.PatchConvey("TestQueryTask_PlanNamespace_AfterPreExecute", t, func() {
		// Setup global meta cache and common mocks
		globalMetaCache = &MetaCache{}
		mockey.Mock((*MetaCache).GetCollectionID).Return(int64(1001), nil).Build()
		mockey.Mock((*MetaCache).GetCollectionInfo).Return(&collectionInfo{updateTimestamp: 12345, consistencyLevel: commonpb.ConsistencyLevel_Strong}, nil).Build()
		mockey.Mock(isPartitionKeyMode).Return(false, nil).Build()
		mockey.Mock(validatePartitionTag).Return(nil).Build()
		mockey.Mock(isIgnoreGrowing).Return(false, nil).Build()

		// Schema with namespace enabled
		mockey.Mock((*MetaCache).GetCollectionSchema).To(func(_ *MetaCache, _ context.Context, _ string, _ string) (*schemaInfo, error) {
			schema := &schemapb.CollectionSchema{
				Name: "test_collection",
				Fields: []*schemapb.FieldSchema{
					{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
					{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
				},
				Properties: []*commonpb.KeyValuePair{{Key: common.NamespaceEnabledKey, Value: "true"}},
			}
			return newSchemaInfo(schema), nil
		}).Build()

		// Capture plan to verify namespace by mocking plan creation inside createPlan
		var capturedPlan *planpb.PlanNode
		mockey.Mock((*queryTask).createPlanArgs).To(func(q *queryTask, ctx context.Context, visitorArgs *planparserv2.ParserVisitorArgs) error {
			capturedPlan = &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}}}
			q.plan = capturedPlan
			q.translatedOutputFields = []string{"id"}
			q.userOutputFields = []string{"id"}
			return nil
		}).Build()

		task := &queryTask{
			Condition:       NewTaskCondition(context.Background()),
			RetrieveRequest: &internalpb.RetrieveRequest{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Retrieve}},
			ctx:             context.Background(),
			request:         &milvuspb.QueryRequest{CollectionName: "test_collection", Expr: "id > 0"},
			result:          &milvuspb.QueryResults{Status: merr.Success()},
		}
		ns := "ns-1"
		task.request.Namespace = &ns
		// Pre-set schema for namespace check before cache fetch inside PreExecute
		task.schema = newSchemaInfo(&schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
			},
			Properties: []*commonpb.KeyValuePair{{Key: common.NamespaceEnabledKey, Value: "true"}},
		})
		// Skip partition name translation to avoid meta fetch
		task.reQuery = true

		err := task.PreExecute(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, capturedPlan)
		assert.NotNil(t, capturedPlan.Namespace)
		assert.Equal(t, *task.request.Namespace, *capturedPlan.Namespace)
	})
}
