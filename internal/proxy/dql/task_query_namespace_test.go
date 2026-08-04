package dql

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func namespaceEnabledSchema(fields ...*schemapb.FieldSchema) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: append(fields, &schemapb.FieldSchema{
			FieldID:        999,
			Name:           common.NamespaceFieldName,
			IsPartitionKey: true,
			DataType:       schemapb.DataType_VarChar,
		}),
		EnableNamespace: true,
	}
}

func nonPartitionKeyPredicate(fieldID int64, dataType schemapb.DataType) *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_UnaryRangeExpr{
			UnaryRangeExpr: &planpb.UnaryRangeExpr{
				ColumnInfo: &planpb.ColumnInfo{
					FieldId:  fieldID,
					DataType: dataType,
				},
				Op:    planpb.OpType_GreaterThan,
				Value: &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 0}},
			},
		},
	}
}

func expectedNamespacePartitionID(namespace string, partitionNames []string, partitionIDs map[string]int64) int64 {
	idx := typeutil.HashString2Uint32(namespace) % uint32(len(partitionNames))
	return partitionIDs[partitionNames[idx]]
}

func TestQueryTask_PlanNamespace_AfterPreExecute(t *testing.T) {
	mockey.PatchConvey("TestQueryTask_PlanNamespace_AfterPreExecute", t, func() {
		cache := newTestCache()
		schema := mustNewSchemaInfo(&schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
			},
			EnableNamespace: true,
		})
		mockTest(t, (*metacache.MetaCache).GetCollectionID, int64(1001), nil)
		mockTest(t, (*metacache.MetaCache).GetCollectionInfo, &collectionInfo{Schema: schema, UpdateTimestamp: 12345, ConsistencyLevel: commonpb.ConsistencyLevel_Strong}, nil)
		mockTest(t, isPartitionKeyMode, false, nil)
		mockTest(t, validatePartitionTag, nil)
		mockTest(t, isIgnoreGrowing, false, nil)

		// Schema with namespace enabled
		mockTest(t, (*metacache.MetaCache).GetCollectionSchema, schema, nil)

		// Capture plan to verify namespace by mocking plan creation inside createPlan
		var capturedPlan *planpb.PlanNode
		mockTestTo(t, (*QueryTask).createPlanArgs, func(q *QueryTask, ctx context.Context, visitorArgs *planparserv2.ParserVisitorArgs) error {
			capturedPlan = &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}}}
			q.plan = capturedPlan
			q.translatedOutputFields = []string{"id"}
			q.userOutputFields = []string{"id"}
			return nil
		})

		task := &QueryTask{
			baseTask:        baseTask{MetaCache: cache},
			Condition:       NewTaskCondition(context.Background()),
			RetrieveRequest: &internalpb.RetrieveRequest{QueryLabel: "query", Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Retrieve}},
			ctx:             context.Background(),
			request:         &milvuspb.QueryRequest{CollectionName: "test_collection", Expr: "id > 0"},
			result:          &milvuspb.QueryResults{Status: merr.Success()},
		}
		ns := "ns-1"
		task.request.Namespace = &ns
		// Pre-set schema for namespace check before cache fetch inside PreExecute
		task.schema = mustNewSchemaInfo(&schemapb.CollectionSchema{
			Name: "test_collection",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int32},
			},
			EnableNamespace: true,
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

func TestQueryTask_NamespaceSetsPartitionIDs(t *testing.T) {
	mockey.PatchConvey("TestQueryTask_NamespaceSetsPartitionIDs", t, func() {
		cache := newTestCache()

		partitionNames := []string{"_default_0", "_default_1"}
		partitionIDs := map[string]int64{"_default_0": 101, "_default_1": 102}
		namespaces := []string{"ns-0", "ns-1", "ns-2", "ns-3", "ns-4", "ns-5", "ns-6", "ns-7"}
		schema := namespaceEnabledSchema(
			&schemapb.FieldSchema{FieldID: 100, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			&schemapb.FieldSchema{FieldID: 101, Name: "value", DataType: schemapb.DataType_Int64},
		)

		mockTest(t, (*metacache.MetaCache).GetCollectionID, int64(1001), nil)
		schemaInfo := mustNewSchemaInfo(schema)
		mockTest(t, (*metacache.MetaCache).GetCollectionInfo, &collectionInfo{Schema: schemaInfo, UpdateTimestamp: 12345, ConsistencyLevel: commonpb.ConsistencyLevel_Strong}, nil)
		mockTest(t, (*metacache.MetaCache).GetCollectionSchema, schemaInfo, nil)
		mockTest(t, (*metacache.MetaCache).GetPartitionsIndex, partitionNames, nil)
		mockTest(t, (*metacache.MetaCache).GetPartitions, partitionIDs, nil)
		mockTest(t, validatePartitionTag, nil)
		mockTest(t, isIgnoreGrowing, false, nil)

		mockTestTo(t, (*QueryTask).createPlanArgs, func(q *QueryTask, ctx context.Context, visitorArgs *planparserv2.ParserVisitorArgs) error {
			q.plan = &planpb.PlanNode{
				Node: &planpb.PlanNode_Query{
					Query: &planpb.QueryPlanNode{
						Predicates: nonPartitionKeyPredicate(101, schemapb.DataType_Int64),
					},
				},
			}
			q.translatedOutputFields = []string{"id"}
			q.userOutputFields = []string{"id"}
			return nil
		})

		for _, ns := range namespaces {
			namespace := ns
			task := &QueryTask{
				baseTask:        baseTask{MetaCache: cache},
				Condition:       NewTaskCondition(context.Background()),
				RetrieveRequest: &internalpb.RetrieveRequest{QueryLabel: "query", Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_Retrieve}},
				ctx:             context.Background(),
				request:         &milvuspb.QueryRequest{CollectionName: "test_collection", Expr: "value > 0", Namespace: &namespace},
				result:          &milvuspb.QueryResults{Status: merr.Success()},
			}

			err := task.PreExecute(context.Background())
			assert.NoError(t, err)
			assert.Equal(t, []int64{expectedNamespacePartitionID(namespace, partitionNames, partitionIDs)}, task.GetPartitionIDs())
		}
	})
}
