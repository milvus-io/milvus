package proxy

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/internal/proxy/shardclient"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/streamrpc"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func Test_getPrimaryKeysFromPlan(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{
		Name:        "test_delete",
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      common.StartOfUserFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      common.StartOfUserFieldID + 1,
				Name:         "non_pk",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
		},
	}
	schema, err := typeutil.CreateSchemaHelper(collSchema)
	require.NoError(t, err)

	t.Run("delete with complex pk expr", func(t *testing.T) {
		expr := "pk < 4"
		plan, err := planparserv2.CreateRetrievePlan(schema, expr, nil)
		assert.NoError(t, err)
		isSimple, _, _ := getPrimaryKeysFromPlan(collSchema, plan)
		assert.False(t, isSimple)
	})

	t.Run("delete with no-pk field expr", func(t *testing.T) {
		expr := "non_pk == 1"
		plan, err := planparserv2.CreateRetrievePlan(schema, expr, nil)
		assert.NoError(t, err)
		isSimple, _, _ := getPrimaryKeysFromPlan(collSchema, plan)
		assert.False(t, isSimple)
	})

	t.Run("delete with simple term expr", func(t *testing.T) {
		expr := "pk in [1, 2, 3]"
		plan, err := planparserv2.CreateRetrievePlan(schema, expr, nil)
		assert.NoError(t, err)
		isSimple, _, rowNum := getPrimaryKeysFromPlan(collSchema, plan)
		assert.True(t, isSimple)
		assert.Equal(t, int64(3), rowNum)
	})

	t.Run("delete with large simple term expr remains simple", func(t *testing.T) {
		values := make([]string, 0, 1024)
		for i := 0; i < 1024; i++ {
			values = append(values, strconv.Itoa(i))
		}
		expr := "pk in [" + strings.Join(values, ",") + "]"
		plan, err := planparserv2.CreateRetrievePlan(schema, expr, nil)
		assert.NoError(t, err)

		isSimple, pks, rowNum := getPrimaryKeysFromPlan(collSchema, plan)
		assert.True(t, isSimple)
		assert.NotNil(t, pks)
		assert.Equal(t, int64(1024), rowNum)
	})

	t.Run("delete with pk equality OR expression is optimized to simple", func(t *testing.T) {
		expr := "pk == 1 or pk == 2 or pk == 3"
		plan, err := planparserv2.CreateRetrievePlan(schema, expr, nil)
		assert.NoError(t, err)

		isSimple, pks, rowNum := getPrimaryKeysFromPlan(collSchema, plan)
		assert.True(t, isSimple)
		assert.NotNil(t, pks)
		assert.Equal(t, int64(3), rowNum)
	})

	t.Run("delete failed with simple term expr", func(t *testing.T) {
		expr := "pk in [1, 2, 3]"
		plan, err := planparserv2.CreateRetrievePlan(schema, expr, nil)
		assert.NoError(t, err)
		termExpr := plan.Node.(*planpb.PlanNode_Query).Query.Predicates.Expr.(*planpb.Expr_TermExpr)
		termExpr.TermExpr.ColumnInfo.DataType = -1

		isSimple, _, _ := getPrimaryKeysFromPlan(collSchema, plan)
		assert.False(t, isSimple)
	})

	t.Run("delete with simple equal expr", func(t *testing.T) {
		expr := "pk == 1"
		plan, err := planparserv2.CreateRetrievePlan(schema, expr, nil)
		assert.NoError(t, err)
		isSimple, _, rowNum := getPrimaryKeysFromPlan(collSchema, plan)
		assert.True(t, isSimple)
		assert.Equal(t, int64(1), rowNum)
	})

	t.Run("delete failed with simple equal expr", func(t *testing.T) {
		expr := "pk == 1"
		plan, err := planparserv2.CreateRetrievePlan(schema, expr, nil)
		assert.NoError(t, err)
		unaryRangeExpr := plan.Node.(*planpb.PlanNode_Query).Query.Predicates.Expr.(*planpb.Expr_UnaryRangeExpr)
		unaryRangeExpr.UnaryRangeExpr.ColumnInfo.DataType = -1

		isSimple, _, _ := getPrimaryKeysFromPlan(collSchema, plan)
		assert.False(t, isSimple)
	})
}

func TestCanUsePredicateDelete(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{
		Name: "test_delete",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.StartOfUserFieldID, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: common.StartOfUserFieldID + 1, Name: "non_pk", DataType: schemapb.DataType_Int64},
			{FieldID: common.StartOfUserFieldID + 2, Name: "other_int", DataType: schemapb.DataType_Int64},
			{FieldID: common.StartOfUserFieldID + 3, Name: "varchar_field", DataType: schemapb.DataType_VarChar},
			{FieldID: common.StartOfUserFieldID + 4, Name: "bool_field", DataType: schemapb.DataType_Bool},
			{FieldID: common.StartOfUserFieldID + 5, Name: "json_field", DataType: schemapb.DataType_JSON},
			{FieldID: common.StartOfUserFieldID + 6, Name: "array_field", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
		},
	}
	schema, err := typeutil.CreateSchemaHelper(collSchema)
	require.NoError(t, err)

	tests := []struct {
		name string
		expr string
		want bool
	}{
		{name: "compare scalar with literal", expr: "non_pk >= 1", want: true},
		{name: "logical operators", expr: "non_pk > 1 and (varchar_field == \"a\" or not (bool_field == true))", want: true},
		{name: "in expression", expr: "varchar_field in [\"a\", \"b\"]", want: true},
		{name: "is null", expr: "varchar_field is null", want: true},
		{name: "json path", expr: "json_field[\"a\"] == 1", want: false},
		{name: "array element", expr: "array_field[0] == 1", want: false},
		{name: "arithmetic", expr: "non_pk + 1 == 2", want: false},
		{name: "column compare", expr: "non_pk < other_int", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan, err := planparserv2.CreateRetrievePlan(schema, tt.expr, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.want, canUsePredicateDelete(plan, collSchema))
		})
	}
}

func Test_canUsePredicateDelete_nilPlan(t *testing.T) {
	collSchema := &schemapb.CollectionSchema{}
	assert.False(t, canUsePredicateDelete(nil, collSchema))
	assert.False(t, canUsePredicateDelete(&planpb.PlanNode{}, collSchema))
	// non-query plan node never reaches predicate delete
	assert.False(t, canUsePredicateDelete(&planpb.PlanNode{
		Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}},
	}, collSchema))
}

func Test_predicateDeleteAllowedCompareOps(t *testing.T) {
	supported := []planpb.OpType{
		planpb.OpType_Equal, planpb.OpType_NotEqual,
		planpb.OpType_LessThan, planpb.OpType_LessEqual,
		planpb.OpType_GreaterThan, planpb.OpType_GreaterEqual,
	}
	for _, op := range supported {
		assert.True(t, predicateDeleteAllowedCompareOps[op], op.String())
	}
	unsupported := []planpb.OpType{
		planpb.OpType_Invalid, planpb.OpType_Range,
		planpb.OpType_In, planpb.OpType_PrefixMatch,
	}
	for _, op := range unsupported {
		assert.False(t, predicateDeleteAllowedCompareOps[op], op.String())
	}
}

func Test_isPredicateDeleteScalarType(t *testing.T) {
	supported := []schemapb.DataType{
		schemapb.DataType_Bool, schemapb.DataType_Int8, schemapb.DataType_Int16,
		schemapb.DataType_Int32, schemapb.DataType_Int64, schemapb.DataType_Float,
		schemapb.DataType_Double, schemapb.DataType_String, schemapb.DataType_VarChar,
	}
	for _, dt := range supported {
		assert.True(t, isPredicateDeleteScalarType(dt), dt.String())
	}
	unsupported := []schemapb.DataType{
		schemapb.DataType_None, schemapb.DataType_JSON,
		schemapb.DataType_Array, schemapb.DataType_FloatVector,
	}
	for _, dt := range unsupported {
		assert.False(t, isPredicateDeleteScalarType(dt), dt.String())
	}
}

func Test_isPredicateDeleteLiteralSupported(t *testing.T) {
	assert.False(t, isPredicateDeleteLiteralSupported(nil))
	assert.True(t, isPredicateDeleteLiteralSupported(&planpb.GenericValue{Val: &planpb.GenericValue_BoolVal{BoolVal: true}}))
	assert.True(t, isPredicateDeleteLiteralSupported(&planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}}))
	assert.True(t, isPredicateDeleteLiteralSupported(&planpb.GenericValue{Val: &planpb.GenericValue_FloatVal{FloatVal: 1.0}}))
	assert.True(t, isPredicateDeleteLiteralSupported(&planpb.GenericValue{Val: &planpb.GenericValue_StringVal{StringVal: "a"}}))
	// array literal is not a supported scalar literal
	assert.False(t, isPredicateDeleteLiteralSupported(&planpb.GenericValue{Val: &planpb.GenericValue_ArrayVal{ArrayVal: &planpb.Array{}}}))
}

func Test_isPredicateDeleteColumnSupported(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64_field", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "dynamic_field", DataType: schemapb.DataType_JSON, IsDynamic: true},
			{FieldID: 102, Name: "json_field", DataType: schemapb.DataType_JSON},
		},
	}
	tests := []struct {
		name string
		sch  *schemapb.CollectionSchema
		col  *planpb.ColumnInfo
		want bool
	}{
		{name: "nil schema", sch: nil, col: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64}, want: false},
		{name: "nil column", sch: schema, col: nil, want: false},
		{name: "nested path", sch: schema, col: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, NestedPath: []string{"a"}}, want: false},
		{name: "element level", sch: schema, col: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64, IsElementLevel: true}, want: false},
		{name: "non scalar column type", sch: schema, col: &planpb.ColumnInfo{FieldId: 102, DataType: schemapb.DataType_JSON}, want: false},
		{name: "field not found", sch: schema, col: &planpb.ColumnInfo{FieldId: 999, DataType: schemapb.DataType_Int64}, want: false},
		{name: "dynamic field", sch: schema, col: &planpb.ColumnInfo{FieldId: 101, DataType: schemapb.DataType_Int64}, want: false},
		{name: "type mismatch", sch: schema, col: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_VarChar}, want: false},
		{name: "supported scalar", sch: schema, col: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isPredicateDeleteColumnSupported(tt.sch, tt.col))
		})
	}
}

func Test_canUsePredicateDeleteExpr_branches(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64_field", DataType: schemapb.DataType_Int64},
		},
	}
	validRange := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
		ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64},
		Op:         planpb.OpType_Equal,
		Value:      &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
	}}}

	tests := []struct {
		name string
		expr *planpb.Expr
		want bool
	}{
		{name: "nil expr", expr: nil, want: false},
		{name: "template expr", expr: &planpb.Expr{IsTemplate: true, Expr: validRange.GetExpr()}, want: false},
		{name: "unsupported expr type", expr: &planpb.Expr{}, want: false},
		{name: "binary and valid", expr: &planpb.Expr{Expr: &planpb.Expr_BinaryExpr{BinaryExpr: &planpb.BinaryExpr{
			Op: planpb.BinaryExpr_LogicalAnd, Left: validRange, Right: validRange,
		}}}, want: true},
		{name: "binary invalid op", expr: &planpb.Expr{Expr: &planpb.Expr_BinaryExpr{BinaryExpr: &planpb.BinaryExpr{
			Op: planpb.BinaryExpr_Invalid, Left: validRange, Right: validRange,
		}}}, want: false},
		{name: "unary not valid", expr: &planpb.Expr{Expr: &planpb.Expr_UnaryExpr{UnaryExpr: &planpb.UnaryExpr{
			Op: planpb.UnaryExpr_Not, Child: validRange,
		}}}, want: true},
		{name: "unary invalid op", expr: &planpb.Expr{Expr: &planpb.Expr_UnaryExpr{UnaryExpr: &planpb.UnaryExpr{
			Op: planpb.UnaryExpr_Invalid, Child: validRange,
		}}}, want: false},
		{name: "unary range template var", expr: &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
			ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64},
			Op:         planpb.OpType_Equal, TemplateVariableName: "v",
		}}}, want: false},
		{name: "unary range extra values", expr: &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
			ColumnInfo:  &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64},
			Op:          planpb.OpType_Equal,
			Value:       &planpb.GenericValue{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}},
			ExtraValues: []*planpb.GenericValue{{Val: &planpb.GenericValue_Int64Val{Int64Val: 2}}},
		}}}, want: false},
		{name: "term is-in-field", expr: &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{
			ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64},
			IsInField:  true,
			Values:     []*planpb.GenericValue{{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}}},
		}}}, want: false},
		{name: "term empty values", expr: &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{
			ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64},
		}}}, want: false},
		{name: "term unsupported column", expr: &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{
			ColumnInfo: &planpb.ColumnInfo{FieldId: 999, DataType: schemapb.DataType_Int64},
			Values:     []*planpb.GenericValue{{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}}},
		}}}, want: false},
		{name: "term unsupported value", expr: &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{
			ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64},
			Values:     []*planpb.GenericValue{{Val: &planpb.GenericValue_ArrayVal{ArrayVal: &planpb.Array{}}}},
		}}}, want: false},
		{name: "term valid", expr: &planpb.Expr{Expr: &planpb.Expr_TermExpr{TermExpr: &planpb.TermExpr{
			ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64},
			Values:     []*planpb.GenericValue{{Val: &planpb.GenericValue_Int64Val{Int64Val: 1}}},
		}}}, want: true},
		{name: "null is-null", expr: &planpb.Expr{Expr: &planpb.Expr_NullExpr{NullExpr: &planpb.NullExpr{
			Op: planpb.NullExpr_IsNull, ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64},
		}}}, want: true},
		{name: "null is-not-null", expr: &planpb.Expr{Expr: &planpb.Expr_NullExpr{NullExpr: &planpb.NullExpr{
			Op: planpb.NullExpr_IsNotNull, ColumnInfo: &planpb.ColumnInfo{FieldId: 100, DataType: schemapb.DataType_Int64},
		}}}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, canUsePredicateDeleteExpr(tt.expr, schema))
		})
	}
}

func TestDeleteTask_GetChannels(t *testing.T) {
	collectionID := UniqueID(0)
	collectionName := "col-0"
	channels := []pChan{"mock-chan-0", "mock-chan-1"}
	cache := NewMockCache(t)
	cache.On("GetCollectionID",
		mock.Anything, // context.Context
		mock.AnythingOfType("string"),
		mock.AnythingOfType("string"),
	).Return(collectionID, nil)

	globalMetaCache = cache
	chMgr := NewMockChannelsMgr(t)
	chMgr.EXPECT().getChannels(mock.Anything).Return(channels, nil)
	dt := deleteTask{
		ctx: context.Background(),
		req: &milvuspb.DeleteRequest{
			CollectionName: collectionName,
		},
		chMgr: chMgr,
	}
	err := dt.setChannels()
	assert.NoError(t, err)
	resChannels := dt.getChannels()
	assert.ElementsMatch(t, channels, resChannels)
	assert.ElementsMatch(t, channels, dt.pChannels)
}

func TestDeleteTask_PreExecuteSkipsNamespaceValidationWhenUnset(t *testing.T) {
	cache := globalMetaCache
	globalMetaCache = nil
	defer func() {
		globalMetaCache = cache
	}()

	dt := deleteTask{
		req: &milvuspb.DeleteRequest{},
	}
	assert.NoError(t, dt.PreExecute(context.Background()))
}

func TestDeleteTask_Execute(t *testing.T) {
	collectionName := "test_delete"
	collectionID := int64(111)
	partitionName := "default"
	partitionID := int64(222)
	channels := []string{"test_channel"}
	dbName := "test_1"
	pk := &schemapb.IDs{
		IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}},
	}

	t.Run("empty expr", func(t *testing.T) {
		dt := deleteTask{}
		assert.Error(t, dt.Execute(context.Background()))
	})

	t.Run("alloc failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		rc := mocks.NewMockRootCoordClient(t)
		allocator, err := allocator.NewIDAllocator(ctx, rc, paramtable.GetNodeID())
		assert.NoError(t, err)
		allocator.Close()

		dt := deleteTask{
			chMgr:        mockMgr,
			collectionID: collectionID,
			partitionID:  partitionID,
			vChannels:    channels,
			idAllocator:  allocator,
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           "pk in [1,2]",
			},
			primaryKeys: pk,
		}
		assert.Error(t, dt.Execute(context.Background()))
	})

	t.Run("delete produce failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		rc := mocks.NewMockRootCoordClient(t)
		rc.EXPECT().AllocID(mock.Anything, mock.Anything).Return(
			&rootcoordpb.AllocIDResponse{
				Status: merr.Success(),
				ID:     0,
				Count:  1,
			}, nil)
		allocator, err := allocator.NewIDAllocator(ctx, rc, paramtable.GetNodeID())
		allocator.Start()
		assert.NoError(t, err)

		dt := deleteTask{
			chMgr:        mockMgr,
			collectionID: collectionID,
			partitionID:  partitionID,
			vChannels:    channels,
			idAllocator:  allocator,
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           "pk in [1,2]",
			},
			primaryKeys: pk,
		}
		streaming.ExpectErrorOnce(errors.New("mock error"))
		assert.Error(t, dt.Execute(context.Background()))
	})
}

func TestAppendPredicateDeleteMessages(t *testing.T) {
	collectionName := "test_delete"
	collectionID := int64(111)
	partitionName := "default"
	partitionID := int64(222)
	dbName := "test_1"
	vChannel := "test_channel"
	deleteTs := uint64(1000)
	serializedPlan := []byte{1, 2, 3}

	t.Run("alloc failed", func(t *testing.T) {
		idAllocator := allocator.NewMockAllocator(t)
		idAllocator.EXPECT().Alloc(uint32(1)).Return(int64(0), int64(0), errors.New("mock alloc error"))

		sessionTS, err := appendPredicateDeleteMessages(
			context.Background(),
			collectionID,
			collectionName,
			partitionID,
			partitionName,
			dbName,
			[]string{vChannel},
			idAllocator,
			deleteTs,
			serializedPlan,
		)

		require.Error(t, err)
		assert.Zero(t, sessionTS)
	})

	t.Run("build message failed", func(t *testing.T) {
		idAllocator := allocator.NewMockAllocator(t)
		idAllocator.EXPECT().Alloc(uint32(1)).Return(int64(10), int64(11), nil)

		// Invalid UTF-8 in a proto3 string field makes the message body marshal fail.
		sessionTS, err := appendPredicateDeleteMessages(
			context.Background(),
			collectionID,
			collectionName,
			partitionID,
			partitionName,
			"\xff\xfe",
			[]string{vChannel},
			idAllocator,
			deleteTs,
			serializedPlan,
		)

		require.Error(t, err)
		assert.Zero(t, sessionTS)
	})

	t.Run("wal append failed", func(t *testing.T) {
		idAllocator := allocator.NewMockAllocator(t)
		idAllocator.EXPECT().Alloc(uint32(1)).Return(int64(10), int64(11), nil)

		mockWAL := mock_streaming.NewMockWALAccesser(t)
		mockWAL.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, msgs ...message.MutableMessage) streaming.AppendResponses {
				resp := types.NewAppendResponseN(len(msgs))
				resp.FillAllError(errors.New("mock wal error"))
				return resp
			},
		)
		prevWAL := streaming.WAL()
		streaming.SetWALForTest(mockWAL)
		defer streaming.SetWALForTest(prevWAL)

		sessionTS, err := appendPredicateDeleteMessages(
			context.Background(),
			collectionID,
			collectionName,
			partitionID,
			partitionName,
			dbName,
			[]string{vChannel},
			idAllocator,
			deleteTs,
			serializedPlan,
		)

		require.Error(t, err)
		assert.Zero(t, sessionTS)
	})

	t.Run("encryption schema fetch failed", func(t *testing.T) {
		mockEncryption := mockey.Mock(hookutil.IsClusterEncryptionEnabled).Return(true).Build()
		defer mockEncryption.UnPatch()

		idAllocator := allocator.NewMockAllocator(t)
		idAllocator.EXPECT().Alloc(uint32(1)).Return(int64(10), int64(11), nil)

		cache := NewMockCache(t)
		cache.EXPECT().GetCollectionSchema(mock.Anything, dbName, collectionName).Return(nil, errors.New("mock schema error"))
		prevMetaCache := globalMetaCache
		globalMetaCache = cache
		defer func() { globalMetaCache = prevMetaCache }()

		sessionTS, err := appendPredicateDeleteMessages(
			context.Background(),
			collectionID,
			collectionName,
			partitionID,
			partitionName,
			dbName,
			[]string{vChannel},
			idAllocator,
			deleteTs,
			serializedPlan,
		)

		require.Error(t, err)
		assert.Zero(t, sessionTS)
	})

	t.Run("encryption enabled success", func(t *testing.T) {
		mockEncryption := mockey.Mock(hookutil.IsClusterEncryptionEnabled).Return(true).Build()
		defer mockEncryption.UnPatch()

		idAllocator := allocator.NewMockAllocator(t)
		idAllocator.EXPECT().Alloc(uint32(1)).Return(int64(10), int64(11), nil)

		cache := NewMockCache(t)
		cache.EXPECT().GetCollectionSchema(mock.Anything, dbName, collectionName).Return(newSchemaInfo(&schemapb.CollectionSchema{Name: collectionName}), nil)
		prevMetaCache := globalMetaCache
		globalMetaCache = cache
		defer func() { globalMetaCache = prevMetaCache }()

		capturedMsgs := make([]message.MutableMessage, 0, 1)
		mockWAL := mock_streaming.NewMockWALAccesser(t)
		mockWAL.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, msgs ...message.MutableMessage) streaming.AppendResponses {
				capturedMsgs = append(capturedMsgs, msgs...)
				resp := types.NewAppendResponseN(len(msgs))
				resp.Responses[0].AppendResult = &types.AppendResult{TimeTick: uint64(200)}
				return resp
			},
		)
		prevWAL := streaming.WAL()
		streaming.SetWALForTest(mockWAL)
		defer streaming.SetWALForTest(prevWAL)

		sessionTS, err := appendPredicateDeleteMessages(
			context.Background(),
			collectionID,
			collectionName,
			partitionID,
			partitionName,
			dbName,
			[]string{vChannel},
			idAllocator,
			deleteTs,
			serializedPlan,
		)

		require.NoError(t, err)
		assert.Equal(t, uint64(200), sessionTS)
		require.Len(t, capturedMsgs, 1)
		deleteMsg := message.MustAsMutableDeleteMessageV1(capturedMsgs[0])
		body := deleteMsg.MustBody()
		assert.Equal(t, vChannel, body.GetShardName())
		assert.Equal(t, deleteTs, body.GetBase().GetTimestamp())
		// A predicate delete carries the single MVCC delete ts in Timestamps[0] and no
		// primary keys; the predicate itself is in SerializedExprPlan.
		assert.Equal(t, []uint64{deleteTs}, body.GetTimestamps())
		assert.Equal(t, serializedPlan, body.GetSerializedExprPlan())
		assert.Equal(t, partitionID, body.GetPartitionID())
		// Regression: the message must pass DeleteMsg.CheckAligned() via the predicate
		// branch (expr plan set, exactly one ts, no pks); otherwise the QueryNode filter
		// node silently drops it.
		require.NoError(t, (&msgstream.DeleteMsg{DeleteRequest: body}).CheckAligned())
	})
}

// poisonedPlanNode returns a plan whose proto.Marshal fails: proto3 rejects
// invalid UTF-8 in string fields.
func poisonedPlanNode() *planpb.PlanNode {
	return &planpb.PlanNode{
		Node: &planpb.PlanNode_Query{
			Query: &planpb.QueryPlanNode{
				Predicates: &planpb.Expr{
					Expr: &planpb.Expr_UnaryRangeExpr{
						UnaryRangeExpr: &planpb.UnaryRangeExpr{
							Value: &planpb.GenericValue{Val: &planpb.GenericValue_StringVal{StringVal: "\xff\xfe"}},
						},
					},
				},
			},
		},
	}
}

func TestDeleteRunner_predicateDelete_errors(t *testing.T) {
	newRunner := func(idAllocator allocator.Interface) *deleteRunner {
		return &deleteRunner{
			collectionID: int64(111),
			partitionIDs: []int64{222},
			vChannels:    []string{"test_channel"},
			idAllocator:  idAllocator,
			result:       &milvuspb.MutationResult{Status: merr.Success(), IDs: &schemapb.IDs{}},
			req: &milvuspb.DeleteRequest{
				CollectionName: "test_delete",
				DbName:         "test_1",
			},
		}
	}

	t.Run("marshal plan failed", func(t *testing.T) {
		dr := newRunner(nil)
		require.Error(t, dr.predicateDelete(context.Background(), poisonedPlanNode(), 10, 100))
	})

	t.Run("alloc msg id failed", func(t *testing.T) {
		idAllocator := allocator.NewMockAllocator(t)
		idAllocator.EXPECT().AllocOne().Return(int64(0), errors.New("mock alloc error"))
		dr := newRunner(idAllocator)
		plan := &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}}}
		require.Error(t, dr.predicateDelete(context.Background(), plan, 10, 100))
	})
}

func TestDeleteRunner_countPredicateDeleteHits_errors(t *testing.T) {
	validPlan := func() *planpb.PlanNode {
		return &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}}}
	}
	newRunner := func(idAllocator allocator.Interface, lb shardclient.LBPolicy) *deleteRunner {
		return &deleteRunner{
			collectionID: int64(111),
			partitionIDs: []int64{222},
			vChannels:    []string{"test_channel"},
			idAllocator:  idAllocator,
			lb:           lb,
			result:       &milvuspb.MutationResult{Status: merr.Success(), IDs: &schemapb.IDs{}},
			req: &milvuspb.DeleteRequest{
				CollectionName: "test_delete",
				DbName:         "test_1",
			},
		}
	}
	// lb that drives the workload against the given QueryNode once.
	newLB := func(t *testing.T, qn *mocks.MockQueryNodeClient) *shardclient.MockLBPolicy {
		lb := shardclient.NewMockLBPolicy(t)
		lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			return workload.Exec(ctx, 1, qn, "test_channel")
		})
		return lb
	}
	newAllocator := func(t *testing.T) *allocator.MockAllocator {
		idAllocator := allocator.NewMockAllocator(t)
		idAllocator.EXPECT().AllocOne().Return(int64(100), nil)
		return idAllocator
	}

	t.Run("clone type mismatch", func(t *testing.T) {
		cloneMock := mockey.Mock(proto.Clone).Return(&planpb.QueryPlanNode{}).Build()
		defer cloneMock.UnPatch()

		dr := newRunner(nil, nil)
		_, err := dr.countPredicateDeleteHits(context.Background(), validPlan(), 100)
		require.Error(t, err)
	})

	t.Run("query plan missing", func(t *testing.T) {
		dr := newRunner(nil, nil)
		_, err := dr.countPredicateDeleteHits(context.Background(), &planpb.PlanNode{}, 100)
		require.Error(t, err)
	})

	t.Run("marshal count plan failed", func(t *testing.T) {
		dr := newRunner(nil, nil)
		_, err := dr.countPredicateDeleteHits(context.Background(), poisonedPlanNode(), 100)
		require.Error(t, err)
	})

	t.Run("alloc msg id failed", func(t *testing.T) {
		idAllocator := allocator.NewMockAllocator(t)
		idAllocator.EXPECT().AllocOne().Return(int64(0), errors.New("mock alloc error"))
		dr := newRunner(idAllocator, nil)
		_, err := dr.countPredicateDeleteHits(context.Background(), validPlan(), 100)
		require.Error(t, err)
	})

	t.Run("nil query result", func(t *testing.T) {
		qn := mocks.NewMockQueryNodeClient(t)
		qn.EXPECT().Query(mock.Anything, mock.Anything).Return(nil, nil)
		dr := newRunner(newAllocator(t), newLB(t, qn))
		_, err := dr.countPredicateDeleteHits(context.Background(), validPlan(), 100)
		require.Error(t, err)
	})

	t.Run("query result status error", func(t *testing.T) {
		qn := mocks.NewMockQueryNodeClient(t)
		qn.EXPECT().Query(mock.Anything, mock.Anything).Return(&internalpb.RetrieveResults{
			Status: merr.Status(errors.New("mock status error")),
		}, nil)
		dr := newRunner(newAllocator(t), newLB(t, qn))
		_, err := dr.countPredicateDeleteHits(context.Background(), validPlan(), 100)
		require.Error(t, err)
	})

	t.Run("count parse failed", func(t *testing.T) {
		qn := mocks.NewMockQueryNodeClient(t)
		// A success status without exactly one count column fails CntOfInternalResult.
		qn.EXPECT().Query(mock.Anything, mock.Anything).Return(&internalpb.RetrieveResults{
			Status: merr.Success(),
		}, nil)
		dr := newRunner(newAllocator(t), newLB(t, qn))
		_, err := dr.countPredicateDeleteHits(context.Background(), validPlan(), 100)
		require.Error(t, err)
	})
}

func TestDeleteRunnerSuite(t *testing.T) {
	suite.Run(t, new(DeleteRunnerSuite))
}

type DeleteRunnerSuite struct {
	suite.Suite

	collectionName string
	collectionID   int64
	partitionName  string
	partitionIDs   []int64

	schema    *schemaInfo
	mockCache *MockCache
}

func (s *DeleteRunnerSuite) SetupSubTest() {
	s.SetupSuite()
}

func (s *DeleteRunnerSuite) SetupSuite() {
	s.collectionName = "test_delete"
	s.collectionID = int64(111)
	s.partitionName = "default"
	s.partitionIDs = []int64{222, 333, 444}

	schema := &schemapb.CollectionSchema{
		Name: s.collectionName,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      common.StartOfUserFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:        common.StartOfUserFieldID + 1,
				Name:           "non_pk",
				DataType:       schemapb.DataType_Int64,
				IsPartitionKey: true,
			},
		},
	}
	s.schema = newSchemaInfo(schema)
	s.mockCache = NewMockCache(s.T())
}

func (s *DeleteRunnerSuite) TestInitSuccess() {
	s.Run("non_pk == 1", func() {
		mockChMgr := NewMockChannelsMgr(s.T())
		dr := deleteRunner{
			req: &milvuspb.DeleteRequest{
				CollectionName: s.collectionName,
				Expr:           "non_pk == 1",
			},
			chMgr: mockChMgr,
		}
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(s.collectionID, nil)
		s.mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
		s.mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(s.schema, nil).Twice()
		s.mockCache.EXPECT().GetPartitionsIndex(mock.Anything, mock.Anything, mock.Anything).Return([]string{"part1", "part2"}, nil)
		s.mockCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).Return(map[string]int64{"part1": 100, "part2": 101}, nil)
		mockChMgr.EXPECT().getVChannels(mock.Anything).Return([]string{"vchan1"}, nil)

		globalMetaCache = s.mockCache
		s.NoError(dr.Init(context.Background()))

		s.Require().Equal(1, len(dr.partitionIDs))
		s.True(typeutil.NewSet[int64](100, 101).Contain(dr.partitionIDs[0]))
	})

	s.Run("non_pk > 1, partition key", func() {
		mockChMgr := NewMockChannelsMgr(s.T())
		dr := deleteRunner{
			req: &milvuspb.DeleteRequest{
				CollectionName: s.collectionName,
				Expr:           "non_pk > 1",
			},
			chMgr: mockChMgr,
		}
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(s.collectionID, nil)
		s.mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
		s.mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(s.schema, nil).Twice()
		s.mockCache.EXPECT().GetPartitionsIndex(mock.Anything, mock.Anything, mock.Anything).Return([]string{"part1", "part2"}, nil)
		s.mockCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).Return(map[string]int64{"part1": 100, "part2": 101}, nil)
		mockChMgr.EXPECT().getVChannels(mock.Anything).Return([]string{"vchan1"}, nil)

		globalMetaCache = s.mockCache
		s.NoError(dr.Init(context.Background()))

		s.Require().Equal(0, len(dr.partitionIDs))
	})

	s.Run("pk == 1, partition key", func() {
		mockChMgr := NewMockChannelsMgr(s.T())
		dr := deleteRunner{
			req: &milvuspb.DeleteRequest{
				CollectionName: s.collectionName,
				Expr:           "pk == 1",
			},
			chMgr: mockChMgr,
		}
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(s.collectionID, nil)
		s.mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
		s.mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(s.schema, nil).Twice()
		s.mockCache.EXPECT().GetPartitionsIndex(mock.Anything, mock.Anything, mock.Anything).Return([]string{"part1", "part2"}, nil)
		s.mockCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).Return(map[string]int64{"part1": 100, "part2": 101}, nil)
		mockChMgr.EXPECT().getVChannels(mock.Anything).Return([]string{"vchan1"}, nil)

		globalMetaCache = s.mockCache
		s.NoError(dr.Init(context.Background()))

		s.Require().Equal(0, len(dr.partitionIDs))
	})

	s.Run("namespace assigns partition", func() {
		mockChMgr := NewMockChannelsMgr(s.T())
		namespace := "ns-1"
		partitionNames := []string{"_default_0", "_default_1"}
		partitionIDs := map[string]int64{"_default_0": 100, "_default_1": 101}
		schema := namespaceEnabledSchema(
			&schemapb.FieldSchema{FieldID: common.StartOfUserFieldID, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			&schemapb.FieldSchema{FieldID: common.StartOfUserFieldID + 1, Name: "non_pk", DataType: schemapb.DataType_Int64},
		)
		s.schema = newSchemaInfo(schema)
		dr := deleteRunner{
			req: &milvuspb.DeleteRequest{
				CollectionName: s.collectionName,
				Expr:           "pk == 1",
				Namespace:      &namespace,
			},
			chMgr: mockChMgr,
		}
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(s.collectionID, nil)
		s.mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
		s.mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(s.schema, nil).Twice()
		s.mockCache.EXPECT().GetPartitionsIndex(mock.Anything, mock.Anything, mock.Anything).Return(partitionNames, nil)
		s.mockCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).Return(partitionIDs, nil)
		mockChMgr.EXPECT().getVChannels(mock.Anything).Return([]string{"vchan1"}, nil)

		globalMetaCache = s.mockCache
		s.NoError(dr.Init(context.Background()))

		s.Equal([]int64{expectedNamespacePartitionID(namespace, partitionNames, partitionIDs)}, dr.partitionIDs)
	})

	s.Run("pk == 1, no partition name", func() {
		mockChMgr := NewMockChannelsMgr(s.T())
		dr := deleteRunner{
			req: &milvuspb.DeleteRequest{
				CollectionName: s.collectionName,
				Expr:           "pk == 1",
			},
			chMgr: mockChMgr,
		}
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(s.collectionID, nil)
		s.mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
		// Schema without PartitionKey
		schema := &schemapb.CollectionSchema{
			Name: s.collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      common.StartOfUserFieldID,
					Name:         "pk",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:        common.StartOfUserFieldID + 1,
					Name:           "non_pk",
					DataType:       schemapb.DataType_Int64,
					IsPartitionKey: false,
				},
			},
		}
		s.schema = newSchemaInfo(schema)
		s.mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(s.schema, nil).Once()
		mockChMgr.EXPECT().getVChannels(mock.Anything).Return([]string{"vchan1"}, nil)

		globalMetaCache = s.mockCache
		s.NoError(dr.Init(context.Background()))

		s.Equal(0, len(dr.partitionIDs))
	})

	s.Run("pk == 1, with partition name", func() {
		mockChMgr := NewMockChannelsMgr(s.T())
		dr := deleteRunner{
			req: &milvuspb.DeleteRequest{
				CollectionName: s.collectionName,
				PartitionName:  "part1",
				Expr:           "pk == 1",
			},
			chMgr: mockChMgr,
		}
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(s.collectionID, nil)
		s.mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
		// Schema without PartitionKey
		schema := &schemapb.CollectionSchema{
			Name: s.collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      common.StartOfUserFieldID,
					Name:         "pk",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:        common.StartOfUserFieldID + 1,
					Name:           "non_pk",
					DataType:       schemapb.DataType_Int64,
					IsPartitionKey: false,
				},
			},
		}
		s.schema = newSchemaInfo(schema)
		s.mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(s.schema, nil).Once()
		mockChMgr.EXPECT().getVChannels(mock.Anything).Return([]string{"vchan1"}, nil)
		s.mockCache.EXPECT().GetPartitionID(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(int64(1000), nil)

		globalMetaCache = s.mockCache
		s.NoError(dr.Init(context.Background()))

		s.Equal(1, len(dr.partitionIDs))
		s.EqualValues(1000, dr.partitionIDs[0])
	})

	s.Run("namespace partition mode routes to namespace partition", func() {
		mockChMgr := NewMockChannelsMgr(s.T())
		namespace := "tenant_partition"
		dr := deleteRunner{
			req: &milvuspb.DeleteRequest{
				CollectionName: s.collectionName,
				Namespace:      &namespace,
				Expr:           "pk == 1",
			},
			chMgr: mockChMgr,
		}
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(s.collectionID, nil)
		s.mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
		schema := &schemapb.CollectionSchema{
			Name:            s.collectionName,
			EnableNamespace: true,
			Properties: []*commonpb.KeyValuePair{
				{Key: common.NamespaceModeKey, Value: common.NamespaceModePartition},
			},
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      common.StartOfUserFieldID,
					Name:         "pk",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:  common.StartOfUserFieldID + 1,
					Name:     "non_pk",
					DataType: schemapb.DataType_Int64,
				},
			},
		}
		schemaInfo := newSchemaInfo(schema)
		s.mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(schemaInfo, nil).Once()
		s.mockCache.EXPECT().GetPartitionID(mock.Anything, mock.Anything, mock.Anything, namespace).Return(int64(1000), nil)
		mockChMgr.EXPECT().getVChannels(mock.Anything).Return([]string{"vchan1"}, nil)

		globalMetaCache = s.mockCache
		s.NoError(dr.Init(context.Background()))

		s.Equal(namespace, dr.req.GetPartitionName())
		s.Equal([]UniqueID{1000}, dr.partitionIDs)
		s.Nil(dr.plan.Namespace)
	})
}

func (s *DeleteRunnerSuite) TestInitFailure() {
	s.Run("empty collection name", func() {
		dr := deleteRunner{}
		s.Error(dr.Init(context.Background()))
	})

	s.Run("namespace disabled", func() {
		mockChMgr := NewMockChannelsMgr(s.T())
		namespace := "ns-1"
		dr := deleteRunner{
			req: &milvuspb.DeleteRequest{
				CollectionName: s.collectionName,
				Expr:           "pk == 1",
				Namespace:      &namespace,
			},
			chMgr: mockChMgr,
		}
		schema := &schemapb.CollectionSchema{
			Name: s.collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      common.StartOfUserFieldID,
					Name:         "pk",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
			},
		}
		s.schema = newSchemaInfo(schema)
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(s.collectionID, nil)
		s.mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(s.schema, nil)

		globalMetaCache = s.mockCache
		s.Error(dr.Init(context.Background()))
	})

	s.Run("fail to get database info", func() {
		dr := deleteRunner{
			req: &milvuspb.DeleteRequest{
				CollectionName: s.collectionName,
			},
		}
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
		globalMetaCache = s.mockCache

		s.Error(dr.Init(context.Background()))
	})
	s.Run("fail to get collection id", func() {
		dr := deleteRunner{
			req: &milvuspb.DeleteRequest{
				CollectionName: s.collectionName,
			},
		}
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).
			Return(int64(0), errors.New("mock get collectionID error"))

		globalMetaCache = s.mockCache
		s.Error(dr.Init(context.Background()))
	})

	s.Run("fail get collection schema", func() {
		dr := deleteRunner{req: &milvuspb.DeleteRequest{
			CollectionName: s.collectionName,
		}}
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).
			Return(s.collectionID, nil)
		s.mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).
			Return(nil, errors.New("mock GetCollectionSchema err"))

		globalMetaCache = s.mockCache
		s.Error(dr.Init(context.Background()))
	})

	s.Run("create plan failed", func() {
		dr := deleteRunner{
			req: &milvuspb.DeleteRequest{
				CollectionName: s.collectionName,
				Expr:           "????",
			},
		}
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).
			Return(s.collectionID, nil)
		s.mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
		s.mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).
			Return(s.schema, nil)

		globalMetaCache = s.mockCache
		s.Error(dr.Init(context.Background()))
	})
	s.Run("delete with always true expression failed", func() {
		alwaysTrueExpr := " "
		dr := deleteRunner{
			req: &milvuspb.DeleteRequest{
				CollectionName: s.collectionName,
				Expr:           alwaysTrueExpr,
			},
		}
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(s.collectionID, nil)
		s.mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
		s.mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).
			Return(s.schema, nil)

		globalMetaCache = s.mockCache
		s.Error(dr.Init(context.Background()))
	})

	s.Run("partition key mode but delete with partition name", func() {
		dr := deleteRunner{req: &milvuspb.DeleteRequest{
			CollectionName: s.collectionName,
			PartitionName:  s.partitionName,
		}}
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).
			Return(s.collectionID, nil)
		s.mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
		s.mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).
			Return(s.schema, nil)
			// The schema enabled partitionKey

		globalMetaCache = s.mockCache
		s.Error(dr.Init(context.Background()))
	})

	s.Run("invalid partition name", func() {
		dr := deleteRunner{
			req: &milvuspb.DeleteRequest{
				CollectionName: s.collectionName,
				PartitionName:  "???",
				Expr:           "non_pk in [1, 2, 3]",
			},
		}
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(s.collectionID, nil)
		s.mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)

		// Schema without PartitionKey
		schema := &schemapb.CollectionSchema{
			Name: s.collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      common.StartOfUserFieldID,
					Name:         "pk",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:        common.StartOfUserFieldID + 1,
					Name:           "non_pk",
					DataType:       schemapb.DataType_Int64,
					IsPartitionKey: false,
				},
			},
		}
		s.schema = newSchemaInfo(schema)
		s.mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).
			Return(s.schema, nil)

		globalMetaCache = s.mockCache
		s.Error(dr.Init(context.Background()))
	})

	s.Run("get partition id failed", func() {
		dr := deleteRunner{
			req: &milvuspb.DeleteRequest{
				CollectionName: s.collectionName,
				PartitionName:  s.partitionName,
				Expr:           "non_pk in [1, 2, 3]",
			},
		}
		// Schema without PartitionKey
		schema := &schemapb.CollectionSchema{
			Name: s.collectionName,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      common.StartOfUserFieldID,
					Name:         "pk",
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
				},
				{
					FieldID:        common.StartOfUserFieldID + 1,
					Name:           "non_pk",
					DataType:       schemapb.DataType_Int64,
					IsPartitionKey: false,
				},
			},
		}
		s.schema = newSchemaInfo(schema)
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(s.collectionID, nil)
		s.mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
		s.mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(s.schema, nil)
		s.mockCache.EXPECT().GetPartitionID(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			Return(int64(0), errors.New("mock GetPartitionID err"))
		globalMetaCache = s.mockCache
		s.Error(dr.Init(context.Background()))
	})

	s.Run("get vchannel failed", func() {
		mockChMgr := NewMockChannelsMgr(s.T())
		dr := deleteRunner{
			req: &milvuspb.DeleteRequest{
				CollectionName: s.collectionName,
				Expr:           "non_pk in [1, 2, 3]",
			},
			chMgr: mockChMgr,
		}
		s.mockCache.EXPECT().GetDatabaseInfo(mock.Anything, mock.Anything).Return(&databaseInfo{dbID: 0}, nil)
		s.mockCache.EXPECT().GetCollectionID(mock.Anything, mock.Anything, mock.Anything).Return(s.collectionID, nil)
		s.mockCache.EXPECT().GetCollectionSchema(mock.Anything, mock.Anything, mock.Anything).Return(s.schema, nil).Twice()
		s.mockCache.EXPECT().GetCollectionInfo(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(&collectionInfo{}, nil)
		s.mockCache.EXPECT().GetPartitionsIndex(mock.Anything, mock.Anything, mock.Anything).Return([]string{"part1", "part2"}, nil)
		s.mockCache.EXPECT().GetPartitions(mock.Anything, mock.Anything, mock.Anything).Return(map[string]int64{"part1": 100, "part2": 101}, nil)
		mockChMgr.EXPECT().getVChannels(mock.Anything).Return(nil, errors.New("mock error"))

		globalMetaCache = s.mockCache
		s.Error(dr.Init(context.Background()))
	})
}

func TestDeleteRunner_Run(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := "test_delete"
	collectionID := int64(111)
	partitionName := "default"
	partitionID := int64(222)
	channels := []string{"test_channel"}
	dbName := "test_1"
	tsoAllocator := &mockTsoAllocator{}
	idAllocator := &mockIDAllocatorInterface{}

	queue, err := newTaskScheduler(ctx, tsoAllocator)
	assert.NoError(t, err)
	queue.Start()
	defer queue.Close()

	collSchema := &schemapb.CollectionSchema{
		Name:        collectionName,
		Description: "",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      common.StartOfUserFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:      common.StartOfUserFieldID + 1,
				Name:         "non_pk",
				IsPrimaryKey: false,
				DataType:     schemapb.DataType_Int64,
			},
		},
	}
	collSchema.Version = 7
	schema := newSchemaInfo(collSchema)

	metaCache := NewMockCache(t)
	metaCache.EXPECT().GetCollectionID(mock.Anything, dbName, collectionName).Return(collectionID, nil).Maybe()
	globalMetaCache = metaCache
	defer func() {
		globalMetaCache = nil
	}()

	setPredicateDelete := func(t *testing.T, enabled bool) {
		old := paramtable.Get().CommonCfg.EnablePredicateDelete.SwapTempValue(strconv.FormatBool(enabled))
		t.Cleanup(func() {
			paramtable.Get().CommonCfg.EnablePredicateDelete.SwapTempValue(old)
		})
	}
	setPredicateDeleteThreshold := func(t *testing.T, threshold int64) {
		old := paramtable.Get().CommonCfg.PredicateDeleteHitCountThreshold.SwapTempValue(strconv.FormatInt(threshold, 10))
		t.Cleanup(func() {
			paramtable.Get().CommonCfg.PredicateDeleteHitCountThreshold.SwapTempValue(old)
		})
	}
	// StorageV3 (common.storage.useLoonFFI) must be enabled for predicate delete to be applied.
	setStorageV3 := func(t *testing.T, enabled bool) {
		old := paramtable.Get().CommonCfg.UseLoonFFI.SwapTempValue(strconv.FormatBool(enabled))
		t.Cleanup(func() {
			paramtable.Get().CommonCfg.UseLoonFFI.SwapTempValue(old)
		})
	}
	assertCountAggRequest := func(t *testing.T, req *querypb.QueryRequest) {
		require.False(t, req.GetReq().GetIsCount())
		require.Len(t, req.GetReq().GetAggregates(), 1)
		require.Equal(t, planpb.AggregateOp_count, req.GetReq().GetAggregates()[0].GetOp())
		require.Equal(t, int64(0), req.GetReq().GetAggregates()[0].GetFieldId())
		require.NotNil(t, req.GetReq().GetSerializedExprPlan())
		countPlan := &planpb.PlanNode{}
		require.NoError(t, proto.Unmarshal(req.GetReq().GetSerializedExprPlan(), countPlan))
		require.Len(t, countPlan.GetQuery().GetAggregates(), 1)
		require.Equal(t, planpb.AggregateOp_count, countPlan.GetQuery().GetAggregates()[0].GetOp())
		require.Equal(t, int64(0), countPlan.GetQuery().GetAggregates()[0].GetFieldId())
		require.Empty(t, countPlan.GetOutputFieldIds())
	}

	t.Run("simple delete task failed", func(t *testing.T) {
		mockMgr := NewMockChannelsMgr(t)
		lb := shardclient.NewMockLBPolicy(t)

		expr := "pk in [1,2,3]"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		dr := deleteRunner{
			chMgr:           mockMgr,
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			tsoAllocatorIns: tsoAllocator,
			idAllocator:     idAllocator,
			queue:           queue.dmQueue,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		mockMgr.EXPECT().getChannels(collectionID).Return(channels, nil)
		streaming.ExpectErrorOnce(errors.New("mock error"))
		assert.Error(t, dr.Run(context.Background()))
		assert.Equal(t, int64(0), dr.result.DeleteCnt)
	})

	t.Run("config disabled complex delete query rpc failed", func(t *testing.T) {
		setPredicateDelete(t, false)

		mockMgr := NewMockChannelsMgr(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)
		expr := "pk < 3"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		dr := deleteRunner{
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			queue:           queue.dmQueue,
			chMgr:           mockMgr,
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs:    &schemapb.IDs{},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		lb.EXPECT().Execute(mock.Anything, mock.Anything).Call.Return(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			return workload.Exec(ctx, 1, qn, "")
		})

		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).Return(nil, errors.New("mock error"))
		assert.Error(t, dr.Run(context.Background()))
		assert.Equal(t, int64(0), dr.result.DeleteCnt)
	})

	t.Run("complex delete query failed", func(t *testing.T) {
		setPredicateDelete(t, false)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)
		expr := "pk < 3"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		dr := deleteRunner{
			queue:           queue.dmQueue,
			chMgr:           mockMgr,
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			tsoAllocatorIns: tsoAllocator,
			idAllocator:     idAllocator,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		mockMgr.EXPECT().getChannels(collectionID).Return(channels, nil)

		lb.EXPECT().Execute(mock.Anything, mock.Anything).Call.Return(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			return workload.Exec(ctx, 1, qn, "")
		})

		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).Call.Return(
			func(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) querypb.QueryNode_QueryStreamClient {
				client := streamrpc.NewLocalQueryClient(ctx)
				server := client.CreateServer()

				server.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{0, 1, 2},
							},
						},
					},
				})

				server.Send(&internalpb.RetrieveResults{
					Status: merr.Status(errors.New("mock error")),
				})
				return client
			}, nil)

		assert.Error(t, dr.Run(ctx))
	})

	t.Run("complex delete rate limit check failed", func(t *testing.T) {
		setPredicateDelete(t, false)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)
		expr := "pk < 3"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		dr := deleteRunner{
			chMgr:           mockMgr,
			queue:           queue.dmQueue,
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			lb:              lb,
			limiter:         &limiterMock{},
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		lb.EXPECT().Execute(mock.Anything, mock.Anything).Call.Return(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			return workload.Exec(ctx, 1, qn, "")
		})

		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).Call.Return(
			func(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) querypb.QueryNode_QueryStreamClient {
				client := streamrpc.NewLocalQueryClient(ctx)
				server := client.CreateServer()

				server.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{0, 1, 2},
							},
						},
					},
				})
				server.FinishSend(nil)
				return client
			}, nil)

		assert.Error(t, dr.Run(ctx))
		assert.Equal(t, int64(0), dr.result.DeleteCnt)
	})

	t.Run("complex delete produce failed", func(t *testing.T) {
		setPredicateDelete(t, false)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)
		expr := "pk < 3"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		dr := deleteRunner{
			chMgr:           mockMgr,
			queue:           queue.dmQueue,
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		mockMgr.EXPECT().getChannels(collectionID).Return(channels, nil)
		lb.EXPECT().Execute(mock.Anything, mock.Anything).Call.Return(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			return workload.Exec(ctx, 1, qn, "")
		})

		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).Call.Return(
			func(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) querypb.QueryNode_QueryStreamClient {
				client := streamrpc.NewLocalQueryClient(ctx)
				server := client.CreateServer()

				server.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{0, 1, 2},
							},
						},
					},
				})
				server.FinishSend(nil)
				return client
			}, nil)

		streaming.ExpectErrorOnce(errors.New("mock error"))
		assert.Error(t, dr.Run(ctx))
		assert.Equal(t, int64(0), dr.result.DeleteCnt)
	})

	t.Run("predicate delete success", func(t *testing.T) {
		setPredicateDelete(t, true)
		setStorageV3(t, true)
		setPredicateDeleteThreshold(t, 1024)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)
		vChannels := []string{"test_channel_0", "test_channel_1"}
		expr := "non_pk == 1"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)
		expectedPlan, err := proto.Marshal(plan)
		require.NoError(t, err)

		capturedMsgs := make([]message.MutableMessage, 0, len(vChannels))
		mockWAL := mock_streaming.NewMockWALAccesser(t)
		mockWAL.EXPECT().AppendMessages(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, msgs ...message.MutableMessage) streaming.AppendResponses {
				capturedMsgs = append(capturedMsgs, msgs...)
				resp := types.NewAppendResponseN(len(msgs))
				for i := range resp.Responses {
					resp.Responses[i].AppendResult = &types.AppendResult{TimeTick: uint64(100 + i)}
				}
				return resp
			},
		)
		prevWAL := streaming.WAL()
		streaming.SetWALForTest(mockWAL)
		defer streaming.SetWALForTest(prevWAL)

		dr := deleteRunner{
			queue:           queue.dmQueue,
			chMgr:           mockMgr,
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       vChannels,
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		lb.EXPECT().Execute(mock.Anything, mock.Anything).Call.Return(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			for _, channel := range vChannels {
				err := workload.Exec(ctx, 1, qn, channel)
				require.NoError(t, err)
			}
			return nil
		})
		qn.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, req *querypb.QueryRequest, opts ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
				assertCountAggRequest(t, req)
				return funcutil.WrapCntToInternalResult(600), nil
			}).Twice()

		assert.NoError(t, dr.Run(ctx))
		// DeleteCnt is the probe-time matched count (600 per vchannel x 2 vchannels = 1200),
		// not 0 — see predicateDelete reporting matchedCount.
		assert.Equal(t, int64(1200), dr.result.DeleteCnt)
		assert.Equal(t, uint64(101), dr.result.Timestamp)
		assert.Len(t, capturedMsgs, len(vChannels))

		seenVChannels := typeutil.NewSet[string]()
		var deleteTs uint64
		for _, msg := range capturedMsgs {
			deleteMsg := message.MustAsMutableDeleteMessageV1(msg)
			body := deleteMsg.MustBody()
			header := deleteMsg.Header()

			seenVChannels.Insert(msg.VChannel())
			require.Nil(t, body.GetPrimaryKeys())
			require.Equal(t, []uint64{body.GetBase().GetTimestamp()}, body.GetTimestamps())
			require.Equal(t, int64(0), body.GetNumRows())
			require.Equal(t, uint64(0), header.GetRows())
			require.Equal(t, collectionID, header.GetCollectionId())
			require.Equal(t, expectedPlan, body.GetSerializedExprPlan())
			require.Equal(t, partitionID, body.GetPartitionID())

			if deleteTs == 0 {
				deleteTs = body.GetBase().GetTimestamp()
			} else {
				require.Equal(t, deleteTs, body.GetBase().GetTimestamp())
			}
		}
		require.Equal(t, typeutil.NewSet(vChannels...), seenVChannels)
	})

	t.Run("predicate delete partition scopes", func(t *testing.T) {
		setPredicateDelete(t, true)
		setStorageV3(t, true)
		setPredicateDeleteThreshold(t, 1024)

		cases := []struct {
			name                string
			partitionIDs        []int64
			expectedPartitionID int64
		}{
			{
				name:                "collection wide",
				partitionIDs:        nil,
				expectedPartitionID: common.AllPartitionsID,
			},
			{
				name:                "multi partition",
				partitionIDs:        []int64{partitionID, partitionID + 1},
				expectedPartitionID: common.AllPartitionsID,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				vChannels := []string{"test_channel_0"}
				expr := "non_pk == 1"
				plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
				require.NoError(t, err)

				var capturedMsg message.MutableMessage
				mockWAL := mock_streaming.NewMockWALAccesser(t)
				mockWAL.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(
					func(ctx context.Context, msgs ...message.MutableMessage) streaming.AppendResponses {
						require.Len(t, msgs, 1)
						capturedMsg = msgs[0]
						resp := types.NewAppendResponseN(len(msgs))
						resp.Responses[0].AppendResult = &types.AppendResult{TimeTick: uint64(100)}
						return resp
					},
				)
				prevWAL := streaming.WAL()
				streaming.SetWALForTest(mockWAL)
				defer streaming.SetWALForTest(prevWAL)

				qn := mocks.NewMockQueryNodeClient(t)
				lb := shardclient.NewMockLBPolicy(t)
				dr := deleteRunner{
					queue:           queue.dmQueue,
					chMgr:           NewMockChannelsMgr(t),
					schema:          schema,
					collectionID:    collectionID,
					partitionIDs:    tc.partitionIDs,
					vChannels:       vChannels,
					idAllocator:     idAllocator,
					tsoAllocatorIns: tsoAllocator,
					lb:              lb,
					result: &milvuspb.MutationResult{
						Status: merr.Success(),
						IDs:    &schemapb.IDs{},
					},
					req: &milvuspb.DeleteRequest{
						CollectionName: collectionName,
						DbName:         dbName,
						Expr:           expr,
					},
					plan: plan,
				}
				lb.EXPECT().Execute(mock.Anything, mock.Anything).Call.Return(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
					return workload.Exec(ctx, 1, qn, vChannels[0])
				})
				qn.EXPECT().Query(mock.Anything, mock.Anything).Return(funcutil.WrapCntToInternalResult(1025), nil)

				require.NoError(t, dr.Run(ctx))
				deleteMsg := message.MustAsMutableDeleteMessageV1(capturedMsg)
				body := deleteMsg.MustBody()
				assert.Equal(t, tc.expectedPartitionID, body.GetPartitionID())
			})
		}
	})

	t.Run("predicate delete falls back to pk delete when count is not above threshold", func(t *testing.T) {
		setPredicateDelete(t, true)
		setStorageV3(t, true)
		setPredicateDeleteThreshold(t, 1024)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)
		expr := "non_pk == 1"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		capturedMsgs := make([]message.MutableMessage, 0, 1)
		mockWAL := mock_streaming.NewMockWALAccesser(t)
		mockWAL.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, msgs ...message.MutableMessage) streaming.AppendResponses {
				capturedMsgs = append(capturedMsgs, msgs...)
				resp := types.NewAppendResponseN(len(msgs))
				for i := range resp.Responses {
					resp.Responses[i].AppendResult = &types.AppendResult{TimeTick: uint64(200 + i)}
				}
				return resp
			},
		)
		prevWAL := streaming.WAL()
		streaming.SetWALForTest(mockWAL)
		defer streaming.SetWALForTest(prevWAL)

		dr := deleteRunner{
			queue:           queue.dmQueue,
			chMgr:           mockMgr,
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs:    &schemapb.IDs{},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		mockMgr.EXPECT().getChannels(collectionID).Return(channels, nil)

		call := 0
		lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			call++
			return workload.Exec(ctx, 1, qn, channels[0])
		}).Twice()
		qn.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, req *querypb.QueryRequest, opts ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
				require.Equal(t, 1, call)
				assertCountAggRequest(t, req)
				return funcutil.WrapCntToInternalResult(1024), nil
			})
		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.QueryNode_QueryStreamClient, error) {
				require.Equal(t, 2, call)
				client := streamrpc.NewLocalQueryClient(ctx)
				server := client.CreateServer()
				server.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
				})
				server.FinishSend(nil)
				return client, nil
			})

		require.NoError(t, dr.Run(ctx))
		assert.Equal(t, int64(3), dr.result.DeleteCnt)
		assert.Equal(t, uint64(200), dr.result.Timestamp)
		require.Len(t, capturedMsgs, 1)
		deleteMsg := message.MustAsMutableDeleteMessageV1(capturedMsgs[0])
		body := deleteMsg.MustBody()
		assert.NotNil(t, body.GetPrimaryKeys())
		assert.Empty(t, body.GetSerializedExprPlan())
	})

	t.Run("predicate delete falls back to pk delete when storageV3 disabled", func(t *testing.T) {
		setPredicateDelete(t, true)
		setStorageV3(t, false)
		setPredicateDeleteThreshold(t, 1024)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)
		expr := "non_pk == 1"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		capturedMsgs := make([]message.MutableMessage, 0, 1)
		mockWAL := mock_streaming.NewMockWALAccesser(t)
		mockWAL.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, msgs ...message.MutableMessage) streaming.AppendResponses {
				capturedMsgs = append(capturedMsgs, msgs...)
				resp := types.NewAppendResponseN(len(msgs))
				for i := range resp.Responses {
					resp.Responses[i].AppendResult = &types.AppendResult{TimeTick: uint64(200 + i)}
				}
				return resp
			},
		)
		prevWAL := streaming.WAL()
		streaming.SetWALForTest(mockWAL)
		defer streaming.SetWALForTest(prevWAL)

		dr := deleteRunner{
			queue:           queue.dmQueue,
			chMgr:           mockMgr,
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs:    &schemapb.IDs{},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		mockMgr.EXPECT().getChannels(collectionID).Return(channels, nil)

		// StorageV3 off -> no count probe; go straight to the complexDelete (PK) stream.
		lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			return workload.Exec(ctx, 1, qn, channels[0])
		}).Once()
		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.QueryNode_QueryStreamClient, error) {
				client := streamrpc.NewLocalQueryClient(ctx)
				server := client.CreateServer()
				server.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
				})
				server.FinishSend(nil)
				return client, nil
			})

		require.NoError(t, dr.Run(ctx))
		assert.Equal(t, int64(3), dr.result.DeleteCnt)
		require.Len(t, capturedMsgs, 1)
		deleteMsg := message.MustAsMutableDeleteMessageV1(capturedMsgs[0])
		body := deleteMsg.MustBody()
		assert.NotNil(t, body.GetPrimaryKeys())
		assert.Empty(t, body.GetSerializedExprPlan())
	})

	t.Run("predicate delete count probe error falls back to pk delete", func(t *testing.T) {
		setPredicateDelete(t, true)
		setStorageV3(t, true)
		setPredicateDeleteThreshold(t, 1024)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)
		expr := "non_pk == 1"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		capturedMsgs := make([]message.MutableMessage, 0, 1)
		mockWAL := mock_streaming.NewMockWALAccesser(t)
		mockWAL.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, msgs ...message.MutableMessage) streaming.AppendResponses {
				capturedMsgs = append(capturedMsgs, msgs...)
				resp := types.NewAppendResponseN(len(msgs))
				for i := range resp.Responses {
					resp.Responses[i].AppendResult = &types.AppendResult{TimeTick: uint64(200 + i)}
				}
				return resp
			},
		)
		prevWAL := streaming.WAL()
		streaming.SetWALForTest(mockWAL)
		defer streaming.SetWALForTest(prevWAL)

		dr := deleteRunner{
			queue:           queue.dmQueue,
			chMgr:           mockMgr,
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs:    &schemapb.IDs{},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		mockMgr.EXPECT().getChannels(collectionID).Return(channels, nil)

		call := 0
		lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			call++
			return workload.Exec(ctx, 1, qn, channels[0])
		}).Twice()
		// First query = count probe -> transient error; the delete must fall back to the
		// PK-delete stream instead of surfacing the error to the user.
		qn.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, req *querypb.QueryRequest, opts ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
				require.Equal(t, 1, call)
				return nil, errors.New("mock count error")
			})
		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.QueryNode_QueryStreamClient, error) {
				require.Equal(t, 2, call)
				client := streamrpc.NewLocalQueryClient(ctx)
				server := client.CreateServer()
				server.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
				})
				server.FinishSend(nil)
				return client, nil
			})

		require.NoError(t, dr.Run(ctx))
		assert.Equal(t, int64(3), dr.result.DeleteCnt)
		require.Len(t, capturedMsgs, 1)
		deleteMsg := message.MustAsMutableDeleteMessageV1(capturedMsgs[0])
		body := deleteMsg.MustBody()
		assert.NotNil(t, body.GetPrimaryKeys())
		assert.Empty(t, body.GetSerializedExprPlan())
	})

	t.Run("predicate delete falls back to pk delete for unsupported expression", func(t *testing.T) {
		setPredicateDelete(t, true)
		setStorageV3(t, true)
		setPredicateDeleteThreshold(t, 1024)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)
		// Arithmetic predicates are not in the predicate-delete allowlist.
		expr := "non_pk % 2 == 0"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)
		require.False(t, canUsePredicateDelete(plan, schema.CollectionSchema))

		capturedMsgs := make([]message.MutableMessage, 0, 1)
		mockWAL := mock_streaming.NewMockWALAccesser(t)
		mockWAL.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, msgs ...message.MutableMessage) streaming.AppendResponses {
				capturedMsgs = append(capturedMsgs, msgs...)
				resp := types.NewAppendResponseN(len(msgs))
				for i := range resp.Responses {
					resp.Responses[i].AppendResult = &types.AppendResult{TimeTick: uint64(200 + i)}
				}
				return resp
			},
		)
		prevWAL := streaming.WAL()
		streaming.SetWALForTest(mockWAL)
		defer streaming.SetWALForTest(prevWAL)

		dr := deleteRunner{
			queue:           queue.dmQueue,
			chMgr:           mockMgr,
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs:    &schemapb.IDs{},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		mockMgr.EXPECT().getChannels(collectionID).Return(channels, nil)

		lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			return workload.Exec(ctx, 1, qn, channels[0])
		})
		// No count probe: the unsupported expression goes straight to the PK-delete stream.
		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) (querypb.QueryNode_QueryStreamClient, error) {
				client := streamrpc.NewLocalQueryClient(ctx)
				server := client.CreateServer()
				server.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
				})
				server.FinishSend(nil)
				return client, nil
			})

		require.NoError(t, dr.Run(ctx))
		assert.Equal(t, int64(3), dr.result.DeleteCnt)
		require.Len(t, capturedMsgs, 1)
		deleteMsg := message.MustAsMutableDeleteMessageV1(capturedMsgs[0])
		body := deleteMsg.MustBody()
		assert.NotNil(t, body.GetPrimaryKeys())
		assert.Empty(t, body.GetSerializedExprPlan())
	})

	t.Run("predicate delete unsupported expression fallback failed", func(t *testing.T) {
		setPredicateDelete(t, true)
		setStorageV3(t, true)
		setPredicateDeleteThreshold(t, 1024)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)
		expr := "non_pk % 2 == 0"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		dr := deleteRunner{
			queue:           queue.dmQueue,
			chMgr:           NewMockChannelsMgr(t),
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs:    &schemapb.IDs{},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			return workload.Exec(ctx, 1, qn, channels[0])
		})
		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).Return(nil, errors.New("mock stream error"))

		require.Error(t, dr.Run(ctx))
	})

	t.Run("predicate delete count probe canceled context", func(t *testing.T) {
		setPredicateDelete(t, true)
		setStorageV3(t, true)
		setPredicateDeleteThreshold(t, 1024)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)
		expr := "non_pk == 1"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		dr := deleteRunner{
			queue:           queue.dmQueue,
			chMgr:           NewMockChannelsMgr(t),
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs:    &schemapb.IDs{},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			return workload.Exec(ctx, 1, qn, channels[0])
		})
		// The count probe fails because the request context is canceled; the runner must
		// surface the cancellation instead of falling back to the PK-delete stream.
		qn.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, req *querypb.QueryRequest, opts ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
				cancel()
				return nil, context.Canceled
			})

		assert.ErrorIs(t, dr.Run(ctx), context.Canceled)
	})

	t.Run("predicate delete count probe error fallback failed", func(t *testing.T) {
		setPredicateDelete(t, true)
		setStorageV3(t, true)
		setPredicateDeleteThreshold(t, 1024)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)
		expr := "non_pk == 1"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		dr := deleteRunner{
			queue:           queue.dmQueue,
			chMgr:           NewMockChannelsMgr(t),
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs:    &schemapb.IDs{},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		call := 0
		lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			call++
			return workload.Exec(ctx, 1, qn, channels[0])
		}).Twice()
		qn.EXPECT().Query(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, req *querypb.QueryRequest, opts ...grpc.CallOption) (*internalpb.RetrieveResults, error) {
				require.Equal(t, 1, call)
				return nil, errors.New("mock count error")
			})
		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).Return(nil, errors.New("mock stream error"))

		require.Error(t, dr.Run(ctx))
	})

	t.Run("predicate delete low hit fallback failed", func(t *testing.T) {
		setPredicateDelete(t, true)
		setStorageV3(t, true)
		setPredicateDeleteThreshold(t, 1024)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)
		expr := "non_pk == 1"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		dr := deleteRunner{
			queue:           queue.dmQueue,
			chMgr:           NewMockChannelsMgr(t),
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs:    &schemapb.IDs{},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			return workload.Exec(ctx, 1, qn, channels[0])
		}).Twice()
		qn.EXPECT().Query(mock.Anything, mock.Anything).Return(funcutil.WrapCntToInternalResult(1), nil)
		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).Return(nil, errors.New("mock stream error"))

		require.Error(t, dr.Run(ctx))
	})

	t.Run("predicate delete wal append failed", func(t *testing.T) {
		setPredicateDelete(t, true)
		setStorageV3(t, true)
		setPredicateDeleteThreshold(t, 1024)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)
		expr := "non_pk == 1"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		mockWAL := mock_streaming.NewMockWALAccesser(t)
		mockWAL.EXPECT().AppendMessages(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, msgs ...message.MutableMessage) streaming.AppendResponses {
				resp := types.NewAppendResponseN(len(msgs))
				resp.FillAllError(errors.New("mock wal error"))
				return resp
			},
		)
		prevWAL := streaming.WAL()
		streaming.SetWALForTest(mockWAL)
		defer streaming.SetWALForTest(prevWAL)

		dr := deleteRunner{
			queue:           queue.dmQueue,
			chMgr:           NewMockChannelsMgr(t),
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs:    &schemapb.IDs{},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		lb.EXPECT().Execute(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			return workload.Exec(ctx, 1, qn, channels[0])
		})
		qn.EXPECT().Query(mock.Anything, mock.Anything).Return(funcutil.WrapCntToInternalResult(2048), nil)

		require.Error(t, dr.Run(ctx))
	})

	t.Run("predicate delete tso alloc failed", func(t *testing.T) {
		setPredicateDelete(t, true)
		setStorageV3(t, true)

		mockTSO := mockey.Mock((*mockTsoAllocator).AllocOne).Return(uint64(0), errors.New("mock tso error")).Build()
		defer mockTSO.UnPatch()

		expr := "non_pk == 1"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		dr := deleteRunner{
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs:    &schemapb.IDs{},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}

		require.Error(t, dr.Run(context.Background()))
	})

	t.Run("complex delete success", func(t *testing.T) {
		setPredicateDelete(t, false)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)
		expr := "pk < 3"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		dr := deleteRunner{
			queue:           queue.dmQueue,
			chMgr:           mockMgr,
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{partitionID},
			vChannels:       channels,
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				PartitionName:  partitionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		mockMgr.EXPECT().getChannels(collectionID).Return(channels, nil)
		lb.EXPECT().Execute(mock.Anything, mock.Anything).Call.Return(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			return workload.Exec(ctx, 1, qn, "")
		})

		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).Call.Return(
			func(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) querypb.QueryNode_QueryStreamClient {
				client := streamrpc.NewLocalQueryClient(ctx)
				server := client.CreateServer()

				server.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{0, 1, 2},
							},
						},
					},
				})
				server.FinishSend(nil)
				return client
			}, nil)
		assert.NoError(t, dr.Run(ctx))
		assert.Equal(t, int64(3), dr.result.DeleteCnt)
	})

	schema.Fields[1].IsPartitionKey = true
	partitionMaps := make(map[string]int64)
	partitionMaps["test_0"] = 1
	partitionMaps["test_1"] = 2
	partitionMaps["test_2"] = 3

	t.Run("complex delete with partitionKey mode success", func(t *testing.T) {
		setPredicateDelete(t, false)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		mockMgr := NewMockChannelsMgr(t)
		qn := mocks.NewMockQueryNodeClient(t)
		lb := shardclient.NewMockLBPolicy(t)

		mockCache := NewMockCache(t)
		mockCache.EXPECT().GetCollectionID(mock.Anything, dbName, collectionName).Return(collectionID, nil).Maybe()
		globalMetaCache = mockCache
		defer func() { globalMetaCache = metaCache }()
		expr := "non_pk in [2, 3]"
		plan, err := planparserv2.CreateRetrievePlan(schema.schemaHelper, expr, nil)
		require.NoError(t, err)

		dr := deleteRunner{
			queue:           queue.dmQueue,
			chMgr:           mockMgr,
			schema:          schema,
			collectionID:    collectionID,
			partitionIDs:    []int64{common.AllPartitionsID},
			vChannels:       channels,
			idAllocator:     idAllocator,
			tsoAllocatorIns: tsoAllocator,
			lb:              lb,
			result: &milvuspb.MutationResult{
				Status: merr.Success(),
				IDs: &schemapb.IDs{
					IdField: nil,
				},
			},
			req: &milvuspb.DeleteRequest{
				CollectionName: collectionName,
				DbName:         dbName,
				Expr:           expr,
			},
			plan: plan,
		}
		mockMgr.EXPECT().getChannels(collectionID).Return(channels, nil)
		lb.EXPECT().Execute(mock.Anything, mock.Anything).Call.Return(func(ctx context.Context, workload shardclient.CollectionWorkLoad) error {
			return workload.Exec(ctx, 1, qn, "")
		})

		qn.EXPECT().QueryStream(mock.Anything, mock.Anything).Call.Return(
			func(ctx context.Context, in *querypb.QueryRequest, opts ...grpc.CallOption) querypb.QueryNode_QueryStreamClient {
				client := streamrpc.NewLocalQueryClient(ctx)
				server := client.CreateServer()
				assert.Greater(t, len(in.Req.PartitionIDs), 0)
				server.Send(&internalpb.RetrieveResults{
					Status: merr.Success(),
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{0, 1, 2},
							},
						},
					},
				})
				server.FinishSend(nil)
				return client
			}, nil)

		assert.NoError(t, dr.Run(ctx))
		assert.Equal(t, int64(3), dr.result.DeleteCnt)
	})
}
