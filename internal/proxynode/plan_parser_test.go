package proxynode

import (
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/stretchr/testify/assert"
	"testing"
)


func newTestSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "test",
		Description: "schema for test used",
		AutoID:      true,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 0, Name: "FieldID", IsPrimaryKey: false, Description: "field no.1", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "vectorField", IsPrimaryKey: false, Description: "field no.2", DataType: schemapb.DataType_FloatVector},
			{FieldID: 100, Name: "int64Field", IsPrimaryKey: false, Description: "field no.1", DataType: schemapb.DataType_Int64},
		},
	}
}

func TestParseQueryExpr_Naive(t *testing.T) {
	exprStr := "int64Field > 3"
	schema := newTestSchema()
	exprProto, err := parseQueryExpr(schema, exprStr)
	assert.Nil(t, err)
	str := proto.MarshalTextString(exprProto)
	println(str)
}

func TestParsePlanNode_Naive(t *testing.T) {
	exprStr := "int64Field > 3"
	schema := newTestSchema()
	queryInfo := &planpb.QueryInfo{
		Topk: 10,
		FieldId: 101,
		MetricType: "L2",
		SearchParams: "{\"nprobe\": 10}",
	}
	planProto, err := CreateQueryPlan(schema, exprStr, queryInfo)
	assert.Nil(t, err)
	dbgStr := proto.MarshalTextString(planProto)
	println(dbgStr)
}