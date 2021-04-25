package proxynode

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/stretchr/testify/assert"
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
	exprProto, err := parseQueryExpr(schema, &exprStr)
	assert.Nil(t, err)
	str := proto.MarshalTextString(exprProto)
	println(str)
}

func TestParsePlanNode_Naive(t *testing.T) {
	exprStr := "int64Field > 3"
	schema := newTestSchema()
	queryInfo := &planpb.QueryInfo{
		Topk:         10,
		MetricType:   "L2",
		SearchParams: "{\"nprobe\": 10}",
	}

	// Note: use pointer to string to represent nullable string
	// TODO: change it to better solution
	planProto, err := CreateQueryPlan(schema, &exprStr, "vectorField", queryInfo)

	assert.Nil(t, err)
	dbgStr := proto.MarshalTextString(planProto)
	println(dbgStr)
}
