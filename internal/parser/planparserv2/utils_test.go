package planparserv2

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func Test_relationalCompatible(t *testing.T) {
	type args struct {
		t1 schemapb.DataType
		t2 schemapb.DataType
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			// both.
			args: args{
				t1: schemapb.DataType_VarChar,
				t2: schemapb.DataType_VarChar,
			},
			want: true,
		},
		{
			// neither.
			args: args{
				t1: schemapb.DataType_Float,
				t2: schemapb.DataType_Float,
			},
			want: true,
		},
		{
			// in-compatible.
			args: args{
				t1: schemapb.DataType_Float,
				t2: schemapb.DataType_VarChar,
			},
			want: false,
		},
		{
			// in-compatible.
			args: args{
				t1: schemapb.DataType_VarChar,
				t2: schemapb.DataType_Float,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := relationalCompatible(tt.args.t1, tt.args.t2); got != tt.want {
				t.Errorf("relationalCompatible() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsAlwaysTruePlan(t *testing.T) {
	type args struct {
		plan *planpb.PlanNode
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{
				plan: nil,
			},
			want: false,
		},
		{
			args: args{
				plan: &planpb.PlanNode{
					Node: &planpb.PlanNode_VectorAnns{
						VectorAnns: &planpb.VectorANNS{
							Predicates: alwaysTrueExpr(),
						},
					},
				},
			},
			want: true,
		},
		{
			args: args{
				plan: &planpb.PlanNode{
					Node: &planpb.PlanNode_Predicates{
						Predicates: alwaysTrueExpr(),
					},
				},
			},
			want: true,
		},
		{
			args: args{
				plan: &planpb.PlanNode{
					Node: &planpb.PlanNode_Query{
						Query: &planpb.QueryPlanNode{
							Predicates: alwaysTrueExpr(),
							IsCount:    false,
						},
					},
				},
			},
			want: true,
		},
		{
			args: args{
				plan: &planpb.PlanNode{
					Node: &planpb.PlanNode_Query{
						Query: &planpb.QueryPlanNode{
							Predicates: alwaysTrueExpr(),
							IsCount:    true,
						},
					},
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, IsAlwaysTruePlan(tt.args.plan), "IsAlwaysTruePlan(%v)", tt.args.plan)
		})
	}
}

func Test_canBeExecuted(t *testing.T) {
	type args struct {
		e *ExprWithType
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			args: args{
				e: &ExprWithType{
					dataType: schemapb.DataType_Int64,
				},
			},
			want: false,
		},
		{
			args: args{
				e: &ExprWithType{
					dataType:      schemapb.DataType_Bool,
					nodeDependent: true,
				},
			},
			want: false,
		},
		{
			args: args{
				e: &ExprWithType{
					dataType:      schemapb.DataType_Bool,
					nodeDependent: false,
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, canBeExecuted(tt.args.e), "canBeExecuted(%v)", tt.args.e)
		})
	}
}

func Test_convertEscapeSingle(t *testing.T) {
	type testCases struct {
		input    string
		expected string
	}
	normalCases := []testCases{
		{`"\'"`, `'`},
		{`"\\'"`, `\'`},
		{`"\\\'"`, `\'`},
		{`"\\\\'"`, `\\'`},
		{`"\\\\\'"`, `\\'`},
		{`'"'`, `"`},
		{`'\"'`, `"`},
		{`'\\"'`, `\"`},
		{`'\\\"'`, `\"`},
		{`'\\\\"'`, `\\"`},
		{`'\\\\\"'`, `\\"`},
	}
	for _, c := range normalCases {
		actual, err := convertEscapeSingle(c.input)
		assert.NoError(t, err)
		assert.Equal(t, c.expected, actual)
	}

	unNormalCases := []testCases{
		{`"\423"`, ``},
		{`'\378'`, ``},
	}
	for _, c := range unNormalCases {
		actual, err := convertEscapeSingle(c.input)
		assert.Error(t, err)
		assert.Equal(t, c.expected, actual)
	}
}
