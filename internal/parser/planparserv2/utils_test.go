package planparserv2

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
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
		{`'""'`, `""`},
		{`'"""'`, `"""`},
		{`'"\""'`, `"""`},
		{`'a"b\"c\\"d'`, `a"b"c\"d`},
		{`"a\"b\"c\\\"d"`, `a"b"c\"d`},
		{`'A "test"'`, `A "test"`},
		{`"A \"test\""`, `A "test"`},
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

func Test_prepareSpecialEscapeCharactersForConvertingEscapeSingle(t *testing.T) {
	escapeCharacters := map[byte]bool{'%': true, '_': true}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "escaped percent gets extra backslash",
			input:    `hello\%world`,
			expected: `hello\\%world`,
		},
		{
			name:     "escaped underscore gets extra backslash",
			input:    `hello\_world`,
			expected: `hello\\_world`,
		},
		{
			name:     "double backslash before percent stays unchanged",
			input:    `hello\\%world`,
			expected: `hello\\%world`,
		},
		{
			name:     "double backslash before underscore stays unchanged",
			input:    `hello\\_world`,
			expected: `hello\\_world`,
		},
		{
			name:     "regular backslash escape unchanged",
			input:    `hello\nworld`,
			expected: `hello\nworld`,
		},
		{
			name:     "no escape characters",
			input:    `hello world`,
			expected: `hello world`,
		},
		{
			name:     "empty string",
			input:    ``,
			expected: ``,
		},
		{
			name:     "only escaped percent",
			input:    `\%`,
			expected: `\\%`,
		},
		{
			name:     "only escaped underscore",
			input:    `\_`,
			expected: `\\_`,
		},
		{
			name:     "multiple escaped specials",
			input:    `\%foo\_bar\%`,
			expected: `\\%foo\\_bar\\%`,
		},
		{
			name:     "trailing backslash",
			input:    `hello\`,
			expected: `hello\`,
		},
		{
			name:     "triple backslash before percent",
			input:    `hello\\\%world`,
			expected: `hello\\\\%world`,
		},
		{
			name:     "percent without backslash unchanged",
			input:    `hello%world`,
			expected: `hello%world`,
		},
		{
			name:     "underscore without backslash unchanged",
			input:    `hello_world`,
			expected: `hello_world`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := prepareSpecialEscapeCharactersForConvertingEscapeSingle(tt.input, escapeCharacters)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func Test_canBeComparedDataType(t *testing.T) {
	type testCases struct {
		left     schemapb.DataType
		right    schemapb.DataType
		expected bool
	}

	cases := []testCases{
		{schemapb.DataType_Bool, schemapb.DataType_Bool, true},
		{schemapb.DataType_Bool, schemapb.DataType_JSON, true},
		{schemapb.DataType_Bool, schemapb.DataType_Int8, false},
		{schemapb.DataType_Bool, schemapb.DataType_Int16, false},
		{schemapb.DataType_Bool, schemapb.DataType_Int32, false},
		{schemapb.DataType_Bool, schemapb.DataType_Int64, false},
		{schemapb.DataType_Bool, schemapb.DataType_Float, false},
		{schemapb.DataType_Bool, schemapb.DataType_Double, false},
		{schemapb.DataType_Bool, schemapb.DataType_String, false},
		{schemapb.DataType_Int8, schemapb.DataType_Int16, true},
		{schemapb.DataType_Int16, schemapb.DataType_Int32, true},
		{schemapb.DataType_Int32, schemapb.DataType_Int64, true},
		{schemapb.DataType_Int64, schemapb.DataType_Float, true},
		{schemapb.DataType_Float, schemapb.DataType_Double, true},
		{schemapb.DataType_Double, schemapb.DataType_Int32, true},
		{schemapb.DataType_Double, schemapb.DataType_String, false},
		{schemapb.DataType_Int64, schemapb.DataType_String, false},
		{schemapb.DataType_Int64, schemapb.DataType_JSON, true},
		{schemapb.DataType_Double, schemapb.DataType_JSON, true},
		{schemapb.DataType_String, schemapb.DataType_Double, false},
		{schemapb.DataType_String, schemapb.DataType_Int64, false},
		{schemapb.DataType_String, schemapb.DataType_JSON, true},
		{schemapb.DataType_String, schemapb.DataType_String, true},
		{schemapb.DataType_String, schemapb.DataType_VarChar, true},
		{schemapb.DataType_VarChar, schemapb.DataType_VarChar, true},
		{schemapb.DataType_VarChar, schemapb.DataType_JSON, true},
		{schemapb.DataType_VarChar, schemapb.DataType_Int64, false},
		{schemapb.DataType_Array, schemapb.DataType_Int64, false},
		{schemapb.DataType_Array, schemapb.DataType_Array, false},
	}

	for _, c := range cases {
		assert.Equal(t, c.expected, canBeComparedDataType(c.left, c.right))
	}
}

func Test_getArrayElementType(t *testing.T) {
	t.Run("array element", func(t *testing.T) {
		expr := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ValueExpr{
					ValueExpr: &planpb.ValueExpr{
						Value: &planpb.GenericValue{
							Val: &planpb.GenericValue_ArrayVal{
								ArrayVal: &planpb.Array{
									Array:       nil,
									ElementType: schemapb.DataType_Int64,
								},
							},
						},
					},
				},
			},
			dataType:      schemapb.DataType_Array,
			nodeDependent: true,
		}

		assert.Equal(t, schemapb.DataType_Int64, getArrayElementType(expr))
	})

	t.Run("array field", func(t *testing.T) {
		expr := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ColumnExpr{
					ColumnExpr: &planpb.ColumnExpr{
						Info: &planpb.ColumnInfo{
							FieldId:        101,
							DataType:       schemapb.DataType_Array,
							IsPrimaryKey:   false,
							IsAutoID:       false,
							NestedPath:     nil,
							IsPartitionKey: false,
							ElementType:    schemapb.DataType_Int64,
						},
					},
				},
			},
			dataType:      schemapb.DataType_Array,
			nodeDependent: true,
		}

		assert.Equal(t, schemapb.DataType_Int64, getArrayElementType(expr))
	})

	t.Run("not array", func(t *testing.T) {
		expr := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ColumnExpr{
					ColumnExpr: &planpb.ColumnExpr{
						Info: &planpb.ColumnInfo{
							FieldId:  102,
							DataType: schemapb.DataType_String,
						},
					},
				},
			},
			dataType:      schemapb.DataType_String,
			nodeDependent: true,
		}

		assert.Equal(t, schemapb.DataType_None, getArrayElementType(expr))
	})
}

func Test_decodeUnicode(t *testing.T) {
	s1 := "A[\"\\u5e74\\u4efd\"][\"\\u6708\\u4efd\"]"

	assert.NotEqual(t, `A["年份"]["月份"]`, s1)
	assert.Equal(t, `A["年份"]["月份"]`, decodeUnicode(s1))
}

func Test_handleCompare(t *testing.T) {
	t.Run("normal field comparison", func(t *testing.T) {
		left := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ColumnExpr{
					ColumnExpr: &planpb.ColumnExpr{
						Info: &planpb.ColumnInfo{
							FieldId:  101,
							DataType: schemapb.DataType_Int64,
						},
					},
				},
			},
			dataType: schemapb.DataType_Int64,
		}

		right := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ColumnExpr{
					ColumnExpr: &planpb.ColumnExpr{
						Info: &planpb.ColumnInfo{
							FieldId:  102,
							DataType: schemapb.DataType_Int64,
						},
					},
				},
			},
			dataType: schemapb.DataType_Int64,
		}

		result, err := handleCompare(planpb.OpType_GreaterThan, left, right)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.NotNil(t, result.GetCompareExpr())
		assert.Equal(t, planpb.OpType_GreaterThan, result.GetCompareExpr().GetOp())
	})

	t.Run("left field is JSON type", func(t *testing.T) {
		left := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ColumnExpr{
					ColumnExpr: &planpb.ColumnExpr{
						Info: &planpb.ColumnInfo{
							FieldId:  101,
							DataType: schemapb.DataType_JSON,
						},
					},
				},
			},
			dataType: schemapb.DataType_JSON,
		}

		right := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ColumnExpr{
					ColumnExpr: &planpb.ColumnExpr{
						Info: &planpb.ColumnInfo{
							FieldId:  102,
							DataType: schemapb.DataType_Int64,
						},
					},
				},
			},
			dataType: schemapb.DataType_Int64,
		}

		result, err := handleCompare(planpb.OpType_GreaterThan, left, right)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "two column comparison with JSON type is not supported")
	})

	t.Run("right field is JSON type", func(t *testing.T) {
		left := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ColumnExpr{
					ColumnExpr: &planpb.ColumnExpr{
						Info: &planpb.ColumnInfo{
							FieldId:  101,
							DataType: schemapb.DataType_Int64,
						},
					},
				},
			},
			dataType: schemapb.DataType_Int64,
		}

		right := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ColumnExpr{
					ColumnExpr: &planpb.ColumnExpr{
						Info: &planpb.ColumnInfo{
							FieldId:  102,
							DataType: schemapb.DataType_JSON,
						},
					},
				},
			},
			dataType: schemapb.DataType_JSON,
		}

		result, err := handleCompare(planpb.OpType_GreaterThan, left, right)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "two column comparison with JSON type is not supported")
	})

	t.Run("both fields are JSON type", func(t *testing.T) {
		left := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ColumnExpr{
					ColumnExpr: &planpb.ColumnExpr{
						Info: &planpb.ColumnInfo{
							FieldId:  101,
							DataType: schemapb.DataType_JSON,
						},
					},
				},
			},
			dataType: schemapb.DataType_JSON,
		}

		right := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ColumnExpr{
					ColumnExpr: &planpb.ColumnExpr{
						Info: &planpb.ColumnInfo{
							FieldId:  102,
							DataType: schemapb.DataType_JSON,
						},
					},
				},
			},
			dataType: schemapb.DataType_JSON,
		}

		result, err := handleCompare(planpb.OpType_GreaterThan, left, right)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "two column comparison with JSON type is not supported")
	})

	t.Run("left field is nil", func(t *testing.T) {
		left := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ValueExpr{
					ValueExpr: &planpb.ValueExpr{
						Value: &planpb.GenericValue{
							Val: &planpb.GenericValue_Int64Val{
								Int64Val: 100,
							},
						},
					},
				},
			},
			dataType: schemapb.DataType_Int64,
		}

		right := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ColumnExpr{
					ColumnExpr: &planpb.ColumnExpr{
						Info: &planpb.ColumnInfo{
							FieldId:  102,
							DataType: schemapb.DataType_Int64,
						},
					},
				},
			},
			dataType: schemapb.DataType_Int64,
		}

		result, err := handleCompare(planpb.OpType_GreaterThan, left, right)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "only comparison between two fields is supported")
	})

	t.Run("right field is nil", func(t *testing.T) {
		left := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ColumnExpr{
					ColumnExpr: &planpb.ColumnExpr{
						Info: &planpb.ColumnInfo{
							FieldId:  101,
							DataType: schemapb.DataType_Int64,
						},
					},
				},
			},
			dataType: schemapb.DataType_Int64,
		}

		right := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ValueExpr{
					ValueExpr: &planpb.ValueExpr{
						Value: &planpb.GenericValue{
							Val: &planpb.GenericValue_Int64Val{
								Int64Val: 100,
							},
						},
					},
				},
			},
			dataType: schemapb.DataType_Int64,
		}

		result, err := handleCompare(planpb.OpType_GreaterThan, left, right)
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "only comparison between two fields is supported")
	})

	t.Run("template expression", func(t *testing.T) {
		left := &ExprWithType{
			expr: &planpb.Expr{
				IsTemplate: true,
				Expr: &planpb.Expr_ValueExpr{
					ValueExpr: &planpb.ValueExpr{
						Value: &planpb.GenericValue{
							Val: &planpb.GenericValue_Int64Val{
								Int64Val: 100,
							},
						},
						TemplateVariableName: "var1",
					},
				},
			},
			dataType: schemapb.DataType_Int64,
		}

		right := &ExprWithType{
			expr: &planpb.Expr{
				Expr: &planpb.Expr_ColumnExpr{
					ColumnExpr: &planpb.ColumnExpr{
						Info: &planpb.ColumnInfo{
							FieldId:  102,
							DataType: schemapb.DataType_Int64,
						},
					},
				},
			},
			dataType: schemapb.DataType_Int64,
		}

		result, err := handleCompare(planpb.OpType_GreaterThan, left, right)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.NotNil(t, result.GetUnaryRangeExpr())
		assert.Equal(t, planpb.OpType_GreaterThan, result.GetUnaryRangeExpr().GetOp())
		assert.Equal(t, "var1", result.GetUnaryRangeExpr().GetTemplateVariableName())
	})
}

// Test_toValueExpr tests the toValueExpr function which converts GenericValue to ExprWithType
// This tests all type branches including the nil return path for unknown types
func Test_toValueExpr(t *testing.T) {
	t.Run("bool value", func(t *testing.T) {
		// Test that bool values are correctly converted to Bool DataType
		value := NewBool(true)
		result := toValueExpr(value)
		assert.NotNil(t, result)
		assert.Equal(t, schemapb.DataType_Bool, result.dataType)
		assert.True(t, result.expr.GetValueExpr().GetValue().GetBoolVal())
	})

	t.Run("int64 value", func(t *testing.T) {
		// Test that int64 values are correctly converted to Int64 DataType
		value := NewInt(42)
		result := toValueExpr(value)
		assert.NotNil(t, result)
		assert.Equal(t, schemapb.DataType_Int64, result.dataType)
		assert.Equal(t, int64(42), result.expr.GetValueExpr().GetValue().GetInt64Val())
	})

	t.Run("float value", func(t *testing.T) {
		// Test that float values are correctly converted to Double DataType
		value := NewFloat(3.14)
		result := toValueExpr(value)
		assert.NotNil(t, result)
		assert.Equal(t, schemapb.DataType_Double, result.dataType)
		assert.Equal(t, 3.14, result.expr.GetValueExpr().GetValue().GetFloatVal())
	})

	t.Run("string value", func(t *testing.T) {
		// Test that string values are correctly converted to VarChar DataType
		value := NewString("hello")
		result := toValueExpr(value)
		assert.NotNil(t, result)
		assert.Equal(t, schemapb.DataType_VarChar, result.dataType)
		assert.Equal(t, "hello", result.expr.GetValueExpr().GetValue().GetStringVal())
	})

	t.Run("array value", func(t *testing.T) {
		// Test that array values are correctly converted to Array DataType
		value := &planpb.GenericValue{
			Val: &planpb.GenericValue_ArrayVal{
				ArrayVal: &planpb.Array{
					Array:       []*planpb.GenericValue{NewInt(1), NewInt(2)},
					ElementType: schemapb.DataType_Int64,
				},
			},
		}
		result := toValueExpr(value)
		assert.NotNil(t, result)
		assert.Equal(t, schemapb.DataType_Array, result.dataType)
	})

	t.Run("nil/unknown value type returns nil", func(t *testing.T) {
		// Test that unknown value types return nil - this covers the default branch
		value := &planpb.GenericValue{
			Val: nil, // nil Val should trigger default case
		}
		result := toValueExpr(value)
		assert.Nil(t, result)
	})
}

// Test_getTargetType tests type inference for binary operations
// This ensures correct type promotion rules are applied
func Test_getTargetType(t *testing.T) {
	tests := []struct {
		name        string
		left        schemapb.DataType
		right       schemapb.DataType
		expected    schemapb.DataType
		expectError bool
	}{
		{
			name:     "JSON with JSON returns JSON",
			left:     schemapb.DataType_JSON,
			right:    schemapb.DataType_JSON,
			expected: schemapb.DataType_JSON,
		},
		{
			name:     "JSON with Float returns Double",
			left:     schemapb.DataType_JSON,
			right:    schemapb.DataType_Float,
			expected: schemapb.DataType_Double,
		},
		{
			name:     "JSON with Int returns Int64",
			left:     schemapb.DataType_JSON,
			right:    schemapb.DataType_Int64,
			expected: schemapb.DataType_Int64,
		},
		{
			name:     "Geometry with Geometry returns Geometry",
			left:     schemapb.DataType_Geometry,
			right:    schemapb.DataType_Geometry,
			expected: schemapb.DataType_Geometry,
		},
		{
			name:     "Timestamptz with Timestamptz returns Timestamptz",
			left:     schemapb.DataType_Timestamptz,
			right:    schemapb.DataType_Timestamptz,
			expected: schemapb.DataType_Timestamptz,
		},
		{
			name:     "Float with JSON returns Double",
			left:     schemapb.DataType_Float,
			right:    schemapb.DataType_JSON,
			expected: schemapb.DataType_Double,
		},
		{
			name:     "Float with Int returns Double",
			left:     schemapb.DataType_Float,
			right:    schemapb.DataType_Int64,
			expected: schemapb.DataType_Double,
		},
		{
			name:     "Int with Float returns Double",
			left:     schemapb.DataType_Int64,
			right:    schemapb.DataType_Float,
			expected: schemapb.DataType_Double,
		},
		{
			name:     "Int with Int returns Int64",
			left:     schemapb.DataType_Int64,
			right:    schemapb.DataType_Int64,
			expected: schemapb.DataType_Int64,
		},
		{
			name:     "Int with JSON returns Int64",
			left:     schemapb.DataType_Int64,
			right:    schemapb.DataType_JSON,
			expected: schemapb.DataType_Int64,
		},
		{
			name:        "String with Int is incompatible",
			left:        schemapb.DataType_VarChar,
			right:       schemapb.DataType_Int64,
			expectError: true,
		},
		{
			name:        "Bool with Int is incompatible",
			left:        schemapb.DataType_Bool,
			right:       schemapb.DataType_Int64,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := getTargetType(tt.left, tt.right)
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "incompatible data type")
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// Test_reverseOrder tests the reverseOrder function which reverses comparison operators
// This is used when the operands of a comparison are swapped
func Test_reverseOrder(t *testing.T) {
	tests := []struct {
		name        string
		input       planpb.OpType
		expected    planpb.OpType
		expectError bool
	}{
		{
			name:     "LessThan reverses to GreaterThan",
			input:    planpb.OpType_LessThan,
			expected: planpb.OpType_GreaterThan,
		},
		{
			name:     "LessEqual reverses to GreaterEqual",
			input:    planpb.OpType_LessEqual,
			expected: planpb.OpType_GreaterEqual,
		},
		{
			name:     "GreaterThan reverses to LessThan",
			input:    planpb.OpType_GreaterThan,
			expected: planpb.OpType_LessThan,
		},
		{
			name:     "GreaterEqual reverses to LessEqual",
			input:    planpb.OpType_GreaterEqual,
			expected: planpb.OpType_LessEqual,
		},
		{
			name:     "Equal stays Equal",
			input:    planpb.OpType_Equal,
			expected: planpb.OpType_Equal,
		},
		{
			name:     "NotEqual stays NotEqual",
			input:    planpb.OpType_NotEqual,
			expected: planpb.OpType_NotEqual,
		},
		{
			name:        "Invalid op type returns error",
			input:       planpb.OpType_Invalid,
			expectError: true,
		},
		{
			name:        "PrefixMatch cannot be reversed",
			input:       planpb.OpType_PrefixMatch,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := reverseOrder(tt.input)
			if tt.expectError {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "cannot reverse order")
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// Test_isIntegerColumn tests the isIntegerColumn helper function
// This function checks if a column can be converted to integer type
func Test_isIntegerColumn(t *testing.T) {
	tests := []struct {
		name     string
		column   *planpb.ColumnInfo
		expected bool
	}{
		{
			name: "Int64 column is integer",
			column: &planpb.ColumnInfo{
				DataType: schemapb.DataType_Int64,
			},
			expected: true,
		},
		{
			name: "Int32 column is integer",
			column: &planpb.ColumnInfo{
				DataType: schemapb.DataType_Int32,
			},
			expected: true,
		},
		{
			name: "JSON column is integer (can contain integers)",
			column: &planpb.ColumnInfo{
				DataType: schemapb.DataType_JSON,
			},
			expected: true,
		},
		{
			name: "Array of Int64 is integer",
			column: &planpb.ColumnInfo{
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int64,
			},
			expected: true,
		},
		{
			name: "Timestamptz is integer",
			column: &planpb.ColumnInfo{
				DataType: schemapb.DataType_Timestamptz,
			},
			expected: true,
		},
		{
			name: "Float column is not integer",
			column: &planpb.ColumnInfo{
				DataType: schemapb.DataType_Float,
			},
			expected: false,
		},
		{
			name: "String column is not integer",
			column: &planpb.ColumnInfo{
				DataType: schemapb.DataType_VarChar,
			},
			expected: false,
		},
		{
			name: "Array of Float is not integer",
			column: &planpb.ColumnInfo{
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Float,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isIntegerColumn(tt.column)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Test_parseJSONValue tests JSON value parsing for various types
// This covers all branches including nested arrays and error cases
func Test_parseJSONValue(t *testing.T) {
	t.Run("parse integer from json.Number", func(t *testing.T) {
		// Test parsing integer values from JSON numbers
		value, dataType, err := parseJSONValue(json.Number("42"))
		assert.NoError(t, err)
		assert.Equal(t, schemapb.DataType_Int64, dataType)
		assert.Equal(t, int64(42), value.GetInt64Val())
	})

	t.Run("parse float from json.Number", func(t *testing.T) {
		// Test parsing float values from JSON numbers
		value, dataType, err := parseJSONValue(json.Number("3.14"))
		assert.NoError(t, err)
		assert.Equal(t, schemapb.DataType_Double, dataType)
		assert.Equal(t, 3.14, value.GetFloatVal())
	})

	t.Run("parse string", func(t *testing.T) {
		// Test parsing string values
		value, dataType, err := parseJSONValue("hello")
		assert.NoError(t, err)
		assert.Equal(t, schemapb.DataType_String, dataType)
		assert.Equal(t, "hello", value.GetStringVal())
	})

	t.Run("parse bool true", func(t *testing.T) {
		// Test parsing boolean true
		value, dataType, err := parseJSONValue(true)
		assert.NoError(t, err)
		assert.Equal(t, schemapb.DataType_Bool, dataType)
		assert.True(t, value.GetBoolVal())
	})

	t.Run("parse bool false", func(t *testing.T) {
		// Test parsing boolean false
		value, dataType, err := parseJSONValue(false)
		assert.NoError(t, err)
		assert.Equal(t, schemapb.DataType_Bool, dataType)
		assert.False(t, value.GetBoolVal())
	})

	t.Run("parse array of integers", func(t *testing.T) {
		// Test parsing arrays with same element types
		arr := []interface{}{json.Number("1"), json.Number("2"), json.Number("3")}
		value, dataType, err := parseJSONValue(arr)
		assert.NoError(t, err)
		assert.Equal(t, schemapb.DataType_Array, dataType)
		assert.True(t, value.GetArrayVal().GetSameType())
		assert.Equal(t, schemapb.DataType_Int64, value.GetArrayVal().GetElementType())
		assert.Len(t, value.GetArrayVal().GetArray(), 3)
	})

	t.Run("parse array of mixed types", func(t *testing.T) {
		// Test parsing arrays with mixed element types - sameType should be false
		arr := []interface{}{json.Number("1"), "hello", true}
		value, dataType, err := parseJSONValue(arr)
		assert.NoError(t, err)
		assert.Equal(t, schemapb.DataType_Array, dataType)
		assert.False(t, value.GetArrayVal().GetSameType())
		assert.Len(t, value.GetArrayVal().GetArray(), 3)
	})

	t.Run("parse empty array", func(t *testing.T) {
		// Test parsing empty arrays
		arr := []interface{}{}
		value, dataType, err := parseJSONValue(arr)
		assert.NoError(t, err)
		assert.Equal(t, schemapb.DataType_Array, dataType)
		assert.Len(t, value.GetArrayVal().GetArray(), 0)
	})

	t.Run("invalid json.Number", func(t *testing.T) {
		// Test that invalid numbers return error
		_, _, err := parseJSONValue(json.Number("not_a_number"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "couldn't convert it")
	})

	t.Run("unknown type returns error", func(t *testing.T) {
		// Test that unknown types return error
		_, _, err := parseJSONValue(struct{}{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unknown type")
	})

	t.Run("nested array with invalid element", func(t *testing.T) {
		// Test that arrays with invalid elements return error
		arr := []interface{}{struct{}{}}
		_, _, err := parseJSONValue(arr)
		assert.Error(t, err)
	})
}

// Test_checkValidPoint tests WKT point validation
// This ensures only valid POINT geometries are accepted
func Test_checkValidPoint(t *testing.T) {
	t.Run("valid point", func(t *testing.T) {
		// Valid POINT geometry should pass
		err := checkValidPoint("POINT(1 2)")
		assert.NoError(t, err)
	})

	t.Run("valid point with decimal", func(t *testing.T) {
		// Valid POINT with decimal coordinates should pass
		err := checkValidPoint("POINT(1.5 2.5)")
		assert.NoError(t, err)
	})

	t.Run("valid point with negative coordinates", func(t *testing.T) {
		// Valid POINT with negative coordinates should pass
		err := checkValidPoint("POINT(-1.5 -2.5)")
		assert.NoError(t, err)
	})

	t.Run("invalid WKT syntax", func(t *testing.T) {
		// Invalid WKT syntax should return error
		err := checkValidPoint("invalid")
		assert.Error(t, err)
	})

	t.Run("empty string", func(t *testing.T) {
		// Empty string should return error
		err := checkValidPoint("")
		assert.Error(t, err)
	})

	t.Run("point with extra spaces", func(t *testing.T) {
		// POINT with extra spaces should pass
		err := checkValidPoint("POINT( 1  2 )")
		assert.NoError(t, err)
	})
}

// Test_convertHanToASCII_FastPath tests the Chinese character to Unicode escape conversion
// This function has a fast path for ASCII-only strings to avoid allocation
func Test_convertHanToASCII_FastPath(t *testing.T) {
	t.Run("ASCII only string returns unchanged (fast path)", func(t *testing.T) {
		// ASCII-only strings should be returned without modification
		// This tests the fast path optimization
		input := "hello world 123"
		result := convertHanToASCII(input)
		assert.Equal(t, input, result)
	})

	t.Run("Chinese characters are converted", func(t *testing.T) {
		// Chinese characters should be converted to Unicode escapes
		input := "年份"
		result := convertHanToASCII(input)
		assert.NotEqual(t, input, result)
		assert.Contains(t, result, "\\u")
	})

	t.Run("mixed ASCII and Chinese", func(t *testing.T) {
		// Mixed strings should only convert Chinese characters
		input := "field年份"
		result := convertHanToASCII(input)
		assert.Contains(t, result, "field")
		assert.Contains(t, result, "\\u")
	})

	t.Run("string with escape sequence", func(t *testing.T) {
		// Escape sequences should be preserved
		input := "\\n"
		result := convertHanToASCII(input)
		assert.Equal(t, input, result)
	})

	t.Run("string with invalid escape returns original", func(t *testing.T) {
		// Invalid escape sequences trigger early return
		input := "\\x"
		result := convertHanToASCII(input)
		assert.Equal(t, input, result)
	})
}

// Test_canArithmetic tests arithmetic operation type compatibility
// This ensures proper type checking for arithmetic expressions
func Test_canArithmetic(t *testing.T) {
	tests := []struct {
		name         string
		left         schemapb.DataType
		leftElement  schemapb.DataType
		right        schemapb.DataType
		rightElement schemapb.DataType
		reverse      bool
		expectError  bool
	}{
		{
			name:  "Int64 with Int64",
			left:  schemapb.DataType_Int64,
			right: schemapb.DataType_Int64,
		},
		{
			name:  "Float with Float",
			left:  schemapb.DataType_Float,
			right: schemapb.DataType_Float,
		},
		{
			name:  "Float with Int64",
			left:  schemapb.DataType_Float,
			right: schemapb.DataType_Int64,
		},
		{
			name:  "JSON with Int64",
			left:  schemapb.DataType_JSON,
			right: schemapb.DataType_Int64,
		},
		{
			name:        "VarChar with Int64 is invalid",
			left:        schemapb.DataType_VarChar,
			right:       schemapb.DataType_Int64,
			expectError: true,
		},
		{
			name:        "Bool with Int64 is invalid",
			left:        schemapb.DataType_Bool,
			right:       schemapb.DataType_Int64,
			expectError: true,
		},
		{
			name:        "Array of Int64 with Int64",
			left:        schemapb.DataType_Array,
			leftElement: schemapb.DataType_Int64,
			right:       schemapb.DataType_Int64,
		},
		{
			name:    "reverse flag swaps operands",
			left:    schemapb.DataType_Int64,
			right:   schemapb.DataType_Float,
			reverse: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := canArithmetic(tt.left, tt.leftElement, tt.right, tt.rightElement, tt.reverse)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Test_checkValidModArith tests modulo operation validation
// Modulo can only be applied to integer types
func Test_checkValidModArith(t *testing.T) {
	t.Run("mod with integers is valid", func(t *testing.T) {
		err := checkValidModArith(planpb.ArithOpType_Mod,
			schemapb.DataType_Int64, schemapb.DataType_None,
			schemapb.DataType_Int64, schemapb.DataType_None)
		assert.NoError(t, err)
	})

	t.Run("mod with float left is invalid", func(t *testing.T) {
		err := checkValidModArith(planpb.ArithOpType_Mod,
			schemapb.DataType_Float, schemapb.DataType_None,
			schemapb.DataType_Int64, schemapb.DataType_None)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "modulo can only apply on integer types")
	})

	t.Run("mod with float right is invalid", func(t *testing.T) {
		err := checkValidModArith(planpb.ArithOpType_Mod,
			schemapb.DataType_Int64, schemapb.DataType_None,
			schemapb.DataType_Float, schemapb.DataType_None)
		assert.Error(t, err)
	})

	t.Run("add operation is always valid", func(t *testing.T) {
		// Non-mod operations should not be validated by this function
		err := checkValidModArith(planpb.ArithOpType_Add,
			schemapb.DataType_Float, schemapb.DataType_None,
			schemapb.DataType_Float, schemapb.DataType_None)
		assert.NoError(t, err)
	})
}

// Test_castRangeValue tests value casting for range operations
// This ensures proper type validation and conversion for range expressions
func Test_castRangeValue(t *testing.T) {
	t.Run("string value for string type", func(t *testing.T) {
		value := NewString("test")
		result, err := castRangeValue(schemapb.DataType_VarChar, value)
		assert.NoError(t, err)
		assert.Equal(t, "test", result.GetStringVal())
	})

	t.Run("non-string value for string type fails", func(t *testing.T) {
		value := NewInt(42)
		_, err := castRangeValue(schemapb.DataType_VarChar, value)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid range operations")
	})

	t.Run("bool type is invalid for range", func(t *testing.T) {
		value := NewBool(true)
		_, err := castRangeValue(schemapb.DataType_Bool, value)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid range operations on boolean expr")
	})

	t.Run("integer value for integer type", func(t *testing.T) {
		value := NewInt(42)
		result, err := castRangeValue(schemapb.DataType_Int64, value)
		assert.NoError(t, err)
		assert.Equal(t, int64(42), result.GetInt64Val())
	})

	t.Run("non-integer value for integer type fails", func(t *testing.T) {
		value := NewFloat(3.14)
		_, err := castRangeValue(schemapb.DataType_Int64, value)
		assert.Error(t, err)
	})

	t.Run("float value for float type", func(t *testing.T) {
		value := NewFloat(3.14)
		result, err := castRangeValue(schemapb.DataType_Float, value)
		assert.NoError(t, err)
		assert.Equal(t, 3.14, result.GetFloatVal())
	})

	t.Run("integer value promoted to float for float type", func(t *testing.T) {
		// Integer values should be promoted to float when target type is float
		value := NewInt(42)
		result, err := castRangeValue(schemapb.DataType_Double, value)
		assert.NoError(t, err)
		assert.Equal(t, float64(42), result.GetFloatVal())
	})

	t.Run("non-number value for float type fails", func(t *testing.T) {
		value := NewString("test")
		_, err := castRangeValue(schemapb.DataType_Float, value)
		assert.Error(t, err)
	})
}

// Test_hexDigit tests the hexDigit helper function
// This is used for Unicode escape encoding
func Test_hexDigit(t *testing.T) {
	// Test digits 0-9
	for i := uint32(0); i < 10; i++ {
		result := hexDigit(i)
		expected := byte(i) + '0'
		assert.Equal(t, expected, result, "hexDigit(%d) should be %c", i, expected)
	}

	// Test hex digits a-f
	for i := uint32(10); i < 16; i++ {
		result := hexDigit(i)
		expected := byte(i-10) + 'a'
		assert.Equal(t, expected, result, "hexDigit(%d) should be %c", i, expected)
	}

	// Test that only lower 4 bits are used
	result := hexDigit(0x1f) // 31 & 0xf = 15 = 'f'
	assert.Equal(t, byte('f'), result)
}

// Test_formatUnicode tests Unicode escape formatting
func Test_formatUnicode(t *testing.T) {
	// Test basic Chinese character
	result := formatUnicode(0x5e74) // '年'
	assert.Equal(t, "\\u5e74", result)

	// Test ASCII character
	result = formatUnicode(0x0041) // 'A'
	assert.Equal(t, "\\u0041", result)
}

// Test_isEscapeCh tests escape character detection
func Test_isEscapeCh(t *testing.T) {
	escapeChs := []uint8{'\\', 'n', 't', 'r', 'f', '"', '\''}
	for _, ch := range escapeChs {
		assert.True(t, isEscapeCh(ch), "isEscapeCh(%c) should be true", ch)
	}

	nonEscapeChs := []uint8{'a', 'b', '1', ' ', 'x'}
	for _, ch := range nonEscapeChs {
		assert.False(t, isEscapeCh(ch), "isEscapeCh(%c) should be false", ch)
	}
}

// Test_isEmptyExpression_Utils tests empty expression detection
func Test_isEmptyExpression_Utils(t *testing.T) {
	assert.True(t, isEmptyExpression(""))
	assert.True(t, isEmptyExpression("   "))
	assert.True(t, isEmptyExpression("\t\n"))
	assert.False(t, isEmptyExpression("a > 1"))
	assert.False(t, isEmptyExpression("  a > 1  "))
}

// Test_checkValidWKT tests WKT validation
func Test_checkValidWKT(t *testing.T) {
	t.Run("valid point", func(t *testing.T) {
		err := checkValidWKT("POINT(1 2)")
		assert.NoError(t, err)
	})

	t.Run("valid polygon", func(t *testing.T) {
		err := checkValidWKT("POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")
		assert.NoError(t, err)
	})

	t.Run("invalid WKT", func(t *testing.T) {
		err := checkValidWKT("invalid geometry")
		assert.Error(t, err)
	})
}

func TestParseISO8601Duration(t *testing.T) {
	testCases := []struct {
		name      string
		input     string
		expected  *planpb.Interval
		expectErr bool
	}{
		{
			name:  "Full duration",
			input: "P1Y2M3DT4H5M6S",
			expected: &planpb.Interval{
				Years:   1,
				Months:  2,
				Days:    3,
				Hours:   4,
				Minutes: 5,
				Seconds: 6,
			},
			expectErr: false,
		},
		{
			name:  "Date part only",
			input: "P3Y6M4D",
			expected: &planpb.Interval{
				Years:  3,
				Months: 6,
				Days:   4,
			},
			expectErr: false,
		},
		{
			name:  "Time part only",
			input: "PT10H30M15S",
			expected: &planpb.Interval{
				Hours:   10,
				Minutes: 30,
				Seconds: 15,
			},
			expectErr: false,
		},
		{
			name:  "handle 0",
			input: "P0D",
			expected: &planpb.Interval{
				Days: 0,
			},
		},
		{
			name:      "Ambiguous M for Month",
			input:     "P2M",
			expected:  &planpb.Interval{Months: 2},
			expectErr: false,
		},
		{
			name:      "Ambiguous M for Minute",
			input:     "PT2M",
			expected:  &planpb.Interval{Minutes: 2},
			expectErr: false,
		},
		{
			name:      "Mixed date and time with missing parts",
			input:     "P1DT12H",
			expected:  &planpb.Interval{Days: 1, Hours: 12},
			expectErr: false,
		},
		{
			name:      "Only P (valid empty duration)",
			input:     "P",
			expected:  &planpb.Interval{},
			expectErr: false,
		},
		{
			name:      "Only PT (valid empty time part)",
			input:     "PT",
			expected:  &planpb.Interval{},
			expectErr: false,
		},
		{
			name:      "Invalid format - no P prefix",
			input:     "1Y2M",
			expected:  nil,
			expectErr: true,
		},
		{
			name:      "Invalid format - unknown character",
			input:     "P1Y2X",
			expected:  nil,
			expectErr: true,
		},
		{
			name:      "Invalid format - time part without T",
			input:     "P1H",
			expected:  nil,
			expectErr: true,
		},
		{
			name:      "Invalid format - empty string",
			input:     "",
			expected:  nil,
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := parseISODuration(tc.input)
			if tc.expectErr {
				if err == nil {
					t.Errorf("expected an error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("did not expect an error but got: %v", err)
			}
			if !reflect.DeepEqual(actual, tc.expected) {
				t.Errorf("result mismatch:\nexpected: %+v\nactual:   %+v", tc.expected, actual)
			}
		})
	}
}
