package planparserv2

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
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
