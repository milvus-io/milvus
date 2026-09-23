// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planparserv2

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/stretchr/testify/require"
)

func TestSequenceMatchParser(t *testing.T) {
	helper, err := typeutil.CreateSchemaHelper(newTestSchema(true))
	require.NoError(t, err)

	filter := `Int64Field > 0 && SEQUENCE_MATCH(struct_array,
		ORDER_BY($[sub_int], $[sub_str]),
		STEP($[sub_str] == "cat"),
		STEP($[sub_str] == @0[sub_str] && $[sub_int] > 0,
			WITHIN($[sub_int], @0[sub_int], 0, 1000)))`
	expr, err := ParseExpr(helper, filter, nil)
	require.NoError(t, err)
	sequence := findSequenceMatch(expr)
	require.NotNil(t, sequence)
	require.Equal(t, "struct_array", sequence.GetStructName())
	require.Len(t, sequence.GetSteps(), 2)
	require.Equal(t, int64(0), sequence.GetSteps()[1].GetWindow().GetMinMs())
	require.Equal(t, int64(1000), sequence.GetSteps()[1].GetWindow().GetMaxMs())
	require.True(t, containsSequenceStepRef(sequence.GetSteps()[1].GetPredicate(), 0))
}

func findSequenceMatch(expr *planpb.Expr) *planpb.SequenceMatchExpr {
	if expr == nil {
		return nil
	}
	if sequence := expr.GetSequenceMatchExpr(); sequence != nil {
		return sequence
	}
	if binary := expr.GetBinaryExpr(); binary != nil {
		if sequence := findSequenceMatch(binary.GetLeft()); sequence != nil {
			return sequence
		}
		return findSequenceMatch(binary.GetRight())
	}
	if unary := expr.GetUnaryExpr(); unary != nil {
		return findSequenceMatch(unary.GetChild())
	}
	return nil
}

func TestSequenceMatchParserRejectsInvalidBindings(t *testing.T) {
	helper, err := typeutil.CreateSchemaHelper(newTestSchema(true))
	require.NoError(t, err)

	invalid := []string{
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] == "cat"))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] == @0[sub_str]), STEP($[sub_int] > 0, WITHIN($[sub_int], @0[sub_int], 0, 1000)))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] == "cat"), STEP($[sub_str] == @1[sub_str], WITHIN($[sub_int], @0[sub_int], 0, 1000)))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] == "cat"), STEP($[sub_int] > 0))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] == "cat", WITHIN($[sub_int], @0[sub_int], 0, 1000)), STEP($[sub_int] > 0, WITHIN($[sub_int], @0[sub_int], 0, 1000)))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] == "cat"), STEP($[sub_int] > 0, WITHIN($[sub_int], @0[sub_int], 1001, 1000)))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_str], $[sub_int]), STEP($[sub_str] == "cat"), STEP($[sub_int] > 0, WITHIN($[sub_int], @0[sub_int], 0, 1000)))`,
	}
	for _, filter := range invalid {
		_, err := ParseExpr(helper, filter, nil)
		require.Error(t, err, filter)
	}
}

func TestSequenceMatchParserRejectsMixedWidthBinding(t *testing.T) {
	schema := newTestSchema(true)
	schema.StructArrayFields[0].Fields = append(schema.StructArrayFields[0].Fields,
		&schemapb.FieldSchema{
			FieldID: 99001, Name: "struct_array[sub_long]",
			DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
		})
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	_, err = ParseExpr(helper, `SEQUENCE_MATCH(struct_array,
		ORDER_BY($[sub_int], $[sub_str]),
		STEP($[sub_long] > 0),
		STEP($[sub_int] == @0[sub_long], WITHIN($[sub_int], @0[sub_int], 0, 1000)))`, nil)
	require.Error(t, err)
}

func containsSequenceStepRef(expr *planpb.Expr, step uint32) bool {
	if expr == nil {
		return false
	}
	if cmp := expr.GetCompareExpr(); cmp != nil {
		for _, col := range []*planpb.ColumnInfo{cmp.GetLeftColumnInfo(), cmp.GetRightColumnInfo()} {
			if col != nil && col.SequenceStepIndex != nil && col.GetSequenceStepIndex() == step {
				return true
			}
		}
	}
	if binary := expr.GetBinaryExpr(); binary != nil {
		return containsSequenceStepRef(binary.GetLeft(), step) || containsSequenceStepRef(binary.GetRight(), step)
	}
	if unary := expr.GetUnaryExpr(); unary != nil {
		return containsSequenceStepRef(unary.GetChild(), step)
	}
	return false
}
