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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const validSequenceMatch = `SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), ` +
	`STEP($[sub_str] == "cat"), ` +
	`STEP($[sub_str] == @0[sub_str] && $[sub_int] > 1, WITHIN($[sub_int], @0[sub_int], 0, 1000)))`

func TestSequenceMatchPlan(t *testing.T) {
	helper := newTestSchemaHelper(t)
	expr, err := ParseExpr(helper, validSequenceMatch, nil)
	require.NoError(t, err)
	sequence := expr.GetSequenceMatchExpr()
	require.NotNil(t, sequence)
	require.Equal(t, "struct_array", sequence.GetStructName())
	require.Equal(t, int64(structSubIntFieldID), sequence.GetOrderTimeFieldId())
	require.Equal(t, int64(structSubStrFieldID), sequence.GetTieFieldId())
	require.Len(t, sequence.GetSteps(), 2)
	require.Nil(t, sequence.Steps[0].GetWindow())
	require.Equal(t, &planpb.SequenceWindow{
		CurrentFieldId: int64(structSubIntFieldID),
		PriorStepIndex: 0,
		PriorFieldId:   int64(structSubIntFieldID),
		MinMs:          0,
		MaxMs:          1000,
	}, sequence.Steps[1].GetWindow())
	comparison := sequence.Steps[1].GetPredicate().GetBinaryExpr().GetLeft().GetCompareExpr()
	require.NotNil(t, comparison)
	require.Nil(t, comparison.GetLeftColumnInfo().SequenceStepIndex)
	require.NotNil(t, comparison.GetRightColumnInfo().SequenceStepIndex)
	require.Equal(t, uint32(0), comparison.GetRightColumnInfo().GetSequenceStepIndex())
	require.True(t, comparison.GetRightColumnInfo().GetIsElementLevel())

	combined, err := ParseExpr(helper, `Int64Field > 0 && `+validSequenceMatch, nil)
	require.NoError(t, err)
	require.NotNil(t, combined.GetBinaryExpr())
}

func TestSequenceMatchRejectsInvalidReferences(t *testing.T) {
	helper := newTestSchemaHelper(t)
	cases := []string{
		`@0[sub_str] == "cat"`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_str], $[sub_int]), STEP($[sub_str] == "cat"), STEP($[sub_int] > 1, WITHIN($[sub_int], @0[sub_int], 0, 1000)))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] == @0[sub_str]), STEP($[sub_int] > 1, WITHIN($[sub_int], @0[sub_int], 0, 1000)))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] == "cat"), STEP($[sub_str] == @1[sub_str], WITHIN($[sub_int], @0[sub_int], 0, 1000)))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] == "cat"), STEP($[sub_str] == @0[missing], WITHIN($[sub_int], @0[sub_int], 0, 1000)))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] == "cat"), STEP($[sub_int] > 1))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] == "cat", WITHIN($[sub_int], @0[sub_int], 0, 1000)), STEP($[sub_int] > 1, WITHIN($[sub_int], @0[sub_int], 0, 1000)))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] == "cat"), STEP($[sub_int] > 1, WITHIN($[sub_int], @0[sub_int], 1000, 0)))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] == "cat"), STEP($[sub_int] > 1, WITHIN($[sub_str], @0[sub_int], 0, 1000)))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP(not ($[sub_str] == "cat")), STEP($[sub_int] > 1, WITHIN($[sub_int], @0[sub_int], 0, 1000)))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] like "cat%"), STEP($[sub_int] > 1, WITHIN($[sub_int], @0[sub_int], 0, 1000)))`,
		`SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] == "cat"))`,
	}
	for _, input := range cases {
		_, err := ParseExpr(helper, input, nil)
		require.Error(t, err, input)
	}
}

func TestSequenceMatchTemplateFill(t *testing.T) {
	helper := newTestSchemaHelper(t)
	expr, err := ParseExprTemplate(helper, `SEQUENCE_MATCH(struct_array, ORDER_BY($[sub_int], $[sub_str]), STEP($[sub_str] == {tag}), STEP($[sub_int] > 1, WITHIN($[sub_int], @0[sub_int], 0, 1000)))`, nil)
	require.NoError(t, err)
	require.True(t, expr.GetIsTemplate())
	require.Error(t, FillExpressionValue(expr, nil))
	require.NoError(t, FillExpressionValue(expr, map[string]*planpb.GenericValue{"tag": NewString("cat")}))
	require.Equal(t, "cat", expr.GetSequenceMatchExpr().GetSteps()[0].GetPredicate().GetUnaryRangeExpr().GetValue().GetStringVal())
}

func TestSequenceMatchWithPhraseFilter(t *testing.T) {
	schema := newTestSchema(false)
	enableMatch(schema)
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	expr, err := ParseExpr(helper, `PHRASE_MATCH(VarCharField, "black cat") && `+validSequenceMatch, nil)
	require.NoError(t, err)
	require.NotNil(t, expr.GetBinaryExpr())
}
