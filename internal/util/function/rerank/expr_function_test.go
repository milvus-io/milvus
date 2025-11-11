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

package rerank

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
)

func TestExprFunctionExpressions(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}},
			},
			{FieldID: 103, Name: "quality_score", DataType: schemapb.DataType_Float},
			{FieldID: 104, Name: "created_at", DataType: schemapb.DataType_Int64},
		},
	}

	expressions := map[string]string{
		"Simple":      "score",
		"Arithmetic":  "score * 1.5 + 0.1",
		"FieldAccess": "score * fields[\"quality_score\"]",
		"MathFunc":    "score * sqrt(fields[\"quality_score\"])",
		"Conditional": "fields[\"quality_score\"] > 50 ? score * 2.0 : score",
		"Complex": `let age_days = (1704067200000 - fields["created_at"]) / 86400000;
			let recency = exp(-0.005 * age_days);
			let quality = fields["quality_score"] / 100.0;
			score * recency * quality`,
	}

	for name, exprCode := range expressions {
		t.Run(name, func(t *testing.T) {
			functionSchema := &schemapb.FunctionSchema{
				Name:            "test",
				Type:            schemapb.FunctionType_Rerank,
				InputFieldNames: []string{"quality_score", "created_at"},
				Params: []*commonpb.KeyValuePair{
					{Key: reranker, Value: ExprName},
					{Key: ExprCodeKey, Value: exprCode},
				},
			}

			funcScores := &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{functionSchema}}
			f, err := NewFunctionScore(schema, funcScores)
			require.NoError(t, err)
			require.Equal(t, ExprName, f.RerankName())

			nq := int64(1)
			data := embedding.GenSearchResultData(nq, 3, schemapb.DataType_Int64, "noExist", 0)
			data.FieldsData = []*schemapb.FieldData{
				testutils.GenerateScalarFieldDataWithValue(schemapb.DataType_Float, "quality_score", 103, []float32{10.0, 60.0, 80.0}),
				testutils.GenerateScalarFieldDataWithValue(schemapb.DataType_Int64, "created_at", 104, []int64{1704067200000, 1704067200000, 1704067200000}),
			}

			searchData := &milvuspb.SearchResults{Results: data}
			ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 0, -1, -1, 1, false, "", []string{"COSINE"}), []*milvuspb.SearchResults{searchData})
			require.NoError(t, err)
			assert.Equal(t, int64(3), ret.Results.TopK)
			assert.Equal(t, int64(len(ret.Results.Scores)), ret.Results.TopK)
		})
	}
}
