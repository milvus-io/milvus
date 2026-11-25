/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"context"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const mmrTestField = "vec"

// mmrFloatDF builds a DataFrame with one chunk per query from float vectors.
func mmrFloatDF(t *testing.T, alloc memory.Allocator, ids []int64, scores []float32, vecs [][]float32, topks []int64) *DataFrame {
	t.Helper()
	dim := int64(len(vecs[0]))
	flat := make([]float32, 0, len(vecs)*int(dim))
	for _, v := range vecs {
		flat = append(flat, v...)
	}
	return mmrDF(t, alloc, ids, scores, topks, &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: mmrTestField,
		FieldId:   102,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  dim,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: flat}},
		}},
	})
}

func mmrDF(t *testing.T, alloc memory.Allocator, ids []int64, scores []float32, topks []int64, field *schemapb.FieldData) *DataFrame {
	t.Helper()
	resultData := &schemapb.SearchResultData{
		NumQueries: int64(len(topks)),
		TopK:       topks[0],
		Topks:      topks,
		Scores:     scores,
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}},
		FieldsData: []*schemapb.FieldData{field},
	}
	df, err := FromSearchResultData(resultData, alloc, testDataFrameInputPlan(resultData, field.GetFieldName()))
	require.NoError(t, err)
	return df
}

func mmrIDs(t *testing.T, df *DataFrame) [][]int64 {
	t.Helper()
	col := df.Column(types.IDFieldName)
	out := make([][]int64, df.NumChunks())
	for c := range df.NumChunks() {
		arr := col.Chunk(c).(*array.Int64)
		out[c] = append([]int64{}, arr.Int64Values()...)
	}
	return out
}

func runMMR(t *testing.T, alloc memory.Allocator, cfg MMRConfig, df *DataFrame) *DataFrame {
	t.Helper()
	op, err := NewMMROp(cfg)
	require.NoError(t, err)
	fc := NewFuncChainWithAllocator(alloc).SetStage(types.StageL2Rerank)
	fc.Add(op)
	out, err := fc.Execute(df)
	require.NoError(t, err)
	return out
}

// referenceMMR is an independent float64 implementation of greedy MMR over
// cosine similarity with relevance (1+score)/2, used to cross-check MMROp.
func referenceMMR(scores []float32, vecs [][]float32, lambda float64, k int) []int {
	n := len(scores)
	cos := func(a, b []float32) float64 {
		var dot, na, nb float64
		for i := range a {
			dot += float64(a[i]) * float64(b[i])
			na += float64(a[i]) * float64(a[i])
			nb += float64(b[i]) * float64(b[i])
		}
		return (1 + dot/math.Sqrt(na*nb)) / 2
	}
	chosen := make([]bool, n)
	var picked []int
	for len(picked) < k {
		best, bestScore := -1, math.Inf(-1)
		for i := 0; i < n; i++ {
			if chosen[i] {
				continue
			}
			red := 0.0
			for _, p := range picked {
				red = math.Max(red, cos(vecs[i], vecs[p]))
			}
			s := lambda*(1+float64(scores[i]))/2 - (1-lambda)*red
			if s > bestScore {
				best, bestScore = i, s
			}
		}
		chosen[best] = true
		picked = append(picked, best)
	}
	return picked
}

func TestMMROp_HandComputedExample(t *testing.T) {
	paramtable.Init()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// A and A2 are near duplicates; B is orthogonal; C sits between them.
	ids := []int64{1, 2, 3, 4}
	scores := []float32{0.9, 0.89, 0.7, 0.6}
	vecs := [][]float32{{1, 0}, {0.99, 0.14}, {0, 1}, {0.7, 0.7}}
	df := mmrFloatDF(t, alloc, ids, scores, vecs, []int64{4})
	defer df.Release()

	out := runMMR(t, alloc, MMRConfig{
		Field: mmrTestField, Lambda: 0.5, CandidateSize: 4,
		Similarity: "COSINE", RelevanceMetric: "COSINE", ScoreDescending: true,
	}, df)
	defer out.Release()

	// A first (most relevant); B beats A2 because A2 duplicates A.
	assert.Equal(t, [][]int64{{1, 3, 2, 4}}, mmrIDs(t, out))

	// Scores are the original relevance scores, not MMR scores.
	sc := out.Column(types.ScoreFieldName).Chunk(0).(*array.Float32).Float32Values()
	assert.Equal(t, []float32{0.9, 0.7, 0.89, 0.6}, sc)
	// The vector column passes through the selection.
	assert.True(t, out.HasColumn(mmrTestField))
}

func TestMMROp_LambdaOneKeepsRelevanceOrder(t *testing.T) {
	paramtable.Init()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// Input deliberately not sorted: MMR orders the window by $score first.
	ids := []int64{10, 11, 12, 13, 14}
	scores := []float32{0.3, 0.9, 0.5, 0.8, 0.1}
	vecs := [][]float32{{1, 0}, {1, 0}, {1, 0}, {1, 0}, {1, 0}}
	df := mmrFloatDF(t, alloc, ids, scores, vecs, []int64{5})
	defer df.Release()

	out := runMMR(t, alloc, MMRConfig{
		Field: mmrTestField, Lambda: 1, CandidateSize: 5, Similarity: "COSINE", ScoreDescending: true,
	}, df)
	defer out.Release()
	assert.Equal(t, [][]int64{{11, 13, 12, 10, 14}}, mmrIDs(t, out))
}

func TestMMROp_AscendingScoresL2(t *testing.T) {
	paramtable.Init()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// L2 distances: smaller is better. Rows 1 and 2 are identical vectors.
	ids := []int64{1, 2, 3}
	scores := []float32{0.1, 0.11, 0.5}
	vecs := [][]float32{{0, 0}, {0, 0}, {3, 4}}
	df := mmrFloatDF(t, alloc, ids, scores, vecs, []int64{3})
	defer df.Release()

	out := runMMR(t, alloc, MMRConfig{
		Field: mmrTestField, Lambda: 0.5, CandidateSize: 3,
		Similarity: "L2", RelevanceMetric: "L2", ScoreDescending: false,
	}, df)
	defer out.Release()
	assert.Equal(t, [][]int64{{1, 3, 2}}, mmrIDs(t, out))
}

func TestMMROp_MatchesReferenceAndIsPrefixStable(t *testing.T) {
	paramtable.Init()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	r := rand.New(rand.NewSource(42))
	const n, dim = 40, 8
	ids := make([]int64, n)
	scores := make([]float32, n)
	vecs := make([][]float32, n)
	for i := range ids {
		ids[i] = int64(100 + i)
		scores[i] = float32(1 - 0.02*float64(i) - 0.001*r.Float64())
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(r.NormFloat64())
		}
		vecs[i] = v
	}
	df := mmrFloatDF(t, alloc, ids, scores, vecs, []int64{n})
	defer df.Release()

	full := runMMR(t, alloc, MMRConfig{
		Field: mmrTestField, Lambda: 0.4, CandidateSize: n,
		Similarity: "COSINE", RelevanceMetric: "COSINE", ScoreDescending: true,
	}, df)
	defer full.Release()
	fullIDs := mmrIDs(t, full)[0]

	ref := referenceMMR(scores, vecs, 0.4, n)
	refIDs := make([]int64, n)
	for i, p := range ref {
		refIDs[i] = ids[p]
	}
	assert.Equal(t, refIDs, fullIDs)

	for _, k := range []int64{1, 5, 17, n} {
		part := runMMR(t, alloc, MMRConfig{
			Field: mmrTestField, Lambda: 0.4, CandidateSize: n, SelectCount: k,
			Similarity: "COSINE", RelevanceMetric: "COSINE", ScoreDescending: true,
		}, df)
		assert.Equal(t, fullIDs[:k], mmrIDs(t, part)[0], "k=%d", k)
		part.Release()
	}
}

func TestMMROp_WindowAndChunks(t *testing.T) {
	paramtable.Init()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// Two queries; the window keeps only the top 2 rows of each.
	ids := []int64{1, 2, 3, 4, 5, 6}
	scores := []float32{0.9, 0.8, 0.7, 0.5, 0.6, 0.4}
	vecs := [][]float32{{1, 0}, {0, 1}, {1, 1}, {1, 0}, {0, 1}, {1, 1}}
	df := mmrFloatDF(t, alloc, ids, scores, vecs, []int64{3, 3})
	defer df.Release()

	out := runMMR(t, alloc, MMRConfig{
		Field: mmrTestField, Lambda: 0.5, CandidateSize: 2, Similarity: "COSINE", ScoreDescending: true,
	}, df)
	defer out.Release()
	assert.Equal(t, [][]int64{{1, 2}, {5, 4}}, mmrIDs(t, out))
}

func TestMMROp_NullVectorsAreNeverRedundant(t *testing.T) {
	paramtable.Init()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// Row 2 has a null vector (compacted payload holds only valid rows).
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: mmrTestField,
		FieldId:   102,
		ValidData: []bool{true, false, true},
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim:  2,
			Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 0, 1, 0}}},
		}},
	}
	df := mmrDF(t, alloc, []int64{1, 2, 3}, []float32{0.9, 0.5, 0.85}, []int64{3}, field)
	defer df.Release()

	out := runMMR(t, alloc, MMRConfig{
		Field: mmrTestField, Lambda: 0.5, CandidateSize: 3,
		Similarity: "COSINE", RelevanceMetric: "COSINE", ScoreDescending: true,
	}, df)
	defer out.Release()
	// Row 3 duplicates row 1; the null row 2 has no redundancy penalty.
	assert.Equal(t, [][]int64{{1, 2, 3}}, mmrIDs(t, out))
}

func TestMMROp_AllNullVectorsWithoutDim(t *testing.T) {
	paramtable.Init()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	// Every candidate's vector is null and the payload carries no dim.
	field := &schemapb.FieldData{
		Type:      schemapb.DataType_FloatVector,
		FieldName: mmrTestField,
		FieldId:   102,
		ValidData: []bool{false, false},
		Field:     &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{}},
	}
	df := mmrDF(t, alloc, []int64{1, 2}, []float32{0.5, 0.9}, []int64{2}, field)
	defer df.Release()

	out := runMMR(t, alloc, MMRConfig{
		Field: mmrTestField, Lambda: 0.5, CandidateSize: 2, Similarity: "COSINE", ScoreDescending: true,
	}, df)
	defer out.Release()
	assert.Equal(t, [][]int64{{2, 1}}, mmrIDs(t, out))
}

func TestMMROp_SimilarityBudget(t *testing.T) {
	paramtable.Init()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := mmrFloatDF(t, alloc, []int64{1, 2, 3}, []float32{0.9, 0.8, 0.7},
		[][]float32{{1, 0}, {0, 1}, {1, 1}}, []int64{3})
	defer df.Release()

	op, err := NewMMROp(MMRConfig{
		Field: mmrTestField, Lambda: 0.5, CandidateSize: 3, Similarity: "COSINE",
		ScoreDescending: true, MaxSimilarityOps: 1,
	})
	require.NoError(t, err)
	fc := NewFuncChainWithAllocator(alloc).SetStage(types.StageL2Rerank)
	fc.Add(op)
	_, err = fc.Execute(df)
	assert.ErrorContains(t, err, "maxSimilarityOps")

	// The window costs 3 pairs * dim 2 = 6 operations, whatever the page size,
	// so every page of a paging session is accepted or rejected alike.
	for _, selectCount := range []int64{1, 3} {
		for budget, ok := range map[int64]bool{5: false, 6: true} {
			op, err := NewMMROp(MMRConfig{
				Field: mmrTestField, Lambda: 0.5, CandidateSize: 3, SelectCount: selectCount, Similarity: "COSINE",
				ScoreDescending: true, MaxSimilarityOps: budget,
			})
			require.NoError(t, err)
			fc := NewFuncChainWithAllocator(alloc).SetStage(types.StageL2Rerank)
			fc.Add(op)
			out, err := fc.Execute(df)
			if ok {
				require.NoError(t, err)
				out.Release()
			} else {
				assert.Error(t, err)
			}
		}
	}
}

func TestMMRSimilarity_WindowOps(t *testing.T) {
	dense := &mmrSimilarity{dataType: schemapb.DataType_FloatVector, dim: 8}
	assert.Equal(t, int64(3*8), dense.windowOps(make([]mmrVector, 3)))

	sparse := &mmrSimilarity{dataType: schemapb.DataType_SparseFloatVector, dim: 100000}
	rows := []mmrVector{{sparseIdx: make([]uint32, 2)}, {sparseIdx: make([]uint32, 3)}, {sparseIdx: make([]uint32, 5)}}
	// Pairs walk (2+3)+(2+5)+(3+5) = 20 non-zeros, plus one per pair; not the vocabulary size.
	assert.Equal(t, int64(23), sparse.windowOps(rows))

	array := &mmrSimilarity{dataType: schemapb.DataType_FloatVector, array: true, dim: 4}
	lists := []mmrVector{{elems: make([]mmrVector, 1)}, {elems: make([]mmrVector, 2)}, {elems: make([]mmrVector, 3)}}
	// Both directions: 2 * (1*2 + 1*3 + 2*3) = 22 element pairs, times dim.
	assert.Equal(t, int64(22*4), array.windowOps(lists))
}

func TestNewMMROp_Validation(t *testing.T) {
	paramtable.Init()
	base := MMRConfig{Field: mmrTestField, Lambda: 0.5, CandidateSize: 10, Similarity: "COSINE"}
	_, err := NewMMROp(base)
	assert.NoError(t, err)

	cases := map[string]func(c *MMRConfig){
		"no field":        func(c *MMRConfig) { c.Field = "" },
		"lambda negative": func(c *MMRConfig) { c.Lambda = -0.1 },
		"lambda > 1":      func(c *MMRConfig) { c.Lambda = 1.1 },
		"lambda NaN":      func(c *MMRConfig) { c.Lambda = math.NaN() },
		"window zero":     func(c *MMRConfig) { c.CandidateSize = 0 },
		"window over max": func(c *MMRConfig) {
			c.CandidateSize = paramtable.Get().FunctionCfg.MMRMaxCandidateSize.GetAsInt64() + 1
		},
		"negative limit": func(c *MMRConfig) { c.SelectCount = -1 },
		"no similarity":  func(c *MMRConfig) { c.Similarity = "" },
	}
	for name, mutate := range cases {
		cfg := base
		mutate(&cfg)
		_, err := NewMMROp(cfg)
		assert.Error(t, err, name)
	}
}

// =============================================================================
// Similarity per vector type
// =============================================================================

func mmrSim(t *testing.T, dataType, elemType schemapb.DataType, dim int64, metricType string, a, b []byte) float64 {
	t.Helper()
	s, err := newMMRSimilarity(dataType, elemType, dim, metricType)
	require.NoError(t, err)
	va, err := s.decode(a, false)
	require.NoError(t, err)
	vb, err := s.decode(b, false)
	require.NoError(t, err)
	return s.similarity(&va, &vb)
}

func TestMMRSimilarity_VectorTypes(t *testing.T) {
	same := []float32{1, 2, 3, 4}
	orth := []float32{-2, 1, -4, 3}

	t.Run("float cosine", func(t *testing.T) {
		a, b := typeutil.Float32ArrayToBytes(same), typeutil.Float32ArrayToBytes(orth)
		assert.InDelta(t, 1.0, mmrSim(t, schemapb.DataType_FloatVector, 0, 4, "COSINE", a, a), 1e-6)
		assert.InDelta(t, 0.5, mmrSim(t, schemapb.DataType_FloatVector, 0, 4, "COSINE", a, b), 1e-6)
	})
	t.Run("float ip and l2", func(t *testing.T) {
		a := typeutil.Float32ArrayToBytes([]float32{1, 0, 0, 0})
		b := typeutil.Float32ArrayToBytes([]float32{0, 1, 0, 0})
		assert.InDelta(t, 0.5+math.Atan(1)/math.Pi, mmrSim(t, schemapb.DataType_FloatVector, 0, 4, "IP", a, a), 1e-6)
		assert.InDelta(t, 1.0, mmrSim(t, schemapb.DataType_FloatVector, 0, 4, "L2", a, a), 1e-6)
		// squared L2 distance is 2.
		assert.InDelta(t, 1-2*math.Atan(2)/math.Pi, mmrSim(t, schemapb.DataType_FloatVector, 0, 4, "L2", a, b), 1e-6)
	})
	t.Run("float16 and bfloat16", func(t *testing.T) {
		for _, tc := range []struct {
			dt    schemapb.DataType
			bytes func([]float32) []byte
		}{
			{schemapb.DataType_Float16Vector, typeutil.Float32ArrayToFloat16Bytes},
			{schemapb.DataType_BFloat16Vector, typeutil.Float32ArrayToBFloat16Bytes},
		} {
			a, b := tc.bytes(same), tc.bytes(orth)
			assert.InDelta(t, 1.0, mmrSim(t, tc.dt, 0, 4, "COSINE", a, a), 1e-3, tc.dt.String())
			assert.InDelta(t, 0.5, mmrSim(t, tc.dt, 0, 4, "COSINE", a, b), 1e-3, tc.dt.String())
		}
	})
	t.Run("int8", func(t *testing.T) {
		a := []byte{1, 2, 3, 4}
		neg := []byte{0xff, 0xfe, 0xfd, 0xfc} // -1 -2 -3 -4
		assert.InDelta(t, 1.0, mmrSim(t, schemapb.DataType_Int8Vector, 0, 4, "COSINE", a, a), 1e-6)
		assert.InDelta(t, 0.0, mmrSim(t, schemapb.DataType_Int8Vector, 0, 4, "COSINE", a, neg), 1e-6)
	})
	t.Run("binary", func(t *testing.T) {
		a := []byte{0b11110000}
		b := []byte{0b11000000}
		// Hamming distance 2 of 8 bits; Jaccard distance 1 - 2/4.
		assert.InDelta(t, 0.75, mmrSim(t, schemapb.DataType_BinaryVector, 0, 8, "HAMMING", a, b), 1e-9)
		assert.InDelta(t, 0.5, mmrSim(t, schemapb.DataType_BinaryVector, 0, 8, "JACCARD", a, b), 1e-9)
		assert.InDelta(t, 1.0, mmrSim(t, schemapb.DataType_BinaryVector, 0, 8, "JACCARD", a, a), 1e-9)
	})
	t.Run("sparse", func(t *testing.T) {
		a := typeutil.CreateSparseFloatRow([]uint32{1, 5}, []float32{3, 4})
		b := typeutil.CreateSparseFloatRow([]uint32{2, 7}, []float32{1, 1})
		c := typeutil.CreateSparseFloatRow([]uint32{5, 9}, []float32{1, 0})
		for _, m := range []string{"COSINE"} {
			assert.InDelta(t, 1.0, mmrSim(t, schemapb.DataType_SparseFloatVector, 0, 0, m, a, a), 1e-6, m)
			assert.InDelta(t, 0.5, mmrSim(t, schemapb.DataType_SparseFloatVector, 0, 0, m, a, b), 1e-6, m)
			// cos = 4/5
			assert.InDelta(t, 0.9, mmrSim(t, schemapb.DataType_SparseFloatVector, 0, 0, m, a, c), 1e-6, m)
		}
		assert.InDelta(t, 0.5+math.Atan(4)/math.Pi, mmrSim(t, schemapb.DataType_SparseFloatVector, 0, 0, "IP", a, c), 1e-6)
	})
	t.Run("array of vector maxsim", func(t *testing.T) {
		row := func(vs ...[]float32) []byte {
			flat := []float32{}
			for _, v := range vs {
				flat = append(flat, v...)
			}
			return typeutil.Float32ArrayToBytes(flat)
		}
		a := row([]float32{1, 0}, []float32{0, 1})
		b := row([]float32{1, 0})
		// a->b: (1 + 0.5)/2 = 0.75; b->a: 1. Symmetric average 0.875.
		assert.InDelta(t, 0.875, mmrSim(t, schemapb.DataType_ArrayOfVector, schemapb.DataType_FloatVector, 2, "MAX_SIM_COSINE", a, b), 1e-6)
		assert.InDelta(t, 1.0, mmrSim(t, schemapb.DataType_ArrayOfVector, schemapb.DataType_FloatVector, 2, "MAX_SIM", a, a), 1e-6)
	})
}

func TestMMRSimilarity_UnsupportedMetrics(t *testing.T) {
	for _, tc := range []struct {
		dt, elem schemapb.DataType
		metric   string
	}{
		{schemapb.DataType_FloatVector, 0, "JACCARD"},
		{schemapb.DataType_BinaryVector, 0, "COSINE"},
		{schemapb.DataType_BinaryVector, 0, "MHJACCARD"},
		{schemapb.DataType_BinaryVector, 0, "SUBSTRUCTURE"},
		{schemapb.DataType_SparseFloatVector, 0, "L2"},
		{schemapb.DataType_SparseFloatVector, 0, "BM25"},
		{schemapb.DataType_ArrayOfVector, schemapb.DataType_FloatVector, "MAX_SIM_HAMMING"},
		{schemapb.DataType_ArrayOfVector, schemapb.DataType_BinaryVector, "MAX_SIM_COSINE"},
	} {
		_, err := newMMRSimilarity(tc.dt, tc.elem, 8, tc.metric)
		assert.ErrorContains(t, err, "similarity", "%s/%s", tc.dt, tc.metric)
	}
}

// =============================================================================
// Vector column import / export
// =============================================================================

func TestVectorColumn_ImportPickAndPruneBeforeExport(t *testing.T) {
	paramtable.Init()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	df := mmrFloatDF(t, alloc, []int64{1, 2, 3}, []float32{0.9, 0.8, 0.7},
		[][]float32{{1, 2}, {3, 4}, {5, 6}}, []int64{3})
	defer df.Release()

	meta := df.fieldMetadata[mmrTestField]
	require.NotNil(t, meta.field)
	assert.Equal(t, schemapb.DataType_FloatVector, meta.field.GetDataType())
	dim, err := typeutil.GetDim(meta.field)
	require.NoError(t, err)
	assert.Equal(t, int64(2), dim)

	fc := NewFuncChainWithAllocator(alloc).SetStage(types.StageL2Rerank)
	fc.Sort(types.ScoreFieldName, false, types.IDFieldName)
	out, err := fc.Execute(df)
	require.NoError(t, err)
	defer out.Release()

	cell := out.Column(mmrTestField).Chunk(0).(*array.Binary).Value(0)
	assert.Equal(t, typeutil.Float32ArrayToBytes([]float32{5, 6}), cell)

	// The proxy executes rerank chains with liveness pruning, so the vector
	// column is dropped after MMR, its last reader, and never reaches export.
	mmr, err := NewMMROp(MMRConfig{
		Field: mmrTestField, Lambda: 0.5, CandidateSize: 3, Similarity: "COSINE", ScoreDescending: true,
	})
	require.NoError(t, err)
	pruned := NewFuncChainWithAllocator(alloc).SetStage(types.StageL2Rerank)
	pruned.Add(mmr)
	res, err := pruned.ExecuteWithOptions(context.Background(), ExecuteOptions{
		EnableColumnPruning: true,
		SystemColumnPolicy:  SystemColumnPolicy{KeepAllSystemColumns: true},
	}, df)
	require.NoError(t, err)
	defer res.Release()
	assert.False(t, res.HasColumn(mmrTestField))
	exported, err := ToSearchResultDataWithOptions(res, nil)
	require.NoError(t, err)
	assert.Empty(t, exported.GetFieldsData())
	assert.Len(t, exported.GetIds().GetIntId().GetData(), 3)
}

func TestVectorColumn_AllTypesImport(t *testing.T) {
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)

	fields := []*schemapb.FieldData{
		{Type: schemapb.DataType_Float16Vector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 2, Data: &schemapb.VectorField_Float16Vector{Float16Vector: make([]byte, 8)},
		}}},
		{Type: schemapb.DataType_BFloat16Vector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 2, Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: make([]byte, 8)},
		}}},
		{Type: schemapb.DataType_Int8Vector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 2, Data: &schemapb.VectorField_Int8Vector{Int8Vector: make([]byte, 4)},
		}}},
		{Type: schemapb.DataType_BinaryVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 8, Data: &schemapb.VectorField_BinaryVector{BinaryVector: []byte{1, 2}},
		}}},
		{Type: schemapb.DataType_SparseFloatVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 10, Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{
				Dim: 10,
				Contents: [][]byte{
					typeutil.CreateSparseFloatRow([]uint32{1}, []float32{1}),
					typeutil.CreateSparseFloatRow([]uint32{2}, []float32{2}),
				},
			}},
		}}},
		{Type: schemapb.DataType_ArrayOfVector, Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 2, Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
				Dim: 2, ElementType: schemapb.DataType_FloatVector,
				Data: []*schemapb.VectorField{
					{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}}},
					{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{3, 4, 5, 6}}}},
				},
			}},
		}}},
	}
	for _, fd := range fields {
		fd.FieldName = mmrTestField
		fd.FieldId = 102
		df := mmrDF(t, alloc, []int64{1, 2}, []float32{0.9, 0.8}, []int64{2}, fd)
		col := df.Column(mmrTestField)
		require.NotNil(t, col, fd.Type.String())
		assert.Equal(t, 2, col.Len(), fd.Type.String())
		meta := df.fieldMetadata[mmrTestField]
		assert.Equal(t, fd.Type, meta.field.GetDataType())
		if fd.Type == schemapb.DataType_ArrayOfVector {
			assert.Equal(t, schemapb.DataType_FloatVector, meta.field.GetElementType())
			cell := col.Chunk(0).(*array.Binary).Value(1)
			assert.Equal(t, typeutil.Float32ArrayToBytes([]float32{3, 4, 5, 6}), cell)
		}
		df.Release()
	}

	// Payload that does not match the row count is rejected.
	bad := &schemapb.FieldData{
		Type: schemapb.DataType_FloatVector, FieldName: mmrTestField, FieldId: 102,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2, 3}}},
		}},
	}
	resultData := &schemapb.SearchResultData{
		NumQueries: 1, TopK: 2, Topks: []int64{2}, Scores: []float32{1, 0.5},
		Ids:        &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1, 2}}}},
		FieldsData: []*schemapb.FieldData{bad},
	}
	_, err := FromSearchResultData(resultData, alloc, testDataFrameInputPlan(resultData, mmrTestField))
	assert.Error(t, err)
}

// =============================================================================
// FunctionScore builder
// =============================================================================

func mmrCollSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "mmr",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "ts", DataType: schemapb.DataType_Int64},
			{
				FieldID: 102, Name: mmrTestField, DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			},
			{
				FieldID: 103, Name: "vec2", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			},
		},
	}
}

func mmrFunction(params map[string]string, inputs ...string) *schemapb.FunctionSchema {
	kvs := []*commonpb.KeyValuePair{{Key: "reranker", Value: "mmr"}}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		kvs = append(kvs, &commonpb.KeyValuePair{Key: k, Value: params[k]})
	}
	if len(inputs) == 0 {
		inputs = []string{mmrTestField}
	}
	return &schemapb.FunctionSchema{Type: schemapb.FunctionType_Rerank, Params: kvs, InputFieldNames: inputs}
}

func mmrSearchParams(limit, offset int64) *SearchParams {
	p := NewSearchParams(1, limit, offset, -1)
	p.AnnsFields = []string{mmrTestField}
	return p
}

// mmrPagingFixture: 10 candidates ordered by score; ids 1-5 form one tight
// cluster and ids 6-10 another.
func mmrPagingFixture() ([]int64, []float32, [][]float32) {
	ids := make([]int64, 10)
	scores := make([]float32, 10)
	vecs := make([][]float32, 10)
	for i := range ids {
		ids[i] = int64(i + 1)
		scores[i] = 0.95 - 0.01*float32(i)
		if i >= 5 {
			vecs[i] = []float32{0.01 * float32(i), 1}
		} else {
			vecs[i] = []float32{1, 0.01 * float32(i)}
		}
	}
	return ids, scores, vecs
}

func TestBuildMMRRerankChain_PagingSlicesFixedWindow(t *testing.T) {
	paramtable.Init()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	ids, scores, vecs := mmrPagingFixture()

	run := func(limit, offset int64) []int64 {
		fs := &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{
			mmrFunction(map[string]string{"candidate_size": "8", "lambda": "0.3"}),
		}}
		fc, err := BuildRerankChain(mmrCollSchema(), fs, []string{"COSINE"}, mmrSearchParams(limit, offset), alloc)
		require.NoError(t, err)
		df := mmrFloatDF(t, alloc, ids, scores, vecs, []int64{10})
		defer df.Release()
		out, err := fc.Execute(df)
		require.NoError(t, err)
		defer out.Release()
		return mmrIDs(t, out)[0]
	}

	full := run(8, 0)
	require.Len(t, full, 8)
	// Only the window (top 8 by score) is ever returned.
	for _, id := range full {
		assert.LessOrEqual(t, id, int64(8))
	}
	// The best row of the second cluster is promoted to second place.
	assert.Equal(t, []int64{1, 6}, full[:2])

	assert.Equal(t, full[0:3], run(3, 0))
	assert.Equal(t, full[3:6], run(3, 3))
	assert.Equal(t, full[6:8], run(3, 6), "short page past the window")
	assert.Empty(t, run(3, 8), "empty page at the window end")
	assert.Empty(t, run(3, 20), "empty page beyond the window")
}

func TestBuildMMRRerankChain_ComposesWithUpstream(t *testing.T) {
	paramtable.Init()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer alloc.AssertSize(t, 0)
	ids, scores, vecs := mmrPagingFixture()

	rrf := &schemapb.FunctionSchema{Type: schemapb.FunctionType_Rerank, Params: []*commonpb.KeyValuePair{
		{Key: "reranker", Value: "rrf"},
	}}
	fs := &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{rrf, mmrFunction(map[string]string{"candidate_size": "10"})}}
	params := NewSearchParams(1, 4, 0, -1)
	params.AnnsFields = []string{mmrTestField, "vec2"}
	fc, err := BuildRerankChain(mmrCollSchema(), fs, []string{"COSINE", "IP"}, params, alloc)
	require.NoError(t, err)

	df1 := mmrFloatDF(t, alloc, ids, scores, vecs, []int64{10})
	defer df1.Release()
	df2 := mmrFloatDF(t, alloc, ids, scores, vecs, []int64{10})
	defer df2.Release()
	out, err := fc.ExecuteWithContext(context.Background(), df1, df2)
	require.NoError(t, err)
	defer out.Release()
	got := mmrIDs(t, out)[0]
	assert.Len(t, got, 4)
	// RRF scores are kept on the output (not replaced by MMR scores).
	sc := out.Column(types.ScoreFieldName).Chunk(0).(*array.Float32).Float32Values()
	for _, v := range sc {
		assert.Less(t, v, float32(0.1))
	}
}

func TestBuildRerankChain_MMRValidation(t *testing.T) {
	paramtable.Init()
	schema := mmrCollSchema()
	rrf := &schemapb.FunctionSchema{Type: schemapb.FunctionType_Rerank, Params: []*commonpb.KeyValuePair{
		{Key: "reranker", Value: "rrf"},
	}}
	build := func(fs []*schemapb.FunctionSchema, params *SearchParams, metrics ...string) error {
		if len(metrics) == 0 {
			metrics = []string{"COSINE"}
		}
		_, err := BuildRerankChain(schema, &schemapb.FunctionScore{Functions: fs}, metrics, params, memory.DefaultAllocator)
		return err
	}

	assert.NoError(t, build([]*schemapb.FunctionSchema{mmrFunction(nil)}, mmrSearchParams(10, 0)))
	assert.ErrorContains(t, build([]*schemapb.FunctionSchema{mmrFunction(nil), rrf}, mmrSearchParams(10, 0)), "last")
	assert.ErrorContains(t, build([]*schemapb.FunctionSchema{mmrFunction(nil), mmrFunction(nil)}, mmrSearchParams(10, 0)), "last")
	assert.ErrorContains(t, build([]*schemapb.FunctionSchema{mmrFunction(map[string]string{"candidate_size": "5"})}, mmrSearchParams(10, 0)),
		"must not exceed candidate_size")
	// offset+limit past the window is allowed (short or empty pages).
	assert.NoError(t, build([]*schemapb.FunctionSchema{mmrFunction(map[string]string{"candidate_size": "10"})}, mmrSearchParams(10, 50)))
	assert.ErrorContains(t, build([]*schemapb.FunctionSchema{mmrFunction(map[string]string{"lambda": "1.5"})}, mmrSearchParams(10, 0)), "lambda")
	assert.ErrorContains(t, build([]*schemapb.FunctionSchema{mmrFunction(map[string]string{"lambda": "x"})}, mmrSearchParams(10, 0)), "lambda")
	assert.ErrorContains(t, build([]*schemapb.FunctionSchema{mmrFunction(map[string]string{"candidate_size": "0"})}, mmrSearchParams(10, 0)), "candidate_size")
	assert.ErrorContains(t, build([]*schemapb.FunctionSchema{mmrFunction(nil, mmrTestField, "vec2")}, mmrSearchParams(10, 0)), "exactly 1")
	assert.ErrorContains(t, build([]*schemapb.FunctionSchema{mmrFunction(nil, "ts")}, mmrSearchParams(10, 0)), "not a vector")
	assert.ErrorContains(t, build([]*schemapb.FunctionSchema{mmrFunction(nil, "missing")}, mmrSearchParams(10, 0)), "not found")

	// Default similarity needs the field to be searched.
	assert.ErrorContains(t, build([]*schemapb.FunctionSchema{mmrFunction(nil, "vec2")}, mmrSearchParams(10, 0)), "similarity")
	assert.NoError(t, build([]*schemapb.FunctionSchema{mmrFunction(map[string]string{"similarity": "ip"}, "vec2")}, mmrSearchParams(10, 0)))
	// Override must fit the type.
	assert.ErrorContains(t, build([]*schemapb.FunctionSchema{mmrFunction(map[string]string{"similarity": "HAMMING"})}, mmrSearchParams(10, 0)), "not supported")

	grouped := NewSearchParamsWithGrouping(1, 10, 0, -1, "ts", 2)
	grouped.AnnsFields = []string{mmrTestField}
	assert.ErrorContains(t, build([]*schemapb.FunctionSchema{mmrFunction(nil)}, grouped), "group_by")
}

func TestValidateMMRInputField(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
			{FieldID: 103, Name: "dense", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name: "bm25", Type: schemapb.FunctionType_BM25,
			InputFieldIds: []int64{101}, InputFieldNames: []string{"text"},
			OutputFieldIds: []int64{102}, OutputFieldNames: []string{"sparse"},
		}},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	require.NoError(t, err)
	assert.NoError(t, ValidateMMRInputField(helper, schema.Fields[3]))
	assert.ErrorContains(t, ValidateMMRInputField(helper, schema.Fields[2]), "BM25")
	assert.ErrorContains(t, ValidateMMRInputField(helper, schema.Fields[1]), "not a vector")

	// FunctionChains reject a BM25 output as mmr input when the plan is compiled.
	repr := &ChainRepr{Operators: []OperatorRepr{{Type: types.OpTypeMMR, Inputs: []string{"sparse"}}}}
	_, err = CompileDataFrameInputPlan(repr, schema)
	assert.ErrorContains(t, err, "BM25")
}

func TestMMRCandidateSizeDefaults(t *testing.T) {
	paramtable.Init()
	maxW := paramtable.Get().FunctionCfg.MMRMaxCandidateSize.GetAsInt64()
	assert.Equal(t, int64(100), DefaultMMRCandidateSize(10))
	assert.Equal(t, int64(150), DefaultMMRCandidateSize(50))
	assert.Equal(t, maxW, DefaultMMRCandidateSize(maxW))

	w, err := MMRCandidateSize(mmrFunction(nil), 40)
	require.NoError(t, err)
	assert.Equal(t, int64(120), w)
	w, err = MMRCandidateSize(mmrFunction(map[string]string{"candidate_size": strconv.Itoa(64)}), 40)
	require.NoError(t, err)
	assert.Equal(t, int64(64), w)

	assert.NotNil(t, FindMMRFunction(&schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{mmrFunction(nil)}}))
	assert.Nil(t, FindMMRFunction(&schemapb.FunctionScore{}))
}

// =============================================================================
// FunctionChains repr factory
// =============================================================================

func TestNewMMROpFromRepr(t *testing.T) {
	paramtable.Init()
	repr := &OperatorRepr{
		Type:   types.OpTypeMMR,
		Inputs: []string{mmrTestField},
		Params: map[string]*schemapb.FunctionParamValue{
			types.MMRParamLambda:        {Value: &schemapb.FunctionParamValue_DoubleValue{DoubleValue: 0.7}},
			types.MMRParamCandidateSize: {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 30}},
			types.MMRParamLimit:         {Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 5}},
		},
	}
	op, err := NewMMROpFromReprWithContext(repr, types.FunctionBuildContext{
		Search: &types.SearchRuntimeInfo{MetricTypes: []string{"L2"}},
	})
	require.NoError(t, err)
	cfg := op.(*MMROp).Config()
	assert.Equal(t, 0.7, cfg.Lambda)
	assert.Equal(t, int64(30), cfg.CandidateSize)
	assert.Equal(t, int64(5), cfg.SelectCount)
	assert.Equal(t, "L2", cfg.Similarity)
	assert.True(t, cfg.ScoreDescending)
	assert.Empty(t, cfg.RelevanceMetric)

	// Two ANN searches need an explicit similarity.
	_, err = NewMMROpFromReprWithContext(repr, types.FunctionBuildContext{
		Search: &types.SearchRuntimeInfo{MetricTypes: []string{"L2", "IP"}},
	})
	assert.ErrorContains(t, err, "similarity")

	repr.Params[types.MMRParamSimilarity] = &schemapb.FunctionParamValue{
		Value: &schemapb.FunctionParamValue_StringValue{StringValue: "COSINE"},
	}
	op, err = NewMMROpFromReprWithContext(repr, types.FunctionBuildContext{
		Search: &types.SearchRuntimeInfo{MetricTypes: []string{"L2", "IP"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "COSINE", op.(*MMROp).Config().Similarity)

	_, err = NewMMROpFromReprWithContext(&OperatorRepr{Type: types.OpTypeMMR, Inputs: []string{"a", "b"}}, types.FunctionBuildContext{})
	assert.ErrorContains(t, err, "exactly 1")
}
