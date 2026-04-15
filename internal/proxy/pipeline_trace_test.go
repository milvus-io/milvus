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

package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestPipelineTrace_NewEnabled(t *testing.T) {
	tr := newPipelineTrace(true)
	assert.NotNil(t, tr)
}

func TestPipelineTrace_NewDisabled(t *testing.T) {
	tr := newPipelineTrace(false)
	assert.Nil(t, tr)
}

func TestPipelineTrace_NilReceiverSafety(t *testing.T) {
	var tr *PipelineTrace
	// None of these should panic.
	tr.Set("key", "value")
	tr.TraceMsg("any_op", opMsg{})
	tr.LogIfEnabled(context.Background(), "test")
	assert.Equal(t, "", tr.String())
}

func TestPipelineTrace_SetAndString(t *testing.T) {
	tr := newPipelineTrace(true)
	tr.Set("a", 1)
	tr.Set("b", "hello")
	assert.Equal(t, "a=1, b=hello", tr.String())
}

func TestCountDistinct(t *testing.T) {
	tests := []struct {
		name     string
		vals     []int64
		wantCard int
	}{
		{"multiple groups", []int64{1, 1, 2, 2, 3}, 3},
		{"interleaved groups", []int64{1, 2, 1, 2, 3}, 3},
		{"empty slice", []int64{}, 0},
		{"single element", []int64{42}, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantCard, countDistinct(tc.vals))
		})
	}
}

func TestScalarGroupByCards_PerNq(t *testing.T) {
	// topks [3, 2] means nq=2: first query has 3 results, second has 2.
	// values: [1,1,2, 3,3] → nq0=[1,1,2] card=2; nq1=[3,3] card=1
	scalar := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_LongData{
			LongData: &schemapb.LongArray{Data: []int64{1, 1, 2, 3, 3}},
		},
	}
	topks := []int64{3, 2}
	cards := scalarGroupByCards(scalar, topks, nil)
	assert.Equal(t, []int{2, 1}, cards)
}

func TestScalarGroupByCards_CompactString(t *testing.T) {
	// topks [3, 2] = 5 logical rows, validData has 1 null in nq0.
	// validData: [true, false, true, true, true]
	// compact StringData: ["a", "b", "c", "c"] (4 entries, no null)
	// nq0 logical [0,1,2]: valid=[true,false,true] → vals ["a","b"] + 1 null → card=3
	// nq1 logical [3,4]:   valid=[true,true]       → vals ["c","c"]          → card=1
	scalar := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_StringData{
			StringData: &schemapb.StringArray{Data: []string{"a", "b", "c", "c"}},
		},
	}
	topks := []int64{3, 2}
	validData := []bool{true, false, true, true, true}
	cards := scalarGroupByCards(scalar, topks, validData)
	assert.Equal(t, []int{3, 1}, cards)
}

func TestScalarGroupByCards_NilScalar(t *testing.T) {
	cards := scalarGroupByCards(nil, []int64{3}, nil)
	assert.Nil(t, cards)
}

func TestFieldDataLen(t *testing.T) {
	tests := []struct {
		name string
		fd   *schemapb.FieldData
		want int
	}{
		{
			name: "nil field data",
			fd:   nil,
			want: 0,
		},
		{
			name: "long scalar",
			fd: &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
						},
					},
				},
			},
			want: 3,
		},
		{
			name: "string scalar",
			fd: &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{"a", "b"}},
						},
					},
				},
			},
			want: 2,
		},
		{
			name: "bool scalar",
			fd: &schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{Data: []bool{true, false, true, true}},
						},
					},
				},
			},
			want: 4,
		},
		{
			name: "float vector",
			fd: &schemapb.FieldData{
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: 4,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{Data: make([]float32, 12)}, // 12/4 = 3 rows
						},
					},
				},
			},
			want: 3,
		},
		{
			name: "binary vector",
			fd: &schemapb.FieldData{
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: 16,
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: make([]byte, 6), // 6 / (16/8) = 6/2 = 3 rows
						},
					},
				},
			},
			want: 3,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, fieldDataLen(tc.fd))
		})
	}
}

// --- helper ---

func makeSearchResultsForTrace(topks []int64, ids []int64, gbv *schemapb.FieldData) *milvuspb.SearchResults {
	results := &schemapb.SearchResultData{
		Topks: topks,
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: ids},
			},
		},
	}
	// Pipeline ops read from the plural channel; the task-output boundary
	// downgrades to singular for legacy-wire clients after these ops run.
	if gbv != nil {
		results.GroupByFieldValues = []*schemapb.FieldData{gbv}
	}
	return &milvuspb.SearchResults{Results: results}
}

// --- TraceMsg: searchReduceOp ---

func TestTraceMsg_SearchReduce_NoGroupBy(t *testing.T) {
	tr := newPipelineTrace(true)
	results := []*milvuspb.SearchResults{
		makeSearchResultsForTrace([]int64{3}, []int64{1, 2, 3}, nil),
	}
	tr.TraceMsg(searchReduceOp, opMsg{reducedMsgKey: results})
	s := tr.String()
	assert.Contains(t, s, "topks")
	assert.Contains(t, s, "totalIDs")
}

func TestTraceMsg_SearchReduce_WithGroupBy(t *testing.T) {
	tr := newPipelineTrace(true)
	gbv := &schemapb.FieldData{
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
				},
			},
		},
	}
	results := []*milvuspb.SearchResults{
		makeSearchResultsForTrace([]int64{3}, []int64{1, 2, 3}, gbv),
	}
	tr.TraceMsg(searchReduceOp, opMsg{reducedMsgKey: results})
	s := tr.String()
	assert.Contains(t, s, "groupByCards")
	assert.Contains(t, s, "groupByRows")
}

func TestTraceMsg_SearchReduce_WrongType(t *testing.T) {
	tr := newPipelineTrace(true)
	tr.TraceMsg(searchReduceOp, opMsg{reducedMsgKey: "bad"})
	assert.Equal(t, "", tr.String())
}

func TestTraceMsg_HybridSearchReduce(t *testing.T) {
	tr := newPipelineTrace(true)
	results := []*milvuspb.SearchResults{
		makeSearchResultsForTrace([]int64{2}, []int64{1, 2}, nil),
	}
	tr.TraceMsg(hybridSearchReduceOp, opMsg{reducedMsgKey: results})
	assert.Contains(t, tr.String(), "topks")
}

// --- TraceMsg: rerankOp ---

func TestTraceMsg_Rerank(t *testing.T) {
	tr := newPipelineTrace(true)
	result := makeSearchResultsForTrace([]int64{2}, []int64{1, 2}, nil)
	tr.TraceMsg(rerankOp, opMsg{rankResultMsgKey: result})
	assert.Contains(t, tr.String(), "rerank.topks")
}

func TestTraceMsg_Rerank_WrongType(t *testing.T) {
	tr := newPipelineTrace(true)
	tr.TraceMsg(rerankOp, opMsg{rankResultMsgKey: "bad"})
	assert.Equal(t, "", tr.String())
}

// --- TraceMsg: endOp ---

func TestTraceMsg_End(t *testing.T) {
	tr := newPipelineTrace(true)
	result := makeSearchResultsForTrace([]int64{2}, []int64{1, 2}, nil)
	tr.TraceMsg(endOp, opMsg{pipelineOutput: result})
	assert.Contains(t, tr.String(), "end.topks")
}

func TestTraceMsg_End_WrongType(t *testing.T) {
	tr := newPipelineTrace(true)
	tr.TraceMsg(endOp, opMsg{pipelineOutput: "bad"})
	assert.Equal(t, "", tr.String())
}

// --- TraceMsg: requeryOp ---

func TestTraceMsg_Requery(t *testing.T) {
	tr := newPipelineTrace(true)
	fields := []*schemapb.FieldData{
		{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
					},
				},
			},
		},
	}
	tr.TraceMsg(requeryOp, opMsg{fieldsMsgKey: fields})
	s := tr.String()
	assert.Contains(t, s, "requery.fields")
	assert.Contains(t, s, "requery.rows")
}

func TestTraceMsg_Requery_EmptyFields(t *testing.T) {
	tr := newPipelineTrace(true)
	tr.TraceMsg(requeryOp, opMsg{fieldsMsgKey: []*schemapb.FieldData{}})
	assert.Contains(t, tr.String(), "requery.fields=0")
}

func TestTraceMsg_Requery_WrongType(t *testing.T) {
	tr := newPipelineTrace(true)
	tr.TraceMsg(requeryOp, opMsg{fieldsMsgKey: "bad"})
	assert.Equal(t, "", tr.String())
}

// --- TraceMsg: organizeOp ---

func TestTraceMsg_Organize(t *testing.T) {
	tr := newPipelineTrace(true)
	batches := [][]*schemapb.FieldData{
		{
			{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{1, 2}},
						},
					},
				},
			},
		},
		{}, // empty batch — rows=0
	}
	tr.TraceMsg(organizeOp, opMsg{organizedFieldsMsgKey: batches})
	s := tr.String()
	assert.Contains(t, s, "organize[0]")
	assert.Contains(t, s, "organize[1]")
}

func TestTraceMsg_Organize_WrongType(t *testing.T) {
	tr := newPipelineTrace(true)
	tr.TraceMsg(organizeOp, opMsg{organizedFieldsMsgKey: "bad"})
	assert.Equal(t, "", tr.String())
}

// --- TraceMsg: unknown op ---

func TestTraceMsg_UnknownOp(t *testing.T) {
	tr := newPipelineTrace(true)
	tr.TraceMsg("no_such_op", opMsg{})
	assert.Equal(t, "", tr.String())
}

// --- LogIfEnabled with non-nil trace ---

func TestLogIfEnabled_NonNil(t *testing.T) {
	tr := newPipelineTrace(true)
	tr.Set("k", "v")
	tr.LogIfEnabled(context.Background(), "testPipeline") // must not panic
}

// --- scalarGroupByCards: other scalar types ---

func TestScalarGroupByCards_IntData(t *testing.T) {
	scalar := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_IntData{
			IntData: &schemapb.IntArray{Data: []int32{1, 1, 2}},
		},
	}
	assert.Equal(t, []int{2}, scalarGroupByCards(scalar, []int64{3}, nil))
}

func TestScalarGroupByCards_BoolData(t *testing.T) {
	scalar := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_BoolData{
			BoolData: &schemapb.BoolArray{Data: []bool{true, false, true}},
		},
	}
	assert.Equal(t, []int{2}, scalarGroupByCards(scalar, []int64{3}, nil))
}

func TestScalarGroupByCards_FloatData(t *testing.T) {
	scalar := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_FloatData{
			FloatData: &schemapb.FloatArray{Data: []float32{1.0, 2.0, 1.0}},
		},
	}
	assert.Equal(t, []int{2}, scalarGroupByCards(scalar, []int64{3}, nil))
}

func TestScalarGroupByCards_DoubleData(t *testing.T) {
	scalar := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_DoubleData{
			DoubleData: &schemapb.DoubleArray{Data: []float64{1.0, 1.0, 2.0}},
		},
	}
	assert.Equal(t, []int{2}, scalarGroupByCards(scalar, []int64{3}, nil))
}

// --- fieldDataLen: ValidData branch ---

func TestFieldDataLen_WithValidData(t *testing.T) {
	// ValidData length takes precedence over scalar array length
	fd := &schemapb.FieldData{
		ValidData: []bool{true, false, true},
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"a", "b"}}, // compact
				},
			},
		},
	}
	assert.Equal(t, 3, fieldDataLen(fd))
}

// --- scalarLen: other types ---

func TestScalarLen_IntData(t *testing.T) {
	s := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_IntData{
			IntData: &schemapb.IntArray{Data: []int32{1, 2, 3, 4}},
		},
	}
	assert.Equal(t, 4, scalarLen(s))
}

func TestScalarLen_FloatData(t *testing.T) {
	s := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_FloatData{
			FloatData: &schemapb.FloatArray{Data: []float32{1.0, 2.0}},
		},
	}
	assert.Equal(t, 2, scalarLen(s))
}

func TestScalarLen_DoubleData(t *testing.T) {
	s := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_DoubleData{
			DoubleData: &schemapb.DoubleArray{Data: []float64{1.0}},
		},
	}
	assert.Equal(t, 1, scalarLen(s))
}

func TestScalarLen_Nil(t *testing.T) {
	assert.Equal(t, 0, scalarLen(nil))
}

// --- vectorLen: Float16 / Bfloat16 / Int8 ---

func TestFieldDataLen_Float16Vector(t *testing.T) {
	fd := &schemapb.FieldData{
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 4,
				Data: &schemapb.VectorField_Float16Vector{
					Float16Vector: make([]byte, 4*2*3), // dim=4, 2 bytes/elem, 3 rows
				},
			},
		},
	}
	assert.Equal(t, 3, fieldDataLen(fd))
}

func TestFieldDataLen_Bfloat16Vector(t *testing.T) {
	fd := &schemapb.FieldData{
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 4,
				Data: &schemapb.VectorField_Bfloat16Vector{
					Bfloat16Vector: make([]byte, 4*2*2), // dim=4, 2 bytes/elem, 2 rows
				},
			},
		},
	}
	assert.Equal(t, 2, fieldDataLen(fd))
}

func TestFieldDataLen_Int8Vector(t *testing.T) {
	fd := &schemapb.FieldData{
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: 4,
				Data: &schemapb.VectorField_Int8Vector{
					Int8Vector: make([]byte, 4*5), // dim=4, 1 byte/elem, 5 rows
				},
			},
		},
	}
	assert.Equal(t, 5, fieldDataLen(fd))
}

func TestVectorLen_ZeroDim(t *testing.T) {
	v := &schemapb.VectorField{
		Dim: 0,
		Data: &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}},
		},
	}
	assert.Equal(t, 0, vectorLen(v))
}

func TestVectorLen_Nil(t *testing.T) {
	assert.Equal(t, 0, vectorLen(nil))
}

func TestVectorLen_BinaryVector_ZeroBytesPerVec(t *testing.T) {
	// dim < 8 means bytesPerVec = 0 → guard returns 0
	v := &schemapb.VectorField{
		Dim: 4, // 4/8 = 0 bytes per vec
		Data: &schemapb.VectorField_BinaryVector{
			BinaryVector: make([]byte, 8),
		},
	}
	assert.Equal(t, 0, vectorLen(v))
}

// --- perNqCardinalityCompact: empty validData fallback ---

func TestPerNqCardinalityCompact_EmptyValidData(t *testing.T) {
	compact := []string{"a", "b", "a", "c"}
	topks := []int64{2, 2}
	// empty validData → falls back to perNqCardinality
	cards := perNqCardinalityCompact(compact, topks, nil)
	assert.Equal(t, []int{2, 2}, cards)
}
