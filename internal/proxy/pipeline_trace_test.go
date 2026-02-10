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

func TestPipelineTrace_EmptyString(t *testing.T) {
	tr := newPipelineTrace(true)
	assert.Equal(t, "", tr.String())
}

// --------------- TraceMsg dispatch tests ---------------

func makeSearchResults(topks []int64, ids []int64, gbField *schemapb.FieldData) *milvuspb.SearchResults {
	return &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Topks: topks,
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_IntId{
					IntId: &schemapb.LongArray{Data: ids},
				},
			},
			GroupByFieldValue: gbField,
		},
	}
}

func TestTraceMsg_SearchReduce(t *testing.T) {
	tr := newPipelineTrace(true)
	sr := makeSearchResults([]int64{2, 1}, []int64{10, 20, 30}, nil)
	msg := opMsg{reducedMsgKey: []*milvuspb.SearchResults{sr}}
	tr.TraceMsg(searchReduceOp, msg)
	s := tr.String()
	assert.Contains(t, s, "search_reduce[0].topks=[2 1]")
	assert.Contains(t, s, "search_reduce[0].totalIDs=3")
}

func TestTraceMsg_HybridSearchReduce(t *testing.T) {
	tr := newPipelineTrace(true)
	sr := makeSearchResults([]int64{3}, []int64{1, 2, 3}, nil)
	msg := opMsg{reducedMsgKey: []*milvuspb.SearchResults{sr}}
	tr.TraceMsg(hybridSearchReduceOp, msg)
	s := tr.String()
	assert.Contains(t, s, "hybrid_search_reduce[0].topks=[3]")
}

func TestTraceMsg_SearchReduce_WithGroupBy(t *testing.T) {
	tr := newPipelineTrace(true)
	gbField := &schemapb.FieldData{
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{1, 1, 2}},
				},
			},
		},
	}
	sr := makeSearchResults([]int64{3}, []int64{10, 20, 30}, gbField)
	msg := opMsg{reducedMsgKey: []*milvuspb.SearchResults{sr}}
	tr.TraceMsg(searchReduceOp, msg)
	s := tr.String()
	assert.Contains(t, s, "groupByRows=3")
	assert.Contains(t, s, "groupByCards=[2]")
}

func TestTraceMsg_SearchReduce_WrongType(t *testing.T) {
	tr := newPipelineTrace(true)
	msg := opMsg{reducedMsgKey: "not_a_slice"}
	tr.TraceMsg(searchReduceOp, msg)
	assert.Equal(t, "", tr.String())
}

func TestTraceMsg_Rerank(t *testing.T) {
	tr := newPipelineTrace(true)
	sr := makeSearchResults([]int64{2}, []int64{10, 20}, nil)
	sr.Results.FieldsData = []*schemapb.FieldData{{}, {}}
	msg := opMsg{rankResultMsgKey: sr}
	tr.TraceMsg(rerankOp, msg)
	s := tr.String()
	assert.Contains(t, s, "rerank.topks=[2]")
	assert.Contains(t, s, "rerank.totalIDs=2")
	assert.Contains(t, s, "rerank.fields=2")
}

func TestTraceMsg_Rerank_WrongType(t *testing.T) {
	tr := newPipelineTrace(true)
	msg := opMsg{rankResultMsgKey: "wrong"}
	tr.TraceMsg(rerankOp, msg)
	assert.Equal(t, "", tr.String())
}

func TestTraceMsg_End(t *testing.T) {
	tr := newPipelineTrace(true)
	sr := makeSearchResults([]int64{1}, []int64{42}, nil)
	sr.Results.FieldsData = []*schemapb.FieldData{{}}
	msg := opMsg{pipelineOutput: sr}
	tr.TraceMsg(endOp, msg)
	s := tr.String()
	assert.Contains(t, s, "end.topks=[1]")
	assert.Contains(t, s, "end.fields=1")
}

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
		{},
	}
	msg := opMsg{fieldsMsgKey: fields}
	tr.TraceMsg(requeryOp, msg)
	s := tr.String()
	assert.Contains(t, s, "requery.fields=2")
	assert.Contains(t, s, "requery.rows=3")
}

func TestTraceMsg_Requery_EmptyFields(t *testing.T) {
	tr := newPipelineTrace(true)
	msg := opMsg{fieldsMsgKey: []*schemapb.FieldData{}}
	tr.TraceMsg(requeryOp, msg)
	s := tr.String()
	assert.Contains(t, s, "requery.fields=0")
	assert.NotContains(t, s, "requery.rows")
}

func TestTraceMsg_Requery_WrongType(t *testing.T) {
	tr := newPipelineTrace(true)
	msg := opMsg{fieldsMsgKey: 123}
	tr.TraceMsg(requeryOp, msg)
	assert.Equal(t, "", tr.String())
}

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
		{}, // empty batch
	}
	msg := opMsg{organizedFieldsMsgKey: batches}
	tr.TraceMsg(organizeOp, msg)
	s := tr.String()
	assert.Contains(t, s, "organize[0]=fields=1 rows=2")
	assert.Contains(t, s, "organize[1]=fields=0 rows=0")
}

func TestTraceMsg_Organize_WrongType(t *testing.T) {
	tr := newPipelineTrace(true)
	msg := opMsg{organizedFieldsMsgKey: "bad"}
	tr.TraceMsg(organizeOp, msg)
	assert.Equal(t, "", tr.String())
}

func TestTraceMsg_UnknownOp(t *testing.T) {
	tr := newPipelineTrace(true)
	tr.TraceMsg("unknown_op", opMsg{})
	assert.Equal(t, "", tr.String())
}

// --------------- countDistinct ---------------

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
		{"all same", []int64{5, 5, 5}, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.wantCard, countDistinct(tc.vals))
		})
	}
}

func TestCountDistinct_String(t *testing.T) {
	assert.Equal(t, 2, countDistinct([]string{"a", "b", "a"}))
}

// --------------- scalarGroupByCards ---------------

func TestScalarGroupByCards_PerNq(t *testing.T) {
	scalar := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_LongData{
			LongData: &schemapb.LongArray{Data: []int64{1, 1, 2, 3, 3}},
		},
	}
	topks := []int64{3, 2}
	cards := scalarGroupByCards(scalar, topks, nil)
	assert.Equal(t, []int{2, 1}, cards)
}

func TestScalarGroupByCards_IntData(t *testing.T) {
	scalar := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_IntData{
			IntData: &schemapb.IntArray{Data: []int32{1, 2, 2}},
		},
	}
	cards := scalarGroupByCards(scalar, []int64{3}, nil)
	assert.Equal(t, []int{2}, cards)
}

func TestScalarGroupByCards_BoolData(t *testing.T) {
	scalar := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_BoolData{
			BoolData: &schemapb.BoolArray{Data: []bool{true, false, true}},
		},
	}
	cards := scalarGroupByCards(scalar, []int64{3}, nil)
	assert.Equal(t, []int{2}, cards)
}

func TestScalarGroupByCards_FloatData(t *testing.T) {
	scalar := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_FloatData{
			FloatData: &schemapb.FloatArray{Data: []float32{1.0, 2.0, 1.0}},
		},
	}
	cards := scalarGroupByCards(scalar, []int64{3}, nil)
	assert.Equal(t, []int{2}, cards)
}

func TestScalarGroupByCards_DoubleData(t *testing.T) {
	scalar := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_DoubleData{
			DoubleData: &schemapb.DoubleArray{Data: []float64{1.0, 2.0, 2.0}},
		},
	}
	cards := scalarGroupByCards(scalar, []int64{3}, nil)
	assert.Equal(t, []int{2}, cards)
}

func TestScalarGroupByCards_CompactString(t *testing.T) {
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

func TestScalarGroupByCards_CompactString_NoValidData(t *testing.T) {
	// When validData is empty, falls back to perNqCardinality (non-compact).
	scalar := &schemapb.ScalarField{
		Data: &schemapb.ScalarField_StringData{
			StringData: &schemapb.StringArray{Data: []string{"a", "a", "b"}},
		},
	}
	cards := scalarGroupByCards(scalar, []int64{3}, nil)
	assert.Equal(t, []int{2}, cards)
}

func TestScalarGroupByCards_NilScalar(t *testing.T) {
	cards := scalarGroupByCards(nil, []int64{3}, nil)
	assert.Nil(t, cards)
}

// --------------- perNqCardinality edge cases ---------------

func TestPerNqCardinality_ValsShort(t *testing.T) {
	// vals shorter than sum(topks) — should not panic.
	cards := perNqCardinality([]int64{1, 2}, []int64{3, 2})
	// nq0: vals[0:2] since len(vals)=2 < end=3 → [1,2] → card=2
	// nq1: vals[2:2] since offset=2 and end=4>2 → empty → card=0
	assert.Equal(t, []int{2, 0}, cards)
}

func TestPerNqCardinality_EmptyTopks(t *testing.T) {
	cards := perNqCardinality([]int64{1, 2, 3}, []int64{})
	assert.Empty(t, cards)
}

// --------------- perNqCardinalityCompact edge cases ---------------

func TestPerNqCardinalityCompact_AllNulls(t *testing.T) {
	// All entries are null → compact is empty, each nq has 1 null group.
	cards := perNqCardinalityCompact([]string{}, []int64{2, 1}, []bool{false, false, false})
	// nq0: 2 logical rows, all null → nqVals=[], hasNull=true → card=1
	// nq1: 1 logical row, null → card=1
	assert.Equal(t, []int{1, 1}, cards)
}

func TestPerNqCardinalityCompact_NoNulls(t *testing.T) {
	// All valid → behaves like non-compact.
	cards := perNqCardinalityCompact([]string{"x", "y", "x"}, []int64{3}, []bool{true, true, true})
	assert.Equal(t, []int{2}, cards)
}

// --------------- fieldDataLen ---------------

func TestFieldDataLen(t *testing.T) {
	tests := []struct {
		name string
		fd   *schemapb.FieldData
		want int
	}{
		{"nil field data", nil, 0},
		{
			"long scalar",
			&schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}},
						},
					},
				},
			},
			3,
		},
		{
			"string scalar",
			&schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: []string{"a", "b"}},
						},
					},
				},
			},
			2,
		},
		{
			"bool scalar",
			&schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_BoolData{
							BoolData: &schemapb.BoolArray{Data: []bool{true, false, true, true}},
						},
					},
				},
			},
			4,
		},
		{
			"int scalar",
			&schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: []int32{1, 2}},
						},
					},
				},
			},
			2,
		},
		{
			"float scalar",
			&schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_FloatData{
							FloatData: &schemapb.FloatArray{Data: []float32{1.0, 2.0, 3.0}},
						},
					},
				},
			},
			3,
		},
		{
			"double scalar",
			&schemapb.FieldData{
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_DoubleData{
							DoubleData: &schemapb.DoubleArray{Data: []float64{1.0}},
						},
					},
				},
			},
			1,
		},
		{
			"float vector",
			&schemapb.FieldData{
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: 4,
						Data: &schemapb.VectorField_FloatVector{
							FloatVector: &schemapb.FloatArray{Data: make([]float32, 12)},
						},
					},
				},
			},
			3,
		},
		{
			"binary vector",
			&schemapb.FieldData{
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim: 16,
						Data: &schemapb.VectorField_BinaryVector{
							BinaryVector: make([]byte, 6),
						},
					},
				},
			},
			3,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, fieldDataLen(tc.fd))
		})
	}
}

func TestFieldDataLen_WithValidData(t *testing.T) {
	// When ValidData is present, it takes precedence.
	fd := &schemapb.FieldData{
		ValidData: []bool{true, false, true, true, false},
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{Data: []string{"a", "b", "c"}}, // only 3 non-null
				},
			},
		},
	}
	assert.Equal(t, 5, fieldDataLen(fd)) // logical rows = len(ValidData) = 5
}

// --------------- scalarLen ---------------

func TestScalarLen_Nil(t *testing.T) {
	assert.Equal(t, 0, scalarLen(nil))
}

func TestScalarLen_AllTypes(t *testing.T) {
	tests := []struct {
		name string
		s    *schemapb.ScalarField
		want int
	}{
		{"long", &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2}}}}, 2},
		{"string", &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"a"}}}}, 1},
		{"int", &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2, 3}}}}, 3},
		{"bool", &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: []bool{true}}}}, 1},
		{"float", &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: []float32{1.0, 2.0}}}}, 2},
		{"double", &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: []float64{1.0}}}}, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, scalarLen(tc.s))
		})
	}
}

// --------------- vectorLen ---------------

func TestVectorLen_NilAndZeroDim(t *testing.T) {
	assert.Equal(t, 0, vectorLen(nil))
	assert.Equal(t, 0, vectorLen(&schemapb.VectorField{Dim: 0}))
}

func TestVectorLen_UnknownType(t *testing.T) {
	// VectorField with no data set → hits default return 0.
	assert.Equal(t, 0, vectorLen(&schemapb.VectorField{Dim: 8}))
}

// --------------- LogIfEnabled ---------------

func TestLogIfEnabled_NonNil(t *testing.T) {
	// Should not panic; just exercises the log path.
	tr := newPipelineTrace(true)
	tr.Set("k", "v")
	tr.LogIfEnabled(context.Background(), "test-pipeline")
}

// --------------- traceSearchResult with groupBy ---------------

func TestTraceMsg_End_WithGroupBy(t *testing.T) {
	tr := newPipelineTrace(true)
	gbField := &schemapb.FieldData{
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{10, 10, 20}},
				},
			},
		},
	}
	sr := makeSearchResults([]int64{3}, []int64{1, 2, 3}, gbField)
	sr.Results.FieldsData = []*schemapb.FieldData{{}}
	msg := opMsg{pipelineOutput: sr}
	tr.TraceMsg(endOp, msg)
	s := tr.String()
	assert.Contains(t, s, "end.groupByRows=3")
	assert.Contains(t, s, "end.groupByCards=[2]")
}

// --------------- Multiple SearchResults in reduce ---------------

func TestTraceMsg_SearchReduce_MultipleResults(t *testing.T) {
	tr := newPipelineTrace(true)
	sr0 := makeSearchResults([]int64{2}, []int64{1, 2}, nil)
	sr1 := makeSearchResults([]int64{1}, []int64{3}, nil)
	msg := opMsg{reducedMsgKey: []*milvuspb.SearchResults{sr0, sr1}}
	tr.TraceMsg(searchReduceOp, msg)
	s := tr.String()
	assert.Contains(t, s, "search_reduce[0].topks=[2]")
	assert.Contains(t, s, "search_reduce[1].topks=[1]")
	assert.Contains(t, s, "search_reduce[0].totalIDs=2")
	assert.Contains(t, s, "search_reduce[1].totalIDs=1")
}

// --------------- Rerank with groupBy ---------------

func TestTraceMsg_Rerank_WithGroupBy(t *testing.T) {
	tr := newPipelineTrace(true)
	gbField := &schemapb.FieldData{
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{Data: []int64{1, 2, 1}},
				},
			},
		},
	}
	sr := makeSearchResults([]int64{3}, []int64{10, 20, 30}, gbField)
	msg := opMsg{rankResultMsgKey: sr}
	tr.TraceMsg(rerankOp, msg)
	s := tr.String()
	assert.Contains(t, s, "rerank.groupByRows=3")
	assert.Contains(t, s, "rerank.groupByCards=[2]")
}

// --------------- Requery with multiple fields, some with rows ---------------

func TestTraceMsg_Requery_WithFields(t *testing.T) {
	tr := newPipelineTrace(true)
	fields := []*schemapb.FieldData{
		{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{Data: []int64{1, 2, 3, 4, 5}},
					},
				},
			},
		},
	}
	msg := opMsg{fieldsMsgKey: fields}
	tr.TraceMsg(requeryOp, msg)
	s := tr.String()
	assert.Contains(t, s, "requery.fields=1")
	assert.Contains(t, s, "requery.rows=5")
}

// --------------- SearchResults with string IDs ---------------

func TestTraceMsg_Rerank_StringIDs(t *testing.T) {
	tr := newPipelineTrace(true)
	sr := &milvuspb.SearchResults{
		Results: &schemapb.SearchResultData{
			Topks: []int64{2},
			Ids: &schemapb.IDs{
				IdField: &schemapb.IDs_StrId{
					StrId: &schemapb.StringArray{Data: []string{"a", "b"}},
				},
			},
		},
	}
	msg := opMsg{rankResultMsgKey: sr}
	tr.TraceMsg(rerankOp, msg)
	s := tr.String()
	assert.Contains(t, s, "rerank.totalIDs=2")
}

// Verify the Rerank with FieldsData path records fields count
func TestTraceMsg_Rerank_FieldsCount(t *testing.T) {
	tr := newPipelineTrace(true)
	sr := makeSearchResults([]int64{1}, []int64{1}, nil)
	sr.Results.FieldsData = []*schemapb.FieldData{{}, {}, {}}
	msg := opMsg{rankResultMsgKey: sr}
	tr.TraceMsg(rerankOp, msg)
	s := tr.String()
	assert.Contains(t, s, "rerank.fields=3")
}
