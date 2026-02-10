package proxy

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

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
