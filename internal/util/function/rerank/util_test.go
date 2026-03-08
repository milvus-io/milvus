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

package rerank

import (
	"math"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestUtil(t *testing.T) {
	suite.Run(t, new(UtilSuite))
}

type UtilSuite struct {
	suite.Suite
	schema    *schemapb.CollectionSchema
	providers []string
}

func mockCols(num int) []*columns {
	cols := []*columns{}
	for i := 0; i < num; i++ {
		c := columns{
			size:   10,
			ids:    []int64{1, 2, 3, 4},
			scores: []float32{1.0 + float32(i), 2.0 + float32(i), 3.0 + float32(i), 4.0 + float32(i)},
		}
		cols = append(cols, &c)
	}
	return cols
}

func (s *UtilSuite) TestScoreMode() {
	{
		_, err := getMergeFunc[int64]("test")
		s.ErrorContains(err, "Unsupport score mode")
	}
	{
		f, err := getMergeFunc[int64]("avg")
		s.NoError(err)
		cols := mockCols(0)
		r := f(cols)
		s.Equal(0, len(r))
		cols = mockCols(1)
		r = f(cols)
		s.Equal(r, map[int64]float32{1: 1.0, 2: 2.0, 3: 3.0, 4: 4.0})
		cols = mockCols(3)
		r = f(cols)
		s.Equal(r, map[int64]float32{1: 2.0, 2: 3.0, 3: 4.0, 4: 5.0})
	}
	{
		f, err := getMergeFunc[int64]("max")
		s.NoError(err)
		cols := mockCols(0)
		r := f(cols)
		s.Equal(0, len(r))
		cols = mockCols(1)
		r = f(cols)
		s.Equal(r, map[int64]float32{1: 1.0, 2: 2.0, 3: 3.0, 4: 4.0})
		cols = mockCols(3)
		r = f(cols)
		s.Equal(r, map[int64]float32{1: 3.0, 2: 4.0, 3: 5.0, 4: 6.0})
	}
	{
		f, err := getMergeFunc[int64]("sum")
		s.NoError(err)
		cols := mockCols(0)
		r := f(cols)
		s.Equal(0, len(r))
		cols = mockCols(1)
		r = f(cols)
		s.Equal(r, map[int64]float32{1: 1.0, 2: 2.0, 3: 3.0, 4: 4.0})
		cols = mockCols(3)
		r = f(cols)
		s.Equal(r, map[int64]float32{1: 6.0, 2: 9.0, 3: 12.0, 4: 15.0})
	}
}

func (s *UtilSuite) TestFuctionNormalize() {
	{
		f := getNormalizeFunc(false, metric.COSINE, false)
		s.Equal(float32(1.0), f(1.0))
	}
	{
		f := getNormalizeFunc(true, metric.COSINE, true)
		s.Equal(float32((1+1.0)*0.5), f(1))
	}
	{
		f := getNormalizeFunc(false, metric.COSINE, true)
		s.Equal(float32(1.0), f(1.0))
	}
	{
		f := getNormalizeFunc(true, metric.COSINE, false)
		s.Equal(float32((1+1.0)*0.5), f(1))
	}
	{
		f := getNormalizeFunc(false, metric.IP, true)
		s.Equal(float32(1.0), f(1.0))
	}
	{
		f := getNormalizeFunc(true, metric.IP, false)
		s.Equal(0.5+float32(math.Atan(float64(1.0)))/math.Pi, f(1))
	}
	{
		f := getNormalizeFunc(false, metric.IP, true)
		s.Equal(float32(1.0), f(1.0))
	}
	{
		f := getNormalizeFunc(true, metric.IP, false)
		s.Equal(0.5+float32(math.Atan(float64(1.0)))/math.Pi, f(1))
	}
	{
		f := getNormalizeFunc(false, metric.BM25, false)
		s.Equal(float32(1.0), f(1.0))
	}
	{
		f := getNormalizeFunc(true, metric.BM25, false)
		s.Equal(2*float32(math.Atan(float64(1.0)))/math.Pi, f(1.0))
	}
	{
		f := getNormalizeFunc(false, metric.BM25, true)
		s.Equal(float32(1.0), f(1.0))
	}
	{
		f := getNormalizeFunc(true, metric.BM25, true)
		s.Equal(2*float32(math.Atan(float64(1.0)))/math.Pi, f(1.0))
	}
	{
		f := getNormalizeFunc(false, metric.L2, true)
		s.Equal((1.0 - 2*float32(math.Atan(float64(1.0)))/math.Pi), f(1.0))
	}
	{
		f := getNormalizeFunc(true, metric.L2, true)
		s.Equal((1.0 - 2*float32(math.Atan(float64(1.0)))/math.Pi), f(1.0))
	}
	{
		f := getNormalizeFunc(false, metric.L2, false)
		s.Equal(float32(1.0), f(1.0))
	}
	{
		f := getNormalizeFunc(true, metric.L2, false)
		s.Equal((1.0 - 2*float32(math.Atan(float64(1.0)))/math.Pi), f(1.0))
	}
}

func (s *UtilSuite) TestIsCrossMetrics() {
	{
		metrics := []string{metric.BM25}
		mixed, descending := classifyMetricsOrder(metrics)
		s.False(mixed)
		s.True(descending)
	}
	{
		metrics := []string{metric.BM25, metric.COSINE, metric.IP}
		mixed, descending := classifyMetricsOrder(metrics)
		s.False(mixed)
		s.True(descending)
	}
	{
		metrics := []string{metric.L2}
		mixed, descending := classifyMetricsOrder(metrics)
		s.False(mixed)
		s.False(descending)
	}
	{
		metrics := []string{metric.L2, metric.BM25}
		mixed, descending := classifyMetricsOrder(metrics)
		s.True(mixed)
		s.True(descending)
	}
	{
		metrics := []string{metric.L2, metric.COSINE}
		mixed, descending := classifyMetricsOrder(metrics)
		s.True(mixed)
		s.True(descending)
	}
	{
		metrics := []string{metric.L2, metric.IP}
		mixed, descending := classifyMetricsOrder(metrics)
		s.True(mixed)
		s.True(descending)
	}
}

// TestNewRerankOutputsWithEmptyFieldsData tests newRerankOutputs when FieldsData is empty (requery scenario)
func (s *UtilSuite) TestNewRerankOutputsWithEmptyFieldsData() {
	// Test case 1: All fieldData have empty FieldsData
	{
		inputs := &rerankInputs{
			fieldData: []*schemapb.SearchResultData{
				{
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{1, 2, 3},
							},
						},
					},
					FieldsData: []*schemapb.FieldData{}, // Empty
				},
				{
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{4, 5, 6},
							},
						},
					},
					FieldsData: []*schemapb.FieldData{}, // Empty
				},
			},
		}
		searchParams := &SearchParams{limit: 10}
		outputs := newRerankOutputs(inputs, searchParams)
		s.NotNil(outputs)
		// FieldsData should be empty since all inputs were empty
		s.Equal(0, len(outputs.searchResultData.FieldsData))
	}

	// Test case 2: First fieldData has empty FieldsData, second has data
	{
		inputs := &rerankInputs{
			fieldData: []*schemapb.SearchResultData{
				{
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{1, 2, 3},
							},
						},
					},
					FieldsData: []*schemapb.FieldData{}, // Empty
				},
				{
					Ids: &schemapb.IDs{
						IdField: &schemapb.IDs_IntId{
							IntId: &schemapb.LongArray{
								Data: []int64{4, 5, 6},
							},
						},
					},
					FieldsData: []*schemapb.FieldData{
						{
							Type:      schemapb.DataType_Int64,
							FieldName: "field1",
							FieldId:   100,
							Field: &schemapb.FieldData_Scalars{
								Scalars: &schemapb.ScalarField{
									Data: &schemapb.ScalarField_LongData{
										LongData: &schemapb.LongArray{
											Data: []int64{40, 50, 60},
										},
									},
								},
							},
						},
					},
				},
			},
		}
		searchParams := &SearchParams{limit: 10}
		outputs := newRerankOutputs(inputs, searchParams)
		s.NotNil(outputs)
		// Should use the second fieldData which has non-empty FieldsData
		s.Greater(len(outputs.searchResultData.FieldsData), 0)
	}

	// Test case 3: nil fieldData
	{
		inputs := &rerankInputs{
			fieldData: []*schemapb.SearchResultData{nil, nil},
		}
		searchParams := &SearchParams{limit: 10}
		outputs := newRerankOutputs(inputs, searchParams)
		s.NotNil(outputs)
		// FieldsData should be empty
		s.Equal(0, len(outputs.searchResultData.FieldsData))
	}
}

// TestAppendResultWithEmptyFieldsData tests appendResult when FieldsData is empty
func (s *UtilSuite) TestAppendResultWithEmptyFieldsData() {
	// Test case: appendResult should not panic when FieldsData is empty
	inputs := &rerankInputs{
		fieldData: []*schemapb.SearchResultData{
			{
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{1, 2, 3},
						},
					},
				},
				FieldsData: []*schemapb.FieldData{}, // Empty
			},
		},
	}
	searchParams := &SearchParams{limit: 10}
	outputs := newRerankOutputs(inputs, searchParams)

	// Create idScores with locations
	idScores := &IDScores[int64]{
		ids:       []int64{1, 2},
		scores:    []float32{0.9, 0.8},
		locations: []IDLoc{{batchIdx: 0, offset: 0}, {batchIdx: 0, offset: 1}},
	}

	// This should not panic even when FieldsData is empty
	s.NotPanics(func() {
		appendResult(inputs, outputs, idScores)
	})

	// Verify that IDs and scores were appended correctly
	s.Equal(int64(2), outputs.searchResultData.Topks[0])
	s.Equal([]float32{0.9, 0.8}, outputs.searchResultData.Scores)
	s.Equal([]int64{1, 2}, outputs.searchResultData.Ids.GetIntId().Data)
	// FieldsData should still be empty
	s.Equal(0, len(outputs.searchResultData.FieldsData))
}

// TestAppendResultWithNullableSparseVector tests appendResult with nullable sparse vector fields.
// This ensures that idxComputers correctly maps row indices to data indices for nullable vectors.
func (s *UtilSuite) TestAppendResultWithNullableSparseVector() {
	// Create sparse vector data: 3 rows where row 1 (middle) is null
	// ValidData: [true, false, true] means rows 0 and 2 have data, row 1 is null
	// Contents only has 2 entries (for the non-null rows at indices 0 and 2)
	sparseContent0 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x3f} // sparse vector data for row 0
	sparseContent2 := []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40} // sparse vector data for row 2

	inputs := &rerankInputs{
		fieldData: []*schemapb.SearchResultData{
			{
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{1, 2, 3}, // 3 rows
						},
					},
				},
				FieldsData: []*schemapb.FieldData{
					{
						Type:      schemapb.DataType_SparseFloatVector,
						FieldName: "sparse_vec",
						FieldId:   101,
						ValidData: []bool{true, false, true}, // Row 1 is null
						Field: &schemapb.FieldData_Vectors{
							Vectors: &schemapb.VectorField{
								Dim: 700,
								Data: &schemapb.VectorField_SparseFloatVector{
									SparseFloatVector: &schemapb.SparseFloatArray{
										Dim:      700,
										Contents: [][]byte{sparseContent0, sparseContent2}, // Only 2 entries
									},
								},
							},
						},
					},
				},
			},
		},
		nq: 1,
	}

	// Initialize idxComputers for the inputs
	inputs.idxComputers = make([]*typeutil.FieldDataIdxComputer, len(inputs.fieldData))
	for i, srd := range inputs.fieldData {
		if srd != nil {
			inputs.idxComputers[i] = typeutil.NewFieldDataIdxComputer(srd.GetFieldsData())
		}
	}

	searchParams := &SearchParams{limit: 10}
	outputs := newRerankOutputs(inputs, searchParams)

	// Select rows in reverse order: row 2 first, then row 0
	// Row 2 is valid (should get sparseContent2), Row 0 is valid (should get sparseContent0)
	idScores := &IDScores[int64]{
		ids:       []int64{3, 1},                                               // IDs for rows 2 and 0
		scores:    []float32{0.9, 0.8},                                         // scores
		locations: []IDLoc{{batchIdx: 0, offset: 2}, {batchIdx: 0, offset: 0}}, // row indices
	}

	// This should NOT panic - the bug was that without idxComputers,
	// it would use row index directly to access Contents, causing index out of range
	s.NotPanics(func() {
		appendResult(inputs, outputs, idScores)
	})

	// Verify results
	s.Equal(int64(2), outputs.searchResultData.Topks[0])
	s.Equal([]int64{3, 1}, outputs.searchResultData.Ids.GetIntId().Data)

	// Verify sparse vector data is correctly copied
	resultSparse := outputs.searchResultData.FieldsData[0]
	s.Equal(schemapb.DataType_SparseFloatVector, resultSparse.GetType())

	// ValidData should reflect the selected rows: both row 2 and row 0 are valid
	s.Equal([]bool{true, true}, resultSparse.GetValidData())

	// Contents should have 2 entries in the correct order
	contents := resultSparse.GetVectors().GetSparseFloatVector().GetContents()
	s.Len(contents, 2)
	s.Equal(sparseContent2, contents[0]) // Row 2's data comes first
	s.Equal(sparseContent0, contents[1]) // Row 0's data comes second
}

// TestAppendResultWithNullableSparseVectorSelectingNullRow tests appendResult when selecting a null row.
func (s *UtilSuite) TestAppendResultWithNullableSparseVectorSelectingNullRow() {
	sparseContent0 := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x3f}

	inputs := &rerankInputs{
		fieldData: []*schemapb.SearchResultData{
			{
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{1, 2, 3},
						},
					},
				},
				FieldsData: []*schemapb.FieldData{
					{
						Type:      schemapb.DataType_SparseFloatVector,
						FieldName: "sparse_vec",
						FieldId:   101,
						ValidData: []bool{true, false, false}, // Only row 0 has data
						Field: &schemapb.FieldData_Vectors{
							Vectors: &schemapb.VectorField{
								Dim: 700,
								Data: &schemapb.VectorField_SparseFloatVector{
									SparseFloatVector: &schemapb.SparseFloatArray{
										Dim:      700,
										Contents: [][]byte{sparseContent0}, // Only 1 entry
									},
								},
							},
						},
					},
				},
			},
		},
		nq: 1,
	}

	inputs.idxComputers = make([]*typeutil.FieldDataIdxComputer, len(inputs.fieldData))
	for i, srd := range inputs.fieldData {
		if srd != nil {
			inputs.idxComputers[i] = typeutil.NewFieldDataIdxComputer(srd.GetFieldsData())
		}
	}

	searchParams := &SearchParams{limit: 10}
	outputs := newRerankOutputs(inputs, searchParams)

	// Select row 1 (null) and row 0 (valid)
	idScores := &IDScores[int64]{
		ids:       []int64{2, 1},
		scores:    []float32{0.9, 0.8},
		locations: []IDLoc{{batchIdx: 0, offset: 1}, {batchIdx: 0, offset: 0}},
	}

	s.NotPanics(func() {
		appendResult(inputs, outputs, idScores)
	})

	// Verify ValidData: row 1 is null, row 0 is valid
	resultSparse := outputs.searchResultData.FieldsData[0]
	s.Equal([]bool{false, true}, resultSparse.GetValidData())

	// Contents should have 1 entry (only for the valid row)
	contents := resultSparse.GetVectors().GetSparseFloatVector().GetContents()
	s.Len(contents, 1)
	s.Equal(sparseContent0, contents[0])
}
