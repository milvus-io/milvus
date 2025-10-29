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
