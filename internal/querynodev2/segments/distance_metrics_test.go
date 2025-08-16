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

package segments

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"
)

type DistanceMetricsSuite struct {
	suite.Suite
}

func (suite *DistanceMetricsSuite) SetupSuite() {
}

func (suite *DistanceMetricsSuite) TestNewDistanceCalculator() {
	tests := []struct {
		name        string
		metricType  string
		expected    DistanceMetric
		expectError bool
	}{
		{"L2 metric", "L2", DistanceMetricL2, false},
		{"IP metric", "IP", DistanceMetricIP, false},
		{"COSINE metric", "COSINE", DistanceMetricCOSINE, false},
		{"lowercase l2", "l2", DistanceMetricL2, false},
		{"lowercase ip", "ip", DistanceMetricIP, false},
		{"lowercase cosine", "cosine", DistanceMetricCOSINE, false},
		{"unsupported metric", "UNKNOWN", "", true},
		{"empty metric", "", "", true},
	}

	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			calculator, err := NewDistanceCalculator(tt.metricType)

			if tt.expectError {
				suite.Error(err)
				suite.Nil(calculator)
			} else {
				suite.NoError(err)
				suite.NotNil(calculator)
				suite.Equal(tt.expected, calculator.GetMetricType())
			}
		})
	}
}

func (suite *DistanceMetricsSuite) TestL2DistanceCalculator_Calculate() {
	calculator, err := NewDistanceCalculator("L2")
	suite.Require().NoError(err)

	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float32
		hasError bool
	}{
		{
			name:     "same vectors",
			a:        []float32{1.0, 2.0, 3.0},
			b:        []float32{1.0, 2.0, 3.0},
			expected: 0.0,
		},
		{
			name:     "orthogonal vectors",
			a:        []float32{1.0, 0.0},
			b:        []float32{0.0, 1.0},
			expected: float32(math.Sqrt(2)),
		},
		{
			name:     "simple case",
			a:        []float32{0.0, 0.0},
			b:        []float32{3.0, 4.0},
			expected: 5.0,
		},
		{
			name:     "dimension mismatch",
			a:        []float32{1.0, 2.0},
			b:        []float32{1.0, 2.0, 3.0},
			hasError: true,
		},
		{
			name:     "empty vectors",
			a:        []float32{},
			b:        []float32{},
			expected: 0.0,
		},
		{
			name:     "one empty vector",
			a:        []float32{1.0, 2.0},
			b:        []float32{},
			hasError: true,
		},
	}

	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			distance, err := calculator.Calculate(tt.a, tt.b)

			if tt.hasError {
				suite.Error(err)
			} else {
				suite.NoError(err)
				suite.InDelta(tt.expected, distance, 1e-6)
			}
		})
	}
}

func (suite *DistanceMetricsSuite) TestIPDistanceCalculator_Calculate() {
	calculator, err := NewDistanceCalculator("IP")
	suite.Require().NoError(err)

	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float32
		hasError bool
	}{
		{
			name:     "orthogonal vectors",
			a:        []float32{1.0, 0.0},
			b:        []float32{0.0, 1.0},
			expected: 0.0,
		},
		{
			name:     "same direction",
			a:        []float32{1.0, 2.0, 3.0},
			b:        []float32{2.0, 4.0, 6.0},
			expected: 28.0, // 1*2 + 2*4 + 3*6 = 2 + 8 + 18 = 28
		},
		{
			name:     "opposite direction",
			a:        []float32{1.0, 2.0},
			b:        []float32{-1.0, -2.0},
			expected: -5.0, // 1*(-1) + 2*(-2) = -1 + -4 = -5
		},
		{
			name:     "dimension mismatch",
			a:        []float32{1.0, 2.0},
			b:        []float32{1.0, 2.0, 3.0},
			hasError: true,
		},
	}

	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			distance, err := calculator.Calculate(tt.a, tt.b)

			if tt.hasError {
				suite.Error(err)
			} else {
				suite.NoError(err)
				suite.InDelta(tt.expected, distance, 1e-6)
			}
		})
	}
}

func (suite *DistanceMetricsSuite) TestCosineDistanceCalculator_Calculate() {
	calculator, err := NewDistanceCalculator("COSINE")
	suite.Require().NoError(err)

	tests := []struct {
		name     string
		a        []float32
		b        []float32
		expected float32
		hasError bool
	}{
		{
			name:     "same vectors",
			a:        []float32{1.0, 2.0, 3.0},
			b:        []float32{1.0, 2.0, 3.0},
			expected: 0.0, // cosine distance = 1 - cosine similarity = 1 - 1 = 0
		},
		{
			name:     "orthogonal vectors",
			a:        []float32{1.0, 0.0},
			b:        []float32{0.0, 1.0},
			expected: 1.0, // cosine distance = 1 - 0 = 1
		},
		{
			name:     "opposite vectors",
			a:        []float32{1.0, 2.0},
			b:        []float32{-1.0, -2.0},
			expected: 2.0, // cosine distance = 1 - (-1) = 2
		},
		{
			name:     "zero vector",
			a:        []float32{0.0, 0.0},
			b:        []float32{1.0, 2.0},
			hasError: true, // Division by zero
		},
		{
			name:     "dimension mismatch",
			a:        []float32{1.0, 2.0},
			b:        []float32{1.0, 2.0, 3.0},
			hasError: true,
		},
	}

	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			distance, err := calculator.Calculate(tt.a, tt.b)

			if tt.hasError {
				suite.Error(err)
			} else {
				suite.NoError(err)
				suite.InDelta(tt.expected, distance, 1e-6)
			}
		})
	}
}

func (suite *DistanceMetricsSuite) TestBatchCalculateDistances() {
	tests := []struct {
		name         string
		leftVectors  [][]float32
		rightVectors [][]float32
		metricType   string
		expectError  bool
		expectedLen  int
	}{
		{
			name: "L2 batch calculation",
			leftVectors: [][]float32{
				{1.0, 2.0},
				{3.0, 4.0},
			},
			rightVectors: [][]float32{
				{1.0, 2.0},
				{5.0, 6.0},
			},
			metricType:  "L2",
			expectedLen: 4, // 2 * 2 = 4 distances
		},
		{
			name: "IP batch calculation",
			leftVectors: [][]float32{
				{1.0, 0.0},
				{0.0, 1.0},
			},
			rightVectors: [][]float32{
				{1.0, 0.0},
				{0.0, 1.0},
			},
			metricType:  "IP",
			expectedLen: 4,
		},
		{
			name:        "empty left vectors",
			leftVectors: [][]float32{},
			rightVectors: [][]float32{
				{1.0, 2.0},
			},
			metricType:  "L2",
			expectedLen: 0,
		},
		{
			name: "empty right vectors",
			leftVectors: [][]float32{
				{1.0, 2.0},
			},
			rightVectors: [][]float32{},
			metricType:   "L2",
			expectedLen:  0,
		},
		{
			name: "unsupported metric",
			leftVectors: [][]float32{
				{1.0, 2.0},
			},
			rightVectors: [][]float32{
				{3.0, 4.0},
			},
			metricType:  "UNKNOWN",
			expectError: true,
		},
		{
			name: "dimension mismatch",
			leftVectors: [][]float32{
				{1.0, 2.0},
			},
			rightVectors: [][]float32{
				{1.0, 2.0, 3.0},
			},
			metricType:  "L2",
			expectError: true,
		},
	}

	for _, tt := range tests {
		suite.T().Run(tt.name, func(t *testing.T) {
			distances, err := BatchCalculateDistances(tt.leftVectors, tt.rightVectors, tt.metricType)

			if tt.expectError {
				suite.Error(err)
			} else {
				suite.NoError(err)
				suite.Equal(tt.expectedLen, len(distances))

				if tt.expectedLen > 0 {
					for _, distance := range distances {
						suite.False(math.IsNaN(float64(distance)))
						suite.False(math.IsInf(float64(distance), 0))
					}
				}
			}
		})
	}
}

func (suite *DistanceMetricsSuite) TestDistanceCalculator_BatchCalculate() {
	calculator, err := NewDistanceCalculator("L2")
	suite.Require().NoError(err)

	leftVectors := [][]float32{
		{1.0, 2.0},
		{3.0, 4.0},
	}
	rightVectors := [][]float32{
		{1.0, 2.0},
		{5.0, 6.0},
	}

	distances, err := calculator.BatchCalculate(leftVectors, rightVectors)
	suite.Require().NoError(err)
	suite.Require().Equal(4, len(distances))

	expectedDistances := []float32{
		0.0,
		float32(math.Sqrt(32)),
		float32(math.Sqrt(8)),
		float32(math.Sqrt(8)),
	}

	for i, expected := range expectedDistances {
		suite.InDelta(expected, distances[i], 1e-6)
	}
}

func (suite *DistanceMetricsSuite) TestDistanceCalculator_Performance() {
	if testing.Short() {
		suite.T().Skip("Skipping performance test in short mode")
	}

	calculator, err := NewDistanceCalculator("L2")
	suite.Require().NoError(err)

	// Generate test vectors
	const numVectors = 1000
	const dimension = 128

	leftVectors := make([][]float32, numVectors)
	rightVectors := make([][]float32, numVectors)

	for i := 0; i < numVectors; i++ {
		leftVectors[i] = make([]float32, dimension)
		rightVectors[i] = make([]float32, dimension)
		for j := 0; j < dimension; j++ {
			leftVectors[i][j] = float32(i*dimension + j)
			rightVectors[i][j] = float32((i+1)*dimension + j)
		}
	}

	distances, err := calculator.BatchCalculate(leftVectors, rightVectors)
	suite.Require().NoError(err)
	suite.Require().Equal(numVectors*numVectors, len(distances))

	for _, distance := range distances {
		suite.False(math.IsNaN(float64(distance)))
		suite.False(math.IsInf(float64(distance), 0))
		suite.True(distance >= 0)
	}
}

func (suite *DistanceMetricsSuite) TestDistanceCalculatorConcurrency() {
	calculator, err := NewDistanceCalculator("L2")
	suite.Require().NoError(err)

	const numGoroutines = 10
	const numCalculations = 100

	// Test concurrent access
	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < numCalculations; j++ {
				a := []float32{float32(id), float32(j)}
				b := []float32{float32(id + 1), float32(j + 1)}

				_, err := calculator.Calculate(a, b)
				if err != nil {
					errors <- err
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		suite.T().Errorf("Concurrent calculation error: %v", err)
	}
}

func TestDistanceMetricsSuite(t *testing.T) {
	suite.Run(t, new(DistanceMetricsSuite))
}

func BenchmarkL2Distance(b *testing.B) {
	calculator, _ := NewDistanceCalculator("L2")
	vec1 := make([]float32, 128)
	vec2 := make([]float32, 128)

	for i := range vec1 {
		vec1[i] = float32(i)
		vec2[i] = float32(i + 1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		calculator.Calculate(vec1, vec2)
	}
}

func BenchmarkIPDistance(b *testing.B) {
	calculator, _ := NewDistanceCalculator("IP")
	vec1 := make([]float32, 128)
	vec2 := make([]float32, 128)

	for i := range vec1 {
		vec1[i] = float32(i)
		vec2[i] = float32(i + 1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		calculator.Calculate(vec1, vec2)
	}
}

func BenchmarkCosineDistance(b *testing.B) {
	calculator, _ := NewDistanceCalculator("COSINE")
	vec1 := make([]float32, 128)
	vec2 := make([]float32, 128)

	for i := range vec1 {
		vec1[i] = float32(i) + 1 // Avoid zero vectors
		vec2[i] = float32(i) + 2
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		calculator.Calculate(vec1, vec2)
	}
}

func BenchmarkBatchCalculate(b *testing.B) {
	calculator, _ := NewDistanceCalculator("L2")

	leftVectors := make([][]float32, 100)
	rightVectors := make([][]float32, 100)

	for i := 0; i < 100; i++ {
		leftVectors[i] = make([]float32, 128)
		rightVectors[i] = make([]float32, 128)
		for j := 0; j < 128; j++ {
			leftVectors[i][j] = float32(i*128 + j)
			rightVectors[i][j] = float32((i+1)*128 + j)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		calculator.BatchCalculate(leftVectors, rightVectors)
	}
}
