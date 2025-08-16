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
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

// DistanceMetric distance metric type
type DistanceMetric string

const (
	// L2 Euclidean distance
	DistanceMetricL2 DistanceMetric = "L2"
	// IP Inner product distance
	DistanceMetricIP DistanceMetric = "IP"
	// COSINE Cosine distance
	DistanceMetricCOSINE DistanceMetric = "COSINE"
)

// DistanceCalculator distance calculator interface
type DistanceCalculator interface {
	// Calculate calculates distance between two vectors
	Calculate(a, b []float32) (float32, error)
	// BatchCalculate batch calculates distances
	BatchCalculate(leftVectors, rightVectors [][]float32) ([]float32, error)
	// GetMetricType gets distance metric type
	GetMetricType() DistanceMetric
}

// l2DistanceCalculator L2 distance calculator
type l2DistanceCalculator struct{}

func (c *l2DistanceCalculator) Calculate(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("vector dimension mismatch: %d vs %d", len(a), len(b))
	}

	// Use float64 to improve numerical precision, avoid accumulation error for large vectors
	var sum float64
	for i := 0; i < len(a); i++ {
		diff := float64(a[i]) - float64(b[i])
		sum += diff * diff
	}
	return float32(math.Sqrt(sum)), nil
}

func (c *l2DistanceCalculator) BatchCalculate(leftVectors, rightVectors [][]float32) ([]float32, error) {
	totalPairs := len(leftVectors) * len(rightVectors)
	results := make([]float32, 0, totalPairs)

	for _, left := range leftVectors {
		for _, right := range rightVectors {
			distance, err := c.Calculate(left, right)
			if err != nil {
				return nil, err
			}
			results = append(results, distance)
		}
	}

	return results, nil
}

func (c *l2DistanceCalculator) GetMetricType() DistanceMetric {
	return DistanceMetricL2
}

// ipDistanceCalculator IP distance calculator
type ipDistanceCalculator struct{}

func (c *ipDistanceCalculator) Calculate(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("vector dimension mismatch: %d vs %d", len(a), len(b))
	}

	// Use float64 to improve inner product calculation numerical precision
	var sum float64
	for i := 0; i < len(a); i++ {
		sum += float64(a[i]) * float64(b[i])
	}
	return float32(sum), nil
}

func (c *ipDistanceCalculator) BatchCalculate(leftVectors, rightVectors [][]float32) ([]float32, error) {
	totalPairs := len(leftVectors) * len(rightVectors)
	results := make([]float32, 0, totalPairs)

	for _, left := range leftVectors {
		for _, right := range rightVectors {
			distance, err := c.Calculate(left, right)
			if err != nil {
				return nil, err
			}
			results = append(results, distance)
		}
	}

	return results, nil
}

func (c *ipDistanceCalculator) GetMetricType() DistanceMetric {
	return DistanceMetricIP
}

// cosineDistanceCalculator cosine distance calculator
type cosineDistanceCalculator struct{}

func (c *cosineDistanceCalculator) Calculate(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("vector dimension mismatch: %d vs %d", len(a), len(b))
	}

	// Use float64 to improve cosine distance calculation numerical precision
	var dotProduct, normA, normB float64
	for i := 0; i < len(a); i++ {
		aVal := float64(a[i])
		bVal := float64(b[i])
		dotProduct += aVal * bVal
		normA += aVal * aVal
		normB += bVal * bVal
	}

	normA = math.Sqrt(normA)
	normB = math.Sqrt(normB)

	// Enhanced zero vector detection, using smaller threshold
	const epsilon = 1e-12
	if normA < epsilon || normB < epsilon {
		return 0, fmt.Errorf("vector norm too small (near zero vector), cannot calculate cosine distance: normA=%.2e, normB=%.2e", normA, normB)
	}

	cosine := dotProduct / (normA * normB)

	// Numerical stability: ensure cosine value is within [-1,1] range
	if cosine > 1.0 {
		cosine = 1.0
	} else if cosine < -1.0 {
		cosine = -1.0
	}

	// Cosine distance = 1 - cosine similarity
	return float32(1.0 - cosine), nil
}

func (c *cosineDistanceCalculator) BatchCalculate(leftVectors, rightVectors [][]float32) ([]float32, error) {
	totalPairs := len(leftVectors) * len(rightVectors)
	results := make([]float32, 0, totalPairs)

	for _, left := range leftVectors {
		for _, right := range rightVectors {
			distance, err := c.Calculate(left, right)
			if err != nil {
				return nil, err
			}
			results = append(results, distance)
		}
	}

	return results, nil
}

func (c *cosineDistanceCalculator) GetMetricType() DistanceMetric {
	return DistanceMetricCOSINE
}

// parallelDistanceCalculator parallel distance calculator
type parallelDistanceCalculator struct {
	baseCalculator DistanceCalculator
	workerCount    int
}

func (c *parallelDistanceCalculator) Calculate(a, b []float32) (float32, error) {
	return c.baseCalculator.Calculate(a, b)
}

func (c *parallelDistanceCalculator) BatchCalculate(leftVectors, rightVectors [][]float32) ([]float32, error) {
	totalPairs := len(leftVectors) * len(rightVectors)
	results := make([]float32, totalPairs)

	// Use work stealing pattern for parallel computation
	taskQueue := make(chan calculationTask, min(totalPairs, 10000)) // 限制队列大小
	var wg sync.WaitGroup
	errCh := make(chan error, c.workerCount)

	// Generate all calculation tasks
	go func() {
		defer close(taskQueue)
		for i, left := range leftVectors {
			for j, right := range rightVectors {
				select {
				case taskQueue <- calculationTask{
					leftVector:  left,
					rightVector: right,
					resultIndex: i*len(rightVectors) + j,
				}:
				case err := <-errCh:
					// If there's an error, stop generating tasks
					log.Debug("stop generating calculation tasks due to detected error", zap.Error(err))
					return
				}
			}
		}
	}()

	// Start worker goroutines
	for workerID := 0; workerID < c.workerCount; workerID++ {
		wg.Add(1)
		go func(wID int) {
			defer wg.Done()

			for task := range taskQueue {
				distance, err := c.baseCalculator.Calculate(task.leftVector, task.rightVector)
				if err != nil {
					select {
					case errCh <- fmt.Errorf("worker %d calculation failed: %w", wID, err):
					default:
						// Error channel is full, ignore additional errors
					}
					return
				}
				results[task.resultIndex] = distance
			}
		}(workerID)
	}

	wg.Wait()
	close(errCh)

	// Check for errors
	select {
	case err := <-errCh:
		return nil, err
	default:
		// No error
	}

	return results, nil
}

// calculationTask calculation task definition
type calculationTask struct {
	leftVector  []float32
	rightVector []float32
	resultIndex int
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (c *parallelDistanceCalculator) GetMetricType() DistanceMetric {
	return c.baseCalculator.GetMetricType()
}

// NewDistanceCalculator creates distance calculator
func NewDistanceCalculator(metricType string) (DistanceCalculator, error) {
	metricType = strings.ToUpper(strings.TrimSpace(metricType))

	var baseCalculator DistanceCalculator

	switch DistanceMetric(metricType) {
	case DistanceMetricL2:
		baseCalculator = &l2DistanceCalculator{}
	case DistanceMetricIP:
		baseCalculator = &ipDistanceCalculator{}
	case DistanceMetricCOSINE:
		baseCalculator = &cosineDistanceCalculator{}
	default:
		return nil, fmt.Errorf("unsupported distance metric type: %s", metricType)
	}

	return baseCalculator, nil
}

// NewParallelDistanceCalculator creates parallel distance calculator
func NewParallelDistanceCalculator(metricType string, totalTasks int) (DistanceCalculator, error) {
	baseCalculator, err := NewDistanceCalculator(metricType)
	if err != nil {
		return nil, err
	}

	// Dynamically calculate optimal worker thread count
	workerCount := optimalWorkerCount(totalTasks)
	if workerCount <= 1 {
		return baseCalculator, nil
	}

	return &parallelDistanceCalculator{
		baseCalculator: baseCalculator,
		workerCount:    workerCount,
	}, nil
}

// optimalWorkerCount dynamically calculates optimal worker thread count based on task count
func optimalWorkerCount(totalTasks int) int {
	cpuCount := runtime.GOMAXPROCS(0)

	// Reduce worker threads when task count is low to avoid context switching overhead
	switch {
	case totalTasks < 10:
		return 1
	case totalTasks < 100:
		return min(cpuCount/2, totalTasks/10)
	case totalTasks < 1000:
		return min(cpuCount, totalTasks/50)
	default:
		return cpuCount
	}
}

// CalculateDistance calculates distance between two vectors (convenience function)
func CalculateDistance(a, b []float32, metricType string) (float32, error) {
	calculator, err := NewDistanceCalculator(metricType)
	if err != nil {
		return 0, err
	}

	return calculator.Calculate(a, b)
}

// BatchCalculateDistances batch calculates vector distances (convenience function, automatically selects parallel strategy)
func BatchCalculateDistances(leftVectors, rightVectors [][]float32, metricType string) ([]float32, error) {
	totalPairs := len(leftVectors) * len(rightVectors)

	// Intelligently select calculation strategy based on task count
	var calculator DistanceCalculator
	var err error

	if totalPairs > 100 {
		// Use parallel calculator for large batch tasks
		calculator, err = NewParallelDistanceCalculator(metricType, totalPairs)
		if err != nil {
			return nil, err
		}
	} else {
		// Use normal calculator for small batch tasks, avoiding parallel overhead
		calculator, err = NewDistanceCalculator(metricType)
		if err != nil {
			return nil, err
		}
	}
	distances, err := calculator.BatchCalculate(leftVectors, rightVectors)
	if err != nil {
		return nil, err
	}

	return distances, nil
}
