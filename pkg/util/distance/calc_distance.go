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

package distance

import (
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
)

/**
 * Delete in #25663 Remove calc_distance
 * Add back partially as clustering feature needs to calculate distance between search vector and clustering center
 */
const (
	// L2 represents the Euclidean distance
	L2 = "L2"
	// IP represents the inner product distance
	IP = "IP"
	// COSINE represents the cosine distance, not supported yet
	COSINE = "COSINE"
)

// ValidateMetricType returns metric text or error
func ValidateMetricType(metric string) (string, error) {
	if metric == "" {
		err := errors.New("metric type is empty")
		return "", err
	}

	m := strings.ToUpper(metric)
	if m == L2 || m == IP {
		return m, nil
	}

	err := errors.New("invalid metric type")
	return metric, err
}

// ValidateFloatArrayLength is used validate float vector length
func ValidateFloatArrayLength(dim int64, length int) error {
	if length == 0 || int64(length)%dim != 0 {
		err := errors.New("invalid float vector length")
		return err
	}

	return nil
}

// CalcL2 returns the Euclidean distance of input vectors
func CalcL2(dim int64, left []float32, lIndex int64, right []float32, rIndex int64) float32 {
	var sum float32
	lFrom := lIndex * dim
	rFrom := rIndex * dim
	for i := int64(0); i < dim; i++ {
		gap := left[lFrom+i] - right[rFrom+i]
		sum += gap * gap
	}

	return sum
}

// CalcIP returns the inner product distance of input vectors
func CalcIP(dim int64, left []float32, lIndex int64, right []float32, rIndex int64) float32 {
	var sum float32
	lFrom := lIndex * dim
	rFrom := rIndex * dim
	for i := int64(0); i < dim; i++ {
		sum += left[lFrom+i] * right[rFrom+i]
	}

	return sum
}

// CalcFFBatch calculate the distance of @left & @right vectors in batch by given @metic, store result in @result
func CalcFFBatch(dim int64, left []float32, lIndex int64, right []float32, metric string, result *[]float32) {
	rightNum := int64(len(right)) / dim
	for i := int64(0); i < rightNum; i++ {
		var distance float32 = -1.0
		if metric == L2 {
			distance = CalcL2(dim, left, lIndex, right, i)
		} else if metric == IP {
			distance = CalcIP(dim, left, lIndex, right, i)
		}
		(*result)[lIndex*rightNum+i] = distance
	}
}

// CalcFloatDistance calculate float distance by given metric
// it will checks input, and calculate the distance concurrently
func CalcFloatDistance(dim int64, left, right []float32, metric string) ([]float32, error) {
	if dim <= 0 {
		err := errors.New("invalid dimension")
		return nil, err
	}

	metricUpper := strings.ToUpper(metric)
	if metricUpper != L2 && metricUpper != IP {
		err := errors.New("invalid metric type")
		return nil, err
	}

	err := ValidateFloatArrayLength(dim, len(left))
	if err != nil {
		return nil, err
	}

	err = ValidateFloatArrayLength(dim, len(right))
	if err != nil {
		return nil, err
	}

	leftNum := int64(len(left)) / dim
	rightNum := int64(len(right)) / dim

	distArray := make([]float32, leftNum*rightNum)

	// Multi-threads to calculate distance. TODO: avoid too many go routines
	var waitGroup sync.WaitGroup
	CalcWorker := func(index int64) {
		CalcFFBatch(dim, left, index, right, metricUpper, &distArray)
		waitGroup.Done()
	}
	for i := int64(0); i < leftNum; i++ {
		waitGroup.Add(1)
		go CalcWorker(i)
	}
	waitGroup.Wait()

	return distArray, nil
}
