// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package distance

import (
	"errors"
	"strings"
	"sync"
)

const (
	L2       = "L2"
	IP       = "IP"
	HAMMING  = "HAMMIN"
	TANIMOTO = "TANIMOTO"
)

func ValidateMetricType(metric string) (string, error) {
	if metric == "" {
		err := errors.New("Metric type is empty")
		return "", err
	}

	m := strings.ToUpper(metric)
	if m == L2 || m == IP || m == HAMMING || m == TANIMOTO {
		return m, nil
	}

	err := errors.New("Invalid metric type")
	return metric, err
}

func ValidateArrayLength(dim int64, length int64) error {
	n := length % dim
	if n != 0 {
		err := errors.New("Invalid float vector length")
		return err
	}

	return nil
}

func CalcL2(dim int64, left []float32, lIndex int64, right []float32, rIndex int64) float32 {
	var sum float32 = 0.0
	lFrom := lIndex * dim
	rFrom := rIndex * dim
	for i := int64(0); i < dim; i++ {
		gap := left[lFrom+i] - right[rFrom+i]
		sum += gap * gap
	}

	return sum
}

func CalcIP(dim int64, left []float32, lIndex int64, right []float32, rIndex int64) float32 {
	var sum float32 = 0.0
	lFrom := lIndex * dim
	rFrom := rIndex * dim
	for i := int64(0); i < dim; i++ {
		sum += left[lFrom+i] * right[rFrom+i]
	}

	return sum
}

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

func CalcFloatDistance(dim int64, left []float32, right []float32, metric string) ([]float32, error) {
	metricUpper := strings.ToUpper(metric)
	if metricUpper != L2 && metricUpper != IP {
		err := errors.New("Invalid metric type")
		return nil, err
	}

	err := ValidateArrayLength(dim, int64(len(left)))
	if err != nil {
		return nil, err
	}

	err = ValidateArrayLength(dim, int64(len(right)))
	if err != nil {
		return nil, err
	}

	leftNum := int64(len(left)) / dim
	rightNum := int64(len(right)) / dim

	distArray := make([]float32, leftNum*rightNum)

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

////////////////////////////////////////////////////////////////////////////////
// Count 1 of uint8
// For 00000010, return 1
// Fro 11111111, return 8
func CountOne(n uint8) int {
	count := 0
	for n != 0 {
		count++
		n = n & (n - 1)
	}
	return count
}

// HAMMIN distance
func BinaryVectorXOR(dim int64, v1 []byte, v2 []byte) ([]byte, error) {
	if len(v1) != len(v2) || len(v1)*8 < int(dim) {
		err := errors.New("Binary vectors length not equal")
		return nil, err
	}

	num := len(v1)
	array := make([]byte, num)
	for i := 0; i < num; i++ {
		array[i] = v1[i] ^ v2[i]

		// The dimension "dim" may not be an integer multiple of 8
		// For example:
		//   dim = 11, each vector has 2 unint8 value
		//   the second uint8, only need to calculate 3 bits, the other 5 bits will be set to 0
		if i == num-1 && num*8 > int(dim) {

		}
	}

	return array, nil
}

func CalcBinaryDistance(dim int64, left []byte, right []byte, metric string) ([]int32, error) {
	metricUpper := strings.ToUpper(metric)
	if metricUpper != HAMMING && metricUpper != TANIMOTO {
		err := errors.New("Invalid metric type")
		return nil, err
	}

	return nil, nil
}
