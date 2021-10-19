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
	// L2 represents the Euclidean distance
	L2 = "L2"
	// IP represents the inner product distance
	IP = "IP"
	// HAMMING represents the hamming distance
	HAMMING = "HAMMING"
	// TANIMOTO represents the tanimoto distance
	TANIMOTO = "TANIMOTO"
)

// ValidateMetricType returns metric text or error
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

// ValidateFloatArrayLength is used validate float vector length
func ValidateFloatArrayLength(dim int64, length int) error {
	if length == 0 || int64(length)%dim != 0 {
		err := errors.New("Invalid float vector length")
		return err
	}

	return nil
}

// CalcL2 returns the Euclidean distance of input vectors
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

// CalcIP returns the inner product distance of input vectors
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
	if dim <= 0 {
		err := errors.New("Invalid dimension")
		return nil, err
	}

	metricUpper := strings.ToUpper(metric)
	if metricUpper != L2 && metricUpper != IP {
		err := errors.New("Invalid metric type")
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
func SingleBitLen(dim int64) int64 {
	if dim%8 == 0 {
		return dim
	}

	return dim + 8 - dim%8
}

func VectorCount(dim int64, length int) int64 {
	singleBitLen := SingleBitLen(dim)
	return int64(length*8) / singleBitLen
}

func ValidateBinaryArrayLength(dim int64, length int) error {
	singleBitLen := SingleBitLen(dim)
	totalBitLen := int64(length * 8)
	if length == 0 || totalBitLen%singleBitLen != 0 {
		err := errors.New("Invalid binary vector length")
		return err
	}

	return nil
}

// Count 1 of uint8
// For 00000010, return 1
// Fro 11111111, return 8
func CountOne(n uint8) int32 {
	count := int32(0)
	for n != 0 {
		count++
		n = n & (n - 1)
	}
	return count
}

// HAMMING distance
func CalcHamming(dim int64, left []byte, lIndex int64, right []byte, rIndex int64) int32 {
	singleBitLen := SingleBitLen(dim)
	numBytes := singleBitLen / 8
	lFrom := lIndex * numBytes
	rFrom := rIndex * numBytes

	var hamming int32 = 0
	for i := int64(0); i < numBytes; i++ {
		var xor uint8 = left[lFrom+i] ^ right[rFrom+i]

		// The dimension "dim" may not be an integer multiple of 8
		// For example:
		//   dim = 11, each vector has 2 uint8 value
		//   the second uint8, only need to calculate 3 bits, the other 5 bits will be set to 0
		if i == numBytes-1 && numBytes*8 > dim {
			offset := numBytes*8 - dim
			xor = xor & (255 << offset)
		}

		hamming += CountOne(xor)
	}

	return hamming
}

func CalcHammingBatch(dim int64, left []byte, lIndex int64, right []byte, result *[]int32) {
	rightNum := VectorCount(dim, len(right))

	for i := int64(0); i < rightNum; i++ {
		hamming := CalcHamming(dim, left, lIndex, right, i)
		(*result)[lIndex*rightNum+i] = hamming
	}
}

func CalcHammingDistance(dim int64, left []byte, right []byte) ([]int32, error) {
	if dim <= 0 {
		err := errors.New("Invalid dimension")
		return nil, err
	}

	err := ValidateBinaryArrayLength(dim, len(left))
	if err != nil {
		return nil, err
	}

	err = ValidateBinaryArrayLength(dim, len(right))
	if err != nil {
		return nil, err
	}

	leftNum := VectorCount(dim, len(left))
	rightNum := VectorCount(dim, len(right))
	distArray := make([]int32, leftNum*rightNum)

	var waitGroup sync.WaitGroup
	CalcWorker := func(index int64) {
		CalcHammingBatch(dim, left, index, right, &distArray)
		waitGroup.Done()
	}
	for i := int64(0); i < leftNum; i++ {
		waitGroup.Add(1)
		go CalcWorker(i)
	}
	waitGroup.Wait()

	return distArray, nil
}

func CalcTanimotoCoefficient(dim int64, hamming []int32) ([]float32, error) {
	if dim <= 0 || len(hamming) == 0 {
		err := errors.New("Invalid input for tanimoto")
		return nil, err
	}

	array := make([]float32, len(hamming))
	for i := 0; i < len(hamming); i++ {
		if hamming[i] > int32(dim) {
			err := errors.New("Invalid hamming for tanimoto")
			return nil, err
		}
		equalBits := int32(dim) - hamming[i]
		array[i] = float32(equalBits) / (float32(dim)*2 - float32(equalBits))
	}

	return array, nil
}
