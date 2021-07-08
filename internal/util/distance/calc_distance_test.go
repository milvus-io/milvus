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
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const PRECISION = 1e-6

func TestValidateMetricType(t *testing.T) {
	invalidMetric := []string{"", "aaa"}
	for _, str := range invalidMetric {
		_, err := ValidateMetricType(str)
		assert.Error(t, err)
	}

	validMetric := []string{"L2", "ip", "Hamming", "Tanimoto"}
	for _, str := range validMetric {
		metric, err := ValidateMetricType(str)
		assert.Nil(t, err)
		assert.True(t, metric == L2 || metric == IP || metric == HAMMING || metric == TANIMOTO)
	}
}

func TestValidateFloatArrayLength(t *testing.T) {
	err := ValidateFloatArrayLength(3, 12)
	assert.Nil(t, err)

	err = ValidateFloatArrayLength(5, 11)
	assert.Error(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func CreateFloatArray(n int64, dim int64) []float32 {
	rand.Seed(time.Now().UnixNano())
	num := n * dim
	array := make([]float32, num)
	for i := int64(0); i < num; i++ {
		array[i] = rand.Float32()
	}

	return array
}

func DistanceL2(left []float32, right []float32) float32 {
	if len(left) != len(right) {
		panic("array dimension not equal")
	}
	var sum float32 = 0.0
	for i := 0; i < len(left); i++ {
		gap := left[i] - right[i]
		sum += gap * gap
	}

	return sum
}

func DistanceIP(left []float32, right []float32) float32 {
	if len(left) != len(right) {
		panic("array dimension not equal")
	}
	var sum float32 = 0.0
	for i := 0; i < len(left); i++ {
		sum += left[i] * right[i]
	}

	return sum
}

func TestCalcL2(t *testing.T) {
	var dim int64 = 128
	var leftNum int64 = 1
	var rightNum int64 = 1

	left := CreateFloatArray(leftNum, dim)
	right := CreateFloatArray(rightNum, dim)

	sum := DistanceL2(left, right)

	distance := CalcL2(dim, left, 0, right, 0)
	assert.Less(t, math.Abs(float64(sum-distance)), PRECISION)

	distance = CalcL2(dim, left, 0, left, 0)
	assert.Less(t, float64(distance), PRECISION)
}

func TestCalcIP(t *testing.T) {
	var dim int64 = 128
	var leftNum int64 = 1
	var rightNum int64 = 1

	left := CreateFloatArray(leftNum, dim)
	right := CreateFloatArray(rightNum, dim)

	sum := DistanceIP(left, right)

	distance := CalcIP(dim, left, 0, right, 0)
	assert.Less(t, math.Abs(float64(sum-distance)), PRECISION)
}

func TestCalcFloatDistance(t *testing.T) {
	var dim int64 = 128
	var leftNum int64 = 10
	var rightNum int64 = 5

	left := CreateFloatArray(leftNum, dim)
	right := CreateFloatArray(rightNum, dim)

	_, err := CalcFloatDistance(dim, left, right, "HAMMIN")
	assert.Error(t, err)

	_, err = CalcFloatDistance(3, left, right, "L2")
	assert.Error(t, err)

	_, err = CalcFloatDistance(dim, left, right, "HAMMIN")
	assert.Error(t, err)

	_, err = CalcFloatDistance(0, left, right, "L2")
	assert.Error(t, err)

	distances, err := CalcFloatDistance(dim, left, right, "L2")
	assert.Nil(t, err)

	for i := int64(0); i < leftNum; i++ {
		for j := int64(0); j < rightNum; j++ {
			v1 := left[i*dim : (i+1)*dim]
			v2 := right[j*dim : (j+1)*dim]
			sum := DistanceL2(v1, v2)
			assert.Less(t, math.Abs(float64(sum-distances[i*rightNum+j])), PRECISION)
		}
	}

	distances, err = CalcFloatDistance(dim, left, right, "IP")
	assert.Nil(t, err)

	for i := int64(0); i < leftNum; i++ {
		for j := int64(0); j < rightNum; j++ {
			v1 := left[i*dim : (i+1)*dim]
			v2 := right[j*dim : (j+1)*dim]
			sum := DistanceIP(v1, v2)
			assert.Less(t, math.Abs(float64(sum-distances[i*rightNum+j])), PRECISION)
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
func CreateBinaryArray(n int64, dim int64) []byte {
	rand.Seed(time.Now().UnixNano())
	num := n * dim / 8
	if num*8 < n*dim {
		num = num + 1
	}
	array := make([]byte, num)
	for i := int64(0); i < num; i++ {
		n := rand.Intn(256)
		array[i] = uint8(n)
	}

	return array
}

func TestSingleBitLen(t *testing.T) {
	n := SingleBitLen(125)
	assert.Equal(t, n, int64(128))

	n = SingleBitLen(133)
	assert.Equal(t, n, int64(136))
}

func TestVectorCount(t *testing.T) {
	n := VectorCount(15, 20)
	assert.Equal(t, n, int64(10))

	n = VectorCount(8, 3)
	assert.Equal(t, n, int64(3))
}

func TestValidateBinaryArrayLength(t *testing.T) {
	err := ValidateBinaryArrayLength(21, 12)
	assert.Nil(t, err)

	err = ValidateBinaryArrayLength(21, 11)
	assert.Error(t, err)
}

func TestCountOne(t *testing.T) {
	n := CountOne(6)
	assert.Equal(t, n, int32(2))

	n = CountOne(0)
	assert.Equal(t, n, int32(0))

	n = CountOne(255)
	assert.Equal(t, n, int32(8))
}

func TestCalcHamming(t *testing.T) {
	var dim int64 = 22
	// v1 = 00000010 00000110 00001000
	v1 := make([]uint8, 3)
	v1[0] = 2
	v1[1] = 6
	v1[2] = 8
	// v2 = 00000001 00000111 00011011
	v2 := make([]uint8, 3)
	v2[0] = 1
	v2[1] = 7
	v2[2] = 27
	n := CalcHamming(dim, v1, 0, v2, 0)
	assert.Equal(t, n, int32(4))

	hamming := make([]int32, 1)
	hamming[0] = n
	tanimoto, err := CalcTanimotoCoefficient(dim, hamming)
	realTanimoto := float64(n) / (float64(dim)*2.0 - float64(n))
	assert.Nil(t, err)
	assert.Less(t, math.Abs(float64(tanimoto[0])-realTanimoto), float64(PRECISION))
}

func TestCalcHamminDistance(t *testing.T) {
	var dim int64 = 125
	var leftNum int64 = 2

	left := CreateBinaryArray(leftNum, dim)

	_, e := CalcHammingDistance(0, left, left)
	assert.Error(t, e)

	distances, err := CalcHammingDistance(dim, left, left)
	assert.Nil(t, err)

	n := CalcHamming(dim, left, 0, left, 0)
	assert.Equal(t, n, int32(0))

	n = CalcHamming(dim, left, 1, left, 1)
	assert.Equal(t, n, int32(0))

	n = CalcHamming(dim, left, 0, left, 1)
	assert.Equal(t, n, distances[1])

	n = CalcHamming(dim, left, 1, left, 0)
	assert.Equal(t, n, distances[2])
}
