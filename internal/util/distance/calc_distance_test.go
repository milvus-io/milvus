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

func TestValidateMetricType(t *testing.T) {
	invalid_metric := []string{"", "aaa"}
	for _, str := range invalid_metric {
		_, err := ValidateMetricType(str)
		assert.Error(t, err)
	}

	valid_metric := []string{"L2", "ip", "Hammin", "Tanimoto"}
	for _, str := range valid_metric {
		metric, err := ValidateMetricType(str)
		assert.Nil(t, err)
		assert.True(t, metric == L2 || metric == IP || metric == HAMMING || metric == TANIMOTO)
	}
}

func TestValidateArrayLength(t *testing.T) {
	err := ValidateArrayLength(3, 12)
	assert.Nil(t, err)

	err = ValidateArrayLength(5, 11)
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
	precision := 1e-6
	assert.Less(t, math.Abs(float64(sum-distance)), precision)

	distance = CalcL2(dim, left, 0, left, 0)
	assert.Less(t, float64(distance), precision)
}

func TestCalcIP(t *testing.T) {
	var dim int64 = 128
	var leftNum int64 = 1
	var rightNum int64 = 1

	left := CreateFloatArray(leftNum, dim)
	right := CreateFloatArray(rightNum, dim)

	sum := DistanceIP(left, right)

	distance := CalcIP(dim, left, 0, right, 0)
	precision := 1e-6
	assert.Less(t, math.Abs(float64(sum-distance)), precision)
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

	distances, e := CalcFloatDistance(dim, left, right, "L2")
	assert.Nil(t, e)

	precision := 1e-6
	for i := int64(0); i < leftNum; i++ {
		for j := int64(0); j < rightNum; j++ {
			v1 := left[i*dim : (i+1)*dim]
			v2 := right[j*dim : (j+1)*dim]
			sum := DistanceL2(v1, v2)
			assert.Less(t, math.Abs(float64(sum-distances[i*rightNum+j])), precision)
		}
	}

	distances, e = CalcFloatDistance(dim, left, right, "IP")
	assert.Nil(t, e)

	for i := int64(0); i < leftNum; i++ {
		for j := int64(0); j < rightNum; j++ {
			v1 := left[i*dim : (i+1)*dim]
			v2 := right[j*dim : (j+1)*dim]
			sum := DistanceIP(v1, v2)
			assert.Less(t, math.Abs(float64(sum-distances[i*rightNum+j])), precision)
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
func CreateBinaryArray(n int64, dim int64) []byte {
	rand.Seed(time.Now().UnixNano())
	num := int64(n * dim / 8)
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

func TestCountOne(t *testing.T) {
	n := CountOne(6)
	assert.Equal(t, n, 2)

	n = CountOne(0)
	assert.Equal(t, n, 0)

	n = CountOne(255)
	assert.Equal(t, n, 8)
}

func TestBinaryVectorXOR(t *testing.T) {
	var dim int64 = 128
	v1 := CreateBinaryArray(1, dim)
	v2 := CreateBinaryArray(1, dim)
	_, err := BinaryVectorXOR(dim, v1, v2)
	assert.Nil(t, err)
}

func TestCalcBinaryDistance(t *testing.T) {

	// var d uint8 = 1
	// b := XOR(k, d)
	// fmt.Printf("XOR %d\n", b)
}
