package distance

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const PRECISION = 1e-5

func TestValidateMetricType(t *testing.T) {
	invalidMetric := []string{"", "aaa"}
	for _, str := range invalidMetric {
		_, err := ValidateMetricType(str)
		assert.Error(t, err)
	}

	validMetric := []string{"L2", "ip", "COSINE"}
	for _, str := range validMetric {
		metric, err := ValidateMetricType(str)
		assert.NoError(t, err)
		assert.True(t, metric == L2 || metric == IP || metric == COSINE)
	}
}

func TestValidateFloatArrayLength(t *testing.T) {
	err := ValidateFloatArrayLength(3, 12)
	assert.NoError(t, err)

	err = ValidateFloatArrayLength(5, 11)
	assert.Error(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func CreateFloatArray(n, dim int64) []float32 {
	rand.Seed(time.Now().UnixNano())
	num := n * dim
	array := make([]float32, num)
	for i := int64(0); i < num; i++ {
		array[i] = rand.Float32()
	}

	return array
}

func DistanceL2(left, right []float32) float32 {
	if len(left) != len(right) {
		panic("array dimension not equal")
	}
	var sum float32
	for i := 0; i < len(left); i++ {
		gap := left[i] - right[i]
		sum += gap * gap
	}

	return sum
}

func DistanceIP(left, right []float32) float32 {
	if len(left) != len(right) {
		panic("array dimension not equal")
	}
	var sum float32
	for i := 0; i < len(left); i++ {
		sum += left[i] * right[i]
	}

	return sum
}

func DistanceCosine(left, right []float32) float32 {
	if len(left) != len(right) {
		panic("array dimension not equal")
	}
	return DistanceIP(left, right) / float32(math.Sqrt(float64(DistanceIP(left, left))*float64(DistanceIP(right, right))))
}

func Test_CalcL2(t *testing.T) {
	var dim int64 = 128
	var leftNum int64 = 1
	var rightNum int64 = 1

	left := CreateFloatArray(leftNum, dim)
	right := CreateFloatArray(rightNum, dim)

	sum := DistanceL2(left, right)

	distance := L2Impl(left, right)
	assert.InEpsilon(t, sum, distance, PRECISION)
	distance = L2ImplPure(left, right)
	assert.InEpsilon(t, sum, distance, PRECISION)

	left = []float32{0, 1, 2}
	right = []float32{1, 2, 3}
	expected := float32(3)
	distance = L2Impl(left, right)
	assert.Equal(t, expected, distance)

	distance = L2ImplPure(left, right)
	assert.Equal(t, expected, distance)
}

func Test_CalcIP(t *testing.T) {
	var dim int64 = 128
	var leftNum int64 = 1
	var rightNum int64 = 1

	left := CreateFloatArray(leftNum, dim)
	right := CreateFloatArray(rightNum, dim)

	sum := DistanceIP(left, right)

	distance := IPImpl(left, right)
	assert.InEpsilon(t, sum, distance, PRECISION)
	distance = IPImplPure(left, right)
	assert.InEpsilon(t, sum, distance, PRECISION)

	left = []float32{0, 1, 2}
	right = []float32{1, 2, 3}
	expected := float32(8)
	distance = IPImpl(left, right)
	assert.Equal(t, expected, distance)
	distance = IPImplPure(left, right)
	assert.Equal(t, expected, distance)
}

func Test_CalcCosine(t *testing.T) {
	var dim int64 = 128
	var leftNum int64 = 1
	var rightNum int64 = 1

	left := CreateFloatArray(leftNum, dim)
	right := CreateFloatArray(rightNum, dim)

	sum := DistanceCosine(left, right)

	distance := CosineImpl(left, right)
	assert.InEpsilon(t, sum, distance, PRECISION)
	distance = CosineImplPure(left, right)
	assert.InEpsilon(t, sum, distance, PRECISION)

	left = []float32{0, 0, 10}
	right = []float32{6, 0, 8}
	expected := float32(0.8)
	distance = CosineImpl(left, right)
	assert.Equal(t, expected, distance)
	distance = CosineImplPure(left, right)
	assert.Equal(t, expected, distance)
}

func Test_CalcFloatDistance(t *testing.T) {
	var dim int64 = 128
	var leftNum int64 = 10
	var rightNum int64 = 5

	left := CreateFloatArray(leftNum, dim)
	right := CreateFloatArray(rightNum, dim)

	// Verify illegal cases
	_, err := CalcFloatDistance(dim, left, right, "HAMMIN")
	assert.Error(t, err)

	_, err = CalcFloatDistance(3, left, right, "L2")
	assert.Error(t, err)

	_, err = CalcFloatDistance(dim, left, right, "HAMMIN")
	assert.Error(t, err)

	_, err = CalcFloatDistance(0, left, right, "L2")
	assert.Error(t, err)

	distances, err := CalcFloatDistance(dim, left, right, "L2")
	assert.NoError(t, err)

	// Verify the L2 distance algorithm is correct
	invalid := CreateFloatArray(rightNum, 10)
	_, err = CalcFloatDistance(dim, left, invalid, "L2")
	assert.Error(t, err)

	for i := int64(0); i < leftNum; i++ {
		for j := int64(0); j < rightNum; j++ {
			v1 := left[i*dim : (i+1)*dim]
			v2 := right[j*dim : (j+1)*dim]
			sum := DistanceL2(v1, v2)
			assert.InEpsilon(t, sum, distances[i*rightNum+j], PRECISION)
		}
	}

	// Verify the IP distance algorithm is correct
	distances, err = CalcFloatDistance(dim, left, right, "IP")
	assert.NoError(t, err)

	for i := int64(0); i < leftNum; i++ {
		for j := int64(0); j < rightNum; j++ {
			v1 := left[i*dim : (i+1)*dim]
			v2 := right[j*dim : (j+1)*dim]
			sum := DistanceIP(v1, v2)
			assert.InEpsilon(t, sum, distances[i*rightNum+j], PRECISION)
		}
	}

	// Verify the COSINE distance algorithm is correct
	distances, err = CalcFloatDistance(dim, left, right, "COSINE")
	assert.NoError(t, err)

	for i := int64(0); i < leftNum; i++ {
		for j := int64(0); j < rightNum; j++ {
			v1 := left[i*dim : (i+1)*dim]
			v2 := right[j*dim : (j+1)*dim]
			sum := DistanceCosine(v1, v2)
			assert.InEpsilon(t, sum, distances[i*rightNum+j], PRECISION)
		}
	}
}
