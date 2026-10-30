package distance

import (
	"math"
	"math/bits"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

// hammingRef is the hand-rolled hamming distance over packed bits (LSB-first
// per byte, matching the Milvus binary vector convention).
func hammingRef(a, b []byte) float32 {
	var sum float32
	for i := range a {
		sum += float32(bits.OnesCount8(a[i] ^ b[i]))
	}
	return sum
}

func Test_CalcBinaryDistance(t *testing.T) {
	dim := int64(16) // 2 bytes per vector
	left := []byte{0b00001111, 0b10101010}
	right := []byte{
		0b00001111, 0b10101010, // identical
		0b00000000, 0b00000000, // 8 bits differ
		0b11111111, 0b01010101, // all 16 bits differ
	}

	distances, err := CalcBinaryDistance(dim, left, right, "HAMMING")
	assert.NoError(t, err)
	expected := []float32{
		hammingRef(left, right[0:2]),
		hammingRef(left, right[2:4]),
		hammingRef(left, right[4:6]),
	}
	assert.Equal(t, expected, distances)

	// Jaccard: 1 - |A∩B| / |A∪B|
	distances, err = CalcBinaryDistance(dim, left, right, "JACCARD")
	assert.NoError(t, err)
	for i := 0; i < 3; i++ {
		var intersect, unionA, unionB float32
		for j := 0; j < 2; j++ {
			unionA += float32(bits.OnesCount8(left[j]))
			unionB += float32(bits.OnesCount8(right[i*2+j]))
			intersect += float32(bits.OnesCount8(left[j] & right[i*2+j]))
		}
		want := 1 - intersect/(unionA+unionB-intersect)
		assert.Equal(t, want, distances[i])
	}

	// error cases
	_, err = CalcBinaryDistance(dim, left, right, "L2")
	assert.Error(t, err)
	_, err = CalcBinaryDistance(7, left, right, "HAMMING") // dim not multiple of 8
	assert.Error(t, err)
	_, err = CalcBinaryDistance(dim, left, []byte{1}, "HAMMING") // bad byte length
	assert.Error(t, err)
}

func sparseRow(indices []uint32, values []float32) []byte {
	indices, values = typeutil.SortSparseFloatRow(indices, values)
	return typeutil.CreateSparseFloatRow(indices, values)
}

func Test_CalcSparseDistance(t *testing.T) {
	left := sparseRow([]uint32{0, 2, 5}, []float32{1, 2, 3})
	rows := [][]byte{
		sparseRow([]uint32{0, 2, 5}, []float32{1, 2, 3}), // identical → IP = 1+4+9
		sparseRow([]uint32{2, 7}, []float32{2, 10}),      // overlap on idx 2 → IP = 4
		sparseRow([]uint32{100}, []float32{5}),           // no overlap → IP = 0
	}

	distances, err := CalcSparseDistance(left, rows, "IP")
	assert.NoError(t, err)
	assert.Equal(t, []float32{14, 4, 0}, distances)

	// cosine: dot / (normA * normB)
	distances, err = CalcSparseDistance(left, rows, "COSINE")
	assert.NoError(t, err)
	normA := float32(math.Sqrt(float64(1*1 + 2*2 + 3*3)))
	assert.InEpsilon(t, 14/(normA*normA), distances[0], PRECISION)
	assert.InEpsilon(t, 4/(normA*float32(math.Sqrt(float64(4+100)))), distances[1], PRECISION)
	// disjoint vectors have zero cosine similarity
	assert.Equal(t, float32(0), distances[2])

	// error cases
	_, err = CalcSparseDistance(left, rows, "L2")
	assert.Error(t, err)
}

func TestBinaryImpls(t *testing.T) {
	a := []byte{0b00001111, 0b10101010}
	b := []byte{0b11111111, 0b01010101}
	// hamming: popcount(0x0f^0xff)=4 + popcount(0xaa^0x55)=8 = 12
	assert.Equal(t, float32(12), HammingImplPure(a, b))
	assert.Equal(t, float32(12), HammingImpl(a, b))
	// jaccard: unionA=8, unionB=12, intersect=4, union=16, 1-4/16
	assert.InEpsilon(t, 0.75, JaccardImplPure(a, b), PRECISION)
	assert.InEpsilon(t, 0.75, JaccardImpl(a, b), PRECISION)
	// two empty rows: no union → 0
	assert.Equal(t, float32(0), JaccardImplPure(nil, nil))
}

func TestSparseImpls(t *testing.T) {
	row1 := typeutil.CreateSparseFloatRow([]uint32{0, 2}, []float32{1, 2})
	row2 := typeutil.CreateSparseFloatRow([]uint32{2, 5}, []float32{2, 3})
	// IP: intersection on index 2 → 2*2
	assert.Equal(t, float32(4), SparseIPImpl(row1, row2))
	// COSINE: 4 / sqrt(5*13)
	assert.InEpsilon(t, 4/float32(math.Sqrt(5*13)), SparseCosineImpl(row1, row2), PRECISION)
	// disjoint rows → IP 0, COSINE 0
	row3 := typeutil.CreateSparseFloatRow([]uint32{100}, []float32{1})
	assert.Equal(t, float32(0), SparseIPImpl(row1, row3))
	assert.Equal(t, float32(0), SparseCosineImpl(row1, row3))
}
