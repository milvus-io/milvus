package distance

import (
	"math"
	"math/bits"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	// zero-norm vectors: cosine is undefined; both paths return 0, never NaN
	zero := []float32{0, 0, 0}
	distance = CosineImplPure(zero, right)
	assert.Equal(t, float32(0), distance)
	distance = CosineImpl(zero, right)
	assert.Equal(t, float32(0), distance)
	distance = CosineImplPure(zero, zero)
	assert.Equal(t, float32(0), distance)
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

	// error cases: only IP is supported for sparse
	_, err = CalcSparseDistance(left, rows, "COSINE")
	assert.Error(t, err)
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

func TestSparseIPImpl(t *testing.T) {
	row1 := typeutil.CreateSparseFloatRow([]uint32{0, 2}, []float32{1, 2})
	row2 := typeutil.CreateSparseFloatRow([]uint32{2, 5}, []float32{2, 3})
	// IP: intersection on index 2 → 2*2
	assert.Equal(t, float32(4), SparseIPImpl(row1, row2))
	// disjoint rows → IP 0
	row3 := typeutil.CreateSparseFloatRow([]uint32{100}, []float32{1})
	assert.Equal(t, float32(0), SparseIPImpl(row1, row3))
}

// TestFusedKernelsExactValues asserts hand-computed truth values for the three
// fused kernels, independent of any reference implementation. left=[1,2,3,4]
// vs row=[5,6,7,8]:
//
//	L2 = (1-5)^2+...+(4-8)^2 = 64
//	IP = 1*5+...+4*8 = 70
//	COSINE = 70 / sqrt(30*174)
//
// All values 1..8 are exactly representable in int8, float16 and bfloat16.
func TestFusedKernelsExactValues(t *testing.T) {
	left := []float32{1, 2, 3, 4}
	cosineWant := 70 / float32(math.Sqrt(30*174))

	cases := []struct {
		name  string
		right []byte
		calc  func(dim int64, left []float32, right []byte, metric string) ([]float32, error)
	}{
		{"int8", []byte{5, 6, 7, 8}, CalcInt8FloatDistance},
		{"float16", typeutil.Float32ArrayToFloat16Bytes([]float32{5, 6, 7, 8}), CalcFloat16FloatDistance},
		{"bfloat16", typeutil.Float32ArrayToBFloat16Bytes([]float32{5, 6, 7, 8}), CalcBFloat16FloatDistance},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.calc(4, left, tc.right, "L2")
			require.NoError(t, err)
			assert.Equal(t, []float32{64}, got, "L2")

			got, err = tc.calc(4, left, tc.right, "IP")
			require.NoError(t, err)
			assert.Equal(t, []float32{70}, got, "IP")

			got, err = tc.calc(4, left, tc.right, "COSINE")
			require.NoError(t, err)
			assert.InDelta(t, cosineWant, got[0], 1e-4, "COSINE")
		})
	}
}

// fused decoder tests: the fused kernels must match the "decode then
// CalcFloatDistance" reference within float32 rounding for the same metric.
func checkFusedDistance(t *testing.T, metric string, got, ref []float32) {
	t.Helper()
	require.Equal(t, len(ref), len(got))
	for i := range ref {
		assert.InDelta(t, ref[i], got[i], 1e-4, "row %d metric %s", i, metric)
	}
}

func TestCalcInt8FloatDistance(t *testing.T) {
	left := []float32{1, 2, 3, 4}
	// two rows: [1,2,3,4] and [-1,-2,-3,-4] as signed bytes
	right := []byte{1, 2, 3, 4, 0xFF, 0xFE, 0xFD, 0xFC}
	refRows := [][]float32{{1, 2, 3, 4}, {-1, -2, -3, -4}}

	for _, metric := range []string{"L2", "IP", "COSINE"} {
		t.Run(metric, func(t *testing.T) {
			got, err := CalcInt8FloatDistance(4, left, right, metric)
			require.NoError(t, err)
			ref, err := CalcFloatDistance(4, left, append(refRows[0], refRows[1]...), metric)
			require.NoError(t, err)
			checkFusedDistance(t, metric, got, ref)
		})
	}

	// invalid inputs
	_, err := CalcInt8FloatDistance(4, []float32{1, 2}, right, "IP")
	require.Error(t, err)
	_, err = CalcInt8FloatDistance(4, left, []byte{1, 2, 3}, "IP") // not a whole row
	require.Error(t, err)
	_, err = CalcInt8FloatDistance(4, left, right, "HAMMING")
	require.Error(t, err)
}

func TestCalcFloat16FloatDistance(t *testing.T) {
	left := []float32{1, 2, 3, 4}
	right := typeutil.Float32ArrayToFloat16Bytes([]float32{1, 2, 3, 4, 5, 6, 7, 8})
	refRows := []float32{1, 2, 3, 4, 5, 6, 7, 8}

	for _, metric := range []string{"L2", "IP", "COSINE"} {
		t.Run(metric, func(t *testing.T) {
			got, err := CalcFloat16FloatDistance(4, left, right, metric)
			require.NoError(t, err)
			ref, err := CalcFloatDistance(4, left, refRows, metric)
			require.NoError(t, err)
			checkFusedDistance(t, metric, got, ref)
		})
	}

	_, err := CalcFloat16FloatDistance(4, left, []byte{0, 0, 1}, "IP")
	require.Error(t, err)
}

func TestCalcBFloat16FloatDistance(t *testing.T) {
	left := []float32{1, 2, 3, 4}
	right := typeutil.Float32ArrayToBFloat16Bytes([]float32{1, 2, 3, 4, 5, 6, 7, 8})
	refRows := []float32{1, 2, 3, 4, 5, 6, 7, 8}

	for _, metric := range []string{"L2", "IP", "COSINE"} {
		t.Run(metric, func(t *testing.T) {
			got, err := CalcBFloat16FloatDistance(4, left, right, metric)
			require.NoError(t, err)
			ref, err := CalcFloatDistance(4, left, refRows, metric)
			require.NoError(t, err)
			checkFusedDistance(t, metric, got, ref)
		})
	}

	_, err := CalcBFloat16FloatDistance(4, left, []byte{0, 0, 1}, "IP")
	require.Error(t, err)
}
