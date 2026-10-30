package distance

import (
	"math"
	"math/bits"
	"strings"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	// COSINE represents the cosine distance
	COSINE = "COSINE"
	// HAMMING represents the hamming distance of binary vectors
	HAMMING = "HAMMING"
	// JACCARD represents the jaccard distance of binary vectors
	JACCARD = "JACCARD"
)

func L2ImplPure(a []float32, b []float32) float32 {
	var sum float32

	for i := range a {
		sum += (a[i] - b[i]) * (a[i] - b[i])
	}

	return sum
}

func IPImplPure(a []float32, b []float32) float32 {
	var sum float32

	for i := range a {
		sum += a[i] * b[i]
	}

	return sum
}

func CosineImplPure(a []float32, b []float32) float32 {
	var sum, normA, normB float32

	for i := range a {
		sum += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	return sum / float32(math.Sqrt(float64(normA)*float64(normB)))
}

var (
	L2Impl     func(a []float32, b []float32) float32 = L2ImplPure
	IPImpl     func(a []float32, b []float32) float32 = IPImplPure
	CosineImpl func(a []float32, b []float32) float32 = CosineImplPure
)

// ValidateMetricType returns metric text or error
func ValidateMetricType(metric string) (string, error) {
	if metric == "" {
		err := merr.WrapErrParameterInvalidMsg("metric type is empty")
		return "", err
	}

	m := strings.ToUpper(metric)
	if m == L2 || m == IP || m == COSINE {
		return m, nil
	}

	err := merr.WrapErrParameterInvalidMsg("invalid metric type")
	return metric, err
}

// ValidateFloatArrayLength is used validate float vector length
func ValidateFloatArrayLength(dim int64, length int) error {
	if length == 0 || int64(length)%dim != 0 {
		err := merr.WrapErrParameterInvalidMsg("invalid float vector length")
		return err
	}

	return nil
}

// CalcFFBatch calculate the distance of @left & @right vectors in batch by given @metic, store result in @result
func CalcFFBatch(dim int64, left []float32, lIndex int64, right []float32, metric string, result *[]float32) {
	rightNum := int64(len(right)) / dim
	for i := int64(0); i < rightNum; i++ {
		var distance float32 = -1.0
		switch metric {
		case L2:
			distance = L2Impl(left[lIndex*dim:lIndex*dim+dim], right[i*dim:i*dim+dim])
		case IP:
			distance = IPImpl(left[lIndex*dim:lIndex*dim+dim], right[i*dim:i*dim+dim])
		case COSINE:
			distance = CosineImpl(left[lIndex*dim:lIndex*dim+dim], right[i*dim:i*dim+dim])
		}
		(*result)[lIndex*rightNum+i] = distance
	}
}

// CalcFloatDistance calculate float distance by given metric
// it will checks input, and calculate the distance concurrently
func CalcFloatDistance(dim int64, left, right []float32, metric string) ([]float32, error) {
	if dim <= 0 {
		err := merr.WrapErrParameterInvalidMsg("invalid dimension")
		return nil, err
	}

	metricUpper := strings.ToUpper(metric)
	if metricUpper != L2 && metricUpper != IP && metricUpper != COSINE {
		err := merr.WrapErrParameterInvalidMsg("invalid metric type")
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

// HammingImplPure returns the number of differing bits between two packed
// binary vectors (equivalent byte lengths).
func HammingImplPure(a, b []byte) float32 {
	var distance float32
	for i := range a {
		distance += float32(bits.OnesCount8(a[i] ^ b[i]))
	}
	return distance
}

// JaccardImplPure returns the Jaccard distance (1 - |A∩B| / |A∪B|) between two
// packed binary vectors. 0 when both vectors are empty.
func JaccardImplPure(a, b []byte) float32 {
	var intersect, unionA, unionB float32
	for i := range a {
		unionA += float32(bits.OnesCount8(a[i]))
		unionB += float32(bits.OnesCount8(b[i]))
		intersect += float32(bits.OnesCount8(a[i] & b[i]))
	}
	union := unionA + unionB - intersect
	if union == 0 {
		return 0
	}
	return 1 - intersect/union
}

var (
	HammingImpl func(a []byte, b []byte) float32 = HammingImplPure
	JaccardImpl func(a []byte, b []byte) float32 = JaccardImplPure
)

// CalcBinaryDistance computes the distance between the packed binary vectors in
// @left and @right (each vector is @dim/8 bytes) by given @metric, storing the
// result in row-major order (@leftNum x @rightNum).
func CalcBinaryDistance(dim int64, left, right []byte, metric string) ([]float32, error) {
	if dim <= 0 || dim%8 != 0 {
		return nil, merr.WrapErrParameterInvalidMsg("invalid binary vector dimension %d, must be a positive multiple of 8", dim)
	}
	metricUpper := strings.ToUpper(metric)
	if metricUpper != HAMMING && metricUpper != JACCARD {
		return nil, merr.WrapErrParameterInvalidMsg("invalid binary metric type %s, only HAMMING/JACCARD are supported", metric)
	}
	vectorBytes := int(dim / 8)
	if len(left)%vectorBytes != 0 || len(right)%vectorBytes != 0 {
		return nil, merr.WrapErrParameterInvalidMsg("invalid binary vector byte length, must be a multiple of %d", vectorBytes)
	}
	leftNum := len(left) / vectorBytes
	rightNum := len(right) / vectorBytes

	distArray := make([]float32, leftNum*rightNum)
	for i := 0; i < leftNum; i++ {
		l := left[i*vectorBytes : (i+1)*vectorBytes]
		for j := 0; j < rightNum; j++ {
			r := right[j*vectorBytes : (j+1)*vectorBytes]
			switch metricUpper {
			case HAMMING:
				distArray[i*rightNum+j] = HammingImpl(l, r)
			case JACCARD:
				distArray[i*rightNum+j] = JaccardImpl(l, r)
			}
		}
	}
	return distArray, nil
}

// SparseIPImpl returns the inner product of two compact sparse rows (see
// typeutil.SparseFloatBytesToMap for the 8-bytes-per-element encoding).
func SparseIPImpl(a, b []byte) float32 {
	left := typeutil.SparseFloatBytesToMap(a)
	right := typeutil.SparseFloatBytesToMap(b)
	if len(left) > len(right) {
		left, right = right, left
	}
	var sum float32
	for idx, value := range left {
		sum += value * right[idx]
	}
	return sum
}

// SparseCosineImpl returns the cosine similarity of two compact sparse rows.
func SparseCosineImpl(a, b []byte) float32 {
	left := typeutil.SparseFloatBytesToMap(a)
	right := typeutil.SparseFloatBytesToMap(b)
	// iterate the smaller map for the dot product over the index intersection
	if len(left) > len(right) {
		left, right = right, left
	}
	var dot, normA, normB float32
	for idx, value := range left {
		if v, ok := right[idx]; ok {
			dot += value * v
		}
		normA += value * value
	}
	for _, value := range right {
		normB += value * value
	}
	if normA == 0 || normB == 0 {
		return 0
	}
	return dot / float32(math.Sqrt(float64(normA)*float64(normB)))
}

// CalcSparseDistance computes the distance between a compact sparse reference
// row and each sparse row in @rows (compact 8-bytes-per-element encoding) by
// given @metric.
func CalcSparseDistance(left []byte, rows [][]byte, metric string) ([]float32, error) {
	metricUpper := strings.ToUpper(metric)
	if metricUpper != IP && metricUpper != COSINE {
		return nil, merr.WrapErrParameterInvalidMsg("invalid sparse metric type %s, only IP/COSINE are supported", metric)
	}
	distances := make([]float32, len(rows))
	for i, row := range rows {
		switch metricUpper {
		case IP:
			distances[i] = SparseIPImpl(left, row)
		case COSINE:
			distances[i] = SparseCosineImpl(left, row)
		}
	}
	return distances, nil
}
