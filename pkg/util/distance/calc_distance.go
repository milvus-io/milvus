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

	// Zero-norm vectors make cosine undefined; return 0 instead of NaN (0/0),
	// matching knowhere's normalize-then-IP search semantics and the fused
	// int8/fp16/bf16 kernels.
	if normA == 0 || normB == 0 {
		return 0
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
	return sparseIPMaps(typeutil.SparseFloatBytesToMap(a), typeutil.SparseFloatBytesToMap(b))
}

// sparseIPMaps returns the inner product of two decoded sparse rows, iterating
// the smaller map.
func sparseIPMaps(left, right map[uint32]float32) float32 {
	if len(left) > len(right) {
		left, right = right, left
	}
	var sum float32
	for idx, value := range left {
		sum += value * right[idx]
	}
	return sum
}

// CalcSparseDistance computes the distance between a compact sparse reference
// row and each sparse row in @rows (compact 8-bytes-per-element encoding) by
// given @metric. The reference row is decoded to a map once and reused across
// rows, so scoring N rows allocates N maps total (one per row plus the
// reference), not 2N.
func CalcSparseDistance(left []byte, rows [][]byte, metric string) ([]float32, error) {
	metricUpper := strings.ToUpper(metric)
	if metricUpper != IP {
		return nil, merr.WrapErrParameterInvalidMsg("invalid sparse metric type %s, only IP is supported", metric)
	}
	leftMap := typeutil.SparseFloatBytesToMap(left)
	distances := make([]float32, len(rows))
	for i, row := range rows {
		distances[i] = sparseIPMaps(leftMap, typeutil.SparseFloatBytesToMap(row))
	}
	return distances, nil
}

// CalcInt8FloatDistance computes the distance between left (a single decoded
// reference vector) and every row of the int8 byte vector right, where each
// row is dim signed bytes. The bytes are decoded inline inside the scoring
// loop, so no intermediate []float32 array is materialized for right.
func CalcInt8FloatDistance(dim int64, left []float32, right []byte, metric string) ([]float32, error) {
	return int8FloatDistanceImpl(dim, left, right, metric)
}

// CalcFloat16FloatDistance computes the distance between left (a single decoded
// reference vector) and every row of the float16 byte vector right, where each
// row is dim little-endian half-precision values. Bytes are decoded inline, so
// no intermediate []float32 array is materialized for right.
func CalcFloat16FloatDistance(dim int64, left []float32, right []byte, metric string) ([]float32, error) {
	return float16FloatDistanceImpl(dim, left, right, metric)
}

// CalcBFloat16FloatDistance computes the distance between left (a single
// decoded reference vector) and every row of the bfloat16 byte vector right,
// where each row is dim little-endian bfloat16 values. Bytes are decoded
// inline, so no intermediate []float32 array is materialized for right.
func CalcBFloat16FloatDistance(dim int64, left []float32, right []byte, metric string) ([]float32, error) {
	return bfloat16FloatDistanceImpl(dim, left, right, metric)
}

// Implementation hooks for the fused decoders. The amd64 SIMD path may replace
// these (e.g. F16C for float16) after a runtime CPUID check; the pure-Go
// versions below are the portable fallback.
var (
	int8FloatDistanceImpl     func(dim int64, left []float32, right []byte, metric string) ([]float32, error) = calcInt8FloatDistancePure
	float16FloatDistanceImpl  func(dim int64, left []float32, right []byte, metric string) ([]float32, error) = calcFloat16FloatDistancePure
	bfloat16FloatDistanceImpl func(dim int64, left []float32, right []byte, metric string) ([]float32, error) = calcBFloat16FloatDistancePure
)

// validateDecodedDistanceArgs validates the shared fused-decoder inputs: left
// is exactly one dim-sized vector, right is whole rows of rowBytes bytes each,
// and the metric is a dense float metric.
func validateDecodedDistanceArgs(dim int64, left []float32, right []byte, rowBytes int64, metric string) (string, int64, error) {
	if dim <= 0 {
		return "", 0, merr.WrapErrParameterInvalidMsg("invalid dimension %d", dim)
	}
	if int64(len(left)) != dim {
		return "", 0, merr.WrapErrParameterInvalidMsg("reference vector length %d does not match dim %d", len(left), dim)
	}
	if int64(len(right))%rowBytes != 0 {
		return "", 0, merr.WrapErrParameterInvalidMsg("vector data length %d is not a multiple of row size %d", len(right), rowBytes)
	}
	m, err := ValidateMetricType(metric)
	if err != nil {
		return "", 0, err
	}
	return m, int64(len(right)) / rowBytes, nil
}

// calcInt8FloatDistancePure is the pure-Go fused int8 decoder + scorer.
func calcInt8FloatDistancePure(dim int64, left []float32, right []byte, metric string) ([]float32, error) {
	metricUpper, rightNum, err := validateDecodedDistanceArgs(dim, left, right, dim, metric)
	if err != nil {
		return nil, err
	}
	distances := make([]float32, rightNum)
	score := newDenseScorer(metricUpper, left)
	for row := int64(0); row < rightNum; row++ {
		base := row * dim
		score.reset()
		for d := int64(0); d < dim; d++ {
			score.accum(left[d], float32(int8(right[base+d])))
		}
		distances[row] = score.result()
	}
	return distances, nil
}

// calcFloat16FloatDistancePure is the pure-Go fused float16 decoder + scorer.
func calcFloat16FloatDistancePure(dim int64, left []float32, right []byte, metric string) ([]float32, error) {
	metricUpper, rightNum, err := validateDecodedDistanceArgs(dim, left, right, dim*2, metric)
	if err != nil {
		return nil, err
	}
	distances := make([]float32, rightNum)
	score := newDenseScorer(metricUpper, left)
	for row := int64(0); row < rightNum; row++ {
		base := row * dim * 2
		score.reset()
		for d := int64(0); d < dim; d++ {
			score.accum(left[d], typeutil.Float16BytesToFloat32(right[base+2*d:base+2*d+2]))
		}
		distances[row] = score.result()
	}
	return distances, nil
}

// calcBFloat16FloatDistancePure is the pure-Go fused bfloat16 decoder + scorer.
func calcBFloat16FloatDistancePure(dim int64, left []float32, right []byte, metric string) ([]float32, error) {
	metricUpper, rightNum, err := validateDecodedDistanceArgs(dim, left, right, dim*2, metric)
	if err != nil {
		return nil, err
	}
	distances := make([]float32, rightNum)
	score := newDenseScorer(metricUpper, left)
	for row := int64(0); row < rightNum; row++ {
		base := row * dim * 2
		score.reset()
		for d := int64(0); d < dim; d++ {
			score.accum(left[d], typeutil.BFloat16BytesToFloat32(right[base+2*d:base+2*d+2]))
		}
		distances[row] = score.result()
	}
	return distances, nil
}

// denseScorer accumulates one row's metric against a fixed reference vector.
// normL is precomputed once for COSINE so it is not recomputed per row.
type denseScorer struct {
	metric string
	normL  float32
	acc    float32
	normR  float32
	cosine bool
}

func newDenseScorer(metric string, left []float32) *denseScorer {
	s := &denseScorer{metric: metric}
	if metric == COSINE {
		s.cosine = true
		for _, v := range left {
			s.normL += v * v
		}
	}
	return s
}

func (s *denseScorer) reset() {
	s.acc = 0
	s.normR = 0
}

func (s *denseScorer) accum(lv, rv float32) {
	switch s.metric {
	case L2:
		diff := lv - rv
		s.acc += diff * diff
	case IP:
		s.acc += lv * rv
	case COSINE:
		s.acc += lv * rv
		s.normR += rv * rv
	}
}

func (s *denseScorer) result() float32 {
	if !s.cosine {
		return s.acc
	}
	if s.normL == 0 || s.normR == 0 {
		return 0
	}
	return s.acc / float32(math.Sqrt(float64(s.normL)*float64(s.normR)))
}
