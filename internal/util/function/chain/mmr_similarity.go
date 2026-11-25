/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"encoding/binary"
	"math"
	"math/bits"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/distance"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// mmrMetricKind is the per-pair computation used between two candidates.
type mmrMetricKind int

const (
	mmrCosine mmrMetricKind = iota
	mmrIP
	mmrL2
	mmrJaccard
	mmrHamming
)

var mmrMetricKinds = map[string]mmrMetricKind{
	metric.COSINE: mmrCosine, metric.IP: mmrIP, metric.L2: mmrL2,
	metric.JACCARD: mmrJaccard, metric.HAMMING: mmrHamming,
}

// mmrVector is one decoded candidate vector. Exactly one representation is
// populated, depending on the vector type.
type mmrVector struct {
	null bool
	// dense holds Float/Float16/BFloat16/Int8 vectors as float32, L2-normalized
	// for cosine.
	dense []float32
	// bin holds BinaryVector bytes.
	bin []byte
	// sparse holds a SparseFloatVector row; indices are sorted ascending.
	sparseIdx  []uint32
	sparseVal  []float32
	sparseNorm float64
	// elems holds the element vectors of an ArrayOfVector row.
	elems []mmrVector
}

// mmrSimilarity computes candidate-to-candidate similarity in [0, 1] for one
// vector field. COSINE, IP, and L2 use the weighted reranker's norm_score
// mappings; JACCARD and HAMMING use their exact complements. ArrayOfVector
// rows use symmetric MaxSim over their elements.
type mmrSimilarity struct {
	// dataType is the vector type, or the element type for ArrayOfVector.
	dataType schemapb.DataType
	array    bool
	dim      int64
	kind     mmrMetricKind
}

// newMMRSimilarity validates the metric for the vector type. metricType is
// the field's index metric or the user-provided override.
func newMMRSimilarity(dataType, elementType schemapb.DataType, dim int64, metricType string) (*mmrSimilarity, error) {
	s := &mmrSimilarity{dataType: dataType, dim: dim}
	m := strings.ToUpper(metricType)
	if dataType == schemapb.DataType_ArrayOfVector {
		s.array, s.dataType = true, elementType
		if m == metric.MaxSim {
			m = metric.COSINE
		}
		m = strings.TrimPrefix(m, "MAX_SIM_")
	}
	kind, ok := mmrMetricKinds[m]
	s.kind = kind
	switch {
	case typeutil.IsDenseFloatVectorType(s.dataType) || typeutil.IsIntVectorType(s.dataType):
		ok = ok && (kind == mmrCosine || kind == mmrIP || kind == mmrL2)
	case typeutil.IsBinaryVectorType(s.dataType):
		ok = ok && (kind == mmrJaccard || kind == mmrHamming)
	case typeutil.IsSparseFloatVectorType(s.dataType) && !s.array:
		ok = ok && (kind == mmrCosine || kind == mmrIP)
	default:
		ok = false
	}
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg(
			"mmr: similarity %q is not supported for %s; specify a supported \"similarity\" parameter", metricType, dataType.String())
	}
	return s, nil
}

// decode converts one opaque vector cell into an mmrVector.
func (s *mmrSimilarity) decode(value []byte, isNull bool) (mmrVector, error) {
	if isNull {
		return mmrVector{null: true}, nil
	}
	if !s.array {
		return s.decodeOne(value)
	}
	width := int(vectorByteWidth(s.dataType, s.dim))
	if width <= 0 || len(value)%width != 0 {
		return mmrVector{}, merr.WrapErrServiceInternalMsg(
			"mmr: vector array cell of %d bytes does not match %s dim %d", len(value), s.dataType, s.dim)
	}
	out := mmrVector{elems: make([]mmrVector, 0, len(value)/width)}
	for i := 0; i < len(value); i += width {
		elem, err := s.decodeOne(value[i : i+width])
		if err != nil {
			return mmrVector{}, err
		}
		out.elems = append(out.elems, elem)
	}
	return out, nil
}

func (s *mmrSimilarity) decodeOne(value []byte) (mmrVector, error) {
	switch {
	case typeutil.IsSparseFloatVectorType(s.dataType):
		return decodeSparse(value), nil
	case typeutil.IsBinaryVectorType(s.dataType):
		return mmrVector{bin: value}, nil
	}
	dense, err := decodeDense(s.dataType, value, s.dim)
	if err != nil {
		return mmrVector{}, err
	}
	if s.kind == mmrCosine {
		normalizeInPlace(dense)
	}
	return mmrVector{dense: dense}, nil
}

// windowOps estimates the vector element operations of ordering a whole
// window, that is, of one similarity per candidate pair.
func (s *mmrSimilarity) windowOps(cands []mmrVector) int64 {
	w := int64(len(cands))
	pairs := w * (w - 1) / 2
	switch {
	case s.array:
		// Pair (i, j) compares n_i * n_j elements in each direction.
		sum, sumSq := int64(0), int64(0)
		for i := range cands {
			n := int64(len(cands[i].elems))
			sum, sumSq = sum+n, sumSq+n*n
		}
		return (sum*sum - sumSq) * max(s.dim, 1)
	case typeutil.IsSparseFloatVectorType(s.dataType):
		// The dim of a sparse field is its vocabulary size; the merge walks
		// only the non-zeros of both rows.
		nnz := int64(0)
		for i := range cands {
			nnz += int64(len(cands[i].sparseIdx))
		}
		return (w-1)*nnz + pairs
	default:
		return pairs * max(s.dim, 1)
	}
}

// similarity returns the similarity of two candidates in [0, 1]. A null vector
// is never redundant with anything.
func (s *mmrSimilarity) similarity(a, b *mmrVector) float64 {
	if a.null || b.null {
		return 0
	}
	if !s.array {
		return s.pair(a, b)
	}
	if len(a.elems) == 0 || len(b.elems) == 0 {
		return 0
	}
	// Symmetric MaxSim: 0.5 * (mean_i max_j sim(a_i, b_j) + mean_j max_i sim(b_j, a_i)).
	directed := func(x, y []mmrVector) float64 {
		sum := 0.0
		for i := range x {
			best := 0.0
			for j := range y {
				best = math.Max(best, s.pair(&x[i], &y[j]))
			}
			sum += best
		}
		return sum / float64(len(x))
	}
	return 0.5 * (directed(a.elems, b.elems) + directed(b.elems, a.elems))
}

// pair is the mapped similarity of two non-array vectors.
func (s *mmrSimilarity) pair(a, b *mmrVector) float64 {
	switch {
	case typeutil.IsSparseFloatVectorType(s.dataType):
		dot := sparseDot(a, b)
		if s.kind == mmrIP {
			return mapSimilarity(mmrIP, dot, s.dim)
		}
		if a.sparseNorm == 0 || b.sparseNorm == 0 {
			return 0
		}
		return mapSimilarity(mmrCosine, dot/(a.sparseNorm*b.sparseNorm), s.dim)
	case typeutil.IsBinaryVectorType(s.dataType):
		return mapSimilarity(s.kind, binaryDistance(s.kind, a.bin, b.bin), s.dim)
	case s.kind == mmrL2:
		return mapSimilarity(mmrL2, float64(distance.L2Impl(a.dense, b.dense)), s.dim)
	default:
		// Cosine vectors are normalized at decode time.
		return mapSimilarity(s.kind, float64(distance.IPImpl(a.dense, b.dense)), s.dim)
	}
}

// mapSimilarity maps a raw metric value to [0, 1], larger is more similar.
// For L2 the raw value is the squared distance, matching Milvus search scores.
func mapSimilarity(kind mmrMetricKind, raw float64, dim int64) float64 {
	var v float64
	switch kind {
	case mmrCosine:
		v = (1 + raw) * 0.5
	case mmrIP:
		v = 0.5 + math.Atan(raw)/math.Pi
	case mmrL2:
		v = 1 - 2*math.Atan(raw)/math.Pi
	case mmrJaccard:
		v = 1 - raw
	case mmrHamming:
		if dim <= 0 {
			return 0
		}
		v = 1 - raw/float64(dim)
	}
	return math.Min(1, math.Max(0, v))
}

// mmrRelevance maps a search score of the given metric to [0, 1] using the
// same scale as mapSimilarity, so relevance and redundancy are comparable when
// MMR runs directly on search scores.
func mmrRelevance(metricType string, score float32, dim int64) float64 {
	if kind, ok := mmrMetricKinds[strings.ToUpper(metricType)]; ok {
		return mapSimilarity(kind, float64(score), dim)
	}
	// MAX_SIM* and other metrics have unbounded scores; reuse the weighted
	// reranker's normalization for them.
	return float64(getNormalizeFunc(metricType)(score))
}

func decodeDense(dataType schemapb.DataType, value []byte, dim int64) ([]float32, error) {
	if int64(len(value)) != vectorByteWidth(dataType, dim) {
		return nil, merr.WrapErrServiceInternalMsg("mmr: %s cell has %d bytes, want dim %d", dataType, len(value), dim)
	}
	switch dataType {
	case schemapb.DataType_FloatVector:
		out := make([]float32, dim)
		for i := range out {
			out[i] = math.Float32frombits(binary.LittleEndian.Uint32(value[i*4:]))
		}
		return out, nil
	case schemapb.DataType_Float16Vector:
		return typeutil.Float16BytesToFloat32Vector(value), nil
	case schemapb.DataType_BFloat16Vector:
		return typeutil.BFloat16BytesToFloat32Vector(value), nil
	case schemapb.DataType_Int8Vector:
		out := make([]float32, len(value))
		for i, b := range value {
			out[i] = float32(int8(b))
		}
		return out, nil
	default:
		return nil, merr.WrapErrServiceInternalMsg("mmr: %s is not a dense vector type", dataType.String())
	}
}

func decodeSparse(value []byte) mmrVector {
	n := typeutil.SparseFloatRowElementCount(value)
	v := mmrVector{sparseIdx: make([]uint32, n), sparseVal: make([]float32, n)}
	sum := 0.0
	for i := 0; i < n; i++ {
		v.sparseIdx[i] = typeutil.SparseFloatRowIndexAt(value, i)
		v.sparseVal[i] = typeutil.SparseFloatRowValueAt(value, i)
		sum += float64(v.sparseVal[i]) * float64(v.sparseVal[i])
	}
	v.sparseNorm = math.Sqrt(sum)
	return v
}

func sparseDot(a, b *mmrVector) float64 {
	i, j, dot := 0, 0, 0.0
	for i < len(a.sparseIdx) && j < len(b.sparseIdx) {
		switch {
		case a.sparseIdx[i] == b.sparseIdx[j]:
			dot += float64(a.sparseVal[i]) * float64(b.sparseVal[j])
			i++
			j++
		case a.sparseIdx[i] < b.sparseIdx[j]:
			i++
		default:
			j++
		}
	}
	return dot
}

func normalizeInPlace(v []float32) {
	norm := math.Sqrt(float64(distance.IPImpl(v, v)))
	if norm == 0 {
		return
	}
	inv := float32(1 / norm)
	for i := range v {
		v[i] *= inv
	}
}

// binaryDistance returns the Jaccard distance or the Hamming distance (bit count).
func binaryDistance(kind mmrMetricKind, a, b []byte) float64 {
	n := min(len(a), len(b))
	if kind == mmrHamming {
		diff := 0
		for i := 0; i < n; i++ {
			diff += bits.OnesCount8(a[i] ^ b[i])
		}
		return float64(diff)
	}
	inter, union := 0, 0
	for i := 0; i < n; i++ {
		inter += bits.OnesCount8(a[i] & b[i])
		union += bits.OnesCount8(a[i] | b[i])
	}
	if union == 0 {
		return 0
	}
	return 1 - float64(inter)/float64(union)
}
