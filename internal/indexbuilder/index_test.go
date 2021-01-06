package indexbuilder

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"testing"
)

const (
	// index type
	INDEX_FAISS_IDMAP       = "FLAT"
	INDEX_FAISS_IVFFLAT     = "IVF_FLAT"
	INDEX_FAISS_IVFPQ       = "IVF_PQ"
	INDEX_FAISS_IVFSQ8      = "IVF_SQ8"
	INDEX_FAISS_IVFSQ8H     = "IVF_SQ8_HYBRID"
	INDEX_FAISS_BIN_IDMAP   = "BIN_FLAT"
	INDEX_FAISS_BIN_IVFFLAT = "BIN_IVF_FLAT"
	INDEX_NSG               = "NSG"

	INDEX_HNSW      = "HNSW"
	INDEX_RHNSWFlat = "RHNSW_FLAT"
	INDEX_RHNSWPQ   = "RHNSW_PQ"
	INDEX_RHNSWSQ   = "RHNSW_SQ"
	INDEX_ANNOY     = "ANNOY"
	INDEX_NGTPANNG  = "NGT_PANNG"
	INDEX_NGTONNG   = "NGT_ONNG"

	// metric type
	L2      = "L2"
	IP      = "IP"
	hamming = "HAMMING"
	Jaccard = "JACCARD"

	dim            = 8
	nlist          = 100
	m              = 4
	nbits          = 8
	nb             = 10000
	nprobe         = 8
	sliceSize      = 4
	efConstruction = 200
	ef             = 200
	edgeSize       = 10
	epsilon        = 0.1
	maxSearchEdges = 50
)

type testCase struct {
	indexType  string
	metricType string
	isBinary   bool
}

func generateFloatVectorTestCases() []testCase {
	return []testCase{
		{INDEX_FAISS_IDMAP, L2, false},
		{INDEX_FAISS_IDMAP, IP, false},
		{INDEX_FAISS_IVFFLAT, L2, false},
		{INDEX_FAISS_IVFFLAT, IP, false},
		{INDEX_FAISS_IVFPQ, L2, false},
		{INDEX_FAISS_IVFPQ, IP, false},
		{INDEX_FAISS_IVFSQ8, L2, false},
		{INDEX_FAISS_IVFSQ8, IP, false},
		//{INDEX_FAISS_IVFSQ8H, L2, false}, // TODO: enable gpu
		//{INDEX_FAISS_IVFSQ8H, IP, false},
		{INDEX_NSG, L2, false},
		{INDEX_NSG, IP, false},
		//{INDEX_HNSW, L2, false}, // TODO: fix json parse exception
		//{INDEX_HNSW, IP, false},
		//{INDEX_RHNSWFlat, L2, false},
		//{INDEX_RHNSWFlat, IP, false},
		//{INDEX_RHNSWPQ, L2, false},
		//{INDEX_RHNSWPQ, IP, false},
		//{INDEX_RHNSWSQ, L2, false},
		//{INDEX_RHNSWSQ, IP, false},
		{INDEX_ANNOY, L2, false},
		{INDEX_ANNOY, IP, false},
		{INDEX_NGTPANNG, L2, false},
		{INDEX_NGTPANNG, IP, false},
		{INDEX_NGTONNG, L2, false},
		{INDEX_NGTONNG, IP, false},
	}
}

func generateBinaryVectorTestCases() []testCase {
	return []testCase{
		{INDEX_FAISS_BIN_IVFFLAT, Jaccard, true},
		{INDEX_FAISS_BIN_IVFFLAT, hamming, true},
		{INDEX_FAISS_BIN_IDMAP, Jaccard, true},
		{INDEX_FAISS_BIN_IDMAP, hamming, true},
	}
}

func generateTestCases() []testCase {
	return append(generateFloatVectorTestCases(), generateBinaryVectorTestCases()...)
}

func generateParams(indexType, metricType string) (map[string]string, map[string]string) {
	typeParams := make(map[string]string)
	indexParams := make(map[string]string)
	indexParams["index_type"] = indexType
	indexParams["metric_type"] = metricType
	if indexType == INDEX_FAISS_IDMAP { // float vector
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == INDEX_FAISS_IVFFLAT {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
	} else if indexType == INDEX_FAISS_IVFPQ {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == INDEX_FAISS_IVFSQ8 {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["nbits"] = strconv.Itoa(nbits)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == INDEX_FAISS_IVFSQ8H {
		// TODO: enable gpu
	} else if indexType == INDEX_NSG {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(163)
		indexParams["nprobe"] = strconv.Itoa(nprobe)
		indexParams["knng"] = strconv.Itoa(20)
		indexParams["search_length"] = strconv.Itoa(40)
		indexParams["out_degree"] = strconv.Itoa(30)
		indexParams["candidate_pool_size"] = strconv.Itoa(100)
	} else if indexType == INDEX_HNSW {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["m"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		indexParams["ef"] = strconv.Itoa(ef)
	} else if indexType == INDEX_RHNSWFlat {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["m"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		indexParams["ef"] = strconv.Itoa(ef)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == INDEX_RHNSWPQ {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["m"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		indexParams["ef"] = strconv.Itoa(ef)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
		indexParams["PQM"] = strconv.Itoa(8)
	} else if indexType == INDEX_RHNSWSQ {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["m"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		indexParams["ef"] = strconv.Itoa(ef)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == INDEX_ANNOY {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["n_trees"] = strconv.Itoa(4)
		indexParams["search_k"] = strconv.Itoa(100)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == INDEX_NGTPANNG {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["edge_size"] = strconv.Itoa(edgeSize)
		indexParams["epsilon"] = fmt.Sprint(epsilon)
		indexParams["max_search_edges"] = strconv.Itoa(maxSearchEdges)
		indexParams["forcedly_pruned_edge_size"] = strconv.Itoa(60)
		indexParams["selectively_pruned_edge_size"] = strconv.Itoa(30)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == INDEX_NGTONNG {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["edge_size"] = strconv.Itoa(edgeSize)
		indexParams["epsilon"] = fmt.Sprint(epsilon)
		indexParams["max_search_edges"] = strconv.Itoa(maxSearchEdges)
		indexParams["outgoing_edge_size"] = strconv.Itoa(5)
		indexParams["incoming_edge_size"] = strconv.Itoa(40)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == INDEX_FAISS_BIN_IVFFLAT { // binary vector
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == INDEX_FAISS_BIN_IDMAP {
		indexParams["dim"] = strconv.Itoa(dim)
	} else {
		panic("")
	}

	return typeParams, indexParams
}

func generateFloatVectors() []float32 {
	vectors := make([]float32, 0)
	for i := 0; i < nb; i++ {
		for j := 0; j < dim; j++ {
			vectors = append(vectors, rand.Float32())
		}
	}
	return vectors
}

func generateBinaryVectors() []byte {
	vectors := make([]byte, 0)
	for i := 0; i < nb; i++ {
		for j := 0; j < dim/8; j++ {
			vectors = append(vectors, byte(rand.Intn(8)))
		}
	}
	return vectors
}

func TestCIndex_New(t *testing.T) {
	for _, c := range generateTestCases() {
		typeParams, indexParams := generateParams(c.indexType, c.metricType)

		index, err := NewCIndex(typeParams, indexParams)
		assert.Equal(t, err, nil)
		assert.NotEqual(t, index, nil)

		err = index.Delete()
		assert.Equal(t, err, nil)
	}
}

func TestCIndex_BuildFloatVecIndexWithoutIds(t *testing.T) {
	for _, c := range generateFloatVectorTestCases() {
		typeParams, indexParams := generateParams(c.indexType, c.metricType)

		index, err := NewCIndex(typeParams, indexParams)
		assert.Equal(t, err, nil)
		assert.NotEqual(t, index, nil)

		vectors := generateFloatVectors()
		err = index.BuildFloatVecIndexWithoutIds(vectors)
		assert.Equal(t, err, nil)

		err = index.Delete()
		assert.Equal(t, err, nil)
	}
}

func TestCIndex_BuildBinaryVecIndexWithoutIds(t *testing.T) {
	for _, c := range generateBinaryVectorTestCases() {
		typeParams, indexParams := generateParams(c.indexType, c.metricType)

		index, err := NewCIndex(typeParams, indexParams)
		assert.Equal(t, err, nil)
		assert.NotEqual(t, index, nil)

		vectors := generateBinaryVectors()
		err = index.BuildBinaryVecIndexWithoutIds(vectors)
		assert.Equal(t, err, nil)

		err = index.Delete()
		assert.Equal(t, err, nil)
	}
}

func TestCIndex_Codec(t *testing.T) {
	for _, c := range generateTestCases() {
		typeParams, indexParams := generateParams(c.indexType, c.metricType)

		index, err := NewCIndex(typeParams, indexParams)
		assert.Equal(t, err, nil)
		assert.NotEqual(t, index, nil)

		if !c.isBinary {
			vectors := generateFloatVectors()
			err = index.BuildFloatVecIndexWithoutIds(vectors)
			assert.Equal(t, err, nil)
		} else {
			vectors := generateBinaryVectors()
			err = index.BuildBinaryVecIndexWithoutIds(vectors)
			assert.Equal(t, err, nil)
		}

		blobs, err := index.Serialize()
		assert.Equal(t, err, nil)

		copyIndex, err := NewCIndex(typeParams, indexParams)
		assert.NotEqual(t, copyIndex, nil)
		assert.Equal(t, err, nil)
		err = copyIndex.Load(blobs)
		assert.Equal(t, err, nil)
		copyBlobs, err := copyIndex.Serialize()
		assert.Equal(t, err, nil)
		assert.Equal(t, len(blobs), len(copyBlobs))
		// TODO: check key, value and more

		err = index.Delete()
		assert.Equal(t, err, nil)
		err = copyIndex.Delete()
		assert.Equal(t, err, nil)
	}
}

func TestCIndex_Delete(t *testing.T) {
	for _, c := range generateTestCases() {
		typeParams, indexParams := generateParams(c.indexType, c.metricType)

		index, err := NewCIndex(typeParams, indexParams)
		assert.Equal(t, err, nil)
		assert.NotEqual(t, index, nil)

		err = index.Delete()
		assert.Equal(t, err, nil)
	}
}
