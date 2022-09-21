//go:build linux
// +build linux

package indexcgowrapper

import (
	"strconv"
	"testing"

	"github.com/milvus-io/milvus/api/schemapb"
	"github.com/stretchr/testify/assert"
)

const (
	// index type
	IndexFaissIDMap      = "FLAT"
	IndexFaissIVFFlat    = "IVF_FLAT"
	IndexFaissIVFPQ      = "IVF_PQ"
	IndexFaissIVFSQ8     = "IVF_SQ8"
	IndexFaissBinIDMap   = "BIN_FLAT"
	IndexFaissBinIVFFlat = "BIN_IVF_FLAT"

	IndexHNSW  = "HNSW"
	IndexANNOY = "ANNOY"

	// metric type
	L2       = "L2"
	IP       = "IP"
	hamming  = "HAMMING"
	Jaccard  = "JACCARD"
	tanimoto = "TANIMOTO"

	dim            = 8
	nlist          = 100
	m              = 4
	nbits          = 8
	nb             = 1000
	sliceSize      = 4
	efConstruction = 200
	ef             = 200
)

type vecTestCase struct {
	indexType  string
	metricType string
	isBinary   bool
	dtype      schemapb.DataType
}

func generateFloatVectorTestCases() []vecTestCase {
	return []vecTestCase{
		{IndexFaissIDMap, L2, false, schemapb.DataType_FloatVector},
		{IndexFaissIDMap, IP, false, schemapb.DataType_FloatVector},
		{IndexFaissIVFFlat, L2, false, schemapb.DataType_FloatVector},
		{IndexFaissIVFFlat, IP, false, schemapb.DataType_FloatVector},
		{IndexFaissIVFPQ, L2, false, schemapb.DataType_FloatVector},
		{IndexFaissIVFPQ, IP, false, schemapb.DataType_FloatVector},
		{IndexFaissIVFSQ8, L2, false, schemapb.DataType_FloatVector},
		{IndexFaissIVFSQ8, IP, false, schemapb.DataType_FloatVector},
		{IndexHNSW, L2, false, schemapb.DataType_FloatVector},
		{IndexHNSW, IP, false, schemapb.DataType_FloatVector},
		{IndexANNOY, L2, false, schemapb.DataType_FloatVector},
		{IndexANNOY, IP, false, schemapb.DataType_FloatVector},
	}
}

func generateBinaryVectorTestCases() []vecTestCase {
	return []vecTestCase{
		{IndexFaissBinIVFFlat, Jaccard, true, schemapb.DataType_BinaryVector},
		{IndexFaissBinIVFFlat, hamming, true, schemapb.DataType_BinaryVector},
		{IndexFaissBinIVFFlat, tanimoto, true, schemapb.DataType_BinaryVector},
		{IndexFaissBinIDMap, Jaccard, true, schemapb.DataType_BinaryVector},
		{IndexFaissBinIDMap, hamming, true, schemapb.DataType_BinaryVector},
	}
}

func generateTestCases() []vecTestCase {
	return append(generateFloatVectorTestCases(), generateBinaryVectorTestCases()...)
}

func generateParams(indexType, metricType string) (map[string]string, map[string]string) {
	typeParams := make(map[string]string)
	indexParams := make(map[string]string)
	indexParams["index_type"] = indexType
	indexParams["metric_type"] = metricType
	if indexType == IndexFaissIDMap { // float vector
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexFaissIVFFlat {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
	} else if indexType == IndexFaissIVFPQ {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexFaissIVFSQ8 {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["nbits"] = strconv.Itoa(nbits)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexHNSW {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["M"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		indexParams["ef"] = strconv.Itoa(ef)
	} else if indexType == IndexANNOY {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["n_trees"] = strconv.Itoa(4)
		indexParams["search_k"] = strconv.Itoa(100)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexFaissBinIVFFlat { // binary vector
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexFaissBinIDMap {
		indexParams["dim"] = strconv.Itoa(dim)
	} else {
		panic("")
	}

	return typeParams, indexParams
}

func TestCIndex_New(t *testing.T) {
	for _, c := range generateTestCases() {
		typeParams, indexParams := generateParams(c.indexType, c.metricType)

		index, err := NewCgoIndex(c.dtype, typeParams, indexParams)
		assert.Equal(t, err, nil)
		assert.NotEqual(t, index, nil)

		err = index.Delete()
		assert.Equal(t, err, nil)
	}
}

func TestCIndex_BuildFloatVecIndex(t *testing.T) {
	for _, c := range generateFloatVectorTestCases() {
		typeParams, indexParams := generateParams(c.indexType, c.metricType)

		index, err := NewCgoIndex(c.dtype, typeParams, indexParams)
		assert.Equal(t, err, nil)
		assert.NotEqual(t, index, nil)

		vectors := generateFloatVectors(nb, dim)
		err = index.Build(GenFloatVecDataset(vectors))
		assert.Equal(t, err, nil)

		err = index.Delete()
		assert.Equal(t, err, nil)
	}
}

func TestCIndex_BuildBinaryVecIndex(t *testing.T) {
	for _, c := range generateBinaryVectorTestCases() {
		typeParams, indexParams := generateParams(c.indexType, c.metricType)

		index, err := NewCgoIndex(c.dtype, typeParams, indexParams)
		assert.Equal(t, err, nil)
		assert.NotEqual(t, index, nil)

		vectors := generateBinaryVectors(nb, dim)
		err = index.Build(GenBinaryVecDataset(vectors))
		assert.Equal(t, err, nil)

		err = index.Delete()
		assert.Equal(t, err, nil)
	}
}

func TestCIndex_Codec(t *testing.T) {
	for _, c := range generateTestCases() {
		typeParams, indexParams := generateParams(c.indexType, c.metricType)

		index, err := NewCgoIndex(c.dtype, typeParams, indexParams)
		assert.Equal(t, err, nil)
		assert.NotEqual(t, index, nil)

		if !c.isBinary {
			vectors := generateFloatVectors(nb, dim)
			err = index.Build(GenFloatVecDataset(vectors))
			assert.Equal(t, err, nil)
		} else {
			vectors := generateBinaryVectors(nb, dim)
			err = index.Build(GenBinaryVecDataset(vectors))
			assert.Equal(t, err, nil)
		}

		blobs, err := index.Serialize()
		assert.Equal(t, err, nil)

		copyIndex, err := NewCgoIndex(c.dtype, typeParams, indexParams)
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

		index, err := NewCgoIndex(c.dtype, typeParams, indexParams)
		assert.Equal(t, err, nil)
		assert.NotEqual(t, index, nil)

		err = index.Delete()
		assert.Equal(t, err, nil)
	}
}

func TestCIndex_Error(t *testing.T) {
	indexParams := make(map[string]string)
	indexParams["index_type"] = "IVF_FLAT"
	indexParams["metric_type"] = "L2"
	indexPtr, err := NewCgoIndex(schemapb.DataType_FloatVector, nil, indexParams)
	assert.Nil(t, err)

	t.Run("Serialize error", func(t *testing.T) {
		blobs, err := indexPtr.Serialize()
		assert.NotNil(t, err)
		assert.Nil(t, blobs)
	})

	t.Run("Load error", func(t *testing.T) {
		blobs := []*Blob{{
			Key:   "test",
			Value: []byte("value"),
		},
		}
		err = indexPtr.Load(blobs)
		assert.NotNil(t, err)
	})

	t.Run("BuildFloatVecIndexWithoutIds error", func(t *testing.T) {
		floatVectors := []float32{1.1, 2.2, 3.3}
		err = indexPtr.Build(GenFloatVecDataset(floatVectors))
		assert.NotNil(t, err)
	})

	t.Run("BuildBinaryVecIndexWithoutIds error", func(t *testing.T) {
		binaryVectors := []byte("binaryVectors")
		err = indexPtr.Build(GenBinaryVecDataset(binaryVectors))
		assert.NotNil(t, err)
	})
}
