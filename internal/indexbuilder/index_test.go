package indexbuilder

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"testing"
)

const (
	IvfPq     = "IVF_PQ"
	IvfFlatNM = "IVF_FLAT"
	BinFlat   = "BIN_FLAT"
	dim       = 8
	nlist     = 100
	m         = 4
	nbits     = 8
	L2        = "L2"
	Jaccard   = "JACCARD"
	nb        = 8 * 10000
)

type testCase struct {
	indexType  string
	metricType string
	isBinary   bool
}

func generateFloatVectorTestCases() []testCase {
	return []testCase{
		{IvfPq, L2, false},
		{IvfFlatNM, L2, false},
	}
}

func generateBinaryVectorTestCases() []testCase {
	return []testCase{
		{BinFlat, Jaccard, true},
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
	if indexType == IvfPq {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
	} else if indexType == IvfFlatNM {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
	} else if indexType == BinFlat {
		indexParams["dim"] = strconv.Itoa(dim)
	}

	return typeParams, indexParams
}

func generateFloatVectors() []float32 {
	vectors := make([]float32, 0)
	for i := 0; i < nb; i++ {
		vectors = append(vectors, rand.Float32())
	}
	return vectors
}

func generateBinaryVectors() []byte {
	vectors := make([]byte, 0)
	for i := 0; i < nb/8; i++ {
		vectors = append(vectors, byte(rand.Intn(8)))
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
