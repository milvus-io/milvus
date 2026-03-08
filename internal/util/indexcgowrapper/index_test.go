package indexcgowrapper

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

const (
	// index type
	IndexFaissIDMap      = "FLAT"
	IndexFaissIVFFlat    = "IVF_FLAT"
	IndexFaissIVFPQ      = "IVF_PQ"
	IndexFaissIVFSQ8     = "IVF_SQ8"
	IndexScaNN           = "SCANN"
	IndexFaissBinIDMap   = "BIN_FLAT"
	IndexFaissBinIVFFlat = "BIN_IVF_FLAT"

	IndexHNSW = "HNSW"

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
		{IndexFaissIDMap, metric.L2, false, schemapb.DataType_FloatVector},
		{IndexFaissIDMap, metric.IP, false, schemapb.DataType_FloatVector},
		{IndexFaissIVFFlat, metric.L2, false, schemapb.DataType_FloatVector},
		{IndexFaissIVFFlat, metric.IP, false, schemapb.DataType_FloatVector},
		{IndexFaissIVFPQ, metric.L2, false, schemapb.DataType_FloatVector},
		{IndexFaissIVFPQ, metric.IP, false, schemapb.DataType_FloatVector},
		{IndexFaissIVFSQ8, metric.L2, false, schemapb.DataType_FloatVector},
		{IndexFaissIVFSQ8, metric.IP, false, schemapb.DataType_FloatVector},
		{IndexScaNN, metric.L2, false, schemapb.DataType_FloatVector},
		{IndexScaNN, metric.IP, false, schemapb.DataType_FloatVector},
		{IndexHNSW, metric.L2, false, schemapb.DataType_FloatVector},
		{IndexHNSW, metric.IP, false, schemapb.DataType_FloatVector},
	}
}

func generateBinaryVectorTestCases() []vecTestCase {
	return []vecTestCase{
		{IndexFaissBinIVFFlat, metric.JACCARD, true, schemapb.DataType_BinaryVector},
		{IndexFaissBinIVFFlat, metric.HAMMING, true, schemapb.DataType_BinaryVector},
		{IndexFaissBinIDMap, metric.JACCARD, true, schemapb.DataType_BinaryVector},
		{IndexFaissBinIDMap, metric.HAMMING, true, schemapb.DataType_BinaryVector},
	}
}

func generateFloat16VectorTestCases() []vecTestCase {
	return []vecTestCase{
		{IndexFaissIDMap, metric.L2, false, schemapb.DataType_Float16Vector},
		{IndexFaissIDMap, metric.IP, false, schemapb.DataType_Float16Vector},
		{IndexFaissIVFFlat, metric.L2, false, schemapb.DataType_Float16Vector},
		{IndexFaissIVFFlat, metric.IP, false, schemapb.DataType_Float16Vector},
		{IndexFaissIVFPQ, metric.L2, false, schemapb.DataType_Float16Vector},
		{IndexFaissIVFPQ, metric.IP, false, schemapb.DataType_Float16Vector},
		{IndexFaissIVFSQ8, metric.L2, false, schemapb.DataType_Float16Vector},
		{IndexFaissIVFSQ8, metric.IP, false, schemapb.DataType_Float16Vector},
	}
}

func generateBFloat16VectorTestCases() []vecTestCase {
	return []vecTestCase{
		{IndexFaissIDMap, metric.L2, false, schemapb.DataType_BFloat16Vector},
		{IndexFaissIDMap, metric.IP, false, schemapb.DataType_BFloat16Vector},
		{IndexFaissIVFFlat, metric.L2, false, schemapb.DataType_BFloat16Vector},
		{IndexFaissIVFFlat, metric.IP, false, schemapb.DataType_BFloat16Vector},
		{IndexFaissIVFPQ, metric.L2, false, schemapb.DataType_BFloat16Vector},
		{IndexFaissIVFPQ, metric.IP, false, schemapb.DataType_BFloat16Vector},
		{IndexFaissIVFSQ8, metric.L2, false, schemapb.DataType_BFloat16Vector},
		{IndexFaissIVFSQ8, metric.IP, false, schemapb.DataType_BFloat16Vector},
	}
}

func generateInt8VectorTestCases() []vecTestCase {
	return []vecTestCase{
		{IndexHNSW, metric.L2, false, schemapb.DataType_Int8Vector},
		{IndexHNSW, metric.IP, false, schemapb.DataType_Int8Vector},
	}
}

func generateTestCases() []vecTestCase {
	return append(generateFloatVectorTestCases(), generateBinaryVectorTestCases()...)
}

func generateParams(indexType, metricType string) (map[string]string, map[string]string) {
	typeParams := make(map[string]string)
	indexParams := make(map[string]string)
	indexParams[common.IndexTypeKey] = indexType
	indexParams[common.MetricTypeKey] = metricType
	if indexType == IndexFaissIDMap { // float vector
		indexParams[common.DimKey] = strconv.Itoa(dim)
	} else if indexType == IndexFaissIVFFlat {
		indexParams[common.DimKey] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
	} else if indexType == IndexFaissIVFPQ {
		indexParams[common.DimKey] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
	} else if indexType == IndexFaissIVFSQ8 {
		indexParams[common.DimKey] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["nbits"] = strconv.Itoa(nbits)
	} else if indexType == IndexScaNN {
		indexParams[common.DimKey] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
	} else if indexType == IndexHNSW {
		indexParams[common.DimKey] = strconv.Itoa(dim)
		indexParams["M"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		indexParams["ef"] = strconv.Itoa(ef)
	} else if indexType == IndexFaissBinIVFFlat { // binary vector
		indexParams[common.DimKey] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
	} else if indexType == IndexFaissBinIDMap {
		indexParams[common.DimKey] = strconv.Itoa(dim)
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

func TestCIndex_BuildFloat16VecIndex(t *testing.T) {
	for _, c := range generateFloat16VectorTestCases() {
		typeParams, indexParams := generateParams(c.indexType, c.metricType)

		index, err := NewCgoIndex(c.dtype, typeParams, indexParams)
		assert.Equal(t, err, nil)
		assert.NotEqual(t, index, nil)

		vectors := generateFloat16Vectors(nb, dim)
		err = index.Build(GenFloat16VecDataset(vectors))
		assert.Equal(t, err, nil)

		err = index.Delete()
		assert.Equal(t, err, nil)
	}
}

func TestCIndex_BuildBFloat16VecIndex(t *testing.T) {
	for _, c := range generateBFloat16VectorTestCases() {
		typeParams, indexParams := generateParams(c.indexType, c.metricType)

		index, err := NewCgoIndex(c.dtype, typeParams, indexParams)
		assert.Equal(t, err, nil)
		assert.NotEqual(t, index, nil)

		vectors := generateBFloat16Vectors(nb, dim)
		err = index.Build(GenBFloat16VecDataset(vectors))
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

func TestCIndex_BuildInt8VecIndex(t *testing.T) {
	for _, c := range generateInt8VectorTestCases() {
		typeParams, indexParams := generateParams(c.indexType, c.metricType)

		index, err := NewCgoIndex(c.dtype, typeParams, indexParams)
		assert.Equal(t, err, nil)
		assert.NotEqual(t, index, nil)

		vectors := generateInt8Vectors(nb, dim)
		err = index.Build(GenInt8VecDataset(vectors))
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
		// IVF_FLAT_NM index don't support load and serialize
		// copyBlobs, err := copyIndex.Serialize()
		// assert.Equal(t, err, nil)
		// assert.Equal(t, len(blobs), len(copyBlobs))
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
	indexParams[common.IndexTypeKey] = "IVF_FLAT"
	indexParams[common.MetricTypeKey] = "L2"
	indexPtr, err := NewCgoIndex(schemapb.DataType_FloatVector, nil, indexParams)
	assert.NoError(t, err)

	t.Run("Serialize error", func(t *testing.T) {
		blobs, err := indexPtr.Serialize()
		assert.Error(t, err)
		assert.Nil(t, blobs)
	})

	t.Run("Load error", func(t *testing.T) {
		blobs := []*Blob{
			{
				Key:   "test",
				Value: []byte("value"),
			},
		}
		err = indexPtr.Load(blobs)
		assert.Error(t, err)
	})

	t.Run("BuildFloatVecIndexWithoutIds error", func(t *testing.T) {
		floatVectors := []float32{1.1, 2.2, 3.3}
		err = indexPtr.Build(GenFloatVecDataset(floatVectors))
		assert.Error(t, err)
	})

	t.Run("BuildBinaryVecIndexWithoutIds error", func(t *testing.T) {
		binaryVectors := []byte("binaryVectors")
		err = indexPtr.Build(GenBinaryVecDataset(binaryVectors))
		assert.Error(t, err)
	})

	t.Run("BuildInt8VecIndexWithoutIds error", func(t *testing.T) {
		int8Vectors := []int8{11, 22, 33, 44}
		err = indexPtr.Build(GenInt8VecDataset(int8Vectors))
		assert.Error(t, err)
	})
}
