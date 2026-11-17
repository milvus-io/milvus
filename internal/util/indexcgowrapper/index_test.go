package indexcgowrapper

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
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
	switch indexType {
	case IndexFaissIDMap: // float vector
		indexParams[common.DimKey] = strconv.Itoa(dim)
	case IndexFaissIVFFlat:
		indexParams[common.DimKey] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
	case IndexFaissIVFPQ:
		indexParams[common.DimKey] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
	case IndexFaissIVFSQ8:
		indexParams[common.DimKey] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["nbits"] = strconv.Itoa(nbits)
	case IndexScaNN:
		indexParams[common.DimKey] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
	case IndexHNSW:
		indexParams[common.DimKey] = strconv.Itoa(dim)
		indexParams["M"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		indexParams["ef"] = strconv.Itoa(ef)
	case IndexFaissBinIVFFlat: // binary vector
		indexParams[common.DimKey] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(nlist)
		indexParams["m"] = strconv.Itoa(m)
		indexParams["nbits"] = strconv.Itoa(nbits)
	case IndexFaissBinIDMap:
		indexParams[common.DimKey] = strconv.Itoa(dim)
	default:
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

func TestCIndex_NewFMIndexInvalidParamPreservesInputError(t *testing.T) {
	index, err := NewCgoIndex(
		schemapb.DataType_VarChar,
		nil,
		map[string]string{
			common.IndexTypeKey: "FMINDEX",
			"fm_sa_sample_rate": "not-an-integer",
		},
	)
	require.Error(t, err)
	assert.Nil(t, index)
	assert.ErrorIs(t, err, merr.ErrSegcore)
	assert.Equal(t, merr.InputError, merr.GetErrorType(err))
	status := merr.Status(err)
	assert.False(t, status.GetRetriable())
	assert.Contains(t, status.GetReason(), "segcoreCode=2042")
	assert.Contains(t, err.Error(), "fm_sa_sample_rate for FMINDEX")
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

func TestCIndex_BuildAllNullNullableVectorsDoesNotPanic(t *testing.T) {
	type testCase struct {
		name   string
		dtype  schemapb.DataType
		raw    any
		params func() (map[string]string, map[string]string)
	}

	cases := []testCase{
		{
			name:  "float",
			dtype: schemapb.DataType_FloatVector,
			raw:   []float32{},
			params: func() (map[string]string, map[string]string) {
				return generateParams(IndexFaissIDMap, metric.L2)
			},
		},
		{
			name:  "binary",
			dtype: schemapb.DataType_BinaryVector,
			raw:   []byte{},
			params: func() (map[string]string, map[string]string) {
				return generateParams(IndexFaissBinIDMap, metric.JACCARD)
			},
		},
		{
			name:  "float16",
			dtype: schemapb.DataType_Float16Vector,
			raw:   []byte{},
			params: func() (map[string]string, map[string]string) {
				return generateParams(IndexFaissIDMap, metric.L2)
			},
		},
		{
			name:  "bfloat16",
			dtype: schemapb.DataType_BFloat16Vector,
			raw:   []byte{},
			params: func() (map[string]string, map[string]string) {
				return generateParams(IndexFaissIDMap, metric.L2)
			},
		},
		{
			name:  "int8",
			dtype: schemapb.DataType_Int8Vector,
			raw:   []int8{},
			params: func() (map[string]string, map[string]string) {
				return generateParams(IndexHNSW, metric.L2)
			},
		},
		{
			name:  "sparse",
			dtype: schemapb.DataType_SparseFloatVector,
			raw:   []byte{},
			params: func() (map[string]string, map[string]string) {
				return map[string]string{}, map[string]string{
					common.IndexTypeKey:  "SPARSE_INVERTED_INDEX",
					common.MetricTypeKey: metric.IP,
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			typeParams, indexParams := tc.params()
			index, err := NewCgoIndex(tc.dtype, typeParams, indexParams)
			require.NoError(t, err)
			require.NotNil(t, index)
			defer func() {
				require.NoError(t, index.Delete())
			}()

			dataset := &Dataset{
				DType: tc.dtype,
				Data: map[string]any{
					keyRawArr:   tc.raw,
					keyValidArr: []bool{false, false, false},
				},
			}
			require.NotPanics(t, func() {
				err = index.Build(dataset)
			})
			require.NoError(t, err)

			blobs, err := index.Serialize()
			require.NoError(t, err)
			require.NotEmpty(t, blobs)

			copyIndex, err := NewCgoIndex(tc.dtype, typeParams, indexParams)
			require.NoError(t, err)
			require.NotNil(t, copyIndex)
			defer func() {
				require.NoError(t, copyIndex.Delete())
			}()
			require.NoError(t, copyIndex.Load(blobs))
		})
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
