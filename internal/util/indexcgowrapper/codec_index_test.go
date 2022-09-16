//go:build linux
// +build linux

package indexcgowrapper

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/milvus-io/milvus/internal/storage"

	"github.com/milvus-io/milvus/api/schemapb"
)

type indexTestCase struct {
	dtype       schemapb.DataType
	typeParams  map[string]string
	indexParams map[string]string
}

func generateBoolArray(numRows int) []bool {
	ret := make([]bool, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, rand.Int()%2 == 0)
	}
	return ret
}

func generateInt8Array(numRows int) []int8 {
	ret := make([]int8, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int8(rand.Int()))
	}
	return ret
}

func generateInt16Array(numRows int) []int16 {
	ret := make([]int16, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int16(rand.Int()))
	}
	return ret
}

func generateInt32Array(numRows int) []int32 {
	ret := make([]int32, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int32(rand.Int()))
	}
	return ret
}

func generateInt64Array(numRows int) []int64 {
	ret := make([]int64, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, int64(rand.Int()))
	}
	return ret
}

func generateFloat32Array(numRows int) []float32 {
	ret := make([]float32, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, rand.Float32())
	}
	return ret
}

func generateFloat64Array(numRows int) []float64 {
	ret := make([]float64, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, rand.Float64())
	}
	return ret
}

func generateStringArray(numRows int) []string {
	ret := make([]string, 0, numRows)
	for i := 0; i < numRows; i++ {
		ret = append(ret, funcutil.GenRandomStr())
	}
	return ret
}

func generateFloatVectors(numRows, dim int) []float32 {
	total := numRows * dim
	ret := make([]float32, 0, total)
	for i := 0; i < total; i++ {
		ret = append(ret, rand.Float32())
	}
	return ret
}

func generateBinaryVectors(numRows, dim int) []byte {
	total := (numRows * dim) / 8
	ret := make([]byte, total)
	_, err := rand.Read(ret)
	if err != nil {
		panic(err)
	}
	return ret
}

func genFieldData(dtype schemapb.DataType, numRows, dim int) storage.FieldData {
	switch dtype {
	case schemapb.DataType_Bool:
		return &storage.BoolFieldData{
			Data: generateBoolArray(numRows),
		}
	case schemapb.DataType_Int8:
		return &storage.Int8FieldData{
			Data: generateInt8Array(numRows),
		}
	case schemapb.DataType_Int16:
		return &storage.Int16FieldData{
			Data: generateInt16Array(numRows),
		}
	case schemapb.DataType_Int32:
		return &storage.Int32FieldData{
			Data: generateInt32Array(numRows),
		}
	case schemapb.DataType_Int64:
		return &storage.Int64FieldData{
			Data: generateInt64Array(numRows),
		}
	case schemapb.DataType_Float:
		return &storage.FloatFieldData{
			Data: generateFloat32Array(numRows),
		}
	case schemapb.DataType_Double:
		return &storage.DoubleFieldData{
			Data: generateFloat64Array(numRows),
		}
	case schemapb.DataType_String:
		return &storage.StringFieldData{
			Data: generateStringArray(numRows),
		}
	case schemapb.DataType_VarChar:
		return &storage.StringFieldData{
			Data: generateStringArray(numRows),
		}
	case schemapb.DataType_BinaryVector:
		return &storage.BinaryVectorFieldData{
			Dim:  dim,
			Data: generateBinaryVectors(numRows, dim),
		}
	case schemapb.DataType_FloatVector:
		return &storage.FloatVectorFieldData{
			Data: generateFloatVectors(numRows, dim),
			Dim:  dim,
		}
	default:
		return nil
	}
}

func genScalarIndexCases(dtype schemapb.DataType) []indexTestCase {
	return []indexTestCase{
		{
			dtype:      dtype,
			typeParams: nil,
			indexParams: map[string]string{
				"index_type": "inverted_index",
			},
		},
		{
			dtype:      dtype,
			typeParams: nil,
			indexParams: map[string]string{
				"index_type": "flat",
			},
		},
	}
}

func genStringIndexCases(dtype schemapb.DataType) []indexTestCase {
	return []indexTestCase{
		{
			dtype:      dtype,
			typeParams: nil,
			indexParams: map[string]string{
				"index_type": "inverted_index",
			},
		},
		{
			dtype:      dtype,
			typeParams: nil,
			indexParams: map[string]string{
				"index_type": "marisa-trie",
			},
		},
	}
}

func genFloatVecIndexCases(dtype schemapb.DataType) []indexTestCase {
	return []indexTestCase{
		{
			dtype:      dtype,
			typeParams: nil,
			indexParams: map[string]string{
				"index_type":  IndexFaissIVFPQ,
				"metric_type": L2,
				"dim":         strconv.Itoa(dim),
				"nlist":       strconv.Itoa(nlist),
				"m":           strconv.Itoa(m),
				"nbits":       strconv.Itoa(nbits),
				"SLICE_SIZE":  strconv.Itoa(sliceSize),
			},
		},
		{
			dtype:      dtype,
			typeParams: nil,
			indexParams: map[string]string{
				"index_type":  IndexFaissIVFFlat,
				"metric_type": L2,
				"dim":         strconv.Itoa(dim),
				"nlist":       strconv.Itoa(nlist),
				"SLICE_SIZE":  strconv.Itoa(sliceSize),
			},
		},
	}
}

func genBinaryVecIndexCases(dtype schemapb.DataType) []indexTestCase {
	return []indexTestCase{
		{
			dtype:      dtype,
			typeParams: nil,
			indexParams: map[string]string{
				"index_type":  IndexFaissBinIVFFlat,
				"metric_type": Jaccard,
				"dim":         strconv.Itoa(dim),
				"nlist":       strconv.Itoa(nlist),
				"nbits":       strconv.Itoa(nbits),
				"SLICE_SIZE":  strconv.Itoa(sliceSize),
			},
		},
	}
}

func genTypedIndexCase(dtype schemapb.DataType) []indexTestCase {
	switch dtype {
	case schemapb.DataType_Bool:
		return genScalarIndexCases(dtype)
	case schemapb.DataType_Int8:
		return genScalarIndexCases(dtype)
	case schemapb.DataType_Int16:
		return genScalarIndexCases(dtype)
	case schemapb.DataType_Int32:
		return genScalarIndexCases(dtype)
	case schemapb.DataType_Int64:
		return genScalarIndexCases(dtype)
	case schemapb.DataType_Float:
		return genScalarIndexCases(dtype)
	case schemapb.DataType_Double:
		return genScalarIndexCases(dtype)
	case schemapb.DataType_String:
		return genScalarIndexCases(dtype)
	case schemapb.DataType_VarChar:
		return genStringIndexCases(dtype)
	case schemapb.DataType_BinaryVector:
		return genBinaryVecIndexCases(dtype)
	case schemapb.DataType_FloatVector:
		return genFloatVecIndexCases(dtype)
	default:
		return nil
	}
}

func genIndexCase() []indexTestCase {
	dtypes := []schemapb.DataType{
		schemapb.DataType_Bool,
		schemapb.DataType_Int8,
		schemapb.DataType_Int16,
		schemapb.DataType_Int32,
		schemapb.DataType_Int64,
		schemapb.DataType_Float,
		schemapb.DataType_Double,
		schemapb.DataType_String,
		schemapb.DataType_VarChar,
		schemapb.DataType_BinaryVector,
		schemapb.DataType_FloatVector,
	}
	var ret []indexTestCase
	for _, dtype := range dtypes {
		ret = append(ret, genTypedIndexCase(dtype)...)
	}
	return ret
}

func TestCgoIndex(t *testing.T) {
	for _, testCase := range genIndexCase() {
		index, err := NewCgoIndex(testCase.dtype, testCase.typeParams, testCase.indexParams)
		assert.NoError(t, err, testCase)

		dataset := GenDataset(genFieldData(testCase.dtype, nb, dim))
		assert.NoError(t, index.Build(dataset), testCase)

		blobs, err := index.Serialize()
		assert.NoError(t, err, testCase)

		copyIndex, err := NewCgoIndex(testCase.dtype, testCase.typeParams, testCase.indexParams)
		assert.NoError(t, err, testCase)

		assert.NoError(t, copyIndex.Load(blobs), testCase)
	}
}
