// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package indexnode

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	// index type
	IndexFaissIDMap      = "FLAT"
	IndexFaissIVFFlat    = "IVF_FLAT"
	IndexFaissIVFPQ      = "IVF_PQ"
	IndexFaissIVFSQ8     = "IVF_SQ8"
	IndexFaissIVFSQ8H    = "IVF_SQ8_HYBRID"
	IndexFaissBinIDMap   = "BIN_FLAT"
	IndexFaissBinIVFFlat = "BIN_IVF_FLAT"
	IndexNsg             = "NSG"

	IndexHNSW      = "HNSW"
	IndexRHNSWFlat = "RHNSW_FLAT"
	IndexRHNSWPQ   = "RHNSW_PQ"
	IndexRHNSWSQ   = "RHNSW_SQ"
	IndexANNOY     = "ANNOY"
	IndexNGTPANNG  = "NGT_PANNG"
	IndexNGTONNG   = "NGT_ONNG"

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
		{IndexFaissIDMap, L2, false},
		{IndexFaissIDMap, IP, false},
		{IndexFaissIVFFlat, L2, false},
		{IndexFaissIVFFlat, IP, false},
		{IndexFaissIVFPQ, L2, false},
		{IndexFaissIVFPQ, IP, false},
		{IndexFaissIVFSQ8, L2, false},
		{IndexFaissIVFSQ8, IP, false},
		//{IndexFaissIVFSQ8H, L2, false}, // TODO: enable gpu
		//{IndexFaissIVFSQ8H, IP, false},
		{IndexNsg, L2, false},
		{IndexNsg, IP, false},
		//{IndexHNSW, L2, false}, // TODO: fix json parse exception
		//{IndexHNSW, IP, false},
		//{IndexRHNSWFlat, L2, false},
		//{IndexRHNSWFlat, IP, false},
		//{IndexRHNSWPQ, L2, false},
		//{IndexRHNSWPQ, IP, false},
		//{IndexRHNSWSQ, L2, false},
		//{IndexRHNSWSQ, IP, false},
		{IndexANNOY, L2, false},
		{IndexANNOY, IP, false},
		{IndexNGTPANNG, L2, false},
		{IndexNGTPANNG, IP, false},
		{IndexNGTONNG, L2, false},
		{IndexNGTONNG, IP, false},
	}
}

func generateBinaryVectorTestCases() []testCase {
	return []testCase{
		{IndexFaissBinIVFFlat, Jaccard, true},
		{IndexFaissBinIVFFlat, hamming, true},
		{IndexFaissBinIVFFlat, tanimoto, true},
		{IndexFaissBinIDMap, Jaccard, true},
		{IndexFaissBinIDMap, hamming, true},
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
	} else if indexType == IndexFaissIVFSQ8H {
		// TODO: enable gpu
	} else if indexType == IndexNsg {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["nlist"] = strconv.Itoa(163)
		indexParams["nprobe"] = strconv.Itoa(nprobe)
		indexParams["knng"] = strconv.Itoa(20)
		indexParams["search_length"] = strconv.Itoa(40)
		indexParams["out_degree"] = strconv.Itoa(30)
		indexParams["candidate_pool_size"] = strconv.Itoa(100)
	} else if indexType == IndexHNSW {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["m"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		indexParams["ef"] = strconv.Itoa(ef)
	} else if indexType == IndexRHNSWFlat {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["m"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		indexParams["ef"] = strconv.Itoa(ef)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexRHNSWPQ {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["m"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		indexParams["ef"] = strconv.Itoa(ef)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
		indexParams["PQM"] = strconv.Itoa(8)
	} else if indexType == IndexRHNSWSQ {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["m"] = strconv.Itoa(16)
		indexParams["efConstruction"] = strconv.Itoa(efConstruction)
		indexParams["ef"] = strconv.Itoa(ef)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexANNOY {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["n_trees"] = strconv.Itoa(4)
		indexParams["search_k"] = strconv.Itoa(100)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexNGTPANNG {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["edge_size"] = strconv.Itoa(edgeSize)
		indexParams["epsilon"] = fmt.Sprint(epsilon)
		indexParams["max_search_edges"] = strconv.Itoa(maxSearchEdges)
		indexParams["forcedly_pruned_edge_size"] = strconv.Itoa(60)
		indexParams["selectively_pruned_edge_size"] = strconv.Itoa(30)
		indexParams["SLICE_SIZE"] = strconv.Itoa(sliceSize)
	} else if indexType == IndexNGTONNG {
		indexParams["dim"] = strconv.Itoa(dim)
		indexParams["edge_size"] = strconv.Itoa(edgeSize)
		indexParams["epsilon"] = fmt.Sprint(epsilon)
		indexParams["max_search_edges"] = strconv.Itoa(maxSearchEdges)
		indexParams["outgoing_edge_size"] = strconv.Itoa(5)
		indexParams["incoming_edge_size"] = strconv.Itoa(40)
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

func TestCIndex_Error(t *testing.T) {
	indexPtr, err := NewCIndex(nil, nil)
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
		err = indexPtr.BuildFloatVecIndexWithoutIds(floatVectors)
		assert.NotNil(t, err)
	})

	t.Run("BuildBinaryVecIndexWithoutIds error", func(t *testing.T) {
		binaryVectors := []byte("binaryVectors")
		err = indexPtr.BuildBinaryVecIndexWithoutIds(binaryVectors)
		assert.NotNil(t, err)
	})
}
