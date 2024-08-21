// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/client/v2/entity"
)

// index param field tag
const (
	IndexTypeKey  = `index_type`
	MetricTypeKey = `metric_type`
	ParamsKey     = `params`
)

// IndexState export index state
type IndexState commonpb.IndexState

// IndexType index type
type IndexType string

// MetricType alias for `entity.MetricsType`.
type MetricType = entity.MetricType

// Index Constants
const (
	Flat       IndexType = "FLAT" // faiss
	BinFlat    IndexType = "BIN_FLAT"
	IvfFlat    IndexType = "IVF_FLAT" // faiss
	BinIvfFlat IndexType = "BIN_IVF_FLAT"
	IvfPQ      IndexType = "IVF_PQ" // faiss
	IvfSQ8     IndexType = "IVF_SQ8"
	HNSW       IndexType = "HNSW"
	IvfHNSW    IndexType = "IVF_HNSW"
	AUTOINDEX  IndexType = "AUTOINDEX"
	DISKANN    IndexType = "DISKANN"
	SCANN      IndexType = "SCANN"

	// Sparse
	SparseInverted IndexType = "SPARSE_INVERTED_INDEX"
	SparseWAND     IndexType = "SPARSE_WAND"

	GPUIvfFlat IndexType = "GPU_IVF_FLAT"
	GPUIvfPQ   IndexType = "GPU_IVF_PQ"

	GPUCagra      IndexType = "GPU_CAGRA"
	GPUBruteForce IndexType = "GPU_BRUTE_FORCE"

	Trie     IndexType = "Trie"
	Sorted   IndexType = "STL_SORT"
	Inverted IndexType = "INVERTED"
	BITMAP   IndexType = "BITMAP"
)
