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

package indexparamcheck

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus/pkg/common"
)

// IndexType string.
type IndexType = string

// IndexType definitions
const (
	// vector index
	IndexGpuBF           IndexType = "GPU_BRUTE_FORCE"
	IndexRaftIvfFlat     IndexType = "GPU_IVF_FLAT"
	IndexRaftIvfPQ       IndexType = "GPU_IVF_PQ"
	IndexRaftCagra       IndexType = "GPU_CAGRA"
	IndexRaftBruteForce  IndexType = "GPU_BRUTE_FORCE"
	IndexFaissIDMap      IndexType = "FLAT" // no index is built.
	IndexFaissIvfFlat    IndexType = "IVF_FLAT"
	IndexFaissIvfPQ      IndexType = "IVF_PQ"
	IndexScaNN           IndexType = "SCANN"
	IndexFaissIvfSQ8     IndexType = "IVF_SQ8"
	IndexFaissBinIDMap   IndexType = "BIN_FLAT"
	IndexFaissBinIvfFlat IndexType = "BIN_IVF_FLAT"
	IndexHNSW            IndexType = "HNSW"
	IndexDISKANN         IndexType = "DISKANN"
	IndexSparseInverted  IndexType = "SPARSE_INVERTED_INDEX"
	IndexSparseWand      IndexType = "SPARSE_WAND"

	// scalar index
	IndexSTLSORT  IndexType = "STL_SORT"
	IndexTRIE     IndexType = "TRIE"
	IndexTrie     IndexType = "Trie"
	IndexBitmap   IndexType = "BITMAP"
	IndexHybrid   IndexType = "HYBRID" // BITMAP + INVERTED
	IndexINVERTED IndexType = "INVERTED"

	AutoIndex IndexType = "AUTOINDEX"
)

func IsGpuIndex(indexType IndexType) bool {
	return indexType == IndexGpuBF ||
		indexType == IndexRaftIvfFlat ||
		indexType == IndexRaftIvfPQ ||
		indexType == IndexRaftCagra
}

// IsVectorMmapIndex check if the vector index can be mmaped
func IsVectorMmapIndex(indexType IndexType) bool {
	return indexType == IndexFaissIDMap ||
		indexType == IndexFaissIvfFlat ||
		indexType == IndexFaissIvfPQ ||
		indexType == IndexFaissIvfSQ8 ||
		indexType == IndexFaissBinIDMap ||
		indexType == IndexFaissBinIvfFlat ||
		indexType == IndexHNSW ||
		indexType == IndexScaNN ||
		indexType == IndexSparseInverted ||
		indexType == IndexSparseWand
}

func IsOffsetCacheSupported(indexType IndexType) bool {
	return indexType == IndexBitmap
}

func IsDiskIndex(indexType IndexType) bool {
	return indexType == IndexDISKANN
}

func IsScalarMmapIndex(indexType IndexType) bool {
	return indexType == IndexINVERTED ||
		indexType == IndexBitmap ||
		indexType == IndexHybrid
}

func ValidateMmapIndexParams(indexType IndexType, indexParams map[string]string) error {
	mmapEnable, ok := indexParams[common.MmapEnabledKey]
	if !ok {
		return nil
	}
	enable, err := strconv.ParseBool(mmapEnable)
	if err != nil {
		return fmt.Errorf("invalid %s value: %s, expected: true, false", common.MmapEnabledKey, mmapEnable)
	}
	mmapSupport := indexType == AutoIndex || IsVectorMmapIndex(indexType) || IsScalarMmapIndex(indexType)
	if enable && !mmapSupport {
		return fmt.Errorf("index type %s does not support mmap", indexType)
	}
	return nil
}

func ValidateOffsetCacheIndexParams(indexType IndexType, indexParams map[string]string) error {
	offsetCacheEnable, ok := indexParams[common.IndexOffsetCacheEnabledKey]
	if !ok {
		return nil
	}
	enable, err := strconv.ParseBool(offsetCacheEnable)
	if err != nil {
		return fmt.Errorf("invalid %s value: %s, expected: true, false", common.IndexOffsetCacheEnabledKey, offsetCacheEnable)
	}
	if enable && !IsOffsetCacheSupported(indexType) {
		return fmt.Errorf("only bitmap index support %s now", common.IndexOffsetCacheEnabledKey)
	}
	return nil
}
