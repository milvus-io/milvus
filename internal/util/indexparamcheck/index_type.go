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

	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/common"
)

// IndexType string.
type IndexType = string

// IndexType definitions
const (
	IndexVector IndexType = "VECINDEX"

	// scalar index
	IndexSTLSORT  IndexType = "STL_SORT"
	IndexTRIE     IndexType = "TRIE"
	IndexTrie     IndexType = "Trie"
	IndexBitmap   IndexType = "BITMAP"
	IndexHybrid   IndexType = "HYBRID" // BITMAP + INVERTED
	IndexINVERTED IndexType = "INVERTED"

	AutoIndex IndexType = "AUTOINDEX"
)

func IsScalarIndexType(indexType IndexType) bool {
	return indexType == IndexSTLSORT || indexType == IndexTRIE || indexType == IndexTrie ||
		indexType == IndexBitmap || indexType == IndexHybrid || indexType == IndexINVERTED
}

func IsGpuIndex(indexType IndexType) bool {
	return vecindexmgr.GetVecIndexMgrInstance().IsGPUVecIndex(indexType)
}

// IsVectorMmapIndex check if the vector index can be mmaped
func IsVectorMmapIndex(indexType IndexType) bool {
	return vecindexmgr.GetVecIndexMgrInstance().IsMMapSupported(indexType)
}

func IsOffsetCacheSupported(indexType IndexType) bool {
	return indexType == IndexBitmap
}

func IsDiskIndex(indexType IndexType) bool {
	return vecindexmgr.GetVecIndexMgrInstance().IsDiskANN(indexType)
}

func IsScalarMmapIndex(indexType IndexType) bool {
	return indexType == IndexINVERTED ||
		indexType == IndexBitmap ||
		indexType == IndexHybrid ||
		indexType == IndexTrie
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
