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

package segments

/*
#cgo pkg-config: milvus_core

#include "segcore/load_index_c.h"
*/
import "C"

import (
	"fmt"
	"unsafe"

	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var indexAttrCache = NewIndexAttrCache()

// getIndexAttrCache use a singleton to store index meta cache.
func getIndexAttrCache() *IndexAttrCache {
	return indexAttrCache
}

// IndexAttrCache index meta cache stores calculated attribute.
type IndexAttrCache struct {
	loadWithDisk *typeutil.ConcurrentMap[typeutil.Pair[string, int32], bool]
	sf           conc.Singleflight[bool]
}

func NewIndexAttrCache() *IndexAttrCache {
	return &IndexAttrCache{
		loadWithDisk: typeutil.NewConcurrentMap[typeutil.Pair[string, int32], bool](),
	}
}

func (c *IndexAttrCache) GetIndexResourceUsage(indexInfo *querypb.FieldIndexInfo, memoryIndexLoadPredictMemoryUsageFactor float64, fieldBinlog *datapb.FieldBinlog) (memory uint64, disk uint64, err error) {
	indexType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.IndexTypeKey, indexInfo.IndexParams)
	if err != nil {
		return 0, 0, fmt.Errorf("index type not exist in index params")
	}
	if vecindexmgr.GetVecIndexMgrInstance().IsDiskANN(indexType) {
		neededMemSize := indexInfo.IndexSize / UsedDiskMemoryRatio
		neededDiskSize := indexInfo.IndexSize - neededMemSize
		return uint64(neededMemSize), uint64(neededDiskSize), nil
	}
	if indexType == indexparamcheck.IndexINVERTED {
		neededMemSize := 0
		// we will mmap the binlog if the index type is inverted index.
		neededDiskSize := indexInfo.IndexSize + getBinlogDataDiskSize(fieldBinlog)
		return uint64(neededMemSize), uint64(neededDiskSize), nil
	}

	engineVersion := indexInfo.GetCurrentIndexVersion()
	isLoadWithDisk, has := c.loadWithDisk.Get(typeutil.NewPair(indexType, engineVersion))
	if !has {
		isLoadWithDisk, _, _ = c.sf.Do(fmt.Sprintf("%s_%d", indexType, engineVersion), func() (bool, error) {
			var result bool
			GetDynamicPool().Submit(func() (any, error) {
				cIndexType := C.CString(indexType)
				defer C.free(unsafe.Pointer(cIndexType))
				cEngineVersion := C.int32_t(indexInfo.GetCurrentIndexVersion())
				result = bool(C.IsLoadWithDisk(cIndexType, cEngineVersion))
				return nil, nil
			}).Await()
			c.loadWithDisk.Insert(typeutil.NewPair(indexType, engineVersion), result)
			return result, nil
		})
	}

	factor := float64(1)
	diskUsage := uint64(0)
	if !isLoadWithDisk {
		factor = memoryIndexLoadPredictMemoryUsageFactor
	} else {
		diskUsage = uint64(indexInfo.IndexSize)
	}

	return uint64(float64(indexInfo.IndexSize) * factor), diskUsage, nil
}
