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

package loadresource

/*
#cgo pkg-config: milvus_core

#include <stdlib.h>
#include "segcore/load_index_c.h"
*/
import "C"

import (
	"context"
	"unsafe"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type IndexResourceUsage struct {
	MaxMemoryBytes   uint64
	MaxDiskBytes     uint64
	FinalMemoryBytes uint64
	FinalDiskBytes   uint64
	HasRawData       bool
}

type Runner func(func() error) error

func EstimateIndexLoadResource(ctx context.Context, field *schemapb.FieldSchema, segment *querypb.SegmentLoadInfo, index *querypb.FieldIndexInfo) (IndexResourceUsage, error) {
	return EstimateIndexLoadResourceWithRunner(ctx, field, segment, index, nil)
}

func EstimateIndexLoadResourceWithRunner(_ context.Context, field *schemapb.FieldSchema, segment *querypb.SegmentLoadInfo, index *querypb.FieldIndexInfo, runner Runner) (IndexResourceUsage, error) {
	if field == nil {
		return IndexResourceUsage{}, merr.WrapErrServiceInternalMsg("field schema is nil")
	}
	if segment == nil {
		return IndexResourceUsage{}, merr.WrapErrServiceInternalMsg("segment load info is nil")
	}
	if index == nil {
		return IndexResourceUsage{}, merr.WrapErrServiceInternalMsg("field index info is nil")
	}

	info, err := buildCLoadIndexInfo(field, segment, index)
	if err != nil {
		return IndexResourceUsage{}, err
	}
	marshaled, err := proto.Marshal(info)
	if err != nil {
		return IndexResourceUsage{}, err
	}
	if len(marshaled) == 0 {
		return IndexResourceUsage{}, merr.WrapErrServiceInternalMsg("serialized load index info is empty")
	}

	var estimate C.LoadResourceRequest
	var status C.CStatus
	if err := run(runner, func() error {
		status = C.EstimateLoadIndexResourceFromSerializedInfo(
			(*C.uint8_t)(unsafe.Pointer(&marshaled[0])),
			(C.uint64_t)(len(marshaled)),
			&estimate,
		)
		return nil
	}); err != nil {
		return IndexResourceUsage{}, err
	}
	if err := consumeCStatus(&status); err != nil {
		return IndexResourceUsage{}, errors.Wrap(err, "EstimateLoadIndexResourceFromSerializedInfo failed")
	}
	return indexResourceUsageFromC(&estimate), nil
}

func run(runner Runner, fn func() error) error {
	if runner == nil {
		return fn()
	}
	return runner(fn)
}

func consumeCStatus(status *C.CStatus) error {
	if status == nil || status.error_code == 0 {
		return nil
	}
	errorCode := status.error_code
	errorMsg := C.GoString(status.error_msg)
	C.free(unsafe.Pointer(status.error_msg))
	return merr.SegcoreError(int32(errorCode), errorMsg)
}

func buildCLoadIndexInfo(field *schemapb.FieldSchema, segment *querypb.SegmentLoadInfo, index *querypb.FieldIndexInfo) (*cgopb.LoadIndexInfo, error) {
	indexParams := funcutil.KeyValuePair2Map(index.GetIndexParams())
	delete(indexParams, common.MmapEnabledKey)

	indexType := indexParams[common.IndexTypeKey]
	if indexType == "" {
		return nil, merr.WrapErrServiceInternalMsg("index type is empty")
	}
	if vecindexmgr.GetVecIndexMgrInstance().IsDiskANN(indexType) {
		if err := indexparams.SetDiskIndexLoadParams(paramtable.Get(), indexParams, index.GetNumRows()); err != nil {
			return nil, err
		}
	}
	if indexType == indexparamcheck.IndexBitmap {
		indexparams.SetBitmapIndexLoadParams(paramtable.Get(), indexParams)
	}
	if err := indexparams.AppendPrepareLoadParams(paramtable.Get(), indexParams); err != nil {
		return nil, err
	}
	if _, ok := indexParams[common.WarmupKey]; !ok {
		if warmupPolicy := getIndexWarmupPolicy(field, index); warmupPolicy != "" {
			indexParams[common.WarmupKey] = warmupPolicy
		}
	}

	return &cgopb.LoadIndexInfo{
		CollectionID:              segment.GetCollectionID(),
		PartitionID:               segment.GetPartitionID(),
		SegmentID:                 segment.GetSegmentID(),
		Field:                     field,
		EnableMmap:                isIndexMmapEnable(field, index),
		IndexID:                   index.GetIndexID(),
		IndexBuildID:              index.GetBuildID(),
		IndexVersion:              index.GetIndexVersion(),
		IndexParams:               indexParams,
		IndexFiles:                index.GetIndexFilePaths(),
		IndexEngineVersion:        index.GetCurrentIndexVersion(),
		IndexFileSize:             index.GetIndexSize(),
		NumRows:                   index.GetNumRows(),
		CurrentScalarIndexVersion: index.GetCurrentScalarIndexVersion(),
		IndexStorePathVersion:     index.GetIndexStorePathVersion(),
	}, nil
}

func isIndexMmapEnable(field *schemapb.FieldSchema, index *querypb.FieldIndexInfo) bool {
	enableMmap, exist := common.IsMmapIndexEnabled(index.GetIndexParams()...)
	if exist && !enableMmap {
		return false
	}

	indexType := common.GetIndexType(index.GetIndexParams())
	if typeutil.IsVectorType(field.GetDataType()) {
		return vecindexmgr.GetVecIndexMgrInstance().IsMMapSupported(indexType) &&
			(paramtable.Get().QueryNodeCfg.MmapVectorIndex.GetAsBool() || enableMmap)
	}
	return indexparamcheck.IsScalarMmapIndex(indexType) &&
		(paramtable.Get().QueryNodeCfg.MmapScalarIndex.GetAsBool() || enableMmap)
}

func getIndexWarmupPolicy(field *schemapb.FieldSchema, index *querypb.FieldIndexInfo) string {
	if policy, exist := common.GetWarmupPolicy(index.GetIndexParams()...); exist {
		return policy
	}
	if typeutil.IsVectorType(field.GetDataType()) {
		return paramtable.Get().QueryNodeCfg.TieredWarmupVectorIndex.GetValue()
	}
	return paramtable.Get().QueryNodeCfg.TieredWarmupScalarIndex.GetValue()
}

func indexResourceUsageFromC(estimate *C.LoadResourceRequest) IndexResourceUsage {
	return IndexResourceUsage{
		MaxMemoryBytes:   uint64(estimate.max_memory_cost),
		MaxDiskBytes:     uint64(estimate.max_disk_cost),
		FinalMemoryBytes: uint64(estimate.final_memory_cost),
		FinalDiskBytes:   uint64(estimate.final_disk_cost),
		HasRawData:       bool(estimate.has_raw_data),
	}
}
