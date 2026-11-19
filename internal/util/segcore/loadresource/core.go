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
#include <stdint.h>
#include <stdlib.h>
#include "common/protobuf_utils_c.h"
#include "segcore/segment_resource_c.h"
*/
import "C"

import (
	"context"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// estimateCoreResources supplies metadata and resolved local index options to
// segcore. All field/index size calculations stay on the loading implementation's side.
func estimateCoreResources(ctx context.Context, schemas []*schemapb.CollectionSchema, loadInfos []*querypb.SegmentLoadInfo, mmapVector, mmapScalar, preferFieldData, finalOnly bool, runner Runner) (*cgopb.BatchLoadResourceResponse, error) {
	if len(schemas) != len(loadInfos) {
		return nil, merr.WrapErrServiceInternalMsg("schema and segment counts differ")
	}
	if len(loadInfos) == 0 {
		return &cgopb.BatchLoadResourceResponse{}, nil
	}
	request := &cgopb.BatchLoadResourceRequest{
		Segments:                           make([]*cgopb.SegmentLoadResources, 0, len(loadInfos)),
		MmapVectorField:                    mmapVector,
		MmapScalarField:                    mmapScalar,
		PreferFieldDataWhenIndexHasRawData: preferFieldData,
		FinalOnly:                          finalOnly,
	}
	for i, loadInfo := range loadInfos {
		schema := schemas[i]
		if schema == nil {
			return nil, merr.WrapErrServiceInternalMsg("collection schema is nil")
		}
		if loadInfo == nil {
			return nil, merr.WrapErrServiceInternalMsg("segment load info is nil")
		}
		helper, err := typeutil.CreateSchemaHelper(schema)
		if err != nil {
			return nil, err
		}
		fields, _ := ResolveSegmentEstimateLogs(schema, loadInfo)
		segment := &cgopb.SegmentLoadResources{
			Schema:             schema,
			Fields:             make([]*segcorepb.FieldBinlog, 0, len(fields)),
			StorageVersion:     loadInfo.GetStorageVersion(),
			ExternalCollection: typeutil.IsExternalCollection(schema),
			Indexes:            make([]*cgopb.LoadIndexInfo, 0, len(loadInfo.GetIndexInfos())),
		}
		for _, field := range fields {
			group := &segcorepb.FieldBinlog{FieldID: field.GetFieldID(), ChildFields: field.GetChildFields(), Binlogs: make([]*segcorepb.Binlog, 0, len(field.GetBinlogs()))}
			for _, log := range field.GetBinlogs() {
				group.Binlogs = append(group.Binlogs, &segcorepb.Binlog{EntriesNum: log.GetEntriesNum(), MemorySize: log.GetMemorySize(), LogSize: log.GetLogSize()})
			}
			segment.Fields = append(segment.Fields, group)
		}

		segment.ExternalLazyLoad = segment.ExternalCollection && isExternalCollectionLazyLoadForLoading(schema)
		for _, index := range loadInfo.GetIndexInfos() {
			if len(index.GetIndexFilePaths()) == 0 {
				continue
			}
			field, err := helper.GetFieldFromID(index.GetFieldID())
			if err != nil {
				mlog.Info(ctx, "skip index for dropped field", mlog.Int64("fieldID", index.GetFieldID()), mlog.String("name", schema.GetName()))
				continue
			}
			info, err := buildCLoadIndexInfo(field, loadInfo, index)
			if err != nil {
				return nil, merr.Wrapf(err, "prepare index resource estimate, collection %d, segment %d, indexBuildID %d", loadInfo.GetCollectionID(), loadInfo.GetSegmentID(), index.GetBuildID())
			}
			segment.Indexes = append(segment.Indexes, info)
		}
		request.Segments = append(request.Segments, segment)
	}
	serialized, err := proto.Marshal(request)
	if err != nil {
		return nil, err
	}
	layout := C.CreateProtoLayout()
	if layout == nil {
		return nil, merr.WrapErrServiceInternalMsg("create segment resource result failed")
	}
	defer C.ReleaseProtoLayout(layout)
	var status C.CStatus
	if err := run(runner, func() error {
		status = C.EstimateSegmentResourcesFromSerializedInfo((*C.uint8_t)(unsafe.Pointer(&serialized[0])), C.uint64_t(len(serialized)), layout)
		return nil
	}); err != nil {
		return nil, err
	}
	if err := consumeCStatus(&status); err != nil {
		return nil, merr.Wrap(err, "estimate segment resources failed")
	}
	result := &cgopb.BatchLoadResourceResponse{}
	if err := proto.Unmarshal(unsafe.Slice((*byte)(layout.blob), int(layout.size)), result); err != nil {
		return nil, err
	}
	if len(result.GetSegments()) != len(loadInfos) || result.GetTotal() == nil {
		return nil, merr.WrapErrServiceInternalMsg("segcore returned an incomplete segment resource estimate")
	}
	return result, nil
}
