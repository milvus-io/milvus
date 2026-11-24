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

import (
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/querymeter"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func collectPlannedReadUsage(segment Segment, fields []int64) (*internalpb.QueryReadUsage, func(), error) {
	unlock := func() {}
	if len(fields) == 0 {
		return nil, unlock, nil
	}
	var fieldSizes *datapb.FieldLogicalSizes
	var growing segcore.CSegment
	switch segment := segment.(type) {
	case *sealedReadState:
		fieldSizes = segment.fieldSizes
	case *LocalSegment:
		if segment.Type() == SegmentTypeGrowing {
			growing = segment.csegment
		} else {
			// Keep the statistics and the native generation aligned through
			// execution if the legacy worker reopens this segment.
			segment.queryMeterMu.RLock()
			unlock = segment.queryMeterMu.RUnlock
			fieldSizes = segment.queryMeterFieldSizes
		}
	case *viewQueryGrowingSegment:
		growing = segment.csegment
	}

	usage := &internalpb.QueryReadUsage{}
	if growing != nil {
		sizes := segcore.GrowingFieldMemoryBytes(growing, fields)
		if len(sizes) != len(fields) {
			return nil, unlock, missingQueryFieldBytes(segment, fields[len(sizes)], "growing memory")
		}
		for _, bytes := range sizes {
			if err := querymeter.AddDataBytes(usage, bytes); err != nil {
				return nil, unlock, err
			}
		}
		return usage, unlock, nil
	}

	if fieldSizes == nil {
		return nil, unlock, missingQueryFieldBytes(segment, fields[0], "sealed manifest")
	}
	for _, fieldID := range fields {
		size := fieldSizes.GetFields()[fieldID]
		if size == nil {
			return nil, unlock, missingQueryFieldBytes(segment, fieldID, "sealed column")
		}
		if err := querymeter.AddColumn(usage, fieldID, size.GetPayloadBytes(), size.GetValidityBits()); err != nil {
			return nil, unlock, err
		}
	}
	return usage, unlock, nil
}

func sealedQueryFieldSizes(info *querypb.SegmentLoadInfo) *datapb.FieldLogicalSizes {
	if info.GetStorageVersion() != storage.StorageV3 || info.GetLevel() == datapb.SegmentLevel_L0 {
		return nil
	}
	sizes := info.GetStats().GetFieldLogicalSizes()
	if !storage.FieldLogicalSizesCover(sizes, info.GetNumOfRows()) {
		return nil
	}
	path := info.GetManifestPath()
	base, version, err := packed.UnmarshalManifestPath(path)
	if err != nil || base == "" || version <= packed.ManifestEarliest ||
		!packed.SameManifestPath(info.GetFieldLogicalSizesManifest(), path) {
		return nil
	}
	return sizes
}

func missingQueryFieldBytes(segment Segment, fieldID int64, source string) error {
	return merr.WrapErrServiceInternalMsg("missing query read usage for segment %d field %d (%s)", segment.ID(), fieldID, source)
}
