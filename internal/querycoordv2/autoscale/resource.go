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

package autoscale

import (
	"context"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/segcore/loadresource"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type ResourceUsage struct {
	MemoryBytes int64
	DiskBytes   int64
}

type EstimateOptions struct {
	MmapVectorField                    bool
	MmapScalarField                    bool
	MmapJSONStats                      bool
	TieredEvictionEnabled              bool
	TieredMemoryCacheRatio             float64
	TieredDiskCacheRatio               float64
	DeltaDataExpansionFactor           float64
	JSONKeyStatsExpansionFactor        float64
	TextIndexExpansionFactor           float64
	PreferFieldDataWhenIndexHasRawData bool
}

func DefaultEstimateOptions() EstimateOptions {
	queryNodeCfg := &paramtable.Get().QueryNodeCfg
	return EstimateOptions{
		MmapVectorField:                    queryNodeCfg.MmapVectorField.GetAsBool(),
		MmapScalarField:                    queryNodeCfg.MmapScalarField.GetAsBool(),
		MmapJSONStats:                      queryNodeCfg.MmapJSONStats.GetAsBool(),
		TieredEvictionEnabled:              queryNodeCfg.TieredEvictionEnabled.GetAsBool(),
		TieredMemoryCacheRatio:             queryNodeCfg.TieredEvictableMemoryCacheRatio.GetAsFloat(),
		TieredDiskCacheRatio:               queryNodeCfg.TieredEvictableDiskCacheRatio.GetAsFloat(),
		DeltaDataExpansionFactor:           queryNodeCfg.DeltaDataExpansionRate.GetAsFloat(),
		JSONKeyStatsExpansionFactor:        queryNodeCfg.JSONKeyStatsExpansionFactor.GetAsFloat(),
		TextIndexExpansionFactor:           queryNodeCfg.TextIndexExpansionFactor.GetAsFloat(),
		PreferFieldDataWhenIndexHasRawData: queryNodeCfg.PreferFieldDataWhenIndexHasRawData.GetAsBool(),
	}
}

func EstimateSegmentsLoadResource(schema *schemapb.CollectionSchema, segments []*datapb.SegmentInfo, indexes map[int64][]*querypb.FieldIndexInfo, options EstimateOptions) (ResourceUsage, error) {
	options = options.normalized()
	if schema == nil {
		return ResourceUsage{}, merr.WrapErrServiceInternalMsg("collection schema is nil")
	}

	usage := ResourceUsage{}
	for _, segment := range segments {
		segmentIndexes := indexes[segment.GetID()]
		finalUsage, err := loadresource.EstimateSegmentFinalResource(
			context.Background(),
			schema,
			segmentLoadInfo(segment, segmentIndexes),
			options.segmentFinalEstimateOptions(),
			nil,
		)
		if err != nil {
			return ResourceUsage{}, merr.Wrapf(err, "failed to estimate load resource for segment %d", segment.GetID())
		}
		usage.add(ResourceUsage{
			MemoryBytes: uint64ToInt64(finalUsage.MemoryBytes),
			DiskBytes:   uint64ToInt64(finalUsage.DiskBytes),
		})
	}
	return usage, nil
}

func EstimateSegmentsLoadResourceForLoadConfig(schema *schemapb.CollectionSchema, segments []*datapb.SegmentInfo, indexes map[int64][]*querypb.FieldIndexInfo, loadFields []int64, fieldIndexID map[int64]int64, options EstimateOptions) (ResourceUsage, error) {
	return EstimateSegmentsLoadResource(
		schema,
		FilterSegmentsByLoadFields(segments, loadFields),
		FilterIndexesByLoadConfig(indexes, loadFields, fieldIndexID),
		options,
	)
}

func FilterSegmentsByLoadFields(segments []*datapb.SegmentInfo, loadFields []int64) []*datapb.SegmentInfo {
	if len(loadFields) == 0 {
		return segments
	}
	loadFieldSet := typeutil.NewSet(loadFields...)
	return lo.Map(segments, func(segment *datapb.SegmentInfo, _ int) *datapb.SegmentInfo {
		filtered := proto.Clone(segment).(*datapb.SegmentInfo)
		filtered.Binlogs = filterFieldBinlogsByLoadFields(filtered.GetBinlogs(), loadFieldSet)
		filtered.Statslogs = filterFieldBinlogsByLoadFields(filtered.GetStatslogs(), loadFieldSet)
		filtered.Bm25Statslogs = filterFieldBinlogsByLoadFields(filtered.GetBm25Statslogs(), loadFieldSet)
		filtered.JsonKeyStats = filterMapByLoadFields(filtered.GetJsonKeyStats(), loadFieldSet)
		filtered.TextStatsLogs = filterMapByLoadFields(filtered.GetTextStatsLogs(), loadFieldSet)
		return filtered
	})
}

func filterFieldBinlogsByLoadFields(fieldBinlogs []*datapb.FieldBinlog, loadFields typeutil.Set[int64]) []*datapb.FieldBinlog {
	return lo.Filter(fieldBinlogs, func(fieldBinlog *datapb.FieldBinlog, _ int) bool {
		childFields := fieldBinlog.GetChildFields()
		if len(childFields) == 0 {
			return shouldKeepFieldForLoadConfig(fieldBinlog.GetFieldID(), loadFields)
		}
		for _, fieldID := range childFields {
			if shouldKeepFieldForLoadConfig(fieldID, loadFields) {
				return true
			}
		}
		return false
	})
}

func filterMapByLoadFields[T any](values map[int64]T, loadFields typeutil.Set[int64]) map[int64]T {
	if len(values) == 0 {
		return values
	}
	filtered := make(map[int64]T)
	for fieldID, value := range values {
		if shouldKeepFieldForLoadConfig(fieldID, loadFields) {
			filtered[fieldID] = value
		}
	}
	return filtered
}

func shouldKeepFieldForLoadConfig(fieldID int64, loadFields typeutil.Set[int64]) bool {
	return common.IsSystemField(fieldID) || loadFields.Contain(fieldID)
}

func FilterIndexesByLoadConfig(indexes map[int64][]*querypb.FieldIndexInfo, loadFields []int64, fieldIndexID map[int64]int64) map[int64][]*querypb.FieldIndexInfo {
	if len(indexes) == 0 || (len(loadFields) == 0 && len(fieldIndexID) == 0) {
		return indexes
	}
	loadFieldSet := typeutil.NewSet(loadFields...)
	filtered := make(map[int64][]*querypb.FieldIndexInfo)
	for segmentID, segmentIndexes := range indexes {
		for _, index := range segmentIndexes {
			if len(loadFieldSet) > 0 && !loadFieldSet.Contain(index.GetFieldID()) {
				continue
			}
			if expectedIndexID, ok := fieldIndexID[index.GetFieldID()]; ok && expectedIndexID != 0 && expectedIndexID != index.GetIndexID() {
				continue
			}
			filtered[segmentID] = append(filtered[segmentID], index)
		}
	}
	return filtered
}

func (options EstimateOptions) normalized() EstimateOptions {
	if options.TieredMemoryCacheRatio < 0 || options.TieredMemoryCacheRatio > 1 {
		options.TieredMemoryCacheRatio = 1
	}
	if options.TieredDiskCacheRatio < 0 || options.TieredDiskCacheRatio > 1 {
		options.TieredDiskCacheRatio = 1
	}
	if options.DeltaDataExpansionFactor <= 0 {
		options.DeltaDataExpansionFactor = 1
	}
	if options.JSONKeyStatsExpansionFactor <= 0 {
		options.JSONKeyStatsExpansionFactor = 1
	}
	if options.TextIndexExpansionFactor <= 0 {
		options.TextIndexExpansionFactor = 1
	}
	return options
}

func (options EstimateOptions) segmentFinalEstimateOptions() loadresource.SegmentFinalEstimateOptions {
	return loadresource.SegmentFinalEstimateOptions{
		MmapVectorField:                    options.MmapVectorField,
		MmapScalarField:                    options.MmapScalarField,
		MmapJSONStats:                      options.MmapJSONStats,
		DeltaDataExpansionFactor:           options.DeltaDataExpansionFactor,
		JSONKeyStatsExpansionFactor:        options.JSONKeyStatsExpansionFactor,
		TextIndexExpansionFactor:           options.TextIndexExpansionFactor,
		TieredEvictionEnabled:              options.TieredEvictionEnabled,
		TieredEvictableMemoryCacheRatio:    options.TieredMemoryCacheRatio,
		TieredEvictableDiskCacheRatio:      options.TieredDiskCacheRatio,
		PreferFieldDataWhenIndexHasRawData: options.PreferFieldDataWhenIndexHasRawData,
	}
}

func segmentLoadInfo(segment *datapb.SegmentInfo, indexes []*querypb.FieldIndexInfo) *querypb.SegmentLoadInfo {
	return &querypb.SegmentLoadInfo{
		CollectionID:     segment.GetCollectionID(),
		PartitionID:      segment.GetPartitionID(),
		SegmentID:        segment.GetID(),
		NumOfRows:        segment.GetNumOfRows(),
		BinlogPaths:      segment.GetBinlogs(),
		Statslogs:        segment.GetStatslogs(),
		Deltalogs:        segment.GetDeltalogs(),
		IndexInfos:       indexes,
		Bm25Logs:         segment.GetBm25Statslogs(),
		JsonKeyStatsLogs: segment.GetJsonKeyStats(),
		TextStatsLogs:    segment.GetTextStatsLogs(),
		InsertChannel:    segment.GetInsertChannel(),
		Level:            segment.GetLevel(),
		StorageVersion:   segment.GetStorageVersion(),
		IsSorted:         segment.GetIsSorted(),
		ManifestPath:     segment.GetManifestPath(),
		CommitTimestamp:  segment.GetCommitTimestamp(),
		DataVersion:      segment.GetDataVersion(),
		CompactionFrom:   segment.GetCompactionFrom(),
	}
}

func uint64ToInt64(value uint64) int64 {
	const maxInt64 = uint64(1<<63 - 1)
	if value > maxInt64 {
		return int64(maxInt64)
	}
	return int64(value)
}

func (usage *ResourceUsage) add(other ResourceUsage) {
	usage.MemoryBytes += other.MemoryBytes
	usage.DiskBytes += other.DiskBytes
}
