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

import (
	"context"
	"math"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type SegmentResourceUsage struct {
	MemoryBytes         uint64
	DiskBytes           uint64
	MmapFieldCount      int
	FieldGPUMemoryBytes []uint64
}

type SegmentFinalEstimateOptions struct {
	MmapVectorField                    bool
	MmapScalarField                    bool
	MmapJSONStats                      bool
	DeltaDataExpansionFactor           float64
	JSONKeyStatsExpansionFactor        float64
	TextIndexExpansionFactor           float64
	TieredEvictionEnabled              bool
	TieredEvictableMemoryCacheRatio    float64
	TieredEvictableDiskCacheRatio      float64
	PreferFieldDataWhenIndexHasRawData bool
}

func DefaultSegmentFinalEstimateOptions() SegmentFinalEstimateOptions {
	queryNodeCfg := &paramtable.Get().QueryNodeCfg
	return SegmentFinalEstimateOptions{
		MmapVectorField:                    queryNodeCfg.MmapVectorField.GetAsBool(),
		MmapScalarField:                    queryNodeCfg.MmapScalarField.GetAsBool(),
		MmapJSONStats:                      queryNodeCfg.MmapJSONStats.GetAsBool(),
		DeltaDataExpansionFactor:           queryNodeCfg.DeltaDataExpansionRate.GetAsFloat(),
		JSONKeyStatsExpansionFactor:        queryNodeCfg.JSONKeyStatsExpansionFactor.GetAsFloat(),
		TextIndexExpansionFactor:           queryNodeCfg.TextIndexExpansionFactor.GetAsFloat(),
		TieredEvictionEnabled:              queryNodeCfg.TieredEvictionEnabled.GetAsBool(),
		TieredEvictableMemoryCacheRatio:    queryNodeCfg.TieredEvictableMemoryCacheRatio.GetAsFloat(),
		TieredEvictableDiskCacheRatio:      queryNodeCfg.TieredEvictableDiskCacheRatio.GetAsFloat(),
		PreferFieldDataWhenIndexHasRawData: queryNodeCfg.PreferFieldDataWhenIndexHasRawData.GetAsBool(),
	}
}

type SegmentLoadingEstimateOptions struct {
	MmapVectorField                    bool
	MmapScalarField                    bool
	MmapJSONStats                      bool
	DeltaDataExpansionFactor           float64
	JSONKeyStatsExpansionFactor        float64
	TextIndexExpansionFactor           float64
	TieredEvictionEnabled              bool
	PreferFieldDataWhenIndexHasRawData bool
	EnableInterimSegmentIndex          bool
	TempSegmentIndexFactor             float64
	ExternalRawDataFactor              float64
	GrowingMmapEnabled                 bool
}

func DefaultSegmentLoadingEstimateOptions() SegmentLoadingEstimateOptions {
	queryNodeCfg := &paramtable.Get().QueryNodeCfg
	return SegmentLoadingEstimateOptions{
		MmapVectorField:                    queryNodeCfg.MmapVectorField.GetAsBool(),
		MmapScalarField:                    queryNodeCfg.MmapScalarField.GetAsBool(),
		MmapJSONStats:                      queryNodeCfg.MmapJSONStats.GetAsBool(),
		DeltaDataExpansionFactor:           queryNodeCfg.DeltaDataExpansionRate.GetAsFloat(),
		JSONKeyStatsExpansionFactor:        queryNodeCfg.JSONKeyStatsExpansionFactor.GetAsFloat(),
		TextIndexExpansionFactor:           queryNodeCfg.TextIndexExpansionFactor.GetAsFloat(),
		TieredEvictionEnabled:              queryNodeCfg.TieredEvictionEnabled.GetAsBool(),
		PreferFieldDataWhenIndexHasRawData: queryNodeCfg.PreferFieldDataWhenIndexHasRawData.GetAsBool(),
		EnableInterimSegmentIndex:          queryNodeCfg.EnableInterminSegmentIndex.GetAsBool(),
		TempSegmentIndexFactor:             queryNodeCfg.InterimIndexMemExpandRate.GetAsFloat(),
		ExternalRawDataFactor:              queryNodeCfg.ExternalCollectionRawDataFactor.GetAsFloat(),
		GrowingMmapEnabled:                 queryNodeCfg.GrowingMmapEnabled.GetAsBool(),
	}
}

func estimateTantivyValidityBitmapBytes(numRows int64) uint64 {
	if numRows <= 0 {
		return 0
	}

	// TextIndexStats does not carry the null count, so reserve the full
	// word-aligned bitmap conservatively. All-valid indexes charge zero actual
	// bitmap bytes after loading.
	const (
		bitsPerWord  = uint64(64)
		bytesPerWord = uint64(8)
	)
	words := (uint64(numRows)-1)/bitsPerWord + 1
	return words * bytesPerWord
}

// ResolveSegmentEstimateLogs returns the raw and delta metadata consumed by
// the estimators. Internal Storage V3 descriptors are adapted to pathless
// FieldBinlogs; all other SegmentLoadInfo metadata remains unchanged.
func ResolveSegmentEstimateLogs(schema *schemapb.CollectionSchema, loadInfo *querypb.SegmentLoadInfo) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog) {
	binlogs := loadInfo.GetBinlogPaths()
	deltalogs := loadInfo.GetDeltalogs()
	if len(binlogs) > 0 {
		return binlogs, deltalogs
	}
	if loadInfo.GetStorageVersion() != storage.StorageV3 || loadInfo.GetManifestPath() == "" || typeutil.IsExternalCollection(schema) {
		return binlogs, deltalogs
	}

	stats := loadInfo.GetStats()
	resource := stats.GetLoadResource()
	if resource == nil {
		return binlogs, deltalogs
	}

	groups := resource.GetColumnGroups()
	binlogs = make([]*datapb.FieldBinlog, 0, len(groups))
	for _, group := range groups {
		binlogs = append(binlogs, &datapb.FieldBinlog{
			FieldID:     group.GetGroupId(),
			ChildFields: append([]int64(nil), group.GetFieldIds()...),
			Binlogs: []*datapb.Binlog{{
				EntriesNum: loadInfo.GetNumOfRows(),
				MemorySize: group.GetMemorySize(),
			}},
		})
	}

	deltalogs = nil
	if deltaSize := stats.GetDeltaBinlogSize(); deltaSize > 0 {
		deltalogs = []*datapb.FieldBinlog{{
			Binlogs: []*datapb.Binlog{{MemorySize: deltaSize}},
		}}
	}

	return binlogs, deltalogs
}

// EstimateBytesPerRow resolves external sampled row sizes before native metadata
// is captured. Keep the supplied value when no row-size sample is available.
func EstimateBytesPerRow(schema *schemapb.CollectionSchema, info *querypb.SegmentLoadInfo) int64 {
	if !typeutil.IsExternalCollection(schema) || info.GetNumOfRows() <= 0 || len(info.GetBinlogPaths()) == 0 {
		return info.GetEstimatedBytesPerRow()
	}
	var bytes int64
	for _, group := range info.GetBinlogPaths() {
		bytes += loadingFieldBinlogMemorySize(group)
	}
	return bytes / info.GetNumOfRows()
}

func EstimateSegmentFinalResource(ctx context.Context, schema *schemapb.CollectionSchema, loadInfo *querypb.SegmentLoadInfo, options SegmentFinalEstimateOptions, runner Runner) (SegmentResourceUsage, error) {
	options = options.normalized()
	core, err := estimateCoreResources(ctx, []*schemapb.CollectionSchema{schema}, []*querypb.SegmentLoadInfo{loadInfo}, options.MmapVectorField, options.MmapScalarField, options.PreferFieldDataWhenIndexHasRawData, true, runner)
	if err != nil {
		return SegmentResourceUsage{}, err
	}
	estimate := core.GetSegments()[0]
	inevictable := SegmentResourceUsage{MemoryBytes: estimate.GetTimestampMemoryBytes()}
	evictable := SegmentResourceUsage{MemoryBytes: estimate.GetFinalMemoryBytes(), DiskBytes: estimate.GetFinalDiskBytes()}
	_, deltalogs := ResolveSegmentEstimateLogs(schema, loadInfo)
	for _, fieldBinlog := range loadInfo.GetStatslogs() {
		inevictable.MemoryBytes += uint64(fieldBinlogMemorySize(fieldBinlog))
	}
	for _, fieldBinlog := range deltalogs {
		expansionFactor := float64(1)
		memSize := fieldBinlogMemorySize(fieldBinlog)
		if memSize == fieldBinlogDiskSize(fieldBinlog) {
			expansionFactor = options.DeltaDataExpansionFactor
		}
		inevictable.MemoryBytes += uint64(float64(memSize) * expansionFactor)
	}
	for _, stats := range loadInfo.GetJsonKeyStatsLogs() {
		size := uint64(math.Ceil(float64(stats.GetMemorySize()) * options.JSONKeyStatsExpansionFactor))
		if options.MmapJSONStats {
			evictable.DiskBytes += size
		} else {
			evictable.MemoryBytes += size
		}
	}
	validityBitmapBytes := estimateTantivyValidityBitmapBytes(loadInfo.GetNumOfRows())
	for _, stats := range loadInfo.GetTextStatsLogs() {
		indexFileBytes := uint64(math.Ceil(float64(stats.GetMemorySize()) * options.TextIndexExpansionFactor))
		evictable.MemoryBytes += validityBitmapBytes
		if options.MmapScalarField {
			evictable.DiskBytes += indexFileBytes
		} else {
			evictable.MemoryBytes += indexFileBytes
		}
	}
	return SegmentResourceUsage{
		MemoryBytes: inevictable.MemoryBytes + applyTieredRatio(evictable.MemoryBytes, options.TieredEvictionEnabled, options.TieredEvictableMemoryCacheRatio),
		DiskBytes:   inevictable.DiskBytes + applyTieredRatio(evictable.DiskBytes, options.TieredEvictionEnabled, options.TieredEvictableDiskCacheRatio),
	}, nil
}

// EstimateSegmentLoadingResource is the single-segment form of the batch estimate.
func EstimateSegmentLoadingResource(ctx context.Context, schema *schemapb.CollectionSchema, loadInfo *querypb.SegmentLoadInfo, options SegmentLoadingEstimateOptions, runner Runner) (SegmentResourceUsage, error) {
	total, _, err := EstimateSegmentsLoadingResource(ctx, []*schemapb.CollectionSchema{schema}, []*querypb.SegmentLoadInfo{loadInfo}, options, runner)
	return total, err
}

// EstimateSegmentsLoadingResource reserves shared field/index scratch once for
// the entire load request. Per-segment estimates remain independent upper bounds.
func EstimateSegmentsLoadingResource(ctx context.Context, schemas []*schemapb.CollectionSchema, loadInfos []*querypb.SegmentLoadInfo, options SegmentLoadingEstimateOptions, runner Runner) (SegmentResourceUsage, []SegmentResourceUsage, error) {
	core, err := estimateCoreResources(ctx, schemas, loadInfos, options.MmapVectorField, options.MmapScalarField, options.PreferFieldDataWhenIndexHasRawData, false, runner, options)
	if err != nil {
		return SegmentResourceUsage{}, nil, err
	}
	total := SegmentResourceUsage{}
	if !options.TieredEvictionEnabled {
		total.MemoryBytes = core.GetTotal().GetMaxMemoryBytes()
		total.DiskBytes = core.GetTotal().GetMaxDiskBytes()
	}
	perSegment := make([]SegmentResourceUsage, len(loadInfos))
	for i, loadInfo := range loadInfos {
		estimate := core.GetSegments()[i]
		extra := estimateSegmentLoadingExtras(schemas[i], loadInfo, options, estimate.GetTimestampMemoryBytes(), estimate.GetInterimIndexBytes())
		total.MemoryBytes += extra.MemoryBytes
		total.DiskBytes += extra.DiskBytes
		usage := extra
		if !options.TieredEvictionEnabled {
			usage.MemoryBytes += estimate.GetMaxMemoryBytes()
			usage.DiskBytes += estimate.GetMaxDiskBytes()
		}
		usage.MmapFieldCount = int(estimate.GetMmapFieldCount())
		usage.FieldGPUMemoryBytes = estimate.GetFieldGpuMemoryBytes()
		total.MmapFieldCount += usage.MmapFieldCount
		total.FieldGPUMemoryBytes = append(total.FieldGPUMemoryBytes, usage.FieldGPUMemoryBytes...)
		perSegment[i] = usage
	}
	return total, perSegment, nil
}

// Stats and policy margins are independent of segcore's
// field/index allocation and shared streaming reservations.
func estimateSegmentLoadingExtras(schema *schemapb.CollectionSchema, loadInfo *querypb.SegmentLoadInfo, options SegmentLoadingEstimateOptions, timestampBytes, interimIndexBytes uint64) SegmentResourceUsage {
	usage := SegmentResourceUsage{MemoryBytes: timestampBytes}
	if !options.TieredEvictionEnabled && options.EnableInterimSegmentIndex && !options.GrowingMmapEnabled {
		usage.MemoryBytes += uint64(float64(interimIndexBytes) * options.TempSegmentIndexFactor)
	}
	binlogs, deltalogs := ResolveSegmentEstimateLogs(schema, loadInfo)
	if typeutil.IsExternalCollection(schema) && loadInfo.GetNumOfRows() > 0 {
		var fakeBinlogMemSize int64
		for _, fb := range binlogs {
			fakeBinlogMemSize += loadingFieldBinlogMemorySize(fb)
		}
		if !externalRawDataLoadingCanBeSkipped(schema, loadInfo, options) && !options.TieredEvictionEnabled && options.ExternalRawDataFactor > 1 {
			usage.MemoryBytes += uint64(float64(fakeBinlogMemSize) * (options.ExternalRawDataFactor - 1))
		}
	}
	// Stats and deletes are not managed by the evictable caching layer.
	for _, fieldBinlog := range loadInfo.GetStatslogs() {
		usage.MemoryBytes += uint64(loadingFieldBinlogMemorySize(fieldBinlog))
	}
	for _, fieldBinlog := range deltalogs {
		expansionFactor := float64(2) // Go and C++ delete buffers coexist.
		memSize := loadingFieldBinlogMemorySize(fieldBinlog)
		if memSize == loadingFieldBinlogDiskSize(fieldBinlog) {
			expansionFactor = options.DeltaDataExpansionFactor
		}
		usage.MemoryBytes += uint64(float64(memSize) * expansionFactor)
	}
	rowCount := uint64(loadInfo.GetNumOfRows())
	for range len(schema.GetStructArrayFields()) {
		usage.MemoryBytes += 4*rowCount + 4*rowCount*10
	}
	if options.TieredEvictionEnabled {
		return usage
	}
	for _, stats := range loadInfo.GetJsonKeyStatsLogs() {
		size := uint64(float64(stats.GetMemorySize()) * options.JSONKeyStatsExpansionFactor)
		if options.MmapJSONStats {
			usage.DiskBytes += size
		} else {
			usage.MemoryBytes += size
		}
	}
	validityBitmapBytes := estimateTantivyValidityBitmapBytes(loadInfo.GetNumOfRows())
	for _, stats := range loadInfo.GetTextStatsLogs() {
		size := uint64(float64(stats.GetMemorySize()) * options.TextIndexExpansionFactor)
		usage.MemoryBytes += validityBitmapBytes
		if options.MmapScalarField {
			usage.DiskBytes += size
		} else {
			usage.MemoryBytes += size
		}
	}
	return usage
}

func (options SegmentFinalEstimateOptions) normalized() SegmentFinalEstimateOptions {
	if options.DeltaDataExpansionFactor <= 0 {
		options.DeltaDataExpansionFactor = 1
	}
	if options.JSONKeyStatsExpansionFactor <= 0 {
		options.JSONKeyStatsExpansionFactor = 1
	}
	if options.TextIndexExpansionFactor <= 0 {
		options.TextIndexExpansionFactor = 1
	}
	if options.TieredEvictableMemoryCacheRatio < 0 || options.TieredEvictableMemoryCacheRatio > 1 {
		options.TieredEvictableMemoryCacheRatio = 1
	}
	if options.TieredEvictableDiskCacheRatio < 0 || options.TieredEvictableDiskCacheRatio > 1 {
		options.TieredEvictableDiskCacheRatio = 1
	}
	return options
}

func supportInterimIndexDataTypeForLoading(dataType schemapb.DataType) bool {
	return dataType == schemapb.DataType_FloatVector ||
		dataType == schemapb.DataType_SparseFloatVector ||
		dataType == schemapb.DataType_Float16Vector ||
		dataType == schemapb.DataType_BFloat16Vector
}

// canSkipExternalRawDataLoading reports whether the loading estimate can omit
// source raw-data memory. It accounts for warmup and mandatory load-time reads.
// Keep this conservative check aligned with ChunkedSegmentSealedImpl's
// resolve_field_data_warmup_policy, CanUseLazyManifestField, Struct offset loading,
// and generate_interim_index. Unlike segcore, this estimate does not have the full
// runtime index state: it retains the budget when interim preparation is possible,
// even if segcore's additional index metadata checks ultimately skip that work.
func canSkipExternalRawDataLoading(schema *schemapb.CollectionSchema, indexedFields map[int64]struct{}, textStats map[int64]*datapb.TextIndexStats, options SegmentLoadingEstimateOptions) bool {
	// Struct offsets read a child column before segment publication, even with warmup disabled.
	if len(schema.GetStructArrayFields()) > 0 {
		return false
	}
	resolver := typeutil.NewStorageColumnResolver(schema)
	for _, field := range schema.GetFields() {
		if !resolver.IsSourceDataField(field) {
			continue
		}
		// Local text-index creation reads the source column regardless of warmup.
		if _, hasTextIndex := textStats[field.GetFieldID()]; !hasTextIndex && typeutil.IsTextMatchEnabled(field) {
			return false
		}
		// Geometry cache construction reads the source column regardless of warmup.
		if field.GetDataType() == schemapb.DataType_Geometry && paramtable.Get().QueryNodeCfg.EnableGeometryCache.GetAsBool() {
			return false
		}
		if resolver.IsMilvusTable() && field.GetIsPrimaryKey() {
			// Real-PK milvus-table segments always load the source PK column and
			// source insert timestamps eagerly so source deltas preserve
			// delete/reinsert ordering.
			return false
		}
		policy := getFieldWarmupPolicyForLoading(field)
		if _, hasIndex := indexedFields[field.GetFieldID()]; typeutil.IsVectorType(field.GetDataType()) && !hasIndex {
			// Interim preparation can read raw vectors even when warmup is disabled.
			if options.EnableInterimSegmentIndex && !options.GrowingMmapEnabled && supportInterimIndexDataTypeForLoading(field.GetDataType()) {
				return false
			}
			// Without a usable index, segcore warms raw vectors as the search index.
			var exist bool
			policy, exist = common.GetWarmupPolicyByKey(common.WarmupVectorIndexKey, schema.GetProperties()...)
			if !exist {
				policy = paramtable.Get().QueryNodeCfg.TieredWarmupVectorIndex.GetValue()
			}
		}
		if policy != common.WarmupDisable {
			return false
		}
	}
	return true
}

// Resolve usable indexes before deciding whether external raw reads are optional.
func externalRawDataLoadingCanBeSkipped(schema *schemapb.CollectionSchema, info *querypb.SegmentLoadInfo, options SegmentLoadingEstimateOptions) bool {
	indexedFields := make(map[int64]struct{}, len(info.GetIndexInfos()))
	for _, index := range info.GetIndexInfos() {
		if len(index.GetIndexFilePaths()) != 0 {
			indexedFields[index.GetFieldID()] = struct{}{}
		}
	}
	return canSkipExternalRawDataLoading(schema, indexedFields, info.GetTextStatsLogs(), options)
}

func getFieldWarmupPolicyForLoading(field *schemapb.FieldSchema) string {
	policy, exist := common.GetWarmupPolicy(field.GetTypeParams()...)
	if exist {
		return policy
	}
	if typeutil.IsVectorType(field.GetDataType()) {
		return paramtable.Get().QueryNodeCfg.TieredWarmupVectorField.GetValue()
	}
	return paramtable.Get().QueryNodeCfg.TieredWarmupScalarField.GetValue()
}

func fieldBinlogMemorySize(fieldBinlog *datapb.FieldBinlog) int64 {
	fieldSize := int64(0)
	for _, binlog := range fieldBinlog.GetBinlogs() {
		if binlog.GetMemorySize() > 0 {
			fieldSize += binlog.GetMemorySize()
			continue
		}
		fieldSize += binlog.GetLogSize()
	}
	return fieldSize
}

func loadingFieldBinlogMemorySize(fieldBinlog *datapb.FieldBinlog) int64 {
	fieldSize := int64(0)
	for _, binlog := range fieldBinlog.Binlogs {
		fieldSize += binlog.GetMemorySize()
	}

	return fieldSize
}

func fieldBinlogDiskSize(fieldBinlog *datapb.FieldBinlog) int64 {
	fieldSize := int64(0)
	for _, binlog := range fieldBinlog.GetBinlogs() {
		fieldSize += binlog.GetLogSize()
	}
	return fieldSize
}

func loadingFieldBinlogDiskSize(fieldBinlog *datapb.FieldBinlog) int64 {
	fieldSize := int64(0)
	for _, binlog := range fieldBinlog.Binlogs {
		fieldSize += binlog.GetLogSize()
	}

	return fieldSize
}

func applyTieredRatio(value uint64, enabled bool, ratio float64) uint64 {
	if !enabled {
		return value
	}
	return uint64(math.Ceil(float64(value) * ratio))
}

func gpuIndexRequiresGpu(indexParams []*commonpb.KeyValuePair) bool {
	indexParamMap := funcutil.KeyValuePair2Map(indexParams)
	indexType := indexParamMap[common.IndexTypeKey]

	switch indexType {
	case "GPU_CAGRA", "GPU_CUVS_CAGRA":
	case "GPU_BRUTE_FORCE", "GPU_CUVS_BRUTE_FORCE",
		"GPU_IVF_FLAT", "GPU_CUVS_IVF_FLAT",
		"GPU_IVF_PQ", "GPU_CUVS_IVF_PQ":
		return true
	default:
		return false
	}

	err := indexparams.AppendPrepareLoadParams(paramtable.Get(), indexParamMap)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to append prepare load params for gpu index resource check",
			mlog.String("indexType", indexType),
			mlog.Err(err))
	}

	adaptForCPU, ok := indexParamMap["adapt_for_cpu"]
	if ok {
		enabled, err := strconv.ParseBool(adaptForCPU)
		if err == nil && enabled {
			return false
		}
	}
	return true
}

// PreparedLoadingResource applies the existing Go policy and ancillary costs
// to a native plan estimate. GPU checks are supplied separately by the loader.
func PreparedLoadingResource(schema *schemapb.CollectionSchema, info *querypb.SegmentLoadInfo, memory, disk, timestamp, interim uint64, mmapCount int) SegmentResourceUsage {
	options := DefaultSegmentLoadingEstimateOptions()
	usage := estimateSegmentLoadingExtras(schema, info, options, timestamp, interim)
	if !options.TieredEvictionEnabled {
		usage.MemoryBytes += memory
		usage.DiskBytes += disk
	}
	usage.MmapFieldCount = mmapCount
	return usage

}
