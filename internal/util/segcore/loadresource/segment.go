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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
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

func EstimateSegmentFinalResource(ctx context.Context, schema *schemapb.CollectionSchema, loadInfo *querypb.SegmentLoadInfo, options SegmentFinalEstimateOptions, runner Runner) (SegmentResourceUsage, error) {
	if schema == nil {
		return SegmentResourceUsage{}, merr.WrapErrServiceInternalMsg("collection schema is nil")
	}
	if loadInfo == nil {
		return SegmentResourceUsage{}, merr.WrapErrServiceInternalMsg("segment load info is nil")
	}

	options = options.normalized()
	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		return SegmentResourceUsage{}, err
	}

	var inevictable SegmentResourceUsage
	var evictable SegmentResourceUsage
	id2Binlogs := make(map[int64]*datapb.FieldBinlog, len(loadInfo.GetBinlogPaths()))
	for _, fieldBinlog := range loadInfo.GetBinlogPaths() {
		id2Binlogs[fieldBinlog.GetFieldID()] = fieldBinlog
	}

	for _, fieldIndexInfo := range loadInfo.GetIndexInfos() {
		if len(fieldIndexInfo.GetIndexFilePaths()) == 0 {
			continue
		}

		fieldID := fieldIndexInfo.GetFieldID()
		fieldSchema, err := schemaHelper.GetFieldFromID(fieldID)
		if err != nil {
			// field might have been dropped, skip its index
			mlog.Info(ctx, "skip index for dropped field", mlog.Int64("fieldID", fieldID), mlog.String("name", schema.GetName()))
			continue
		}

		estimate, err := EstimateIndexLoadResourceWithRunner(ctx, fieldSchema, loadInfo, fieldIndexInfo, runner)
		if err != nil {
			return SegmentResourceUsage{}, errors.Wrapf(err, "failed to estimate final resource usage of index, collection %d, segment %d, indexBuildID %d",
				loadInfo.GetCollectionID(),
				loadInfo.GetSegmentID(),
				fieldIndexInfo.GetBuildID())
		}
		evictable.MemoryBytes += estimate.FinalMemoryBytes
		evictable.DiskBytes += estimate.FinalDiskBytes

		if estimate.HasRawData && !options.PreferFieldDataWhenIndexHasRawData {
			delete(id2Binlogs, fieldID)
			continue
		}

		if !typeutil.IsVectorType(fieldSchema.GetDataType()) {
			continue
		}

		metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.MetricTypeKey, fieldIndexInfo.GetIndexParams())
		if err != nil {
			return SegmentResourceUsage{}, errors.Wrapf(err, "failed to estimate final resource usage of index, metric type not found, collection %d, segment %d, indexBuildID %d",
				loadInfo.GetCollectionID(),
				loadInfo.GetSegmentID(),
				fieldIndexInfo.GetBuildID())
		}
		if metricType == metric.BM25 {
			delete(id2Binlogs, fieldID)
		}
	}

	for fieldID, fieldBinlog := range id2Binlogs {
		fieldIDs := fieldBinlog.GetChildFields()
		if len(fieldIDs) == 0 {
			fieldIDs = []int64{fieldID}
		}
		binlogSize := uint64(fieldBinlogMemorySize(fieldBinlog))

		var containsTimestampField bool
		var doubleMemoryDataField bool
		mmapEnabled := true
		isVectorType := true
		hasLiveField := false
		for _, fieldID := range fieldIDs {
			fieldSchema, err := schemaHelper.GetFieldFromID(fieldID)
			if err != nil {
				// field might have been dropped, skip it and continue processing
				// other fields in the same column group
				mlog.Info(ctx, "skip binlog for dropped field", mlog.Int64("fieldID", fieldID), mlog.String("name", schema.GetName()))
				continue
			}
			hasLiveField = true
			isVectorType = isVectorType && typeutil.IsVectorType(fieldSchema.GetDataType())
			mmapEnabled = mmapEnabled && isDataMmapEnabled(fieldSchema, options)
			containsTimestampField = containsTimestampField || fieldSchema.GetFieldID() == common.TimeStampField
			doubleMemoryDataField = doubleMemoryDataField || doubleMemoryDataType(fieldSchema.GetDataType())
		}
		if !hasLiveField {
			continue
		}

		if containsTimestampField {
			timestampSize := int64(0)
			for _, binlog := range fieldBinlog.GetBinlogs() {
				timestampSize += binlog.GetEntriesNum() * 4
			}
			inevictable.MemoryBytes += uint64(2 * timestampSize)
		}

		if isVectorType {
			if options.MmapVectorField {
				evictable.DiskBytes += binlogSize
			} else {
				evictable.MemoryBytes += binlogSize
			}
			continue
		}

		if !mmapEnabled {
			evictable.MemoryBytes += binlogSize
			if doubleMemoryDataField {
				evictable.MemoryBytes += binlogSize
			}
		} else {
			evictable.DiskBytes += binlogSize
		}
	}

	for _, fieldBinlog := range loadInfo.GetStatslogs() {
		inevictable.MemoryBytes += uint64(fieldBinlogMemorySize(fieldBinlog))
	}
	for _, fieldBinlog := range loadInfo.GetDeltalogs() {
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
	for _, stats := range loadInfo.GetTextStatsLogs() {
		size := uint64(math.Ceil(float64(stats.GetMemorySize()) * options.TextIndexExpansionFactor))
		if options.MmapScalarField {
			evictable.DiskBytes += size
		} else {
			evictable.MemoryBytes += size
		}
	}

	return SegmentResourceUsage{
		MemoryBytes: inevictable.MemoryBytes + applyTieredRatio(evictable.MemoryBytes, options.TieredEvictionEnabled, options.TieredEvictableMemoryCacheRatio),
		DiskBytes:   inevictable.DiskBytes + applyTieredRatio(evictable.DiskBytes, options.TieredEvictionEnabled, options.TieredEvictableDiskCacheRatio),
	}, nil
}

// EstimateSegmentLoadingResource estimates the resource usage of the segment when loading.
// It returns different results depending on whether tiered eviction is enabled:
//   - when tiered eviction is enabled, the result is the max resource usage of the segment
//     that cannot be managed by caching layer, which should be a subset of the segment inevictable part
//   - when tiered eviction is disabled, the result is the max resource usage of both
//     the segment evictable and inevictable part
func EstimateSegmentLoadingResource(ctx context.Context, schema *schemapb.CollectionSchema, loadInfo *querypb.SegmentLoadInfo, options SegmentLoadingEstimateOptions, runner Runner) (SegmentResourceUsage, error) {
	var segMemoryLoadingSize, segDiskLoadingSize uint64
	var indexMemorySize uint64
	var fieldGPUMemorySize []uint64

	id2Binlogs := lo.SliceToMap(loadInfo.BinlogPaths, func(fieldBinlog *datapb.FieldBinlog) (int64, *datapb.FieldBinlog) {
		return fieldBinlog.GetFieldID(), fieldBinlog
	})

	schemaHelper, err := typeutil.CreateSchemaHelper(schema)
	if err != nil {
		mlog.Warn(ctx, "failed to create schema helper", mlog.String("name", schema.GetName()), mlog.Err(err))
		return SegmentResourceUsage{}, err
	}
	indexedFields := make(map[int64]struct{})

	// PART 1: calculate size of indexes
	for _, fieldIndexInfo := range loadInfo.IndexInfos {
		fieldID := fieldIndexInfo.GetFieldID()
		if len(fieldIndexInfo.GetIndexFilePaths()) > 0 {
			fieldSchema, err := schemaHelper.GetFieldFromID(fieldID)
			if err != nil {
				// field might have been dropped, skip its index
				mlog.Info(ctx, "skip index for dropped field", mlog.Int64("fieldID", fieldID), mlog.String("name", schema.GetName()))
				continue
			}
			indexedFields[fieldID] = struct{}{}

			isVectorType := typeutil.IsVectorType(fieldSchema.GetDataType())

			estimateResult, err := EstimateIndexLoadResourceWithRunner(ctx, fieldSchema, loadInfo, fieldIndexInfo, runner)
			if err != nil {
				return SegmentResourceUsage{}, errors.Wrapf(err, "failed to estimate loading resource usage of index, collection %d, segment %d, indexBuildID %d",
					loadInfo.GetCollectionID(),
					loadInfo.GetSegmentID(),
					fieldIndexInfo.GetBuildID())
			}

			if !options.TieredEvictionEnabled {
				indexMemorySize += estimateResult.MaxMemoryBytes
				segDiskLoadingSize += estimateResult.MaxDiskBytes
			}

			if gpuIndexRequiresGpu(fieldIndexInfo.IndexParams) {
				fieldGPUMemorySize = append(fieldGPUMemorySize, estimateResult.MaxMemoryBytes)
			}

			// could skip binlog or
			// could be missing for new field or storage v2 group 0
			if estimateResult.HasRawData &&
				!options.PreferFieldDataWhenIndexHasRawData {
				delete(id2Binlogs, fieldID)
				continue
			}

			// BM25 only checks vector datatype
			// scalar index does not have metrics type key
			if !isVectorType {
				continue
			}

			metricType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.MetricTypeKey, fieldIndexInfo.IndexParams)
			if err != nil {
				return SegmentResourceUsage{}, errors.Wrapf(err, "failed to estimate loading resource usage of index, metric type not found, collection %d, segment %d, indexBuildID %d",
					loadInfo.GetCollectionID(),
					loadInfo.GetSegmentID(),
					fieldIndexInfo.GetBuildID())
			}
			// skip raw data for BM25 index
			if metricType == metric.BM25 {
				delete(id2Binlogs, fieldID)
			}
		}
	}

	// PART 2: calculate size of binlogs
	for fieldID, fieldBinlog := range id2Binlogs {
		fieldIDs := fieldBinlog.GetChildFields()
		// legacy default split
		if len(fieldIDs) == 0 {
			fieldIDs = []int64{fieldID}
		}
		binlogSize := uint64(loadingFieldBinlogMemorySize(fieldBinlog))

		var supportInterimIndexDataType bool
		var containsTimestampField bool
		var doubleMomoryDataField bool
		var legacyNilSchema bool
		mmapEnabled := true
		isVectorType := true
		hasIndex := true

		for _, fieldID := range fieldIDs {
			// get field schema from fieldID
			fieldSchema, err := schemaHelper.GetFieldFromID(fieldID)
			if err != nil {
				// field might have been dropped, skip it and continue processing
				// other fields in the same column group
				mlog.Info(ctx, "skip binlog for dropped field", mlog.Int64("fieldID", fieldID), mlog.String("name", schema.GetName()))
				continue
			}
			if _, ok := indexedFields[fieldID]; !ok {
				hasIndex = false
			}

			// missing mapping, shall be "0" group for storage v2
			if fieldSchema == nil {
				if !options.TieredEvictionEnabled {
					segMemoryLoadingSize += binlogSize
				}
				legacyNilSchema = true
				break
			}

			supportInterimIndexDataType = supportInterimIndexDataType || supportInterimIndexDataTypeForLoading(fieldSchema.GetDataType())
			isVectorType = isVectorType && typeutil.IsVectorType(fieldSchema.GetDataType())
			mmapEnabled = mmapEnabled && isLoadingDataMmapEnabled(fieldSchema, options)
			containsTimestampField = containsTimestampField || fieldSchema.GetFieldID() == common.TimeStampField
			doubleMomoryDataField = doubleMomoryDataField || doubleMemoryDataType(fieldSchema.GetDataType())
		}
		// legacy v2 segment without children
		if legacyNilSchema {
			continue
		}

		if !hasIndex {
			if !options.TieredEvictionEnabled {
				interimIndexEnable := options.EnableInterimSegmentIndex && !options.GrowingMmapEnabled && supportInterimIndexDataType
				if interimIndexEnable {
					segMemoryLoadingSize += uint64(float64(binlogSize) * options.TempSegmentIndexFactor)
				}
			}
		}

		if isVectorType {
			if options.MmapVectorField {
				if !options.TieredEvictionEnabled {
					segDiskLoadingSize += binlogSize
				}
			} else {
				if !options.TieredEvictionEnabled {
					segMemoryLoadingSize += binlogSize
				}
			}
			continue
		}

		// timestamp field double in InsertRecord & TimestampIndex
		if containsTimestampField {
			timestampSize := lo.SumBy(fieldBinlog.GetBinlogs(), func(binlog *datapb.Binlog) int64 {
				return binlog.GetEntriesNum() * 4
			})
			segMemoryLoadingSize += 2 * uint64(timestampSize)
		}

		if !mmapEnabled {
			if !options.TieredEvictionEnabled {
				segMemoryLoadingSize += binlogSize
				if doubleMomoryDataField {
					segMemoryLoadingSize += binlogSize
				}
			}
		} else {
			if !options.TieredEvictionEnabled {
				segDiskLoadingSize += uint64(loadingFieldBinlogMemorySize(fieldBinlog))
			}
		}
	}

	// PART 2.5: external segment adjustments
	//
	// External segments carry pre-computed MemorySize in fake binlogs (from
	// DataNode Take sampling). Adjust the memory estimate for two external-
	// specific behaviors:
	//   1. Non-lazy path: apply externalRawDataFactor to cover the peak
	//      transient memory during download + decompress + Arrow deserialize
	//      (normal packed segments do not have this peak because their
	//      binlogs are already in Arrow IPC format).
	//   2. Full-lazy path (all external fields warmup=disable): no eager
	//      load, so subtract the raw data size that PART 2 added.
	// Also propagate EstimatedBytesPerRow to the C++ ManifestGroupTranslator
	// so the tiered-cache layer sizes chunks correctly.
	if typeutil.IsExternalCollection(schema) && loadInfo.GetNumOfRows() > 0 {
		var fakeBinlogMemSize int64
		for _, fb := range loadInfo.BinlogPaths {
			fakeBinlogMemSize += loadingFieldBinlogMemorySize(fb)
		}
		loadInfo.EstimatedBytesPerRow = fakeBinlogMemSize / loadInfo.GetNumOfRows()

		if isExternalCollectionLazyLoadForLoading(schema) {
			// Full-lazy -> zero eager load. Undo PART 2's rawSize addition.
			// Safety factor does not apply: no peak to cover.
			if segMemoryLoadingSize >= uint64(fakeBinlogMemSize) {
				segMemoryLoadingSize -= uint64(fakeBinlogMemSize)
			} else {
				segMemoryLoadingSize = 0
			}
		} else if factor := options.ExternalRawDataFactor; factor > 1.0 {
			// Non-lazy -> add peak margin on top of rawSize that PART 2 added.
			segMemoryLoadingSize += uint64(float64(fakeBinlogMemSize) * (factor - 1.0))
		}
	}

	// PART 3: calculate size of stats data
	// stats data isn't managed by the caching layer, so its size should always be included,
	// regardless of the tiered eviction value
	for _, fieldBinlog := range loadInfo.GetStatslogs() {
		segMemoryLoadingSize += uint64(loadingFieldBinlogMemorySize(fieldBinlog))
	}

	// PART 4: calculate size of delete data
	// delete data isn't managed by the caching layer, so its size should always be included,
	// regardless of the tiered eviction value
	for _, fieldBinlog := range loadInfo.GetDeltalogs() {
		// MemorySize of filedBinlog is the actual size in memory, but we should also consider
		// the memcpy from golang to cpp side, so the expansionFactor is set to 2.
		expansionFactor := float64(2)
		memSize := loadingFieldBinlogMemorySize(fieldBinlog)

		// Note: If MemorySize == DiskSize, it means the segment comes from Milvus 2.3,
		//   MemorySize is actually compressed DiskSize of deltalog, so we'll fallback to use
		//   deltaExpansionFactor to compromise the compression ratio.
		if memSize == loadingFieldBinlogDiskSize(fieldBinlog) {
			expansionFactor = options.DeltaDataExpansionFactor
		}
		segMemoryLoadingSize += uint64(float64(memSize) * expansionFactor)
	}

	// PART 5: calculate size of json key stats data
	for _, jsonKeyStats := range loadInfo.GetJsonKeyStatsLogs() {
		if options.MmapJSONStats {
			if !options.TieredEvictionEnabled {
				segDiskLoadingSize += uint64(float64(jsonKeyStats.GetMemorySize()) * options.JSONKeyStatsExpansionFactor)
			}
		} else {
			if !options.TieredEvictionEnabled {
				segMemoryLoadingSize += uint64(float64(jsonKeyStats.GetMemorySize()) * options.JSONKeyStatsExpansionFactor)
			}
		}
	}

	// per struct memory size, used to keep mapping between row id and element id
	var structArrayOffsetsSize uint64
	// PART 6: calculate size of struct array offsets
	// The memory size is 4 * row_count + 4 * total_element_count
	// We cannot easily get the element count, so we estimate it by the row count * 10
	rowCount := uint64(loadInfo.GetNumOfRows())
	for range len(schema.GetStructArrayFields()) {
		structArrayOffsetsSize += 4*rowCount + 4*rowCount*10
	}

	// PART 7: calculate size of text index stats data
	// text index data is managed by the caching layer when tiered eviction is enabled,
	// so it only needs to be included when tiered eviction is disabled.
	// Text match index mmap is driven by scalar_field_enable_mmap (same as raw scalar data).
	// memory_size = sum of Tantivy index file sizes (same value as C++ ByteSize() after load),
	// so 1.0x is the baseline; textIndexExpansionFactor allows tuning if needed.
	for _, textStats := range loadInfo.GetTextStatsLogs() {
		if options.MmapScalarField {
			if !options.TieredEvictionEnabled {
				segDiskLoadingSize += uint64(float64(textStats.GetMemorySize()) * options.TextIndexExpansionFactor)
			}
		} else {
			if !options.TieredEvictionEnabled {
				segMemoryLoadingSize += uint64(float64(textStats.GetMemorySize()) * options.TextIndexExpansionFactor)
			}
		}
	}

	return SegmentResourceUsage{
		MemoryBytes:         segMemoryLoadingSize + indexMemorySize + structArrayOffsetsSize,
		DiskBytes:           segDiskLoadingSize,
		FieldGPUMemoryBytes: fieldGPUMemorySize,
	}, nil
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

func isDataMmapEnabled(field *schemapb.FieldSchema, options SegmentFinalEstimateOptions) bool {
	if enabled, ok := common.IsMmapDataEnabled(field.GetTypeParams()...); ok {
		return enabled
	}
	if typeutil.IsVectorType(field.GetDataType()) {
		return options.MmapVectorField
	}
	return options.MmapScalarField
}

func isLoadingDataMmapEnabled(field *schemapb.FieldSchema, options SegmentLoadingEstimateOptions) bool {
	enableMmap, exist := common.IsMmapDataEnabled(field.GetTypeParams()...)
	if exist {
		return enableMmap
	}
	if typeutil.IsVectorType(field.GetDataType()) {
		return options.MmapVectorField
	}
	return options.MmapScalarField
}

func doubleMemoryDataType(dataType schemapb.DataType) bool {
	return dataType == schemapb.DataType_String ||
		dataType == schemapb.DataType_VarChar ||
		dataType == schemapb.DataType_JSON
}

func supportInterimIndexDataTypeForLoading(dataType schemapb.DataType) bool {
	return dataType == schemapb.DataType_FloatVector ||
		dataType == schemapb.DataType_SparseFloatVector ||
		dataType == schemapb.DataType_Float16Vector ||
		dataType == schemapb.DataType_BFloat16Vector
}

func isExternalCollectionLazyLoadForLoading(schema *schemapb.CollectionSchema) bool {
	resolver := typeutil.NewStorageColumnResolver(schema)
	for _, field := range schema.GetFields() {
		if !resolver.IsSourceDataField(field) {
			continue
		}
		if resolver.IsMilvusTable() && field.GetIsPrimaryKey() {
			// Real-PK milvus-table segments always load the source PK column and
			// source insert timestamps eagerly so source deltas preserve
			// delete/reinsert ordering.
			return false
		}
		policy := getFieldWarmupPolicyForLoading(field)
		if policy != common.WarmupDisable {
			return false
		}
	}
	return true
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
		if binlog.GetLogSize() > 0 {
			fieldSize += binlog.GetLogSize()
			continue
		}
		fieldSize += binlog.GetMemorySize()
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
