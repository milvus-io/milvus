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

package compactor

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/datanode/util"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/indexcgowrapper"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const compactionBatchSize = 100

func createTextIndex(ctx context.Context,
	cm storage.ChunkManager,
	plan *datapb.CompactionPlan,
	compactionParams compaction.Params,
	storageVersion int64,
	collectionID int64,
	partitionID int64,
	segmentID int64,
	taskID int64,
	segment *datapb.CompactionSegment,
) (map[int64]*datapb.TextIndexStats, error) {
	log := mlog.With(
		mlog.FieldCollectionID(collectionID),
		mlog.FieldPartitionID(partitionID),
		mlog.FieldSegmentID(segmentID),
	)

	fieldBinlogs := lo.GroupBy(segment.GetInsertLogs(), func(binlog *datapb.FieldBinlog) int64 {
		return binlog.GetFieldID()
	})

	getInsertFiles := func(fieldID int64) ([]string, error) {
		if storageVersion == storage.StorageV2 || storageVersion == storage.StorageV3 {
			return []string{}, nil
		}
		binlogs, ok := fieldBinlogs[fieldID]
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("field binlog not found for field %d", fieldID)
		}
		result := make([]string, 0, len(binlogs))
		for _, binlog := range binlogs {
			for _, file := range binlog.GetBinlogs() {
				result = append(result, metautil.BuildInsertLogPath(compactionParams.StorageConfig.GetRootPath(),
					collectionID, partitionID, segmentID, fieldID, file.GetLogID()))
			}
		}
		return result, nil
	}

	newStorageConfig, err := util.ParseStorageConfig(compactionParams.StorageConfig)
	if err != nil {
		return nil, err
	}
	pluginContext, err := hookutil.GetRequiredCPluginContext(plan.GetPluginContext(), collectionID)
	if err != nil {
		return nil, err
	}

	var (
		mu            sync.Mutex
		textIndexLogs = make(map[int64]*datapb.TextIndexStats)
	)

	eg, egCtx := errgroup.WithContext(ctx)

	var analyzerExtraInfo string
	if len(plan.GetFileResources()) > 0 {
		err := fileresource.GlobalFileManager.Download(ctx, cm, plan.GetFileResources()...)
		if err != nil {
			return nil, err
		}
		defer fileresource.GlobalFileManager.Release(plan.GetFileResources()...)
		analyzerExtraInfo, err = analyzer.BuildExtraResourceInfo(compactionParams.StorageConfig.GetRootPath(), plan.GetFileResources())
		if err != nil {
			return nil, err
		}
	}

	for _, field := range plan.GetSchema().GetFields() {
		field := field
		h := typeutil.CreateFieldSchemaHelper(field)
		if !h.EnableMatch() {
			continue
		}
		log.Info(ctx, "field enable match, ready to create text index", mlog.Int64("field id", field.GetFieldID()))

		eg.Go(func() error {
			files, err := getInsertFiles(field.GetFieldID())
			if err != nil {
				return err
			}

			statsBasePath := metautil.BuildTextIndexPrefix(compactionParams.StorageConfig.GetRootPath(),
				plan.GetPlanID(), 0, collectionID, partitionID, segmentID, field.GetFieldID())
			if segment.GetManifest() != "" {
				basePath, _, err := packed.UnmarshalManifestPath(segment.GetManifest())
				if err != nil {
					return merr.Wrap(err, "failed to unmarshal manifest path for text_index basePath")
				}
				statsBasePath = fmt.Sprintf("%s/_stats/text_index.%d", basePath, field.GetFieldID())
			}

			buildIndexParams := &indexcgopb.BuildIndexInfo{
				BuildID:                   taskID,
				CollectionID:              collectionID,
				PartitionID:               partitionID,
				SegmentID:                 segmentID,
				IndexVersion:              0,
				InsertFiles:               files,
				FieldSchema:               field,
				StorageConfig:             newStorageConfig,
				CurrentScalarIndexVersion: common.ClampScalarIndexVersion(plan.GetCurrentScalarIndexVersion()),
				StorageVersion:            storageVersion,
				Manifest:                  segment.GetManifest(),
				StatsBasePath:             statsBasePath,
				StoragePluginContext:      pluginContext,
				IndexParams: []*commonpb.KeyValuePair{
					{Key: "index_type", Value: "INVERTED"},
					{Key: "is_text_match", Value: "true"},
				},
			}

			if len(analyzerExtraInfo) > 0 {
				buildIndexParams.AnalyzerExtraInfo = analyzerExtraInfo
			}

			if storageVersion == storage.StorageV2 || storageVersion == storage.StorageV3 {
				buildIndexParams.SegmentInsertFiles = util.GetSegmentInsertFiles(
					segment.GetInsertLogs(),
					compactionParams.StorageConfig,
					collectionID,
					partitionID,
					segmentID)
			}

			index, err := indexcgowrapper.CreateIndex(egCtx, buildIndexParams)
			if err != nil {
				return err
			}
			defer index.Delete()

			indexStats, err := index.UpLoad()
			if err != nil {
				return err
			}

			uploaded := make(map[string]int64)
			for _, info := range indexStats.GetSerializedIndexInfos() {
				uploaded[info.FileName] = info.FileSize
			}

			statsFiles := metautil.BuildStatsFilePaths(statsBasePath, lo.Keys(uploaded))

			mu.Lock()
			totalSize := lo.SumBy(lo.Values(uploaded), func(fileSize int64) int64 { return fileSize })
			textIndexLogs[field.GetFieldID()] = &datapb.TextIndexStats{
				FieldID:                   field.GetFieldID(),
				Version:                   0,
				BuildID:                   taskID,
				Files:                     statsFiles,
				LogSize:                   totalSize,
				MemorySize:                totalSize,
				CurrentScalarIndexVersion: common.ClampScalarIndexVersion(plan.GetCurrentScalarIndexVersion()),
			}
			mu.Unlock()

			log.Info(ctx, "field enable match, create text index done",
				mlog.FieldSegmentID(segmentID),
				mlog.Int64("field id", field.GetFieldID()),
				mlog.Strings("files", statsFiles),
			)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return textIndexLogs, nil
}

// Storage readers do not share compaction's schema-reconciliation contract, so filter physical fields before opening them.
func newCompactionSegmentRecordReader(ctx context.Context, segment *datapb.CompactionSegmentBinlogs, schema *schemapb.CollectionSchema, storageConfig *indexpb.StorageConfig, opts ...storage.RwOption) (storage.RecordReader, map[int64]struct{}, error) {
	existingFields, err := compactionSegmentStorageFields(segment, storageConfig)
	if err != nil {
		return nil, nil, err
	}
	return newCompactionSegmentRecordReaderWithFields(ctx, segment, schema, storageConfig, existingFields, opts...)
}

func compactionSegmentStorageFields(segment *datapb.CompactionSegmentBinlogs, storageConfig *indexpb.StorageConfig) (map[int64]struct{}, error) {
	if segment.GetManifest() != "" {
		return packed.GetManifestFieldIDs(segment.GetManifest(), storageConfig)
	}
	return compactionSegmentBinlogFields(segment), nil
}

func compactionSegmentBinlogFields(segment *datapb.CompactionSegmentBinlogs) map[int64]struct{} {
	fields := make(map[int64]struct{})
	for _, fieldBinlog := range segment.GetFieldBinlogs() {
		if len(fieldBinlog.GetChildFields()) == 0 {
			fields[fieldBinlog.GetFieldID()] = struct{}{}
			continue
		}
		for _, childFieldID := range fieldBinlog.GetChildFields() {
			fields[childFieldID] = struct{}{}
		}
	}
	return fields
}

func collectionSchemaFields(schema *schemapb.CollectionSchema) map[int64]struct{} {
	fields := make(map[int64]struct{}, typeutil.GetTotalFieldsNum(schema))
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		fields[field.GetFieldID()] = struct{}{}
	}
	return fields
}

func missingSchemaFunctions(schema *schemapb.CollectionSchema, existingFields map[int64]struct{}) []*schemapb.FunctionSchema {
	var missing []*schemapb.FunctionSchema
	for _, functionSchema := range schema.GetFunctions() {
		for _, outputFieldID := range functionSchema.GetOutputFieldIds() {
			if _, ok := existingFields[outputFieldID]; !ok {
				missing = append(missing, functionSchema)
				break
			}
		}
	}
	return missing
}

func droppedSchemaFieldIDs(schema *schemapb.CollectionSchema, existingFields map[int64]struct{}) []int64 {
	targetFields := collectionSchemaFields(schema)
	dropped := make([]int64, 0)
	for fieldID := range existingFields {
		if fieldID < common.StartOfUserFieldID {
			continue
		}
		if _, ok := targetFields[fieldID]; !ok {
			dropped = append(dropped, fieldID)
		}
	}
	sort.Slice(dropped, func(i, j int) bool { return dropped[i] < dropped[j] })
	return dropped
}

func segmentDroppedFieldIDs(schema *schemapb.CollectionSchema, segment *datapb.CompactionSegmentBinlogs, storageConfig *indexpb.StorageConfig) ([]int64, error) {
	existingFields, err := compactionSegmentStorageFields(segment, storageConfig)
	if err != nil {
		return nil, err
	}
	return droppedSchemaFieldIDs(schema, existingFields), nil
}

func newCompactionSegmentRecordReaderWithFields(ctx context.Context, segment *datapb.CompactionSegmentBinlogs, schema *schemapb.CollectionSchema, storageConfig *indexpb.StorageConfig, existingFields map[int64]struct{}, opts ...storage.RwOption) (storage.RecordReader, map[int64]struct{}, error) {
	readSchema := compactionReadSchema(schema, existingFields)

	if segment.GetManifest() != "" {
		reader, err := storage.NewManifestRecordReader(ctx, segment.GetManifest(), readSchema, opts...)
		return reader, existingFields, err
	}

	readFields := collectionSchemaFields(readSchema)
	fieldBinlogs := filterCompactionFieldBinlogs(segment.GetFieldBinlogs(), readFields)
	rootPath := ""
	if storageConfig != nil {
		rootPath = storageConfig.GetRootPath()
	}
	if err := binlog.DecompressBinLogWithRootPath(rootPath, storage.InsertBinlog,
		segment.GetCollectionID(), segment.GetPartitionID(), segment.GetSegmentID(), fieldBinlogs); err != nil {
		return nil, nil, err
	}

	reader, err := storage.NewBinlogRecordReader(ctx, fieldBinlogs, readSchema, opts...)
	return reader, existingFields, err
}

func compactionReadSchema(schema *schemapb.CollectionSchema, existingFields map[int64]struct{}) *schemapb.CollectionSchema {
	if schema == nil {
		return nil
	}
	readSchema := proto.Clone(schema).(*schemapb.CollectionSchema)

	fields := make([]*schemapb.FieldSchema, 0, len(readSchema.GetFields()))
	for _, field := range readSchema.GetFields() {
		if compactionFieldReadable(field, existingFields) {
			fields = append(fields, field)
		}
	}
	readSchema.Fields = fields

	structFields := make([]*schemapb.StructArrayFieldSchema, 0, len(readSchema.GetStructArrayFields()))
	for _, structField := range readSchema.GetStructArrayFields() {
		childFields := make([]*schemapb.FieldSchema, 0, len(structField.GetFields()))
		for _, field := range structField.GetFields() {
			if compactionFieldReadable(field, existingFields) {
				childFields = append(childFields, field)
			}
		}
		if len(childFields) > 0 {
			structField.Fields = childFields
			structFields = append(structFields, structField)
		}
	}
	readSchema.StructArrayFields = structFields
	return readSchema
}

func compactionFieldReadable(field *schemapb.FieldSchema, existingFields map[int64]struct{}) bool {
	_, ok := existingFields[field.GetFieldID()]
	return ok
}

func filterCompactionFieldBinlogs(fieldBinlogs []*datapb.FieldBinlog, readFields map[int64]struct{}) []*datapb.FieldBinlog {
	filtered := make([]*datapb.FieldBinlog, 0, len(fieldBinlogs))
	for _, fieldBinlog := range fieldBinlogs {
		if compactionFieldBinlogReadable(fieldBinlog, readFields) {
			filtered = append(filtered, fieldBinlog)
		}
	}
	return filtered
}

func compactionFieldBinlogReadable(fieldBinlog *datapb.FieldBinlog, readFields map[int64]struct{}) bool {
	if fieldBinlog == nil {
		return false
	}
	if _, ok := readFields[fieldBinlog.GetFieldID()]; ok {
		return true
	}
	for _, childFieldID := range fieldBinlog.GetChildFields() {
		if _, ok := readFields[childFieldID]; ok {
			return true
		}
	}
	return false
}

type EntityFilter struct {
	deletedPkTs map[interface{}]typeutil.Timestamp // pk2ts
	ttl         int64                              // nanoseconds
	currentTime time.Time

	expiredCount int
	deletedCount int
}

func newEntityFilter(deletedPkTs map[interface{}]typeutil.Timestamp, ttl int64, currTime time.Time) *EntityFilter {
	if deletedPkTs == nil {
		deletedPkTs = make(map[interface{}]typeutil.Timestamp)
	}
	return &EntityFilter{
		deletedPkTs: deletedPkTs,
		ttl:         ttl,
		currentTime: currTime,
	}
}

func (filter *EntityFilter) Filtered(pk any, ts typeutil.Timestamp) bool {
	if filter.isEntityDeleted(pk, ts) {
		filter.deletedCount++
		return true
	}

	// Filtering expired entity
	if filter.isEntityExpired(ts) {
		filter.expiredCount++
		return true
	}
	return false
}

func (filter *EntityFilter) GetExpiredCount() int {
	return filter.expiredCount
}

func (filter *EntityFilter) GetDeletedCount() int {
	return filter.deletedCount
}

func (filter *EntityFilter) GetDeltalogDeleteCount() int {
	return len(filter.deletedPkTs)
}

func (filter *EntityFilter) GetMissingDeleteCount() int {
	diff := filter.GetDeltalogDeleteCount() - filter.GetDeletedCount()
	if diff <= 0 {
		diff = 0
	}
	return diff
}

func (filter *EntityFilter) isEntityDeleted(pk interface{}, pkTs typeutil.Timestamp) bool {
	if deleteTs, ok := filter.deletedPkTs[pk]; ok {
		// insert task and delete task has the same ts when upsert
		// here should be < instead of <=
		// to avoid the upsert data to be deleted after compact
		if pkTs < deleteTs {
			return true
		}
	}
	return false
}

func (filter *EntityFilter) isEntityExpired(entityTs typeutil.Timestamp) bool {
	// entity expire is not enabled if duration <= 0
	if filter.ttl <= 0 {
		return false
	}
	entityTime, _ := tsoutil.ParseTS(entityTs)

	// this dur can represents 292 million years before or after 1970, enough for milvus
	// ttl calculation
	dur := filter.currentTime.UnixMilli() - entityTime.UnixMilli()

	// filter.ttl is nanoseconds
	return filter.ttl/int64(time.Millisecond) <= dur
}

// TODO: remove, used in test only
func serializeWrite(ctx context.Context, allocator allocator.Interface, writer *SegmentWriter) (kvs map[string][]byte, fieldBinlogs map[int64]*datapb.FieldBinlog, err error) {
	_, span := otel.Tracer(typeutil.DataNodeRole).Start(ctx, "serializeWrite")
	defer span.End()

	blobs, tr, err := writer.SerializeYield()
	startID, _, err := allocator.Alloc(uint32(len(blobs)))
	if err != nil {
		return nil, nil, err
	}

	kvs = make(map[string][]byte)
	fieldBinlogs = make(map[int64]*datapb.FieldBinlog)
	for i := range blobs {
		// Blob Key is generated by Serialize from int64 fieldID in collection schema, which won't raise error in ParseInt
		fID, _ := strconv.ParseInt(blobs[i].GetKey(), 10, 64)
		key, _ := binlog.BuildLogPath(storage.InsertBinlog, writer.GetCollectionID(), writer.GetPartitionID(), writer.GetSegmentID(), fID, startID+int64(i))

		kvs[key] = blobs[i].GetValue()
		fieldBinlogs[fID] = &datapb.FieldBinlog{
			FieldID: fID,
			Binlogs: []*datapb.Binlog{
				{
					LogSize:       int64(len(blobs[i].GetValue())),
					MemorySize:    blobs[i].GetMemorySize(),
					LogPath:       key,
					EntriesNum:    blobs[i].RowNum,
					TimestampFrom: tr.GetMinTimestamp(),
					TimestampTo:   tr.GetMaxTimestamp(),
				},
			},
		}
	}

	return
}

func getTTLFieldID(schema *schemapb.CollectionSchema) int64 {
	ttlFieldName := ""
	for _, pair := range schema.GetProperties() {
		if pair.GetKey() == common.CollectionTTLFieldKey {
			ttlFieldName = pair.GetValue()
			break
		}
	}
	if ttlFieldName == "" {
		return -1
	}
	for _, field := range schema.GetFields() {
		if field.GetName() == ttlFieldName && field.GetDataType() == schemapb.DataType_Timestamptz {
			return field.GetFieldID()
		}
	}
	return -1
}
