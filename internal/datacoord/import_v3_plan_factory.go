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

package datacoord

// This file owns the derivation of Import V3 execution plans. The live
// configuration those derivations read is loaded in exactly one place,
// loadImportV3Config, so Planning and dispatch derive the writer layout, the
// fan-in and the slot charge from the same explicit snapshot instead of 16
// hidden Params reads. DataCoord's planners use the pure helpers for
// validation and slot sizing; the task adapters (import_task_v3.go) use them to
// rebuild the plan at dispatch. No planning object is written to object
// storage: the plan is re-derived from durable records on every consumer.

import (
	"math"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/internal/util/importutilv2/reshardmem"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// importV3Config is the explicit, single-read snapshot of the live
// configuration the Import V3 plan derivations consume. Every value here is
// refreshable (paramtable) and both Planning and dispatch re-derive their
// plan on every tick, so the snapshot is loaded per derivation
// (loadImportV3Config) rather than cached at package scope: a cached copy
// would pin a hot-refreshed value and let the charge drift from the run.
type importV3Config struct {
	// Storage / writer layout.
	storageVersion          int64
	storageFormat           string
	bloomType               string
	bloomFpp                float64
	textInlineLimit         int64
	textMaxLobFileBytes     int64
	textFlushThresholdBytes int64

	// Reshard packing.
	fragmentSizeInMB         int64
	fragmentSizeInMBRaw      string
	maxSizeInMBPerImportTask int64

	// Fan-in and slot sizing.
	fragmentMergeFanIn           int
	importBaseBufferSize         int64
	workerSlotUnit               int64
	reshardFlushConcurrency      int64
	reshardMemoryExpansionFactor float64
	reshardResidentBucketCap     int64
	importMemoryLimitPerSlot     int64
}

// getImportV3Config reads every Param the Import V3 plan factory and the
// slot/log-range calculations consume, in one place.
func getImportV3Config() importV3Config {
	return importV3Config{
		storageVersion:          importStorageVersion(false),
		storageFormat:           Params.DataNodeCfg.StorageFormat.GetValue(),
		bloomType:               Params.CommonCfg.BloomFilterType.GetValue(),
		bloomFpp:                Params.CommonCfg.MaxBloomFalsePositive.GetAsFloat(),
		textInlineLimit:         Params.DataNodeCfg.TextInlineThreshold.GetAsInt64(),
		textMaxLobFileBytes:     Params.DataNodeCfg.TextMaxLobFileBytes.GetAsInt64(),
		textFlushThresholdBytes: Params.DataNodeCfg.TextFlushThresholdBytes.GetAsInt64(),

		fragmentSizeInMB:         Params.DataCoordCfg.ImportFragmentSizeInMB.GetAsInt64(),
		fragmentSizeInMBRaw:      Params.DataCoordCfg.ImportFragmentSizeInMB.GetValue(),
		maxSizeInMBPerImportTask: Params.DataCoordCfg.MaxSizeInMBPerImportTask.GetAsInt64(),

		fragmentMergeFanIn:           Params.DataCoordCfg.FragmentMergeFanIn.GetAsInt(),
		importBaseBufferSize:         Params.DataNodeCfg.ImportBaseBufferSize.GetAsInt64(),
		workerSlotUnit:               Params.DataNodeCfg.WorkerSlotUnit.GetAsInt64(),
		reshardFlushConcurrency:      Params.DataCoordCfg.ReshardFlushConcurrency.GetAsInt64(),
		reshardMemoryExpansionFactor: Params.DataCoordCfg.ReshardMemoryExpansionFactor.GetAsFloat(),
		reshardResidentBucketCap:     Params.DataCoordCfg.ReshardResidentBucketCap.GetAsInt64(),
		importMemoryLimitPerSlot:     Params.DataCoordCfg.ImportMemoryLimitPerSlot.GetAsInt64(),
	}
}

// writerSpec derives the writer parameters from the target schema and the
// configuration snapshot.
func writerSpec(segmentSchema *schemapb.CollectionSchema, cfg importV3Config) (*importv3pb.WriterSpec, error) {
	spec := &importv3pb.WriterSpec{
		StorageVersion: cfg.storageVersion,
		Format:         cfg.storageFormat,
		V2:             &importv3pb.V2PackedIOConfig{BufferSize: packed.DefaultWriteBufferSize, MultipartSize: packed.DefaultMultiPartUploadSize},
		BloomType:      cfg.bloomType,
		BloomFpp:       cfg.bloomFpp,
	}
	for _, field := range typeutil.GetAllFieldSchemas(segmentSchema) {
		if field.GetDataType() == schemapb.DataType_Text {
			spec.Text = append(spec.Text, &importv3pb.TextColumnWriteSpec{
				FieldId: field.GetFieldID(), InlineLimit: cfg.textInlineLimit,
				LobLimit: cfg.textMaxLobFileBytes, FlushLimit: cfg.textFlushThresholdBytes,
			})
		}
	}
	columnGroups := storagecommon.SplitColumns(
		typeutil.GetAllFieldSchemas(segmentSchema),
		map[int64]storagecommon.ColumnStats{},
		storagecommon.DefaultPolicies()...)
	columnGroups = storagecommon.FillColumnGroupFormats(columnGroups, cfg.storageFormat)
	spec.Groups = make([]*importv3pb.ColumnGroupSpec, 0, len(columnGroups))
	for _, group := range columnGroups {
		spec.Groups = append(spec.Groups, &importv3pb.ColumnGroupSpec{Id: group.GroupID, FieldIds: append([]int64(nil), group.Fields...), Format: group.Format})
	}
	return spec, nil
}

// reshardPlan re-derives the complete execution input of one ReshardTask at
// dispatch time from the frozen job, the task's source ownership and the
// configuration snapshot.
func reshardPlan(job ImportJob, p *datapb.ReshardTask, cfg importV3Config) (*importv3pb.ReshardTaskPlan, error) {
	jobFiles := lo.SliceToMap(job.GetFiles(), func(file *internalpb.ImportFile) (int64, *internalpb.ImportFile) {
		return file.GetId(), file
	})
	files := make([]*internalpb.ImportFile, 0, len(p.GetFileIds()))
	for _, fileID := range p.GetFileIds() {
		file := jobFiles[fileID]
		if file == nil {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task %d references source %d outside the job", p.GetTaskId(), fileID)
		}
		files = append(files, file)
	}
	// Validate each non-backup source's reader type here, so a bad suffix fails
	// planning instead of every dispatch. The DataNode re-derives the type from
	// the path; only the validation result matters here.
	if !importutilv2.IsBackup(job.GetOptions()) {
		for _, file := range files {
			if _, err := importutilv2.GetFileType(file); err != nil {
				return nil, err
			}
		}
	}
	fragmentSize, err := importFragmentSizeBytes(cfg)
	if err != nil {
		return nil, err
	}
	return &importv3pb.ReshardTaskPlan{
		CollectionId:      job.GetCollectionID(),
		CollectionSchema:  job.GetSchema(),
		Vchannels:         append([]string(nil), job.GetVchannels()...),
		PartitionIds:      append([]int64(nil), job.GetPartitionIDs()...),
		Files:             append([]*internalpb.ImportFile(nil), files...),
		Options:           append([]*commonpb.KeyValuePair(nil), job.GetOptions()...),
		FragmentSize:      fragmentSize,
		RlsCheckPredicate: job.GetRlsCheckPredicate(),
	}, nil
}

// importPlan re-derives the complete execution input of one ImportTaskV3 and
// its slot charge from durable records and the configuration snapshot. Both
// derive from the same fan-in computation, so the charged slot and the merge
// that runs always agree.
func importPlan(job ImportJob, p *datapb.ImportTaskV3, cfg importV3Config) (*importv3pb.ImportTaskPlan, int64, error) {
	if p.GetRows() <= 0 {
		return nil, 0, merr.WrapErrImportSysFailedMsg("import v3 task %d has no planned rows", p.GetTaskId())
	}
	backup := importutilv2.IsBackup(job.GetOptions())
	segmentSchema := typeutil.AppendSystemFields(job.GetSchema())
	if err := validateImportV3StorageVersion(segmentSchema, cfg.storageVersion); err != nil {
		return nil, 0, err
	}
	spec, err := writerSpec(segmentSchema, cfg)
	if err != nil {
		return nil, 0, err
	}
	fanIn := getImportV3FanIn(cfg.fragmentMergeFanIn, len(p.GetFragments()))
	slot := calculateImportTaskV3Slot(cfg.importBaseBufferSize, packed.DefaultWriteBufferSize, cfg.importMemoryLimitPerSlot, fanIn)
	return &importv3pb.ImportTaskPlan{
		PartitionId:      p.GetPartitionId(),
		Fragments:        p.GetFragments(),
		Rows:             p.GetRows(),
		FanIn:            int32(fanIn),
		Writer:           spec,
		CollectionSchema: job.GetSchema(),
		DataTs:           job.GetDataTs(),
		CollectionId:     job.GetCollectionID(),
		Backup:           backup,
	}, slot, nil
}

// reshardSlot derives the reshard task's slot charge from the frozen job and
// the configuration snapshot: the same job-level inputs the DataNode reshard
// run is budgeted with. It is recomputed at dispatch (and by the scheduler)
// instead of being persisted, so a refreshable-config change cannot make the
// charge drift from the run.
func reshardSlot(job ImportJob, cfg importV3Config) int64 {
	fragmentSize, err := importFragmentSizeBytes(cfg)
	if err != nil {
		// Planning rejects an invalid fragment size before any task exists; a
		// later refresh to an invalid value must still yield a positive charge.
		return 1
	}
	return calculateReshardTaskSlot(reshardmem.Model{
		ReadBuffer:       cfg.importBaseBufferSize,
		FragmentTarget:   fragmentSize,
		FlushConcurrency: cfg.reshardFlushConcurrency,
		ExpansionFactor:  cfg.reshardMemoryExpansionFactor,
	}, reshardmem.MemoryPerSlot(cfg.workerSlotUnit),
		int64(len(job.GetVchannels())*len(job.GetPartitionIDs())),
		cfg.reshardResidentBucketCap,
		int64(len(typeutil.GetAllFieldSchemas(importutilv2.FragmentSchema(job.GetSchema(), importutilv2.IsBackup(job.GetOptions()))))),
	)
}

// importSlot derives the ImportTaskV3 slot charge from the fan-in that
// importPlan computes from the same fragment count under the same config, so
// the charged slot and the merge that runs always agree.
func importSlot(fragmentCount int, cfg importV3Config) int64 {
	fanIn := getImportV3FanIn(cfg.fragmentMergeFanIn, fragmentCount)
	return calculateImportTaskV3Slot(cfg.importBaseBufferSize, packed.DefaultWriteBufferSize, cfg.importMemoryLimitPerSlot, fanIn)
}

// validateImportV3StorageVersion fails a TEXT collection when the given storage
// version cannot write TEXT LOB columns.
func validateImportV3StorageVersion(schema *schemapb.CollectionSchema, storageVersion int64) error {
	if storageVersion == storage.StorageV3 {
		return nil
	}
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if field.GetDataType() == schemapb.DataType_Text {
			return merr.WrapErrImportSysFailedMsg("import v3 TEXT columns require common.storage.useLoonFFI")
		}
	}
	return nil
}

// importFragmentSizeBytes resolves the fragment size config in bytes. The
// parameter is refreshable but only validated once at startup, so a hot
// refresh to an empty, non-numeric or non-positive value must fail the consumer
// loudly instead of reaching the DataNode as a zero fragment target (which
// would flush every non-empty bucket after every batch).
func importFragmentSizeBytes(cfg importV3Config) (int64, error) {
	if cfg.fragmentSizeInMB <= 0 {
		return 0, merr.WrapErrImportSysFailedMsg(
			"dataCoord.import.fragmentSizeInMB must be positive, got %q", cfg.fragmentSizeInMBRaw)
	}
	return cfg.fragmentSizeInMB * 1024 * 1024, nil
}

// bm25OutputFieldCount returns the number of BM25 function output columns in
// the schema. Import V3 reserves one log ID per BM25 output column plus the
// row's own ID; the DataNode writer derives those columns from the schema it
// receives, so DataCoord is the only reader of this count.
func bm25OutputFieldCount(schema *schemapb.CollectionSchema) int {
	n := 0
	for _, function := range schema.GetFunctions() {
		if function.GetType() == schemapb.FunctionType_BM25 {
			n += len(function.GetOutputFieldIds())
		}
	}
	return n
}

// importV3LogRangeWidth returns the number of log IDs one imported segment
// consumes under the given writer spec: one manifest plus one log per BM25
// field, and for StorageV2 one packed binlog per column group. Every (re)alloc
// of a task's LogRange must size it from the same spec the dispatch plan
// carries, so a hot-flipped storage version cannot exhaust a range that was
// sized for the old version.
func importV3LogRangeWidth(writerSpec *importv3pb.WriterSpec, bm25Fields int) (int64, error) {
	perSegment := int64(1 + bm25Fields)
	if writerSpec.GetStorageVersion() == storage.StorageV2 {
		perSegment += int64(len(writerSpec.GetGroups()))
	}
	if perSegment <= 0 || perSegment > math.MaxUint32 {
		return 0, merr.WrapErrImportSysFailedMsg("import v3 log id budget is invalid")
	}
	return perSegment, nil
}

// getImportV3FanIn is the fan-in written into a per-segment task plan.
// A segment never opens more readers than it has fragments, so slot estimation
// charges only for the readers that can actually run instead of the global
// configured maximum.
func getImportV3FanIn(configured, fragmentCount int) int {
	fanIn := configured
	if fragmentCount > 0 && fragmentCount < fanIn {
		fanIn = fragmentCount
	}
	// fragmentMergeFanIn is refreshable but only range-checked at startup, so
	// clamp the upper bound here too; MergeExecutor rejects fan-in > 1024.
	if fanIn > 1024 {
		fanIn = 1024
	}
	// MergeExecutor validates fan-in >= 2; a single-fragment segment still
	// runs the normal one-head merge path with that validation intact.
	if fanIn < 2 {
		fanIn = 2
	}
	return fanIn
}
