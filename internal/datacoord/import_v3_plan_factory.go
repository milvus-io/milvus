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

// This file owns the derivation of Import V3 execution plans. A importV3PlanFactory
// is bound to one frozen ImportJob and derives every execution input from it:
// the sort spec, the temporary schema, the writer spec, the per-source reader
// specs, and the two dispatch-time task plans. DataCoord's planners use it for
// validation and slot sizing; the task adapters (import_task_v3.go) use it to
// rebuild the plan at dispatch. No planning object is written to object
// storage: the plan is re-derived from durable records on every consumer.

import (
	"fmt"
	"math"
	"sort"

	"github.com/samber/lo"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// importV3PlanFactory derives execution plans and writer specs from one frozen
// ImportJob. It is a value object: construct it where a plan is needed.
type importV3PlanFactory struct {
	job ImportJob
}

func newImportV3PlanFactory(job ImportJob) *importV3PlanFactory {
	return &importV3PlanFactory{job: job}
}

func (f *importV3PlanFactory) isBackup() bool {
	return importutilv2.IsBackup(f.job.GetOptions())
}

// sortSpec derives the default sort: partition key first for namespace
// collections, primary key always. Planning only validates the spec; dispatch
// re-derives it from the frozen job schema.
func (f *importV3PlanFactory) sortSpec() (*datapb.SortSpec, error) {
	schema := f.job.GetSchema()
	pk, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, merr.Wrap(err, "import v3 schema has no primary key")
	}
	toSpec := func(field *schemapb.FieldSchema) (*datapb.SortFieldSpec, error) {
		return &datapb.SortFieldSpec{FieldId: field.GetFieldID(), DataType: field.GetDataType()}, nil
	}
	spec := &datapb.SortSpec{}
	if schema.GetEnableNamespace() {
		partitionKey, err := typeutil.GetPartitionKeyFieldSchema(schema)
		if err != nil {
			return nil, merr.Wrap(err, "import v3 namespace schema has no partition key")
		}
		field, err := toSpec(partitionKey)
		if err != nil {
			return nil, err
		}
		spec.Fields = append(spec.Fields, field)
	}
	field, err := toSpec(pk)
	if err != nil {
		return nil, err
	}
	spec.Fields = append(spec.Fields, field)
	return spec, nil
}

// tempSchema describes the fields physically present in immutable fragments.
// Ordinary fragments carry user fields plus materialized PK/RowID and every
// function output column (Reshard computes the missing ones); timestamp is
// supplied by the import data timestamp at final merge. Backup fragments
// retain source timestamp and their source-provided function outputs, so they
// use the full system field schema.
func (f *importV3PlanFactory) tempSchema() *schemapb.CollectionSchema {
	cloned := proto.Clone(f.job.GetSchema()).(*schemapb.CollectionSchema)
	// Temporary fragments and intermediate merge runs store TEXT as raw UTF-8
	// strings, not as manifest binary LOB references. Map TEXT to VarChar in the
	// temporary schema so the ordinary storage sort/merge/writer paths can handle
	// it without a storage-layer special case. The synthetic max_length keeps
	// TEXT's full length budget rather than VarChar's smaller default.
	for _, field := range cloned.Fields {
		if field.GetDataType() == schemapb.DataType_Text {
			field.DataType = schemapb.DataType_VarChar
			field.TypeParams = append(field.TypeParams, &commonpb.KeyValuePair{
				Key:   common.MaxLengthKey,
				Value: fmt.Sprintf("%d", Params.ProxyCfg.MaxTextLength.GetAsInt64()),
			})
		}
	}
	if f.isBackup() {
		return typeutil.AppendSystemFields(cloned)
	}
	cloned.Fields = append(cloned.Fields, &schemapb.FieldSchema{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64})
	return cloned
}

// writerSpec derives the writer parameters from the frozen target schema and
// current configuration.
func (f *importV3PlanFactory) writerSpec(targetSchema *schemapb.CollectionSchema) (*datapb.WriterSpec, error) {
	ttl, err := common.GetCollectionTTL(targetSchema.GetProperties())
	if err != nil {
		return nil, err
	}
	writerFormat := Params.DataNodeCfg.StorageFormat.GetValue()
	writerSpec := &datapb.WriterSpec{
		StorageVersion: importStorageVersion(false),
		SchemaVersion:  int64(targetSchema.GetVersion()),
		Format:         writerFormat,
		V2:             &datapb.V2PackedIOConfig{BufferSize: packed.DefaultWriteBufferSize, MultipartSize: packed.DefaultMultiPartUploadSize},
		TtlNanos:       ttl.Nanoseconds(),
		BloomType:      Params.CommonCfg.BloomFilterType.GetValue(),
		BloomFpp:       Params.CommonCfg.MaxBloomFalsePositive.GetAsFloat(),
	}
	for _, function := range targetSchema.GetFunctions() {
		if function.GetType() == schemapb.FunctionType_BM25 {
			writerSpec.Bm25Fields = append(writerSpec.Bm25Fields, function.GetOutputFieldIds()...)
		}
	}
	sort.Slice(writerSpec.Bm25Fields, func(i, j int) bool { return writerSpec.Bm25Fields[i] < writerSpec.Bm25Fields[j] })
	for _, field := range typeutil.GetAllFieldSchemas(targetSchema) {
		if field.GetDataType() == schemapb.DataType_Text {
			writerSpec.Text = append(writerSpec.Text, &datapb.TextColumnWriteSpec{
				FieldId: field.GetFieldID(), InlineLimit: Params.DataNodeCfg.TextInlineThreshold.GetAsInt64(),
				LobLimit: Params.DataNodeCfg.TextMaxLobFileBytes.GetAsInt64(), FlushLimit: Params.DataNodeCfg.TextFlushThresholdBytes.GetAsInt64(),
			})
		}
	}
	columnGroups := storagecommon.SplitColumns(typeutil.GetAllFieldSchemas(targetSchema), map[int64]storagecommon.ColumnStats{}, storagecommon.DefaultPolicies()...)
	columnGroups = storagecommon.FillColumnGroupFormats(columnGroups, writerFormat)
	writerSpec.Groups = make([]*datapb.ColumnGroupSpec, 0, len(columnGroups))
	for _, group := range columnGroups {
		writerSpec.Groups = append(writerSpec.Groups, &datapb.ColumnGroupSpec{Id: group.GroupID, Fields: append([]int64(nil), group.Fields...), Format: group.Format})
	}
	return writerSpec, nil
}

// sourceFileSpec derives one source's reader spec: file type, CSV options, or
// backup time range/storage version.
func (f *importV3PlanFactory) sourceFileSpec(file *internalpb.ImportFile) (*datapb.SourceFileSpec, error) {
	var fileType datapb.ImportFileType
	var err error
	if f.isBackup() {
		fileType = datapb.ImportFileType_BackupBinlog
	} else {
		fileType, err = importutilv2.GetFileType(file)
		if err != nil {
			return nil, err
		}
	}
	spec := &datapb.SourceFileSpec{
		File:     proto.Clone(file).(*internalpb.ImportFile),
		FileType: fileType,
		Options:  &datapb.ReaderOptions{},
	}
	if fileType == datapb.ImportFileType_Csv {
		separator, err := importutilv2.GetCSVSep(f.job.GetOptions())
		if err != nil {
			return nil, err
		}
		nullKey, err := importutilv2.GetCSVNullKey(f.job.GetOptions())
		if err != nil {
			return nil, err
		}
		spec.Options.Separator = string(separator)
		spec.Options.NullKey = nullKey
	}
	if fileType == datapb.ImportFileType_BackupBinlog {
		startTS, endTS, err := importutilv2.ParseTimeRange(f.job.GetOptions())
		if err != nil {
			return nil, err
		}
		storageVersion, err := importutilv2.GetStorageVersion(f.job.GetOptions())
		if err != nil {
			return nil, err
		}
		spec.Options.StartTs = startTS
		spec.Options.EndTs = endTS
		spec.Options.StorageVersion = storageVersion
	}
	return spec, nil
}

// reshardPlan re-derives the complete execution input of one ReshardTask at
// dispatch time from the frozen ImportJob, the task's source ownership and
// current configuration.
func (f *importV3PlanFactory) reshardPlan(p *datapb.ReshardTask) (*datapb.ReshardTaskPlan, error) {
	sortSpec, err := f.sortSpec()
	if err != nil {
		return nil, err
	}
	jobFiles := lo.SliceToMap(f.job.GetFiles(), func(file *internalpb.ImportFile) (int64, *internalpb.ImportFile) {
		return file.GetId(), file
	})
	specs := make([]*datapb.SourceFileSpec, 0, len(p.GetSourceIds()))
	for _, fileID := range p.GetSourceIds() {
		file := jobFiles[fileID]
		if file == nil {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task %d references source %d outside the job", p.GetTaskId(), fileID)
		}
		spec, err := f.sourceFileSpec(file)
		if err != nil {
			return nil, err
		}
		specs = append(specs, spec)
	}
	fragmentSize, err := importFragmentSizeBytes()
	if err != nil {
		return nil, err
	}
	return &datapb.ReshardTaskPlan{
		CollectionId: f.job.GetCollectionID(),
		Schema:       f.job.GetSchema(),
		TempSchema:   f.tempSchema(),
		Vchannels:    append([]string(nil), f.job.GetVchannels()...),
		Partitions:   append([]int64(nil), f.job.GetPartitionIDs()...),
		Sort:         sortSpec,
		FragmentSize: fragmentSize,
		Sources:      specs,
		Backup:       f.isBackup(),
	}, nil
}

// importPlan re-derives the complete execution input of one ImportTaskV3 at
// dispatch time from durable records and current configuration. Only fragment
// ownership lives on the task record.
func (f *importV3PlanFactory) importPlan(p *datapb.ImportTaskV3) (*datapb.ImportTaskPlan, error) {
	if p.GetRows() <= 0 {
		return nil, merr.WrapErrImportSysFailedMsg("import v3 task %d has no planned rows", p.GetTaskId())
	}
	sortSpec, err := f.sortSpec()
	if err != nil {
		return nil, err
	}
	backup := f.isBackup()
	targetSchema := typeutil.AppendSystemFields(f.job.GetSchema())
	if err := validateImportV3StorageVersion(targetSchema); err != nil {
		return nil, err
	}
	writerSpec, err := f.writerSpec(targetSchema)
	if err != nil {
		return nil, err
	}
	writerSpec.PkCapacity = max(p.GetRows(), 1)
	return &datapb.ImportTaskPlan{
		Sort:         sortSpec,
		Vchannel:     p.GetVchannel(),
		PartitionId:  p.GetPartitionId(),
		Fragments:    p.GetFragments(),
		Rows:         p.GetRows(),
		FanIn:        int32(effectiveImportV3FanIn(Params.DataCoordCfg.FragmentMergeFanIn.GetAsInt(), len(p.GetFragments()))),
		Writer:       writerSpec,
		Schema:       targetSchema,
		TempSchema:   f.tempSchema(),
		DataTs:       f.job.GetDataTs(),
		CollectionId: f.job.GetCollectionID(),
		Backup:       backup,
	}, nil
}

// validateImportV3StorageVersion fails a TEXT collection when the current
// storage version cannot write TEXT LOB columns.
func validateImportV3StorageVersion(schema *schemapb.CollectionSchema) error {
	if importStorageVersion(false) == storage.StorageV3 {
		return nil
	}
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if field.GetDataType() == schemapb.DataType_Text {
			return merr.WrapErrImportSysFailedMsg("import v3 TEXT columns require common.storage.useLoonFFI")
		}
	}
	return nil
}

// importFragmentSizeBytes resolves the live fragment size config in bytes. The
// parameter is refreshable but only validated once at startup, so a hot
// refresh to an empty, non-numeric or non-positive value must fail the consumer
// loudly instead of reaching the DataNode as a zero fragment target (which
// would flush every non-empty bucket after every batch).
func importFragmentSizeBytes() (int64, error) {
	sizeInMB := Params.DataCoordCfg.ImportFragmentSizeInMB.GetAsInt64()
	if sizeInMB <= 0 {
		return 0, merr.WrapErrImportSysFailedMsg(
			"dataCoord.import.fragmentSizeInMB must be positive, got %q",
			Params.DataCoordCfg.ImportFragmentSizeInMB.GetValue())
	}
	return sizeInMB * 1024 * 1024, nil
}

// importV3LogRangeWidth returns the number of log IDs one imported segment
// consumes under the given writer spec: one manifest plus one log per BM25
// field, and for StorageV2 one packed binlog per column group. Every (re)alloc
// of a task's LogRange must size it from the same spec the dispatch plan
// carries, so a hot-flipped storage version cannot exhaust a range that was
// sized for the old version.
func importV3LogRangeWidth(writerSpec *datapb.WriterSpec) (int64, error) {
	perSegment := int64(1 + len(writerSpec.GetBm25Fields()))
	if writerSpec.GetStorageVersion() == storage.StorageV2 {
		perSegment += int64(len(writerSpec.GetGroups()))
	}
	if perSegment <= 0 || perSegment > math.MaxUint32 {
		return 0, merr.WrapErrImportSysFailedMsg("import v3 log id budget is invalid")
	}
	return perSegment, nil
}

// effectiveImportV3FanIn is the fan-in written into a per-segment task plan.
// A segment never opens more readers than it has fragments, so slot estimation
// charges only for the readers that can actually run instead of the global
// configured maximum.
func effectiveImportV3FanIn(configured, fragmentCount int) int {
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
