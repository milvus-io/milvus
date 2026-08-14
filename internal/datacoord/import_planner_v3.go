package datacoord

// The V3 planner is deliberately small and deterministic.  It persists every
// object before publishing the next catalog marker, so a restart can finish the
// same task set without re-reading source files or depending on map iteration.

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc64"
	"math"
	"path"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	importbinlog "github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const importV3Root = metautil.ImportV3RootPath

func importV3Digest(data []byte) []byte {
	v := crc64.Checksum(data, crc64.MakeTable(crc64.ECMA))
	return []byte(fmt.Sprintf("crc64-ecma:%016x", v))
}

func writeImportV3Proto(ctx context.Context, cm storage.ChunkManager, prefix, name string, msg proto.Message) (string, []byte, error) {
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(msg)
	if err != nil {
		return "", nil, merr.WrapErrSerializationFailed(err, "marshal import v3 object")
	}
	digest := importV3Digest(payload)
	digestToken := strings.TrimPrefix(string(digest), "crc64-ecma:")
	ref := path.Join(prefix, name+"_"+digestToken+".pb")
	if existing, err := cm.Read(ctx, ref); err == nil {
		if !bytes.Equal(existing, payload) {
			return "", nil, merr.WrapErrDataIntegrityMsg("import v3 object already exists with different content: %s", ref)
		}
		return ref, digest, nil
	}
	if err := cm.Write(ctx, ref, payload); err != nil {
		return "", nil, merr.Wrap(err, "write import v3 object")
	}
	return ref, digest, nil
}

func importV3SourceFormat(file *internalpb.ImportFile) datapb.ImportSourceFormat {
	ft, _ := importutilv2.GetFileType(file)
	switch ft {
	case importutilv2.JSON:
		return datapb.ImportSourceFormat_IMPORT_SOURCE_FORMAT_JSON
	case importutilv2.JSONLines:
		return datapb.ImportSourceFormat_IMPORT_SOURCE_FORMAT_JSON_LINES
	case importutilv2.CSV:
		return datapb.ImportSourceFormat_IMPORT_SOURCE_FORMAT_CSV
	case importutilv2.Parquet:
		return datapb.ImportSourceFormat_IMPORT_SOURCE_FORMAT_PARQUET
	case importutilv2.Numpy:
		return datapb.ImportSourceFormat_IMPORT_SOURCE_FORMAT_NUMPY
	default:
		return datapb.ImportSourceFormat_IMPORT_SOURCE_FORMAT_BACKUP_BINLOG
	}
}

func (c *importChecker) createV3ReshardTasks(job ImportJob) error {
	files := job.GetFiles()
	if len(files) == 0 {
		state := internalpb.ImportJobState_Uncommitted
		if job.GetAutoCommit() {
			state = internalpb.ImportJobState_Completed
		}
		return c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(state))
	}
	bins, err := c.groupV3ReshardSources(files)
	if err != nil {
		return err
	}
	sortSpec, err := v3DefaultSortSpec(job.GetSchema())
	if err != nil {
		return err
	}
	if existing := c.importMeta.GetTaskBy(c.ctx, WithType(ReshardTaskType), WithJob(job.GetJobID())); len(existing) > 0 {
		if len(existing) == len(bins) {
			if err := c.validateV3ReshardTaskSet(job, existing, bins); err != nil {
				return err
			}
			return c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_PreImporting))
		}
		// Pending is the unpublished task-set state.  With the inspector gate,
		// these tasks cannot have run, so a crash after saving only a prefix of
		// the set is recovered by removing that prefix and rebuilding once.
		for _, task := range existing {
			if task.GetState() != datapb.ImportTaskStateV2_Pending || task.GetNodeID() != NullNodeID {
				return merr.WrapErrDataIntegrityMsg("partial import v3 reshard task set was dispatched before publication")
			}
		}
		for _, task := range existing {
			if err := c.importMeta.RemoveTask(c.ctx, task.GetTaskID()); err != nil {
				return err
			}
		}
	}
	start, _, err := c.alloc.AllocN(int64(len(bins)))
	if err != nil {
		return err
	}
	for i, bin := range bins {
		taskID := start + int64(i)
		sources := make([]*datapb.SourceFileSpec, 0, len(bin.sources))
		for _, source := range bin.sources {
			spec, err := c.buildV3SourceFileSpec(job, source)
			if err != nil {
				return err
			}
			sources = append(sources, spec)
		}
		plan := &datapb.ReshardTaskPlan{
			FormatVersion: 1,
			JobId:         job.GetJobID(), TaskId: taskID, CollectionId: job.GetCollectionID(),
			SourceSchema:               proto.Clone(job.GetSchema()).(*schemapb.CollectionSchema),
			TemporarySchema:            buildImportV3TemporarySchema(job.GetSchema(), importutilv2.IsBackup(job.GetOptions())),
			Vchannels:                  append([]string(nil), job.GetVchannels()...),
			PartitionIds:               append([]int64(nil), job.GetPartitionIDs()...),
			SortSpec:                   proto.Clone(sortSpec).(*datapb.SortSpec),
			FragmentTargetLogicalBytes: Params.DataCoordCfg.ImportV3FragmentSizeInMB.GetAsInt64() * 1024 * 1024,
			Sources:                    sources,
		}
		prefix := metautil.BuildImportV3ReshardPlanPath(job.GetJobID(), taskID)
		ref, digest, err := writeImportV3Proto(c.ctx, c.meta.chunkManager, prefix, "plan", plan)
		if err != nil {
			return err
		}
		task := newReshardTask(&datapb.ReshardTask{JobId: job.GetJobID(), TaskId: taskID, CollectionId: job.GetCollectionID(), State: datapb.ReshardTask_Pending, TaskPlanRef: ref, TaskPlanDigest: digest, RunId: 1, NodeId: NullNodeID, OutputPrefix: metautil.BuildImportV3ReshardOutputPath(job.GetJobID(), taskID), TaskSlot: 1}, c.importMeta, c.meta, c.alloc)
		if err := c.importMeta.AddTask(c.ctx, task); err != nil {
			return err
		}
	}
	return c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_PreImporting))
}

func (c *importChecker) buildV3SourceFileSpec(job ImportJob, source v3ReshardSource) (*datapb.SourceFileSpec, error) {
	format := importV3SourceFormat(source.file)
	spec := &datapb.SourceFileSpec{
		SourceOrdinal:        int32(source.ordinal),
		File:                 proto.Clone(source.file).(*internalpb.ImportFile),
		EstimatedSourceBytes: source.size,
		Format:               format,
		IsBackup:             importutilv2.IsBackup(job.GetOptions()),
		ReaderOptions:        &datapb.ReaderOptions{},
	}
	if format == datapb.ImportSourceFormat_IMPORT_SOURCE_FORMAT_CSV {
		separator, err := importutilv2.GetCSVSep(job.GetOptions())
		if err != nil {
			return nil, err
		}
		nullKey, err := importutilv2.GetCSVNullKey(job.GetOptions())
		if err != nil {
			return nil, err
		}
		spec.ReaderOptions.CsvSeparator = string(separator)
		spec.ReaderOptions.CsvNullKey = nullKey
	}
	if !spec.GetIsBackup() {
		return spec, nil
	}
	startTS, endTS, err := importutilv2.ParseTimeRange(job.GetOptions())
	if err != nil {
		return nil, err
	}
	storageVersion, err := importutilv2.GetStorageVersion(job.GetOptions())
	if err != nil {
		return nil, err
	}
	insertObjects, deltaObjects, err := importbinlog.ExpandObjects(c.ctx, c.meta.chunkManager, source.file.GetPaths())
	if err != nil {
		return nil, err
	}
	fieldIDs := make([]int64, 0, len(insertObjects))
	for fieldID := range insertObjects {
		fieldIDs = append(fieldIDs, fieldID)
	}
	sort.Slice(fieldIDs, func(i, j int) bool { return fieldIDs[i] < fieldIDs[j] })
	for _, fieldID := range fieldIDs {
		spec.ExpandedInsertFields = append(spec.ExpandedInsertFields, &datapb.ExpandedBinlogField{
			FieldOrGroupId: fieldID,
			Paths:          append([]string(nil), insertObjects[fieldID]...),
		})
	}
	spec.ExpandedDeltaObjects = append([]string(nil), deltaObjects...)
	spec.ReaderOptions.BackupStartTs = startTS
	spec.ReaderOptions.BackupEndTs = endTS
	spec.ReaderOptions.SourceStorageVersion = storageVersion
	return spec, nil
}

func (c *importChecker) validateV3ReshardTaskSet(job ImportJob, tasks []ImportTask, bins []v3ReshardBin) error {
	expected := make(map[string]int, len(bins))
	for _, bin := range bins {
		ids := make([]int64, 0, len(bin.sources))
		for _, source := range bin.sources {
			ids = append(ids, source.file.GetId())
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		expected[fmt.Sprint(ids)]++
	}
	actual := make(map[string]int, len(tasks))
	for _, generic := range tasks {
		task, ok := generic.(*reshardTask)
		if !ok {
			return merr.WrapErrDataIntegrityMsg("import v3 reshard task set contains an unexpected task type")
		}
		p := task.task.Load()
		plan := &datapb.ReshardTaskPlan{}
		prefix := metautil.BuildImportV3ReshardPlanPath(job.GetJobID(), p.GetTaskId())
		if err := loadImportV3Proto(c.ctx, c.meta.chunkManager, p.GetTaskPlanRef(), prefix, p.GetTaskPlanDigest(), plan); err != nil {
			return err
		}
		if plan.GetJobId() != job.GetJobID() || plan.GetTaskId() != p.GetTaskId() || plan.GetCollectionId() != job.GetCollectionID() {
			return merr.WrapErrDataIntegrityMsg("import v3 reshard task plan identity mismatch")
		}
		ids := make([]int64, 0, len(plan.GetSources()))
		for _, source := range plan.GetSources() {
			if source == nil || source.GetFile() == nil {
				return merr.WrapErrDataIntegrityMsg("import v3 reshard task plan has a nil source")
			}
			ids = append(ids, source.GetFile().GetId())
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		actual[fmt.Sprint(ids)]++
	}
	if !mapsEqual(expected, actual) {
		return merr.WrapErrDataIntegrityMsg("import v3 reshard task grouping does not match the deterministic BFD plan")
	}
	return nil
}

func mapsEqual(left, right map[string]int) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

type v3ReshardSource struct {
	file    *internalpb.ImportFile
	ordinal int
	size    int64
}

type v3ReshardBin struct {
	sources []v3ReshardSource
	size    int64
}

// groupV3ReshardSources implements stable one-dimensional BFD. ImportFile is
// the atom: no path/file splitting, no backtracking, and an oversized file owns
// one bin. Equal-size files keep their job ordinal; equal-fit bins keep their
// creation ordinal.
func (c *importChecker) groupV3ReshardSources(files []*internalpb.ImportFile) ([]v3ReshardBin, error) {
	sources := make([]v3ReshardSource, 0, len(files))
	for ordinal, file := range files {
		size, err := storage.GetFilesSize(c.ctx, file.GetPaths(), c.meta.chunkManager)
		if err != nil {
			return nil, merr.Wrapf(err, "estimate import v3 source file %d", file.GetId())
		}
		sources = append(sources, v3ReshardSource{file: file, ordinal: ordinal, size: size})
	}
	sort.SliceStable(sources, func(i, j int) bool {
		return sources[i].size > sources[j].size
	})
	target := Params.DataCoordCfg.MaxSizeInMBPerImportTask.GetAsInt64() * 1024 * 1024
	if target <= 0 {
		return nil, merr.WrapErrImportSysFailedMsg("import v3 reshard BFD target must be positive")
	}
	bins := make([]v3ReshardBin, 0)
	for _, source := range sources {
		best := -1
		bestRemaining := int64(math.MaxInt64)
		if source.size <= target {
			for i := range bins {
				remaining := target - bins[i].size - source.size
				if remaining >= 0 && remaining < bestRemaining {
					best, bestRemaining = i, remaining
				}
			}
		}
		if best < 0 {
			bins = append(bins, v3ReshardBin{sources: []v3ReshardSource{source}, size: source.size})
			continue
		}
		bins[best].sources = append(bins[best].sources, source)
		bins[best].size += source.size
	}
	return bins, nil
}

func v3DefaultSortSpec(schema *schemapb.CollectionSchema) (*datapb.SortSpec, error) {
	pk, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, merr.Wrap(err, "import v3 schema has no primary key")
	}
	toSpec := func(field *schemapb.FieldSchema) (*datapb.SortFieldSpec, error) {
		var keyType datapb.SortKeyType
		switch field.GetDataType() {
		case schemapb.DataType_Int64:
			keyType = datapb.SortKeyType_SORT_KEY_TYPE_INT64
		case schemapb.DataType_VarChar:
			keyType = datapb.SortKeyType_SORT_KEY_TYPE_STRING
		default:
			return nil, merr.WrapErrImportSysFailedMsg("import v3 sort field %d has unsupported type %s", field.GetFieldID(), field.GetDataType())
		}
		return &datapb.SortFieldSpec{FieldId: field.GetFieldID(), KeyType: keyType}, nil
	}
	spec := &datapb.SortSpec{FormatVersion: 1}
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

// buildImportV3TemporarySchema describes the fields physically present in
// immutable fragments. Ordinary fragments carry user fields plus materialized
// PK/RowID, but timestamp is supplied by the import data timestamp at final
// merge. Backup fragments retain source timestamp, so they use the full system
// field schema. Function outputs are produced only by the final transform and
// are therefore omitted from the temporary reader schema.
func buildImportV3TemporarySchema(schema *schemapb.CollectionSchema, backup bool) *schemapb.CollectionSchema {
	cloned := proto.Clone(schema).(*schemapb.CollectionSchema)
	fields := make([]*schemapb.FieldSchema, 0, len(cloned.GetFields())+2)
	for _, field := range cloned.GetFields() {
		if field.GetIsFunctionOutput() && !backup {
			continue
		}
		fields = append(fields, field)
	}
	cloned.Fields = fields
	if backup {
		return typeutil.AppendSystemFields(cloned)
	}
	cloned.Fields = append(cloned.Fields, &schemapb.FieldSchema{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64})
	return cloned
}

type v3PlanningFragment struct {
	ref   *datapb.FragmentRef
	bytes int64
}

func loadImportV3Proto(ctx context.Context, cm storage.ChunkManager, ref, expectedPrefix string, digest []byte, target proto.Message) error {
	data, err := loadImportV3Object(ctx, cm, ref, expectedPrefix, digest)
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(data, target); err != nil {
		return merr.WrapErrDataIntegrity(err, "unmarshal import v3 planning object")
	}
	return nil
}

func (c *importChecker) summarizeV3ReshardResults(job ImportJob) (bool, int64, error) {
	tasks := c.importMeta.GetTaskBy(c.ctx, WithType(ReshardTaskType), WithJob(job.GetJobID()))
	if len(tasks) == 0 {
		return false, 0, nil
	}
	var totalRows int64
	for _, generic := range tasks {
		task, ok := generic.(*reshardTask)
		if !ok {
			return false, 0, merr.WrapErrDataIntegrityMsg("import v3 reshard result set contains an unexpected task type")
		}
		if task.GetState() != datapb.ImportTaskStateV2_Completed {
			return false, 0, nil
		}
		p := task.task.Load()
		manifest, err := loadReshardResultManifest(c.ctx, c.meta.chunkManager, p.GetResultRef(), p.GetOutputPrefix(), p.GetResultDigest())
		if err != nil {
			return false, 0, err
		}
		if err := validateReshardManifest(manifest, job.GetJobID(), p.GetTaskId(), p.GetRunId(), p.GetTaskPlanDigest()); err != nil {
			return false, 0, err
		}
		totalRows += manifest.GetTotalRows()
	}
	return true, totalRows, nil
}

func (c *importChecker) cleanupUnpublishedV3ImportTasks(job ImportJob) error {
	tasks := c.importMeta.GetTaskBy(c.ctx, WithType(ImportTaskV3Type), WithJob(job.GetJobID()))
	for _, generic := range tasks {
		task, ok := generic.(*importTaskV3)
		if !ok {
			return merr.WrapErrDataIntegrityMsg("import v3 planning task set contains an unexpected task type")
		}
		if task.GetState() != datapb.ImportTaskStateV2_Pending || task.GetNodeID() != NullNodeID {
			return merr.WrapErrDataIntegrityMsg("unpublished import v3 task %d was dispatched", task.GetTaskID())
		}
	}
	for _, generic := range tasks {
		task := generic.(*importTaskV3)
		segmentIDs := task.task.Load().GetOutputSegmentIds()
		if len(segmentIDs) > 0 {
			if err := c.meta.UpdateSegmentsInfo(c.ctx, dropImportV3Skeletons(segmentIDs, false)); err != nil {
				return err
			}
		}
		if err := c.importMeta.RemoveTask(c.ctx, task.GetTaskID()); err != nil {
			return err
		}
	}
	return nil
}

func (c *importChecker) validatePublishedV3Plan(job ImportJob) error {
	if job.GetPlanningGeneration() <= 0 || job.GetPlanningSnapshotRef() == "" || len(job.GetPlanningSnapshotDigest()) == 0 ||
		job.GetImportPlanIndexRef() == "" || len(job.GetImportPlanIndexDigest()) == 0 {
		return merr.WrapErrImportSysFailedMsg("import v3 published planning marker is incomplete")
	}
	prefix := metautil.BuildImportV3PlanningPath(job.GetJobID(), job.GetPlanningGeneration())
	snapshot := &datapb.PlanningSnapshot{}
	if err := loadImportV3Proto(c.ctx, c.meta.chunkManager, job.GetPlanningSnapshotRef(), prefix, job.GetPlanningSnapshotDigest(), snapshot); err != nil {
		return err
	}
	if snapshot.GetJobId() != job.GetJobID() || snapshot.GetGeneration() != job.GetPlanningGeneration() || snapshot.GetCollectionId() != job.GetCollectionID() {
		return merr.WrapErrDataIntegrityMsg("import v3 planning snapshot identity mismatch")
	}
	index := &datapb.ImportPlanIndex{}
	if err := loadImportV3Proto(c.ctx, c.meta.chunkManager, job.GetImportPlanIndexRef(), prefix, job.GetImportPlanIndexDigest(), index); err != nil {
		return err
	}
	if index.GetJobId() != job.GetJobID() || index.GetPlanningGeneration() != job.GetPlanningGeneration() ||
		index.GetSnapshotRef() != job.GetPlanningSnapshotRef() || !bytes.Equal(index.GetSnapshotDigest(), job.GetPlanningSnapshotDigest()) {
		return merr.WrapErrDataIntegrityMsg("import v3 plan index identity mismatch")
	}
	tasks := c.importMeta.GetTaskBy(c.ctx, WithType(ImportTaskV3Type), WithJob(job.GetJobID()))
	byID := make(map[int64]*importTaskV3, len(tasks))
	for _, generic := range tasks {
		task, ok := generic.(*importTaskV3)
		if !ok {
			return merr.WrapErrDataIntegrityMsg("import v3 published task set contains an unexpected task type")
		}
		byID[task.GetTaskID()] = task
	}
	if len(byID) != len(index.GetTasks()) {
		return merr.WrapErrDataIntegrityMsg("import v3 published task count mismatch: tasks=%d index=%d", len(byID), len(index.GetTasks()))
	}
	seen := make(map[int64]struct{}, len(index.GetTasks()))
	for _, entry := range index.GetTasks() {
		if entry == nil || entry.GetTaskId() == 0 || entry.GetPlanRef() == "" || len(entry.GetPlanDigest()) == 0 {
			return merr.WrapErrDataIntegrityMsg("import v3 plan index has an incomplete task entry")
		}
		if _, duplicate := seen[entry.GetTaskId()]; duplicate {
			return merr.WrapErrDataIntegrityMsg("import v3 plan index has duplicate task %d", entry.GetTaskId())
		}
		seen[entry.GetTaskId()] = struct{}{}
		task := byID[entry.GetTaskId()]
		if task == nil {
			return merr.WrapErrDataIntegrityMsg("import v3 plan index task %d is missing", entry.GetTaskId())
		}
		p := task.task.Load()
		if p.GetPlanningGeneration() != job.GetPlanningGeneration() || p.GetTaskPlanRef() != entry.GetPlanRef() || !bytes.Equal(p.GetTaskPlanDigest(), entry.GetPlanDigest()) {
			return merr.WrapErrDataIntegrityMsg("import v3 task %d does not match plan index", entry.GetTaskId())
		}
		plan := &datapb.ImportTaskPlan{}
		if err := loadImportV3Proto(c.ctx, c.meta.chunkManager, entry.GetPlanRef(), metautil.BuildImportV3ImportPlanPath(job.GetJobID(), entry.GetTaskId()), entry.GetPlanDigest(), plan); err != nil {
			return err
		}
		if plan.GetJobId() != job.GetJobID() || plan.GetTaskId() != entry.GetTaskId() || plan.GetPlanningGeneration() != job.GetPlanningGeneration() ||
			plan.GetPlanningSnapshotRef() != job.GetPlanningSnapshotRef() || !bytes.Equal(plan.GetPlanningSnapshotDigest(), job.GetPlanningSnapshotDigest()) ||
			len(plan.GetSegmentPlans()) != len(p.GetOutputSegmentIds()) || p.GetLogIdRange() == nil || plan.GetRequiredLogIds() != p.GetLogIdRange().GetEnd()-p.GetLogIdRange().GetBegin() {
			return merr.WrapErrDataIntegrityMsg("import v3 task %d plan contract mismatch", entry.GetTaskId())
		}
	}
	return nil
}

func (c *importChecker) planV3Job(job ImportJob) error {
	if _, err := validateImportV3Schema(c.meta, job.GetCollectionID(), job.GetSchema()); err != nil {
		return err
	}
	if job.GetPlanningSnapshotRef() != "" || job.GetImportPlanIndexRef() != "" {
		if err := c.validatePublishedV3Plan(job); err != nil {
			return err
		}
		return c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Importing))
	}
	if err := c.cleanupUnpublishedV3ImportTasks(job); err != nil {
		return err
	}
	sortSpec, err := v3DefaultSortSpec(job.GetSchema())
	if err != nil {
		return err
	}
	reshards := c.importMeta.GetTaskBy(c.ctx, WithType(ReshardTaskType), WithJob(job.GetJobID()))
	if len(reshards) == 0 {
		return merr.WrapErrImportSysFailedMsg("import v3 planning has no completed reshard tasks")
	}
	if len(job.GetVchannels()) == 0 {
		return merr.WrapErrDataIntegrityMsg("import v3 job has no vchannels")
	}
	fragments := make([]v3PlanningFragment, 0)
	for _, generic := range reshards {
		t := generic.(*reshardTask)
		if t.GetState() != datapb.ImportTaskStateV2_Completed {
			return nil
		}
		p := t.task.Load()
		manifest, err := loadReshardResultManifest(c.ctx, c.meta.chunkManager, p.GetResultRef(), p.GetOutputPrefix(), p.GetResultDigest())
		if err != nil {
			return err
		}
		if err := validateReshardManifest(manifest, job.GetJobID(), p.GetTaskId(), p.GetRunId(), p.GetTaskPlanDigest()); err != nil {
			return err
		}
		for _, f := range manifest.GetFragments() {
			fragments = append(fragments, v3PlanningFragment{ref: &datapb.FragmentRef{SourceTaskId: p.GetTaskId(), SourceManifestDigest: append([]byte(nil), p.GetResultDigest()...), VchannelOrdinal: f.GetVchannelOrdinal(), Vchannel: f.GetVchannel(), PartitionOrdinal: f.GetPartitionOrdinal(), PartitionId: f.GetPartitionId(), FragmentSeq: f.GetFragmentSeq(), Path: f.GetPath(), Rows: f.GetRows(), StartRow: 0, EndRow: f.GetRows(), LogicalBytes: f.GetLogicalBytes(), EstimatedFinalBytes: f.GetEstimatedFinalBytes(), PhysicalBytes: f.GetPhysicalBytes(), FirstSortKey: cloneSortKey(f.GetFirstSortKey()), LastSortKey: cloneSortKey(f.GetLastSortKey()), Format: f.GetFormat(), ColumnSizeStats: cloneColumnSizeStats(f.GetColumnSizeStats())}, bytes: f.GetEstimatedFinalBytes()})
		}
	}
	sort.Slice(fragments, func(i, j int) bool {
		a, b := fragments[i].ref, fragments[j].ref
		if a.GetVchannelOrdinal() != b.GetVchannelOrdinal() {
			return a.GetVchannelOrdinal() < b.GetVchannelOrdinal()
		}
		if a.GetPartitionOrdinal() != b.GetPartitionOrdinal() {
			return a.GetPartitionOrdinal() < b.GetPartitionOrdinal()
		}
		if a.GetSourceTaskId() != b.GetSourceTaskId() {
			return a.GetSourceTaskId() < b.GetSourceTaskId()
		}
		return a.GetFragmentSeq() < b.GetFragmentSeq()
	})
	gen := job.GetPlanningGeneration() + 1
	if gen <= 0 {
		gen = 1
	}
	targetSchema := typeutil.AppendSystemFields(job.GetSchema())
	temporarySchema := buildImportV3TemporarySchema(job.GetSchema(), importutilv2.IsBackup(job.GetOptions()))
	snapshot := &datapb.PlanningSnapshot{FormatVersion: 1, JobId: job.GetJobID(), Generation: gen, SortSpec: sortSpec, MergeFanInCap: int32(Params.DataCoordCfg.ImportMaxMergeFanIn.GetAsInt()), TargetSchema: targetSchema, TemporarySchema: temporarySchema, DataTs: job.GetDataTs(), CollectionId: job.GetCollectionID(), ClusterId: Params.CommonCfg.ClusterPrefix.GetValue()}
	target := getExpectedSegmentSize(c.meta, job.GetCollectionID(), job.GetSchema())
	var current *datapb.SegmentPlan
	var currentBytes int64
	for _, f := range fragments {
		if current == nil || current.GetVchannelOrdinal() != f.ref.GetVchannelOrdinal() || current.GetPartitionOrdinal() != f.ref.GetPartitionOrdinal() || (currentBytes > 0 && currentBytes+f.bytes > target) {
			current = &datapb.SegmentPlan{LogicalSegmentOrdinal: int64(len(snapshot.GetSegmentPlans())), VchannelOrdinal: f.ref.GetVchannelOrdinal(), Vchannel: f.ref.GetVchannel(), PartitionOrdinal: f.ref.GetPartitionOrdinal(), PartitionId: f.ref.GetPartitionId(), WriterSpecIndex: 0}
			snapshot.SegmentPlans = append(snapshot.SegmentPlans, current)
			currentBytes = 0
		}
		current.Fragments = append(current.Fragments, f.ref)
		current.PlannedRows += f.ref.GetRows()
		current.PlannedLogicalBytes += f.ref.GetLogicalBytes()
		current.EstimatedFinalBytes += f.ref.GetEstimatedFinalBytes()
		currentBytes += f.bytes
		snapshot.TotalRows += f.ref.GetRows()
		snapshot.TotalLogicalBytes += f.ref.GetLogicalBytes()
	}
	ttl, ttlErr := common.GetCollectionTTL(job.GetSchema().GetProperties())
	if ttlErr != nil {
		return ttlErr
	}
	writerFormat := Params.DataNodeCfg.StorageFormat.GetValue()
	maxPlannedRows := int64(1)
	for _, segment := range snapshot.GetSegmentPlans() {
		maxPlannedRows = max(maxPlannedRows, segment.GetPlannedRows())
	}
	writerSpec := &datapb.WriterSpec{
		FormatVersion:         1,
		TargetStorageVersion:  importStorageVersion(false),
		TargetSchemaVersion:   int64(targetSchema.GetVersion()),
		TargetSchemaDigest:    importV3Digest(mustMarshalProto(targetSchema)),
		WriterFormat:          writerFormat,
		V2Io:                  &datapb.V2PackedIOConfig{BufferSize: packed.DefaultWriteBufferSize, MultipartUploadSize: packed.DefaultMultiPartUploadSize},
		CollectionTtlNanos:    ttl.Nanoseconds(),
		PkStatsCapacity:       maxPlannedRows,
		BloomFilterType:       Params.CommonCfg.BloomFilterType.GetValue(),
		MaxBloomFalsePositive: Params.CommonCfg.MaxBloomFalsePositive.GetAsFloat(),
	}
	for _, function := range targetSchema.GetFunctions() {
		if function.GetType() == schemapb.FunctionType_BM25 {
			writerSpec.Bm25OutputFieldIds = append(writerSpec.Bm25OutputFieldIds, function.GetOutputFieldIds()...)
		}
	}
	sort.Slice(writerSpec.Bm25OutputFieldIds, func(i, j int) bool { return writerSpec.Bm25OutputFieldIds[i] < writerSpec.Bm25OutputFieldIds[j] })
	for _, field := range typeutil.GetAllFieldSchemas(targetSchema) {
		if field.GetDataType() == schemapb.DataType_Text {
			writerSpec.TextColumns = append(writerSpec.TextColumns, &datapb.TextColumnWriteSpec{
				FieldId: field.GetFieldID(), InlineThreshold: Params.DataNodeCfg.TextInlineThreshold.GetAsInt64(),
				MaxLobFileBytes: Params.DataNodeCfg.TextMaxLobFileBytes.GetAsInt64(), FlushThresholdBytes: Params.DataNodeCfg.TextFlushThresholdBytes.GetAsInt64(),
			})
		}
	}
	snapshot.WriterSpecs = []*datapb.WriterSpec{writerSpec}
	columnGroups := storagecommon.SplitColumns(typeutil.GetAllFieldSchemas(targetSchema), map[int64]storagecommon.ColumnStats{}, storagecommon.DefaultPolicies()...)
	columnGroups = storagecommon.FillColumnGroupFormats(columnGroups, writerFormat)
	snapshot.WriterSpecs[0].ColumnGroups = make([]*datapb.ColumnGroupSpec, 0, len(columnGroups))
	for _, group := range columnGroups {
		snapshot.WriterSpecs[0].ColumnGroups = append(snapshot.WriterSpecs[0].ColumnGroups, &datapb.ColumnGroupSpec{GroupId: group.GroupID, FieldIds: append([]int64(nil), group.Fields...), Format: group.Format})
	}
	snapshot.SchemaDigest = importV3Digest(mustMarshalProto(job.GetSchema()))
	writerSpecsDigest := importV3Digest(mustMarshalProto(&datapb.PlanningSnapshot{WriterSpecs: snapshot.GetWriterSpecs()}))
	planningPrefix := metautil.BuildImportV3PlanningPath(job.GetJobID(), gen)
	snapshotRef, snapshotDigest, err := writeImportV3Proto(c.ctx, c.meta.chunkManager, planningPrefix, "snapshot", snapshot)
	if err != nil {
		return err
	}
	planIndex := &datapb.ImportPlanIndex{FormatVersion: 1, JobId: job.GetJobID(), PlanningGeneration: gen, SnapshotRef: snapshotRef, SnapshotDigest: snapshotDigest}
	for _, segment := range snapshot.GetSegmentPlans() {
		_ = segment
	}
	activeChannelCount := 0
	for _, channel := range job.GetVchannels() {
		for _, s := range snapshot.GetSegmentPlans() {
			if s.GetVchannel() == channel {
				activeChannelCount++
				break
			}
		}
	}
	taskStart, _, err := c.alloc.AllocN(int64(activeChannelCount))
	if err != nil {
		return err
	}
	taskOrdinal := 0
	for _, channel := range job.GetVchannels() {
		segments := make([]*datapb.SegmentPlan, 0)
		for _, s := range snapshot.GetSegmentPlans() {
			if s.GetVchannel() == channel {
				segments = append(segments, s)
			}
		}
		if len(segments) == 0 {
			continue
		}
		// Worker result ordinals are task-local because output_segment_ids is
		// carried on one ImportTaskV3 record. Snapshot plans keep their global
		// order, while each task plan uses a cloned contiguous 0-based view.
		taskSegments := make([]*datapb.SegmentPlan, len(segments))
		for i, segment := range segments {
			taskSegments[i] = proto.Clone(segment).(*datapb.SegmentPlan)
			taskSegments[i].LogicalSegmentOrdinal = int64(i)
		}
		taskID := taskStart + int64(taskOrdinal)
		taskOrdinal++
		outputIDs := make([]int64, len(taskSegments))
		for j, segment := range taskSegments {
			seg, err := AllocImportSegment(c.ctx, c.alloc, c.meta, job.GetJobID(), taskID, job.GetCollectionID(), segment.GetPartitionId(), segment.GetVchannel(), job.GetDataTs(), datapb.SegmentLevel_L1, writerSpec.GetTargetStorageVersion())
			if err != nil {
				return err
			}
			if err := c.meta.UpdateSegmentsInfo(c.ctx, prepareImportV3Skeleton(seg.GetID(), int32(writerSpec.GetTargetSchemaVersion()))); err != nil {
				return err
			}
			outputIDs[j] = seg.GetID()
		}
		perSegment := int64(1 + len(writerSpec.GetBm25OutputFieldIds()))
		if writerSpec.GetTargetStorageVersion() == storage.StorageV2 {
			perSegment += int64(len(writerSpec.GetColumnGroups()))
		}
		required := int64(len(taskSegments)) * perSegment
		if required <= 0 || required > math.MaxUint32 {
			return merr.WrapErrImportSysFailedMsg("import v3 log id budget is invalid")
		}
		logBegin, logEnd, err := c.alloc.AllocN(required)
		if err != nil {
			return err
		}
		plan := &datapb.ImportTaskPlan{FormatVersion: 1, JobId: job.GetJobID(), TaskId: taskID, PlanningGeneration: gen, PlanningSnapshotDigest: snapshotDigest, SortSpec: proto.Clone(snapshot.GetSortSpec()).(*datapb.SortSpec), SegmentPlans: taskSegments, MergeFanIn: int32(snapshot.GetMergeFanInCap()), RequiredLogIds: required, WriterSpecsDigest: writerSpecsDigest, TaskSlot: 1, PlanningSnapshotRef: snapshotRef}
		planRef, planDigest, err := writeImportV3Proto(c.ctx, c.meta.chunkManager, metautil.BuildImportV3ImportPlanPath(job.GetJobID(), taskID), "plan", plan)
		if err != nil {
			return err
		}
		planIndex.Tasks = append(planIndex.Tasks, &datapb.ImportPlanIndexEntry{TaskId: taskID, PlanRef: planRef, PlanDigest: planDigest})
		task := newImportTaskV3(&datapb.ImportTaskV3{JobId: job.GetJobID(), TaskId: taskID, CollectionId: job.GetCollectionID(), State: datapb.ImportTaskV3_Pending, TaskPlanRef: planRef, TaskPlanDigest: planDigest, RunId: 1, NodeId: NullNodeID, OutputPrefix: metautil.BuildImportV3ImportOutputPath(job.GetJobID(), taskID), OutputSegmentIds: outputIDs, LogIdRange: &datapb.IDRange{Begin: logBegin, End: logEnd}, PlanningGeneration: gen, TaskSlot: 1}, c.importMeta, c.meta, c.alloc)
		if err := c.importMeta.AddTask(c.ctx, task); err != nil {
			return err
		}
	}
	indexRef, indexDigest, err := writeImportV3Proto(c.ctx, c.meta.chunkManager, planningPrefix, "index", planIndex)
	if err != nil {
		return err
	}
	return c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobPlanning(gen, snapshotRef, snapshotDigest, indexRef, indexDigest), UpdateJobState(internalpb.ImportJobState_Importing))
}

func mustMarshalProto(m proto.Message) []byte {
	b, _ := (proto.MarshalOptions{Deterministic: true}).Marshal(m)
	return b
}

func cloneSortKey(k *datapb.SortKey) *datapb.SortKey {
	if k == nil {
		return nil
	}
	return proto.Clone(k).(*datapb.SortKey)
}

func cloneColumnSizeStats(in []*datapb.ColumnSizeStat) []*datapb.ColumnSizeStat {
	out := make([]*datapb.ColumnSizeStat, 0, len(in))
	for _, stat := range in {
		if stat != nil {
			out = append(out, proto.Clone(stat).(*datapb.ColumnSizeStat))
		}
	}
	return out
}
