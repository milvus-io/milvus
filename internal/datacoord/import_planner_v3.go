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
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

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

const importV3Root = "import_v3"

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
	if existing := c.importMeta.GetTaskBy(c.ctx, WithType(ReshardTaskType), WithJob(job.GetJobID())); len(existing) > 0 {
		if len(existing) != len(bins) {
			return merr.WrapErrDataIntegrityMsg("import v3 reshard task set is incomplete: got=%d want=%d", len(existing), len(bins))
		}
		return c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_PreImporting))
	}
	start, _, err := c.alloc.AllocN(int64(len(bins)))
	if err != nil {
		return err
	}
	for i, bin := range bins {
		taskID := start + int64(i)
		sources := make([]*datapb.SourceFileSpec, 0, len(bin.sources))
		for _, source := range bin.sources {
			sources = append(sources, &datapb.SourceFileSpec{
				SourceOrdinal: int32(source.ordinal),
				File:          proto.Clone(source.file).(*internalpb.ImportFile),
				Format:        importV3SourceFormat(source.file),
				IsBackup:      importutilv2.IsBackup(job.GetOptions()),
			})
		}
		plan := &datapb.ReshardTaskPlan{
			FormatVersion: 1,
			JobId:         job.GetJobID(), TaskId: taskID, CollectionId: job.GetCollectionID(),
			SourceSchema:               proto.Clone(job.GetSchema()).(*schemapb.CollectionSchema),
			TemporarySchema:            buildImportV3TemporarySchema(job.GetSchema(), importutilv2.IsBackup(job.GetOptions())),
			Vchannels:                  append([]string(nil), job.GetVchannels()...),
			PartitionIds:               append([]int64(nil), job.GetPartitionIDs()...),
			SortSpec:                   v3DefaultSortSpec(job.GetSchema()),
			FragmentTargetLogicalBytes: 128 * 1024 * 1024,
			Sources:                    sources,
		}
		prefix := path.Join(importV3Root, strconv.FormatInt(job.GetJobID(), 10), "reshard", strconv.FormatInt(taskID, 10))
		ref, digest, err := writeImportV3Proto(c.ctx, c.meta.chunkManager, prefix, "plan", plan)
		if err != nil {
			return err
		}
		task := newReshardTask(&datapb.ReshardTask{JobId: job.GetJobID(), TaskId: taskID, CollectionId: job.GetCollectionID(), State: datapb.ReshardTask_Pending, TaskPlanRef: ref, TaskPlanDigest: digest, RunId: 1, NodeId: NullNodeID, OutputPrefix: path.Join(importV3Root, strconv.FormatInt(job.GetJobID(), 10), "reshard", strconv.FormatInt(taskID, 10), "run", "1"), TaskSlot: 1}, c.importMeta, c.meta)
		if err := c.importMeta.AddTask(c.ctx, task); err != nil {
			return err
		}
	}
	return c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_PreImporting))
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

func v3DefaultSortSpec(schema *schemapb.CollectionSchema) *datapb.SortSpec {
	pk, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return &datapb.SortSpec{FormatVersion: 1}
	}
	keyType := datapb.SortKeyType_SORT_KEY_TYPE_INT64
	if pk.GetDataType() == schemapb.DataType_VarChar {
		keyType = datapb.SortKeyType_SORT_KEY_TYPE_STRING
	}
	return &datapb.SortSpec{FormatVersion: 1, Fields: []*datapb.SortFieldSpec{{FieldId: pk.GetFieldID(), KeyType: keyType}}}
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

func (c *importChecker) planV3Job(job ImportJob) error {
	if _, err := validateImportV3Schema(c.meta, job.GetCollectionID(), job.GetSchema()); err != nil {
		return err
	}
	if job.GetPlanningSnapshotRef() != "" || job.GetImportPlanIndexRef() != "" {
		return c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Importing))
	}
	reshards := c.importMeta.GetTaskBy(c.ctx, WithType(ReshardTaskType), WithJob(job.GetJobID()))
	if len(reshards) == 0 {
		return c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Importing))
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
			fragments = append(fragments, v3PlanningFragment{ref: &datapb.FragmentRef{SourceTaskId: p.GetTaskId(), SourceManifestDigest: append([]byte(nil), p.GetResultDigest()...), VchannelOrdinal: f.GetVchannelOrdinal(), Vchannel: f.GetVchannel(), PartitionOrdinal: f.GetPartitionOrdinal(), PartitionId: f.GetPartitionId(), FragmentSeq: f.GetFragmentSeq(), Path: f.GetPath(), Rows: f.GetRows(), LogicalBytes: f.GetLogicalBytes(), EstimatedFinalBytes: f.GetEstimatedFinalBytes(), PhysicalBytes: f.GetPhysicalBytes(), FirstSortKey: cloneSortKey(f.GetFirstSortKey()), LastSortKey: cloneSortKey(f.GetLastSortKey()), Format: f.GetFormat(), ColumnSizeStats: cloneColumnSizeStats(f.GetColumnSizeStats())}, bytes: f.GetEstimatedFinalBytes()})
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
	snapshot := &datapb.PlanningSnapshot{FormatVersion: 1, JobId: job.GetJobID(), Generation: gen, SortSpec: v3DefaultSortSpec(job.GetSchema()), MergeFanInCap: int32(Params.DataCoordCfg.ImportMaxMergeFanIn.GetAsInt()), TargetSchema: targetSchema, TemporarySchema: temporarySchema, DataTs: job.GetDataTs(), CollectionId: job.GetCollectionID(), ClusterId: Params.CommonCfg.ClusterPrefix.GetValue()}
	target := int64(128 * 1024 * 1024)
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
	planningPrefix := path.Join(importV3Root, strconv.FormatInt(job.GetJobID(), 10), "planning", strconv.FormatInt(gen, 10))
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
			seg, err := AllocImportSegment(c.ctx, c.alloc, c.meta, job.GetJobID(), taskID, job.GetCollectionID(), segment.GetPartitionId(), segment.GetVchannel(), job.GetDataTs(), datapb.SegmentLevel_L1, importStorageVersion(importutilv2.IsL0Import(job.GetOptions())))
			if err != nil {
				return err
			}
			if err := c.meta.UpdateSegmentsInfo(c.ctx, SetSegmentIsInvisible(seg.GetID(), true)); err != nil {
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
		planRef, planDigest, err := writeImportV3Proto(c.ctx, c.meta.chunkManager, path.Join(planningPrefix, "tasks"), strconv.FormatInt(taskID, 10), plan)
		if err != nil {
			return err
		}
		planIndex.Tasks = append(planIndex.Tasks, &datapb.ImportPlanIndexEntry{TaskId: taskID, PlanRef: planRef, PlanDigest: planDigest})
		task := newImportTaskV3(&datapb.ImportTaskV3{JobId: job.GetJobID(), TaskId: taskID, CollectionId: job.GetCollectionID(), State: datapb.ImportTaskV3_Pending, TaskPlanRef: planRef, TaskPlanDigest: planDigest, RunId: 1, NodeId: NullNodeID, OutputPrefix: path.Join(importV3Root, strconv.FormatInt(job.GetJobID(), 10), "import", strconv.FormatInt(taskID, 10), "run", "1"), OutputSegmentIds: outputIDs, LogIdRange: &datapb.IDRange{Begin: logBegin, End: logEnd}, PlanningGeneration: gen, TaskSlot: 1}, c.importMeta, c.meta)
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
