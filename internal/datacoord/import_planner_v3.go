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

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
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

func writeImportV3Proto(ctx context.Context, cm storage.ChunkManager, ref string, msg proto.Message) ([]byte, error) {
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(msg)
	if err != nil {
		return nil, merr.WrapErrSerializationFailed(err, "marshal import v3 object")
	}
	digest := importV3Digest(payload)
	if existing, err := cm.Read(ctx, ref); err == nil {
		if !bytes.Equal(existing, payload) {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 object already exists with different content: %s", ref)
		}
		return digest, nil
	}
	if err := cm.Write(ctx, ref, payload); err != nil {
		return nil, merr.Wrap(err, "write import v3 object")
	}
	return digest, nil
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
		return c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Importing))
	}
	if existing := c.importMeta.GetTaskBy(c.ctx, WithType(ReshardTaskType), WithJob(job.GetJobID())); len(existing) > 0 {
		if len(existing) != len(files) {
			return merr.WrapErrDataIntegrityMsg("import v3 reshard task set is incomplete: got=%d want=%d", len(existing), len(files))
		}
		return c.importMeta.UpdateJob(c.ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_PreImporting))
	}
	start, _, err := c.alloc.AllocN(int64(len(files)))
	if err != nil {
		return err
	}
	for i, file := range files {
		taskID := start + int64(i)
		plan := &datapb.ReshardTaskPlan{
			FormatVersion: 1,
			JobId:         job.GetJobID(), TaskId: taskID, CollectionId: job.GetCollectionID(),
			SourceSchema:               proto.Clone(job.GetSchema()).(*schemapb.CollectionSchema),
			TemporarySchema:            typeutil.AppendSystemFields(job.GetSchema()),
			Vchannels:                  append([]string(nil), job.GetVchannels()...),
			PartitionIds:               append([]int64(nil), job.GetPartitionIDs()...),
			SortSpec:                   v3DefaultSortSpec(job.GetSchema()),
			FragmentTargetLogicalBytes: 128 * 1024 * 1024,
			Sources:                    []*datapb.SourceFileSpec{{SourceOrdinal: 0, File: proto.Clone(file).(*internalpb.ImportFile), Format: importV3SourceFormat(file), IsBackup: importutilv2.IsBackup(job.GetOptions())}},
		}
		ref := path.Join(importV3Root, strconv.FormatInt(job.GetJobID(), 10), "reshard", strconv.FormatInt(taskID, 10), "plan.pb")
		digest, err := writeImportV3Proto(c.ctx, c.meta.chunkManager, ref, plan)
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
		if field.GetIsFunctionOutput() {
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
	snapshot.WriterSpecs = []*datapb.WriterSpec{{FormatVersion: 1, TargetStorageVersion: importStorageVersion(false), TargetSchemaVersion: job.GetSchema().GetVersion()}}
	snapshot.SchemaDigest = importV3Digest(mustMarshalProto(job.GetSchema()))
	snapshotRef := path.Join(importV3Root, strconv.FormatInt(job.GetJobID(), 10), "planning", strconv.FormatInt(gen, 10), "snapshot.pb")
	snapshotDigest, err := writeImportV3Proto(c.ctx, c.meta.chunkManager, snapshotRef, snapshot)
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
		taskID := taskStart + int64(taskOrdinal)
		taskOrdinal++
		outputIDs := make([]int64, len(segments))
		for j, segment := range segments {
			seg, err := AllocImportSegment(c.ctx, c.alloc, c.meta, job.GetJobID(), taskID, job.GetCollectionID(), segment.GetPartitionId(), segment.GetVchannel(), job.GetDataTs(), datapb.SegmentLevel_L1, importStorageVersion(false))
			if err != nil {
				return err
			}
			seg.SegmentInfo.IsInvisible = true
			if err := c.meta.UpdateSegmentsInfo(c.ctx, func(pack *updateSegmentPack) bool {
				s := pack.Get(seg.GetID())
				if s == nil {
					return false
				}
				s.IsInvisible = true
				return true
			}); err != nil {
				return err
			}
			outputIDs[j] = seg.GetID()
		}
		required := int64(len(segments) * 16)
		if required <= 0 || required > math.MaxUint32 {
			return merr.WrapErrImportSysFailedMsg("import v3 log id budget is invalid")
		}
		logBegin, logEnd, err := c.alloc.AllocN(required)
		if err != nil {
			return err
		}
		plan := &datapb.ImportTaskPlan{FormatVersion: 1, JobId: job.GetJobID(), TaskId: taskID, PlanningGeneration: gen, PlanningSnapshotDigest: snapshotDigest, SortSpec: proto.Clone(snapshot.GetSortSpec()).(*datapb.SortSpec), SegmentPlans: segments, MergeFanIn: int32(snapshot.GetMergeFanInCap()), RequiredLogIds: required, TaskSlot: 1, PlanningSnapshotRef: snapshotRef}
		planRef := path.Join(importV3Root, strconv.FormatInt(job.GetJobID(), 10), "planning", strconv.FormatInt(gen, 10), "tasks", strconv.FormatInt(taskID, 10)+".pb")
		planDigest, err := writeImportV3Proto(c.ctx, c.meta.chunkManager, planRef, plan)
		if err != nil {
			return err
		}
		planIndex.Tasks = append(planIndex.Tasks, &datapb.ImportPlanIndexEntry{TaskId: taskID, PlanRef: planRef, PlanDigest: planDigest})
		task := newImportTaskV3(&datapb.ImportTaskV3{JobId: job.GetJobID(), TaskId: taskID, CollectionId: job.GetCollectionID(), State: datapb.ImportTaskV3_Pending, TaskPlanRef: planRef, TaskPlanDigest: planDigest, RunId: 1, NodeId: NullNodeID, OutputPrefix: path.Join(importV3Root, strconv.FormatInt(job.GetJobID(), 10), "import", strconv.FormatInt(taskID, 10), "run", "1"), OutputSegmentIds: outputIDs, LogIdRange: &datapb.IDRange{Begin: logBegin, End: logEnd}, PlanningGeneration: gen, TaskSlot: 1}, c.importMeta, c.meta)
		if err := c.importMeta.AddTask(c.ctx, task); err != nil {
			return err
		}
	}
	indexRef := path.Join(importV3Root, strconv.FormatInt(job.GetJobID(), 10), "planning", strconv.FormatInt(gen, 10), "index.pb")
	indexDigest, err := writeImportV3Proto(c.ctx, c.meta.chunkManager, indexRef, planIndex)
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
