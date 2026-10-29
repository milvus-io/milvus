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

// This file owns the Import V3 Planning stage: it reads the completed Reshard
// manifests, packs fragments into per-segment ImportTaskV3s, checks the disk
// quota and advances the job to Importing. It is deliberately small and
// deterministic: a restart keeps ready tasks, fills the missing logical tasks,
// and then advances the job state without depending on map iteration. The pure
// packing helpers live at the bottom of the file.

import (
	"sort"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func planImportV3(jc *importV3JobContext) error {
	ctx, job := jc.ctx, jc.job
	cfg := getImportV3Config()
	if _, err := validateImportV3Schema(jc.meta, job.GetCollectionID(), job.GetSchema()); err != nil {
		return err
	}
	if err := validateImportV3StorageVersion(job.GetSchema(), cfg.storageVersion); err != nil {
		return err
	}
	existingFragments, err := loadExistingFragments(jc)
	if err != nil {
		return err
	}
	// Planning only validates that the schema yields a sort order; dispatch
	// re-derives it from the frozen job schema.
	if _, err := importutilv2.SortFieldIDs(job.GetSchema()); err != nil {
		return err
	}
	reshards := jc.importMeta.GetTaskByJob(ctx, job.GetJobID(), WithType(ReshardTaskType))
	if len(reshards) == 0 {
		return merr.WrapErrImportSysFailedMsg("import v3 planning has no completed reshard tasks")
	}
	if len(job.GetVchannels()) == 0 {
		return merr.WrapErrDataIntegrityMsg("import v3 job has no vchannels")
	}
	fragments := make([]importV3PlanningFragment, 0)
	for _, generic := range reshards {
		t := generic.(*reshardTask)
		if t.GetState() != datapb.ImportTaskStateV2_Completed {
			return nil
		}
		task := t.task.Load()
		manifest, err := loadReshardResultManifest(ctx, jc.meta.chunkManager, task.GetJobId(), task.GetTaskId(), task.GetRunId())
		if err != nil {
			return err
		}
		if err := validateReshardManifest(manifest); err != nil {
			return err
		}
		for _, f := range manifest.GetFragments() {
			fragments = append(fragments, importV3PlanningFragment{
				sourceID: task.GetTaskId(), channelIndex: f.GetVchannelIndex(), partitionID: f.GetPartitionId(),
				seq: f.GetSeq(), path: f.GetPath(), rows: f.GetRows(), bytes: f.GetLogicalBytes(),
			})
		}
	}
	sort.Slice(fragments, func(i, j int) bool {
		a, b := fragments[i], fragments[j]
		if a.channelIndex != b.channelIndex {
			return a.channelIndex < b.channelIndex
		}
		if a.partitionID != b.partitionID {
			return a.partitionID < b.partitionID
		}
		if a.sourceID != b.sourceID {
			return a.sourceID < b.sourceID
		}
		return a.seq < b.seq
	})
	segmentSchema := typeutil.AppendSystemFields(job.GetSchema())
	// The segment target and the fragment bytes below must stay in the same
	// normalized decoded-bytes metric: changing one side's metric without the
	// other reopens a fragment/segment size mismatch.
	target := getExpectedSegmentSize(jc.meta, job.GetCollectionID(), job.GetSchema())
	jobPartitions := make(map[int64]struct{}, len(job.GetPartitionIDs()))
	for _, partitionID := range job.GetPartitionIDs() {
		jobPartitions[partitionID] = struct{}{}
	}
	for _, f := range fragments {
		if f.channelIndex < 0 || int(f.channelIndex) >= len(job.GetVchannels()) {
			return merr.WrapErrDataIntegrityMsg("import v3 fragment channel ordinal is out of range")
		}
		if _, ok := jobPartitions[f.partitionID]; !ok {
			return merr.WrapErrDataIntegrityMsg("import v3 fragment carries partition %d outside the job", f.partitionID)
		}
	}
	totalFragmentBytes := lo.SumBy(fragments, func(f importV3PlanningFragment) int64 { return f.bytes })
	totalRows := lo.SumBy(fragments, func(f importV3PlanningFragment) int64 { return f.rows })
	// Empty input job (zero total rows): shortcut to Uncommitted/Completed
	// exactly like the legacy import path does for empty preimports. The
	// Reshard-stage latency was already recorded by the resharding handler; only
	// the total latency remains. buildImportV3SegmentPlans always yields at least one
	// plan for non-empty fragments, so a non-empty job never lands here.
	if totalRows == 0 {
		if len(existingFragments) > 0 {
			return merr.WrapErrDataIntegrityMsg("import v3 planning has tasks but no segment plan")
		}
		if err := jc.finishEmptyJob(); err != nil {
			return err
		}
		if jc.job.GetAutoCommit() {
			jc.metrics.observeTotal(job.GetTR().ElapseSpan())
		}
		return nil
	}
	segmentPlans := buildImportV3SegmentPlans(fragments, job.GetVchannels(), target)
	ws, err := writerSpec(segmentSchema, cfg)
	if err != nil {
		return err
	}
	taskSpecs := segmentPlans
	// totalFragmentBytes is the normalized data volume (same metric as the
	// legacy CheckDiskQuota's TotalMemorySize); CheckImportV3DiskQuota reserves
	// it as the job's object-store footprint (merge intermediates are local).
	requestedDiskSize, err := CheckImportV3DiskQuota(ctx, job, jc.meta, jc.importMeta, totalFragmentBytes)
	if err != nil {
		return err
	}
	mlog.Info(ctx, "import v3 planning packed segment plans",
		mlog.FieldJobID(job.GetJobID()),
		mlog.Int("reshardTasks", len(reshards)),
		mlog.Int("fragments", len(fragments)),
		mlog.Int("segmentPlans", len(segmentPlans)),
		mlog.Int("existingTasks", len(existingFragments)),
		mlog.Int64("rows", totalRows),
		mlog.Int64("logicalBytes", totalFragmentBytes),
		mlog.Int64("segmentTargetBytes", target),
		mlog.Int64("requestedDiskSize", requestedDiskSize),
	)

	var missing []importTaskV3Spec
	if len(existingFragments) == 0 {
		missing = taskSpecs
	} else {
		missing, err = missingImportTaskV3Specs(fragments, existingFragments, job.GetVchannels(), target)
		if err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		taskStart, _, err := jc.alloc.AllocN(int64(len(missing)))
		if err != nil {
			return err
		}
		segmentStart, _, err := jc.alloc.AllocN(int64(len(missing)))
		if err != nil {
			return err
		}
		for i, spec := range missing {
			if err := createImportTaskV3(jc, ws, taskStart+int64(i), segmentStart+int64(i), spec); err != nil {
				return err
			}
		}
	}
	return jc.transitionTo(internalpb.ImportJobState_Importing, UpdateRequestedDiskSize(requestedDiskSize))
}

// loadExistingFragments returns the fragment refs already owned by the job's
// persisted ImportTaskV3 records. Fragment ownership is the only planning fact
// kept on the task record; it is what Planning recovery needs to fill missing
// coverage without re-assigning fragments.
func loadExistingFragments(jc *importV3JobContext) ([]*importv3pb.FragmentRef, error) {
	job := jc.job
	tasks := jc.importMeta.GetTaskByJob(jc.ctx, job.GetJobID(), WithType(ImportTaskV3Type))
	existing := make([]*importv3pb.FragmentRef, 0)
	for _, generic := range tasks {
		task, ok := generic.(*importTaskV3)
		if !ok {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 planning task set contains an unexpected task type")
		}
		if task.GetState() == datapb.ImportTaskStateV2_Failed {
			return nil, merr.WrapErrImportSysFailedMsg("import v3 task %d failed: %s", task.GetTaskID(), task.GetReason())
		}
		t := task.task.Load()
		if t.GetSegmentId() == 0 {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 task %d has no segment", t.GetTaskId())
		}
		if t.GetCollectionId() != job.GetCollectionID() {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 task identity mismatch")
		}
		if t.GetVchannel() == "" || t.GetPartitionId() == 0 || len(t.GetFragments()) == 0 {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 task segment is incomplete")
		}
		existing = append(existing, t.GetFragments()...)
	}
	return existing, nil
}

func createImportTaskV3(
	jc *importV3JobContext,
	writerSpec *importv3pb.WriterSpec,
	taskID, segmentID int64,
	spec importTaskV3Spec,
) error {
	job := jc.job
	if job.GetDataTs() == 0 {
		// DataTs drives the formal writer's row timestamps. Allocating log IDs
		// here and discarding them only advances the allocator; dispatch would
		// still carry zero and corrupt TTL/commit fencing, so fail the planning
		// step instead.
		return merr.WrapErrImportSysFailedMsg("import v3 job %d has no data timestamp", job.GetJobID())
	}
	perSegment, err := importV3LogRangeWidth(writerSpec, bm25OutputFieldCount(job.GetSchema()))
	if err != nil {
		return err
	}
	logBegin, logEnd, err := jc.alloc.AllocN(perSegment)
	if err != nil {
		return err
	}
	// The task is written directly as Pending, carrying the reserved segment id
	// but no segment record: only the dispatch that actually runs the task
	// creates its segment, so a task never owns a segment it does not use.
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: job.GetJobID(), TaskId: taskID, CollectionId: job.GetCollectionID(),
		State: datapb.ImportTaskStateV2_Pending, RunId: 1, NodeId: NullNodeID,
		SegmentId: segmentID, LogRange: &datapb.IDRange{Begin: logBegin, End: logEnd},
		Rows:      spec.rows,
		Fragments: append([]*importv3pb.FragmentRef(nil), spec.fragments...),
		Vchannel:  spec.channel, PartitionId: spec.partitionID,
		CreatedTime: time.Now().Format(time.RFC3339),
	}, jc.importMeta, jc.meta, jc.alloc)
	return jc.importMeta.AddTask(jc.ctx, task)
}

type importV3PlanningFragment struct {
	sourceID     int64
	channelIndex int32
	// partitionID is the fragment's absolute target partition, carried by the
	// fragment descriptor itself. Planning never re-interprets it against the
	// job's partition array, so tasks with different partition sets (snapshot
	// import partition mapping) pack correctly.
	partitionID int64
	seq         int64
	path        string
	rows        int64
	// bytes carries the fragment's normalized decoded bytes as reported by the
	// reshard manifest (see writeReshardFragment), the same metric the fragment
	// target uses for cutting. Planning packs dataCoord.segment.maxSize against
	// this metric.
	bytes int64
}

type importTaskV3Spec struct {
	channel     string
	partitionID int64
	fragments   []*importv3pb.FragmentRef
	rows        int64
}

// buildImportV3SegmentPlans packs the canonical fragment sequence into per-bucket
// task specs. Each fragment carries its absolute partition id, so no job-wide
// partition array is consulted. Both the target and f.bytes are normalized
// decoded logical bytes (see importV3PlanningFragment.bytes): with the default 1GiB
// segment target and 128MiB fragment target a plan holds on the order of 8
// fragments, inside fragmentMergeFanIn (16), so segments usually merge in
// one round; the exact count is schema-dependent.
func buildImportV3SegmentPlans(fragments []importV3PlanningFragment, vchannels []string, target int64) []importTaskV3Spec {
	specs := make([]importTaskV3Spec, 0)
	var current *importTaskV3Spec
	var currentBytes int64
	for _, f := range fragments {
		vchannel := vchannels[f.channelIndex]
		if current == nil || current.channel != vchannel || current.partitionID != f.partitionID || (currentBytes > 0 && currentBytes+f.bytes > target) {
			specs = append(specs, importTaskV3Spec{channel: vchannel, partitionID: f.partitionID})
			current = &specs[len(specs)-1]
			currentBytes = 0
		}
		current.fragments = append(current.fragments, &importv3pb.FragmentRef{Path: f.path, Rows: f.rows})
		current.rows += f.rows
		currentBytes += f.bytes
	}
	return specs
}

// missingImportTaskV3Specs returns the per-segment plans that must still be
// created during Planning recovery. Existing tasks are kept as-is; only
// fragment coverage matters, and newly created tasks are packed from the
// canonical fragment sequence after skipping fragments already owned by a
// ready task.
//
// A path identifies a fragment ({task}/{run}_{seq}, and a reshard retry always
// gets a new run_id), so the match is by path; the row count is checked
// separately, so a same-path row-count mismatch reports the two counts instead
// of pointing at a fragment that is in fact present. A ref whose path is no
// longer in the input, and two refs for one path, are integrity errors.
func missingImportTaskV3Specs(
	fragments []importV3PlanningFragment,
	existing []*importv3pb.FragmentRef,
	vchannels []string,
	target int64,
) ([]importTaskV3Spec, error) {
	byPath := make(map[string]importV3PlanningFragment, len(fragments))
	for _, f := range fragments {
		byPath[f.path] = f
	}
	covered := make(map[string]struct{}, len(fragments))
	for _, ref := range existing {
		f, ok := byPath[ref.GetPath()]
		if !ok {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 task references fragment outside current planning input: %s", ref.GetPath())
		}
		if ref.GetRows() != f.rows {
			return nil, merr.WrapErrDataIntegrityMsg(
				"import v3 task fragment %s row count %d does not match the manifest row count %d",
				ref.GetPath(),
				ref.GetRows(),
				f.rows,
			)
		}
		if _, ok := covered[ref.GetPath()]; ok {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 tasks contain duplicate fragment: %s", ref.GetPath())
		}
		covered[ref.GetPath()] = struct{}{}
	}
	missingFragments := make([]importV3PlanningFragment, 0, len(fragments)-len(covered))
	for _, f := range fragments {
		if _, ok := covered[f.path]; !ok {
			missingFragments = append(missingFragments, f)
		}
	}
	if len(missingFragments) == 0 {
		return nil, nil
	}
	return buildImportV3SegmentPlans(missingFragments, vchannels, target), nil
}
