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

// This file owns the two Import V3 planning stages:
//
//   - reshardTaskPlanner (Pending tail): groups the job's source files into
//     ReshardTasks by stable BFD packing and advances the job to Resharding.
//   - importV3Planner (Planning): reads the reshard manifests, packs fragments
//     into per-segment ImportTaskV3s, checks disk quota and advances the job
//     to Importing.
//
// Both planners are deliberately small and deterministic. A restart keeps
// ready tasks, fills the missing logical tasks, and then advances the job
// state without depending on map iteration. Pure packing helpers live at the
// bottom of the file.

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	importbinlog "github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/internal/util/importutilv2/reshardmem"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// reshardTaskPlanner turns an Import V3 job's source files into ReshardTasks.
// It is recovery-safe: files already covered by an existing task are skipped,
// so a crash between task creation and the job-state write replays cleanly.
type reshardTaskPlanner struct {
	jc *importV3JobContext
}

func newReshardTaskPlanner(jc *importV3JobContext) *reshardTaskPlanner {
	return &reshardTaskPlanner{jc: jc}
}

// CreateTasks groups the not-yet-covered source files and creates one
// ReshardTask per group, then advances the job to Resharding.
func (p *reshardTaskPlanner) CreateTasks() error {
	ctx, job := p.jc.ctx, p.jc.job
	files := job.GetFiles()
	// Planning only validates the sort spec; dispatch re-derives it from the
	// frozen job schema.
	factory := newImportV3PlanFactory(job)
	if _, err := factory.sortSpec(); err != nil {
		return err
	}
	covered, err := p.coveredSourceIDs(ctx, job)
	if err != nil {
		return err
	}
	missingFiles := lo.Filter(files, func(file *internalpb.ImportFile, _ int) bool {
		_, ok := covered[file.GetId()]
		return !ok
	})
	// Nothing to plan when every source already has a task: skip the BFD
	// grouping entirely so a covered-retry only advances the job state.
	var missing [][]reshardSource
	if len(missingFiles) > 0 {
		missing, err = p.groupSources(ctx, job, missingFiles, importutilv2.IsBackup(job.GetOptions()))
		if err != nil {
			return err
		}
	}
	if len(missing) > 0 {
		start, _, err := p.jc.alloc.AllocN(int64(len(missing)))
		if err != nil {
			return err
		}
		for i, sources := range missing {
			if err := p.createTask(ctx, job, start+int64(i), sources); err != nil {
				return err
			}
		}
	}
	if err := p.jc.importMeta.UpdateJob(ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Resharding)); err != nil {
		return err
	}
	pendingDuration := job.GetTR().RecordSpan()
	p.jc.metrics.observeStage(metrics.ImportStagePending, pendingDuration)
	return nil
}

func (p *reshardTaskPlanner) createTask(ctx context.Context, job ImportJob, taskID int64, sources []reshardSource) error {
	sourceIDs := lo.Map(sources, func(s reshardSource, _ int) int64 { return s.file.GetId() })
	fragmentSize, err := importFragmentSizeBytes()
	if err != nil {
		return err
	}
	slot := calculateReshardTaskSlot(reshardmem.Model{
		ReadBuffer:       Params.DataNodeCfg.ImportBaseBufferSize.GetAsInt64(),
		FragmentTarget:   fragmentSize,
		FlushConcurrency: Params.DataCoordCfg.ReshardFlushConcurrency.GetAsInt64(),
		ExpansionFactor:  Params.DataCoordCfg.ReshardMemoryExpansionFactor.GetAsFloat(),
	}, reshardmem.MemoryPerSlot(Params.DataNodeCfg.WorkerSlotUnit.GetAsInt64()),
		int64(len(job.GetVchannels())*len(job.GetPartitionIDs())),
		Params.DataCoordCfg.ReshardResidentBucketCap.GetAsInt64(),
		int64(len(typeutil.GetAllFieldSchemas(newImportV3PlanFactory(job).tempSchema()))),
	)
	task := newReshardTask(&datapb.ReshardTask{JobId: job.GetJobID(), TaskId: taskID, CollectionId: job.GetCollectionID(), State: datapb.ImportTaskStateV2_Pending, RunId: 1, NodeId: NullNodeID, Slot: slot, SourceIds: sourceIDs}, p.jc.importMeta, p.jc.meta, p.jc.alloc)
	return p.jc.importMeta.AddTask(ctx, task)
}

// coveredSourceIDs returns the source file IDs already owned by the job's
// ReshardTasks, validating the task set against the job's file list.
func (p *reshardTaskPlanner) coveredSourceIDs(ctx context.Context, job ImportJob) (map[int64]struct{}, error) {
	tasks := p.jc.importMeta.GetTaskByJob(ctx, job.GetJobID(), WithType(ReshardTaskType))
	jobFiles := make(map[int64]struct{}, len(job.GetFiles()))
	for _, file := range job.GetFiles() {
		jobFiles[file.GetId()] = struct{}{}
	}
	covered := make(map[int64]struct{}, len(jobFiles))
	for _, generic := range tasks {
		task, ok := generic.(*reshardTask)
		if !ok {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task set contains an unexpected task type")
		}
		if task.GetState() == datapb.ImportTaskStateV2_Failed {
			return nil, merr.WrapErrImportSysFailedMsg("import v3 reshard task %d failed: %s", task.GetTaskID(), task.GetReason())
		}
		t := task.task.Load()
		if t.GetCollectionId() != job.GetCollectionID() {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task identity mismatch")
		}
		if len(t.GetSourceIds()) == 0 {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task has no source")
		}
		for _, fileID := range t.GetSourceIds() {
			if _, ok := jobFiles[fileID]; !ok {
				return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task contains source %d outside the job", fileID)
			}
			if _, ok := covered[fileID]; ok {
				return nil, merr.WrapErrDataIntegrityMsg("import v3 reshard task set contains duplicate source %d", fileID)
			}
			covered[fileID] = struct{}{}
		}
	}
	return covered, nil
}

type reshardSource struct {
	file *internalpb.ImportFile
	size int64
}

// groupSources implements stable one-dimensional BFD. ImportFile is the atom:
// no path/file splitting, no backtracking, and an oversized file owns one bin.
// Equal-size files keep their job ordinal; equal-fit bins keep their creation
// ordinal. Only the grouped sources are returned; bin sizes are a transient
// packing detail.
//
// The bin target mirrors RegroupImportFiles: one task holds at most one
// segment's worth of data per (vchannel, partition) bucket, capped by
// MaxSizeInMBPerImportTask. Import V3 never handles L0, so the segment target
// comes straight from getExpectedSegmentSize.
//
// Source size is the preimport-measured decoded (logical) size where available,
// so the packing unit matches the target's unit. Backup sources (which skip
// preimport) and files whose preimport produced no size fall back to the physical
// object size, which is a conservative proxy (compressed bytes <= decoded bytes).
func (p *reshardTaskPlanner) groupSources(ctx context.Context, job ImportJob, files []*internalpb.ImportFile, backup bool) ([][]reshardSource, error) {
	logicalByFile := preImportV2LogicalBytes(p.jc.importMeta.GetTaskByJob(ctx, job.GetJobID(), WithType(PreImportV2TaskType)))
	// Expand each file's object list first (serial; it is a per-file prefix
	// listing), then stat all objects across all files concurrently. The
	// checker goroutine serially processes every V3 job, so a serial per-object
	// cm.Size loop would delay state transitions of all other jobs for the
	// duration of one backup import's sizing (V2 sizes files in parallel on
	// DataNodes during PreImport).
	type sizedFile struct {
		file  *internalpb.ImportFile
		paths []string
		size  int64
	}
	expanded := make([]sizedFile, 0, len(files))
	for _, file := range files {
		paths := append([]string(nil), file.GetPaths()...)
		if backup {
			insertObjects, deltaObjects, err := importbinlog.ExpandObjects(ctx, p.jc.meta.chunkManager, paths)
			if err != nil {
				return nil, err
			}
			paths = paths[:0]
			for _, fieldPaths := range insertObjects {
				paths = append(paths, fieldPaths...)
			}
			paths = append(paths, deltaObjects...)
		}
		expanded = append(expanded, sizedFile{file: file, paths: paths})
	}
	sizes := make([]int64, len(expanded))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(16)
	for i := range expanded {
		g.Go(func() error {
			size, err := storage.GetFilesSize(gctx, expanded[i].paths, p.jc.meta.chunkManager)
			if err != nil {
				return merr.Wrapf(err, "estimate import v3 source file %d", expanded[i].file.GetId())
			}
			sizes[i] = size
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	sources := make([]reshardSource, 0, len(expanded))
	for i := range expanded {
		size := sizes[i]
		if logical, ok := logicalByFile[expanded[i].file.GetId()]; ok && logical > 0 {
			size = logical
		}
		sources = append(sources, reshardSource{file: expanded[i].file, size: size})
	}
	sort.SliceStable(sources, func(i, j int) bool {
		return sources[i].size > sources[j].size
	})
	fragmentTarget := Params.DataCoordCfg.ImportFragmentSizeInMB.GetAsInt64() * 1024 * 1024
	threshold := Params.DataCoordCfg.MaxSizeInMBPerImportTask.GetAsInt64() * 1024 * 1024
	target := min(fragmentTarget*int64(len(job.GetVchannels()))*int64(len(job.GetPartitionIDs())*2), threshold)
	if target <= 0 {
		return nil, merr.WrapErrImportSysFailedMsg("import v3 reshard BFD target must be positive")
	}
	type bin struct {
		sources []reshardSource
		size    int64
	}
	bins := make([]bin, 0)
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
			bins = append(bins, bin{sources: []reshardSource{source}, size: source.size})
			continue
		}
		bins[best].sources = append(bins[best].sources, source)
		bins[best].size += source.size
	}
	return lo.Map(bins, func(b bin, _ int) []reshardSource { return b.sources }), nil
}

// importV3Planner turns the completed ReshardTasks' manifests into per-segment
// ImportTaskV3s and advances the job to Importing.
type importV3Planner struct {
	jc *importV3JobContext
}

func newImportV3Planner(jc *importV3JobContext) *importV3Planner {
	return &importV3Planner{jc: jc}
}

func (p *importV3Planner) Plan() error {
	ctx, job := p.jc.ctx, p.jc.job
	if _, err := validateImportV3Schema(p.jc.meta, job.GetCollectionID(), job.GetSchema()); err != nil {
		return err
	}
	if err := validateImportV3StorageVersion(job.GetSchema()); err != nil {
		return err
	}
	if err := p.cleanupPreparingTasks(ctx, job); err != nil {
		return err
	}
	existingFragments, err := p.loadExistingFragments(ctx, job)
	if err != nil {
		return err
	}
	// Planning only validates the sort spec; dispatch re-derives it from the
	// frozen job schema.
	factory := newImportV3PlanFactory(job)
	if _, err := factory.sortSpec(); err != nil {
		return err
	}
	reshards := p.jc.importMeta.GetTaskByJob(ctx, job.GetJobID(), WithType(ReshardTaskType))
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
		manifest, err := loadReshardResultManifest(ctx, p.jc.meta.chunkManager, task.GetJobId(), task.GetTaskId(), task.GetRunId())
		if err != nil {
			return err
		}
		if err := validateReshardManifest(manifest); err != nil {
			return err
		}
		for _, f := range manifest.GetFragments() {
			fragments = append(fragments, importV3PlanningFragment{
				sourceID: task.GetTaskId(), channelIndex: f.GetChannelIndex(), partitionID: f.GetPartitionId(),
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
	targetSchema := typeutil.AppendSystemFields(job.GetSchema())
	// The segment target and the fragment bytes below must stay in the same
	// normalized decoded-bytes metric: changing one side's metric without the
	// other reopens a fragment/segment size mismatch.
	target := getExpectedSegmentSize(p.jc.meta, job.GetCollectionID(), job.GetSchema())
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
		state := internalpb.ImportJobState_Uncommitted
		if job.GetAutoCommit() {
			state = internalpb.ImportJobState_Completed
		}
		if err := p.jc.importMeta.UpdateJob(ctx, job.GetJobID(), UpdateJobState(state)); err != nil {
			return err
		}
		if state == internalpb.ImportJobState_Completed {
			p.jc.metrics.observeTotal(job.GetTR().ElapseSpan())
		}
		return nil
	}
	segmentPlans := buildImportV3SegmentPlans(fragments, job.GetVchannels(), target)
	writerSpec, err := factory.writerSpec(targetSchema)
	if err != nil {
		return err
	}
	taskSpecs := segmentPlans
	// totalFragmentBytes is the normalized data volume (same metric as the
	// legacy CheckDiskQuota's TotalMemorySize); CheckImportV3DiskQuota reserves
	// it as the job's object-store footprint (merge intermediates are local).
	requestedDiskSize, err := CheckImportV3DiskQuota(ctx, job, p.jc.meta, p.jc.importMeta, totalFragmentBytes)
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
		taskStart, _, err := p.jc.alloc.AllocN(int64(len(missing)))
		if err != nil {
			return err
		}
		segmentStart, _, err := p.jc.alloc.AllocN(int64(len(missing)))
		if err != nil {
			return err
		}
		for i, spec := range missing {
			if err := p.createTask(ctx, job, writerSpec, taskStart+int64(i), segmentStart+int64(i), spec); err != nil {
				return err
			}
		}
	}
	if err := p.jc.importMeta.UpdateJob(ctx, job.GetJobID(), UpdateJobState(internalpb.ImportJobState_Importing), UpdateRequestedDiskSize(requestedDiskSize)); err != nil {
		return err
	}
	planningDuration := job.GetTR().RecordSpan()
	p.jc.metrics.observeStage(metrics.ImportStagePlanning, planningDuration)
	return nil
}

// cleanupPreparingTasks removes the leftover tasks of a crashed planning round
// (state None) together with their preallocated segments, so the new round
// starts from a clean slate.
func (p *importV3Planner) cleanupPreparingTasks(ctx context.Context, job ImportJob) error {
	tasks := p.jc.importMeta.GetTaskByJob(ctx, job.GetJobID(), WithType(ImportTaskV3Type))
	for _, generic := range tasks {
		task, ok := generic.(*importTaskV3)
		if !ok {
			return merr.WrapErrDataIntegrityMsg("import v3 planning task set contains an unexpected task type")
		}
		if task.GetState() != datapb.ImportTaskStateV2_None {
			continue
		}
		if segmentID := task.task.Load().GetSegmentId(); segmentID != 0 {
			if err := p.jc.meta.UpdateSegmentsInfo(ctx, dropImportV3Segments([]int64{segmentID})); err != nil {
				return err
			}
		}
		if err := p.jc.importMeta.RemoveTask(ctx, task.GetTaskID()); err != nil {
			return err
		}
	}
	return nil
}

// loadExistingFragments returns the fragment refs already owned by the job's
// persisted ImportTaskV3 records. Fragment ownership is the only planning fact
// kept on the task record; it is what Planning recovery needs to fill missing
// coverage without re-assigning fragments.
func (p *importV3Planner) loadExistingFragments(ctx context.Context, job ImportJob) ([]*datapb.FragmentRef, error) {
	tasks := p.jc.importMeta.GetTaskByJob(ctx, job.GetJobID(), WithType(ImportTaskV3Type))
	existing := make([]*datapb.FragmentRef, 0)
	for _, generic := range tasks {
		task, ok := generic.(*importTaskV3)
		if !ok {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 planning task set contains an unexpected task type")
		}
		if task.GetState() == datapb.ImportTaskStateV2_None {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 task %d is still preparing", task.GetTaskID())
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

func (p *importV3Planner) createTask(
	ctx context.Context,
	job ImportJob,
	writerSpec *datapb.WriterSpec,
	taskID, segmentID int64,
	spec importTaskV3Spec,
) error {
	if job.GetDataTs() == 0 {
		// DataTs drives the formal writer's row timestamps. Allocating log IDs
		// here and discarding them only advances the allocator; dispatch would
		// still carry zero and corrupt TTL/commit fencing, so fail the planning
		// step instead.
		return merr.WrapErrImportSysFailedMsg("import v3 job %d has no data timestamp", job.GetJobID())
	}
	perSegment, err := importV3LogRangeWidth(writerSpec)
	if err != nil {
		return err
	}
	logBegin, logEnd, err := p.jc.alloc.AllocN(perSegment)
	if err != nil {
		return err
	}
	fanIn := effectiveImportV3FanIn(Params.DataCoordCfg.FragmentMergeFanIn.GetAsInt(), len(spec.fragments))
	slot := calculateImportTaskV3Slot(
		Params.DataNodeCfg.ImportBaseBufferSize.GetAsInt64(),
		packed.DefaultWriteBufferSize,
		Params.DataCoordCfg.ImportMemoryLimitPerSlot.GetAsInt64(),
		fanIn,
	)
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: job.GetJobID(), TaskId: taskID, CollectionId: job.GetCollectionID(),
		State: datapb.ImportTaskStateV2_None, RunId: 1, NodeId: NullNodeID,
		SegmentId: segmentID, LogRange: &datapb.IDRange{Begin: logBegin, End: logEnd},
		Slot: slot, Rows: spec.rows,
		Fragments: append([]*datapb.FragmentRef(nil), spec.fragments...),
		Vchannel:  spec.channel, PartitionId: spec.partitionID,
	}, p.jc.importMeta, p.jc.meta, p.jc.alloc)
	if err := p.jc.importMeta.AddTask(ctx, task); err != nil {
		return err
	}
	if _, err := addImportSegment(ctx, p.jc.meta, segmentID, job.GetJobID(), taskID, job.GetCollectionID(), spec.partitionID, spec.channel, datapb.SegmentLevel_L1, writerSpec.GetStorageVersion(), int32(writerSpec.GetSchemaVersion())); err != nil {
		return err
	}
	return p.jc.importMeta.UpdateTask(ctx, taskID, UpdateState(datapb.ImportTaskStateV2_Pending))
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
	fragments   []*datapb.FragmentRef
	rows        int64
}

func planningFragmentKey(path string, rows int64) string {
	return fmt.Sprintf("%s/%d", path, rows)
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
		current.fragments = append(current.fragments, &datapb.FragmentRef{Path: f.path, RowCount: f.rows})
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
func missingImportTaskV3Specs(
	fragments []importV3PlanningFragment,
	existing []*datapb.FragmentRef,
	vchannels []string,
	target int64,
) ([]importTaskV3Spec, error) {
	fullKeys := make(map[string]struct{}, len(fragments))
	for _, f := range fragments {
		fullKeys[planningFragmentKey(f.path, f.rows)] = struct{}{}
	}
	covered := make(map[string]struct{}, len(fragments))
	for _, ref := range existing {
		key := planningFragmentKey(ref.GetPath(), ref.GetRowCount())
		if _, ok := fullKeys[key]; !ok {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 task references fragment outside current planning input: %s", ref.GetPath())
		}
		if _, ok := covered[key]; ok {
			return nil, merr.WrapErrDataIntegrityMsg("import v3 tasks contain duplicate fragment: %s", ref.GetPath())
		}
		covered[key] = struct{}{}
	}
	missingFragments := make([]importV3PlanningFragment, 0, len(fragments)-len(covered))
	for _, f := range fragments {
		if _, ok := covered[planningFragmentKey(f.path, f.rows)]; !ok {
			missingFragments = append(missingFragments, f)
		}
	}
	if len(missingFragments) == 0 {
		return nil, nil
	}
	return buildImportV3SegmentPlans(missingFragments, vchannels, target), nil
}

func calculateImportV3Slots(workingSet, memoryPerSlot int64) int64 {
	if memoryPerSlot <= 0 {
		// A zero or negative slot size is a configuration error. Keep the slot
		// helper total and fail closed instead of dividing by zero; paramtable
		// startup validation is the primary guard for the real config value.
		return 1
	}
	return max((workingSet+memoryPerSlot-1)/memoryPerSlot, 1)
}

// calculateReshardTaskSlot converts the shared reshard memory model
// (importutilv2/reshardmem -- the single source of truth also used by the
// DataNode runtime) into slots. The charge covers the GC-scaled resident set
// including the structural overhead of shredded fragments (nFields is the
// temporary schema's field count, the same schema the DataNode accounts
// with), the prepare pipeline and one sort copy. The DataNode's per-bucket
// tail cap bounds the resident accounting at min(buckets, cap) x F, and its
// dynamic free-memory checkpoint spills below that ceiling whenever the real
// process memory is tighter than the per-task budgets assumed.
func calculateReshardTaskSlot(mem reshardmem.Model, memoryPerSlot, buckets, bucketCap, nFields int64) int64 {
	return calculateImportV3Slots(mem.WorkingSet(buckets, bucketCap, nFields), memoryPerSlot)
}

func calculateImportTaskV3Slot(readBuffer, writerBuffer, memoryPerSlot int64, fanIn int) int64 {
	workingSet := int64(fanIn)*2*readBuffer + 3*readBuffer + writerBuffer
	return calculateImportV3Slots(workingSet, memoryPerSlot)
}
