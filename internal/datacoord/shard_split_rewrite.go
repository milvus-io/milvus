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

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/split"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (s *Server) splitSourceSegments(ctx context.Context, task *datapb.SplitShardTask) []split.SourceSegment {
	var segments []split.SourceSegment
	for _, segment := range s.meta.SelectSegments(ctx, meta.WithCollection(task.GetCollectionId()), meta.WithChannel(task.GetSourceVchannel())) {
		segments = append(segments, split.SourceSegment{Info: segment.SegmentInfo, Compacting: segment.IsCompacting, Size: segment.GetSegmentSize()})
	}
	return segments
}

// Rewrite is derived, cluster-local work of the durable SplitShard record.
// Recovery reruns the same decisions; compaction protects each admitted input.
func (s *Server) startSplitRewriteLoop(ctx context.Context) {
	s.serverLoopWg.Add(1)
	go func() {
		defer s.serverLoopWg.Done()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, task := range s.splitStore.List() {
					if task.GetState() != datapb.SplitShardTaskState_SplitShardTaskResidualFlushed {
						continue
					}
					if err := s.rewriteShardSplit(ctx, task); err != nil {
						mlog.Warn(ctx, "shard split rewrite waits", mlog.Int64("splitTaskID", task.GetTaskId()), mlog.Err(err))
					}
				}
			}
		}
	}()
}

func (s *Server) rewriteShardSplit(ctx context.Context, task *datapb.SplitShardTask) error {
	if s.compactionInspector == nil {
		return merr.WrapErrServiceNotReadyMsg("split rewrite needs the compaction inspector")
	}
	step, err := split.PlanRewrite(task, s.splitSourceSegments(ctx, task), 0)
	if err != nil {
		return err
	}
	if step.Ended {
		return s.splitStore.Advance(ctx, task.GetTaskId(), datapb.SplitShardTaskState_SplitShardTaskRewriteDone)
	}
	if step.FoldL0 {
		// S is frozen. Explicitly trigger the finite residual L0 work even when
		// its volume is below ordinary background thresholds.
		if s.compactionTriggerManager != nil {
			_, err = s.compactionTriggerManager.ManualTrigger(ctx, &milvuspb.ManualCompactionRequest{CollectionID: task.GetCollectionId(), L0Compaction: true})
		}
		return err
	}
	collection := s.meta.GetCollection(task.GetCollectionId())
	if collection == nil {
		return merr.WrapErrServiceNotReadyMsg("split collection metadata is unavailable")
	}
	ttl, err := common.GetCollectionTTL(collection.Properties)
	if err != nil {
		return err
	}
	for _, batch := range step.Batches {
		for _, id := range batch.SegmentIDs {
			segment := s.meta.GetHealthySegment(ctx, id)
			if segment == nil || segment.IsCompacting {
				continue
			}
			planID, err := s.allocator.AllocID(ctx)
			if err != nil {
				return err
			}
			// Each worker rewrites one source segment. The allocation leaves room
			// for size-cut outputs on both residue classes.
			maxSize := Params.DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024
			if maxSize <= 0 {
				maxSize = 512 * 1024 * 1024
			}
			outputCount := 2 * (segment.GetSegmentSize()/maxSize + 2)
			begin, end, err := s.allocator.AllocN(outputCount)
			if err != nil {
				return err
			}
			attempt := &datapb.CompactionTask{
				PlanID: planID, TriggerID: task.GetTaskId(), Type: datapb.CompactionType_SplitShardRewriteCompaction, State: datapb.CompactionTaskState_pipelining,
				CollectionID: task.GetCollectionId(), CollectionTtl: ttl.Nanoseconds(), PartitionID: segment.GetPartitionID(), Channel: task.GetSourceVchannel(), Schema: collection.Schema,
				InputSegments: []int64{id}, TotalRows: segment.GetNumOfRows(), MaxSize: maxSize, PreAllocatedSegmentIDs: &datapb.IDRange{Begin: begin, End: end}, SplitRewrite: split.RewritePlan(task),
			}
			if err := s.compactionInspector.EnqueueCompaction(attempt); err != nil {
				return err
			}
		}
	}
	return nil
}

// AdoptShardSplit is called by the view coordinator while its lifecycle guard
// excludes schema and generation transitions. The task state is advanced only
// after the one atomic location + DataView write is durable.
func (s *Server) AdoptShardSplit(ctx context.Context, taskID int64) error {
	if s.splitStore == nil || s.dataViewManager == nil {
		return merr.WrapErrServiceNotReadyMsg("split metadata is not recovered")
	}
	task := s.splitStore.Get(taskID)
	if task == nil {
		return merr.WrapErrServiceNotReadyMsg("split task %d is not recorded", taskID)
	}
	if task.GetState() >= datapb.SplitShardTaskState_SplitShardTaskAdopted {
		return nil
	}
	catalog, ok := s.meta.Catalog.(model.SplitAdoptionCatalog)
	if !ok {
		return merr.WrapErrServiceNotReadyMsg("catalog does not support staged split adoption")
	}
	record, err := catalog.LoadSplitAdoption(ctx, task.GetCollectionId(), taskID)
	if err != nil {
		return err
	}
	if record == nil || record.State == "Preparing" || record.State == "Prepared" {
		moves, err := split.PlanAdoption(task, s.splitSourceSegments(ctx, task))
		if err != nil {
			return err
		}
		releaseInputs, protectErr := s.meta.ProtectSplitAdoptionInputs(moves)
		if protectErr != nil {
			return protectErr
		}
		defer releaseInputs()
		record, err = s.meta.PrepareSplitAdoption(ctx, task, moves)
		if err != nil {
			return err
		}
	}
	if record.CollectionID != task.GetCollectionId() || record.TaskID != taskID || record.Source != task.GetSourceVchannel() {
		return merr.WrapErrDataIntegrityMsg("split adoption does not match task identity")
	}
	expectedTargets := make(map[string]uint64, len(task.GetTargets()))
	for _, target := range task.GetTargets() {
		expectedTargets[target.GetVchannel()] = split.TransformPin(task.GetSwitchTimeTick(), target.GetGenesisTimeTick())
	}
	for _, move := range record.Moves {
		if expectedTargets[move.Target] == 0 || expectedTargets[move.Target] != move.TransformStart {
			return merr.WrapErrDataIntegrityMsg("split adoption target/cursor does not match task %d", taskID)
		}
	}
	event := dataview.AdoptSplitDataViewEvent{CollectionID: task.GetCollectionId(), Source: task.GetSourceVchannel()}
	if record.State == "Prepared" {
		for _, move := range record.Moves {
			event.Moves = append(event.Moves, dataview.SplitAdoptionMove{SegmentID: move.SegmentID, VChannel: move.Target, TransformStartAfterTimetick: move.TransformStart})
		}
	}
	// Prepare against the latest snapshot only after all invisible batches are
	// durable: target flushes that ran during preparation remain in this version.
	view, commit, abort, err := s.dataViewManager.PrepareAdoptSplit(ctx, event)
	if err != nil {
		return err
	}
	defer abort()
	if err := s.meta.AdoptSplit(ctx, record, view); err != nil {
		return err
	}
	commit()
	if err := s.meta.FinishSplitAdoption(ctx, record); err != nil {
		return err
	}
	return s.splitStore.Advance(ctx, taskID, datapb.SplitShardTaskState_SplitShardTaskAdopted)
}

// splitHistoryCatalog keeps cluster-local finite history boundaries outside the
// replicated task proto. Boundary creation holds the collection lifecycle
// guard; pin-release polling only reads these immutable records.
type splitHistoryCatalog interface {
	LoadSplitHistoryBoundary(context.Context, *datapb.SplitShardTask, *datapb.SplitShardTaskTarget) (uint64, error)
	SaveSplitHistoryBoundary(context.Context, *datapb.SplitShardTask, *datapb.SplitShardTaskTarget, uint64) error
}

// ResumeShardSplitL0 fixes every F_i after g2 publication and g1 revocation,
// before permitting target L0 compaction. A partially completed attempt reuses
// durable boundaries; append success without a durable record may be retried.
func (s *Server) ResumeShardSplitL0(ctx context.Context, taskID int64) error {
	task := s.splitStore.Get(taskID)
	if task == nil || task.GetState() < datapb.SplitShardTaskState_SplitShardTaskAdopted {
		return merr.WrapErrServiceNotReadyMsg("split task %d has not been adopted", taskID)
	}
	if s.meta == nil {
		return merr.WrapErrServiceNotReadyMsg("split metadata is not initialized")
	}
	catalog, ok := s.meta.Catalog.(splitHistoryCatalog)
	if !ok {
		return merr.WrapErrServiceNotReadyMsg("catalog cannot persist split history boundaries")
	}
	for _, target := range task.GetTargets() {
		through, err := catalog.LoadSplitHistoryBoundary(ctx, task, target)
		if err != nil {
			return err
		}
		if through != 0 {
			continue
		}
		accesser := streaming.WAL()
		if accesser == nil {
			return merr.WrapErrServiceNotReadyMsg("streaming client is not initialized")
		}
		msg := message.NewManualFlushMessageBuilderV2().WithUnreplicable().WithVChannel(target.GetVchannel()).
			WithHeader(&message.ManualFlushMessageHeader{CollectionId: task.GetCollectionId()}).
			WithBody(&message.ManualFlushMessageBody{}).MustBuildMutable()
		result, err := accesser.RawAppend(ctx, msg)
		if err != nil {
			return merr.Wrapf(err, "request split target %s history boundary", target.GetVchannel())
		}
		if result == nil {
			return merr.WrapErrServiceInternalMsg("split history flush returned no append result")
		}
		if err := catalog.SaveSplitHistoryBoundary(ctx, task, target, result.TimeTick); err != nil {
			return err
		}
	}
	return s.splitStore.ResumeL0(ctx, taskID)
}

// ReleaseShardSplitPins closes only the fixed history through each F_i. Newer
// target Deletes cannot move this boundary and indefinitely postpone retirement.
func (s *Server) ReleaseShardSplitPins(ctx context.Context, taskID int64) (bool, error) {
	task := s.splitStore.Get(taskID)
	if task == nil || !task.GetL0Resumed() {
		return false, merr.WrapErrServiceNotReadyMsg("split task %d has not resumed target compaction", taskID)
	}
	if task.GetPinsReleased() {
		return true, nil
	}
	if s.meta == nil {
		return false, merr.WrapErrServiceNotReadyMsg("split metadata is not initialized")
	}
	catalog, ok := s.meta.Catalog.(splitHistoryCatalog)
	if !ok {
		return false, merr.WrapErrServiceNotReadyMsg("catalog cannot persist split history boundaries")
	}
	// The handover driver always calls Resume under its lifecycle guard,
	// including for legacy already-resumed tasks. Polling runs outside that
	// guard and must never create or replace a missing F.
	bounds := make([]uint64, 0, len(task.GetTargets()))
	for _, target := range task.GetTargets() {
		through, err := catalog.LoadSplitHistoryBoundary(ctx, task, target)
		if err != nil {
			return false, err
		}
		if through == 0 {
			return false, merr.WrapErrServiceNotReadyMsg("split task %d has no fixed target history boundary; resume it before pin release", taskID)
		}
		bounds = append(bounds, through)
	}
	if s.meta.StreamingNodeCatalog == nil {
		return false, nil
	}
	ready := true
	for i, target := range task.GetTargets() {
		through := bounds[i]
		// Capture persisted M before SegmentMeta. Append acknowledgement or dirty M
		// cannot prove that every earlier Delete's L0 registration is durable.
		base, err := s.meta.StreamingNodeCatalog.GetVChannelBase(ctx, funcutil.ToPhysicalChannel(target.GetVchannel()), target.GetVchannel())
		if err != nil {
			return false, err
		}
		if base == nil || base.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED || base.GetTransformMaterializedTimeTick() < through {
			ready = false
			continue
		}
		blockers, err := s.meta.AdvanceSplitFoldedCursors(ctx, task.GetCollectionId(), target.GetVchannel(), through)
		if err != nil {
			return false, err
		}
		if len(blockers) > 0 {
			if err := s.compactionTriggerManager.TriggerL0Segments(ctx, task.GetCollectionId(), blockers); err != nil {
				return false, err
			}
			ready = false
			continue
		}
		for _, segment := range s.meta.GetSegmentsByChannel(target.GetVchannel()) {
			if segment.GetLevel() == datapb.SegmentLevel_L0 {
				continue
			}
			// New WAL bases initialize C at their birth; imports initialize C
			// at commit; replacements inherit C. Rely on that explicit proof,
			// never infer coverage from an output's later physical timestamp.
			if segment.GetTransformStartAfterTimetick() < through {
				ready = false
				break
			}
		}
	}
	if !ready {
		return false, nil
	}
	if err := s.splitStore.ReleasePins(ctx, taskID); err != nil {
		return false, err
	}
	s.meta.RecomputeDataView(ctx, task.GetCollectionId())
	return true, nil
}

// checkSplitRewriteCapacity checks actual per-value and publication limits,
// rather than imposing an all-segment transaction cap. Membership and moves
// are paged; only two bounded roots participate in the adoption commit.
func (s *Server) checkSplitRewriteCapacity(ctx context.Context, collectionID int64, source string) error {
	if _, ok := s.meta.Catalog.(model.SplitAdoptionCatalog); !ok {
		return merr.WrapErrServiceNotReadyMsg("catalog does not support staged split adoption")
	}
	if limits, ok := s.meta.Catalog.(interface{ AtomicAdoptionLimit() int }); !ok || limits.AtomicAdoptionLimit() < 2 {
		return merr.WrapErrOperationNotSupportedMsg("catalog cannot publish two split adoption roots atomically")
	}
	for _, segment := range s.meta.GetSegmentsByChannel(source) {
		if proto.Size(segment.SegmentInfo) > (1<<20)-4096 {
			return merr.WrapErrOperationNotSupportedMsg("split source segment %d exceeds metadata single-value budget", segment.GetID())
		}
	}
	if collection := s.meta.GetCollection(collectionID); collection != nil {
		for _, field := range collection.Schema.GetFields() {
			if field.GetDataType() == schemapb.DataType_Text {
				return merr.WrapErrOperationNotSupportedMsg("shard split rewrite of TEXT is not supported in this implementation")
			}
		}
	}
	return nil
}
