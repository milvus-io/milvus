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

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Result mutation of a hash-split rewrite.
//
// A rewrite runs the mix compaction lifecycle — same scheduling, same executor
// hand-off — but its RESULT cannot be committed the mix way, in three places:
//
//  1. Its outputs belong to DIFFERENT vchannels. Mix stamps every output with
//     the plan's channel; here that is the source, so every rewritten row would
//     land back on the shard it was being moved off.
//  2. Its inputs must SURVIVE. Mix retires the inputs the moment the outputs are
//     published, because the outputs replace them. Here the source segments are
//     what the source delegator is still serving through the fronting window,
//     and they may only be dropped at adoption.
//  3. Its outputs are not yet serving. They are published so the split task can
//     see them (and so the child delegators can load them), not to replace
//     anything.
//
// Design: docs/design-docs/design_docs/20260610-shard_split.md §6.5.

// completeHashSplitCompactionMutation publishes the outputs of one hash-split
// rewrite, leaving its input segments untouched.
func (m *meta) completeHashSplitCompactionMutation(
	t *datapb.CompactionTask,
	result *datapb.CompactionPlanResult,
) ([]*SegmentInfo, *segMetricMutation, error) {
	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}

	// The inputs are read, not retired. They are still needed: the source
	// delegator serves the window from them, and adoption is what drops them.
	inputs := make([]*SegmentInfo, 0, len(t.GetInputSegments()))
	inputIDs := make([]int64, 0, len(t.GetInputSegments()))
	for _, segmentID := range t.GetInputSegments() {
		segment := m.segments.GetSegment(segmentID)
		if segment == nil {
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID)
		}
		if !isSegmentHealthy(segment) {
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID,
				"input segment was dropped during the hash split rewrite")
		}
		inputs = append(inputs, segment)
		inputIDs = append(inputIDs, segment.GetID())
	}
	if len(inputs) == 0 {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("hash split rewrite has no input segment")
	}
	if t.GetSchema() == nil {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("hash split rewrite task schema is nil")
	}

	targets := hashSplitTargetChannels(t)
	fallbackStart, fallbackDml := getCompactionFallbackPositions(inputs)

	outputs := make([]*SegmentInfo, 0, len(result.GetSegments()))
	for _, out := range result.GetSegments() {
		// The channel comes from the RESULT, not from the plan. The datanode
		// bound each output writer to its target's vchannel, and that binding is
		// the whole point of the rewrite; taking the plan's channel here would
		// silently put every output back on the source.
		channel := out.GetChannel()
		if channel == "" {
			return nil, nil, merr.WrapErrIllegalCompactionPlanMsg(
				"hash split output segment %d carries no channel", out.GetSegmentID())
		}
		if len(targets) > 0 && !targets.Contain(channel) {
			// A result naming a channel this plan never targeted would put rows
			// on a shard that does not own their keys.
			return nil, nil, merr.WrapErrIllegalCompactionPlanMsg(
				"hash split output segment %d names channel %q, not one of the plan's targets",
				out.GetSegmentID(), channel)
		}

		startPos, dmlPos := recalculateSegmentPosition(out.GetInsertLogs(), channel, fallbackStart, fallbackDml)
		proto := &datapb.SegmentInfo{
			ID:            out.GetSegmentID(),
			CollectionID:  inputs[0].CollectionID,
			PartitionID:   inputs[0].PartitionID,
			InsertChannel: channel,
			NumOfRows:     out.GetNumOfRows(),
			State:         commonpb.SegmentState_Flushed,
			MaxRowNum:     inputs[0].MaxRowNum,
			Binlogs:       out.GetInsertLogs(),
			Statslogs:     out.GetField2StatslogPaths(),
			Deltalogs:     out.GetDeltalogs(),
			Bm25Statslogs: out.GetBm25Logs(),
			TextStatsLogs: out.GetTextStatsLogs(),

			CreatedByCompaction: true,
			// The lineage is what lets the split task recognize a source segment
			// as rewritten: it scans the targets and retires any source it finds
			// named here. Judging completion from meta rather than from the plan
			// report is what makes a crash between the two converge.
			CompactionFrom:      inputIDs,
			LastExpireTime:      tsoutil.ComposeTSByTime(time.Unix(t.GetStartTime(), 0)),
			Level:               datapb.SegmentLevel_L1,
			StorageVersion:      out.GetStorageVersion(),
			StartPosition:       startPos,
			DmlPosition:         dmlPos,
			IsSorted:            out.GetIsSorted(),
			ManifestPath:        out.GetManifest(),
			IsSortedByNamespace: out.GetIsSortedByNamespace(),
			ExpirQuantiles:      out.GetExpirQuantiles(),
			SchemaVersion:       t.GetSchema().GetVersion(),
			CommitTimestamp:     0,
		}
		proto.Stats = out.GetStats()
		info := NewSegmentInfo(proto)

		// An empty half is legal — a source segment whose keys all fall on one
		// side produces nothing for the other — but it must still be published,
		// because the split's completion check counts committed outputs.
		if info.GetNumOfRows() == 0 {
			info.State = commonpb.SegmentState_Dropped
		}
		metricMutation.addNewSeg(info.GetState(), info.GetLevel(), info.GetIsSorted(),
			info.GetStorageVersion(), segmentMetricFormatLabel(info), info.GetNumOfRows())
		outputs = append(outputs, info)
	}

	actions := make([]metastore.UpdateAction, 0, len(outputs))
	for _, info := range outputs {
		actions = append(actions, metastore.AddSegment(info.SegmentInfo))
	}
	if err := m.catalog.Update(m.ctx, actions...); err != nil {
		mlog.Warn(m.ctx, "fail to publish hash split rewrite outputs", mlog.Err(err))
		return nil, nil, err
	}
	lo.ForEach(outputs, func(info *SegmentInfo, _ int) {
		m.segments.SetSegment(info.GetID(), info)
	})

	mlog.Info(context.TODO(), "published hash split rewrite outputs",
		mlog.Int64("planID", t.GetPlanID()),
		mlog.Int64s("sourceSegments", inputIDs),
		mlog.Int("outputs", len(outputs)))
	return outputs, metricMutation, nil
}

// RetireSplitSourceSegments marks every healthy segment of the given handed-off
// split source channels Dropped, and reports how many it changed.
//
// This is the step that closes a hash split: the rewrite deliberately leaves its
// inputs alive (see completeHashSplitCompactionMutation), because the source
// delegator serves the whole key space from them for the entire fronting window.
// Once the targets own the routing, those segments are dead weight whose rows
// now live in the rewrite outputs — kept around they would be a full extra copy
// of the collection that GC can never reclaim, and any path that reads a
// channel's segments rather than the collection's serving topology would find
// them and hand back rows the targets already answer for.
//
// Idempotent: a segment already Dropped is skipped, so a crash between the
// routing commit and this call converges on the retry.
func (m *meta) RetireSplitSourceSegments(ctx context.Context, channels []string) (int, error) {
	if len(channels) == 0 {
		return 0, nil
	}
	sources := typeutil.NewSet(channels...)

	m.segMu.Lock()
	defer m.segMu.Unlock()

	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}
	modSegments := make([]*SegmentInfo, 0)
	protos := make([]*datapb.SegmentInfo, 0)
	for _, segment := range m.segments.segments {
		if !sources.Contain(segment.GetInsertChannel()) || !isSegmentHealthy(segment) {
			continue
		}
		cloned := segment.Clone()
		updateSegStateAndPrepareMetrics(cloned, commonpb.SegmentState_Dropped, metricMutation)
		modSegments = append(modSegments, cloned)
		protos = append(protos, cloned.SegmentInfo)
	}
	if len(protos) == 0 {
		return 0, nil
	}
	if err := m.catalog.SaveDroppedSegmentsInBatch(ctx, protos); err != nil {
		return 0, err
	}
	for _, segment := range modSegments {
		m.segments.SetSegment(segment.GetID(), segment)
	}
	metricMutation.commit()
	mlog.Info(ctx, "retired the segments of a handed-off split source",
		mlog.Strings("sourceChannels", channels),
		mlog.Int("segments", len(protos)))
	return len(protos), nil
}

// hashSplitTargetChannels lists the vchannels a rewrite plan is allowed to write
// to. Empty when the plan carries no targets, in which case the caller does not
// constrain the result.
func hashSplitTargetChannels(t *datapb.CompactionTask) typeutil.Set[string] {
	targets := typeutil.NewSet[string]()
	for _, target := range t.GetHashSplitTargets() {
		if vchannel := target.GetVchannel(); vchannel != "" {
			targets.Insert(vchannel)
		}
	}
	return targets
}
