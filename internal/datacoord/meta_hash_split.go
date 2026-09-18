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

// Result mutation of a shard split rewrite (design doc §6.3).
//
// A rewrite runs the mix compaction lifecycle -- same scheduling, same executor
// hand-off -- and commits the mix way in all but one respect:
//
//  1. Its outputs belong to OTHER vchannels. Mix stamps every output with the
//     plan's channel; here that is the source, so every rewritten row would
//     land back on the shard it is being moved off. Each output takes the
//     channel its writer was bound to, from the result, and only a channel the
//     plan targets is accepted.
//  2. Its input is dropped in the SAME catalog write that publishes the
//     outputs, exactly as mix retires its inputs. The commit is the rewrite's
//     frontier: every recovery view sees either the input or its outputs, never
//     both and never neither. While the source is listed, its merged view takes
//     in the targets' flushed segments and attributes them to the source, so
//     the source delegator swaps the input for its outputs at one synced target
//     version. And a source whose segments are all rewritten holds nothing live,
//     which is what the adoption's drain (CheckShardSplitDrained) waits for.

// completeHashSplitCompactionMutation publishes the outputs of one shard split
// rewrite and drops its input segment, in one catalog write.
func (m *meta) completeHashSplitCompactionMutation(
	t *datapb.CompactionTask,
	result *datapb.CompactionPlanResult,
) ([]*SegmentInfo, *segMetricMutation, error) {
	metricMutation := &segMetricMutation{stateChange: make(segmentMetricStateChange)}

	// The inputs are retired by this commit, as mix retires its own. They are
	// cloned, so a failed catalog write leaves the in-memory segments untouched.
	inputs := make([]*SegmentInfo, 0, len(t.GetInputSegments()))
	inputIDs := make([]int64, 0, len(t.GetInputSegments()))
	for _, segmentID := range t.GetInputSegments() {
		segment := m.segments.GetSegment(segmentID)
		if segment == nil {
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID)
		}
		if !isSegmentHealthy(segment) {
			// A second plan for an input an earlier plan already rewrote lands
			// here, and is refused rather than publishing a duplicate set of
			// outputs.
			return nil, nil, merr.WrapErrSegmentNotFound(segmentID,
				"input segment was dropped during the shard split rewrite")
		}
		cloned := segment.Clone()
		cloned.DroppedAt = uint64(time.Now().UnixNano())
		cloned.Compacted = true
		updateSegStateAndPrepareMetrics(cloned, commonpb.SegmentState_Dropped, metricMutation)
		inputs = append(inputs, cloned)
		inputIDs = append(inputIDs, cloned.GetID())
	}
	if len(inputs) == 0 {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("shard split rewrite has no input segment")
	}
	if t.GetSchema() == nil {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("shard split rewrite task schema is nil")
	}

	targets := hashSplitTargetChannels(t)
	if targets.Len() == 0 {
		return nil, nil, merr.WrapErrIllegalCompactionPlan("shard split rewrite task names no target vchannel")
	}
	fallbackStart, fallbackDml := getCompactionFallbackPositions(inputs)

	outputs := make([]*SegmentInfo, 0, len(result.GetSegments()))
	for _, out := range result.GetSegments() {
		// The channel comes from the RESULT, not from the plan: the datanode
		// bound each output writer to its target's vchannel, and taking the
		// plan's channel here would silently put every output back on the
		// source. A channel the plan never targeted would put rows on a shard
		// that does not own their keys.
		channel := out.GetChannel()
		if !targets.Contain(channel) {
			return nil, nil, merr.WrapErrIllegalCompactionPlanMsg(
				"shard split rewrite output segment %d names channel %q, not one of the plan's targets",
				out.GetSegmentID(), channel)
		}

		startPos, dmlPos := recalculateSegmentPosition(out.GetInsertLogs(), channel, fallbackStart, fallbackDml)
		segment := &datapb.SegmentInfo{
			ID:                  out.GetSegmentID(),
			CollectionID:        inputs[0].CollectionID,
			PartitionID:         inputs[0].PartitionID,
			InsertChannel:       channel,
			NumOfRows:           out.GetNumOfRows(),
			State:               commonpb.SegmentState_Flushed,
			MaxRowNum:           inputs[0].MaxRowNum,
			Binlogs:             out.GetInsertLogs(),
			Statslogs:           out.GetField2StatslogPaths(),
			Deltalogs:           out.GetDeltalogs(),
			Bm25Statslogs:       out.GetBm25Logs(),
			TextStatsLogs:       out.GetTextStatsLogs(),
			CreatedByCompaction: true,
			// The lineage is what the rewrite recognizes a source segment as
			// rewritten by, and what the recovery view's compaction-lineage
			// normalization (retrieveSegment) keys on.
			CompactionFrom:      inputIDs,
			LastExpireTime:      tsoutil.ComposeTSByTime(time.Unix(t.GetStartTime(), 0)),
			CreateTs:            compactionTaskCreateTS(t),
			Level:               datapb.SegmentLevel_L1,
			StorageVersion:      out.GetStorageVersion(),
			StartPosition:       startPos,
			DmlPosition:         dmlPos,
			IsSorted:            out.GetIsSorted(),
			ManifestPath:        out.GetManifest(),
			IsSortedByNamespace: out.GetIsSortedByNamespace(),
			ExpirQuantiles:      out.GetExpirQuantiles(),
			SchemaVersion:       t.GetSchema().GetVersion(),
			CommitTimestamp:     0, // Normalized: the datanode rewrote the row timestamps.
		}
		segment.Stats = out.GetStats()
		info := NewSegmentInfo(segment)

		// An empty output is published Dropped rather than omitted, as mix does,
		// to keep the segment ledger and its metrics complete.
		if info.GetNumOfRows() == 0 {
			info.State = commonpb.SegmentState_Dropped
		}
		metricMutation.addNewSeg(info.GetState(), info.GetLevel(), info.GetIsSorted(),
			info.GetStorageVersion(), segmentMetricFormatLabel(info), info.GetNumOfRows())
		outputs = append(outputs, info)
	}

	// Outputs first, then the input, in one write: on the ordered fallback path
	// an input is never retired before its outputs are published. A plan with no
	// output at all -- every row of its input deleted or expired -- takes this
	// same path and only drops its input: no output names it in its lineage, so
	// its Dropped state is the only record that it is rewritten.
	actions := make([]metastore.UpdateAction, 0, len(outputs)+len(inputs))
	for _, info := range outputs {
		actions = append(actions, metastore.AddSegment(info.SegmentInfo))
	}
	for _, info := range inputs {
		// The legacy encoding, as mix retires its inputs (see
		// completeMixCompactionMutation).
		actions = append(actions, metastore.AlterSegment(info.SegmentInfo))
	}
	if err := m.catalog.Update(m.ctx, actions...); err != nil {
		mlog.Warn(m.ctx, "fail to publish the shard split rewrite outputs", mlog.Int64("planID", t.GetPlanID()), mlog.Err(err))
		return nil, nil, err
	}
	lo.ForEach(inputs, func(info *SegmentInfo, _ int) {
		m.segments.SetSegment(info.GetID(), info)
	})
	lo.ForEach(outputs, func(info *SegmentInfo, _ int) {
		m.segments.SetSegment(info.GetID(), info)
	})

	mlog.Info(m.ctx, "published the shard split rewrite outputs",
		mlog.Int64("planID", t.GetPlanID()),
		mlog.Int64s("sourceSegments", inputIDs),
		mlog.Int64s("outputs", lo.Map(outputs, func(info *SegmentInfo, _ int) int64 { return info.GetID() })))
	return outputs, metricMutation, nil
}

// hashSplitTargetChannels lists the vchannels a rewrite plan may write to.
func hashSplitTargetChannels(t *datapb.CompactionTask) typeutil.Set[string] {
	targets := typeutil.NewSet[string]()
	for _, target := range t.GetHashSplitTargets() {
		if vchannel := target.GetVchannel(); vchannel != "" {
			targets.Insert(vchannel)
		}
	}
	return targets
}
