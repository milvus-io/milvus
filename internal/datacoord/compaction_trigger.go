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
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"go.uber.org/zap"
)

type compactTime struct {
	travelTime Timestamp
	expireTime Timestamp
}

type trigger interface {
	start()
	stop()
	// triggerCompaction triggers a compaction if any compaction condition satisfy.
	triggerCompaction(compactTime *compactTime) error
	// triggerSingleCompaction triggers a compaction bundled with collection-partition-channel-segment
	triggerSingleCompaction(collectionID, partitionID, segmentID int64, channel string, compactTime *compactTime) error
	// forceTriggerCompaction force to start a compaction
	forceTriggerCompaction(collectionID int64, compactTime *compactTime) (UniqueID, error)
}

type compactionSignal struct {
	id           UniqueID
	isForce      bool
	isGlobal     bool
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	channel      string
	compactTime  *compactTime
}

var _ trigger = (*compactionTrigger)(nil)

type compactionTrigger struct {
	meta              *meta
	allocator         allocator
	signals           chan *compactionSignal
	compactionHandler compactionPlanContext
	globalTrigger     *time.Ticker
	forceMu           sync.Mutex
	quit              chan struct{}
	wg                sync.WaitGroup
	segRefer          *SegmentReferenceManager
}

func newCompactionTrigger(meta *meta, compactionHandler compactionPlanContext, allocator allocator,
	segRefer *SegmentReferenceManager) *compactionTrigger {
	return &compactionTrigger{
		meta:              meta,
		allocator:         allocator,
		signals:           make(chan *compactionSignal, 100),
		compactionHandler: compactionHandler,
		segRefer:          segRefer,
	}
}

func (t *compactionTrigger) start() {
	t.quit = make(chan struct{})
	t.globalTrigger = time.NewTicker(Params.DataCoordCfg.GlobalCompactionInterval)
	t.wg.Add(2)
	go func() {
		defer logutil.LogPanic()
		defer t.wg.Done()

		for {
			select {
			case <-t.quit:
				log.Info("compaction trigger quit")
				return
			case signal := <-t.signals:
				switch {
				case signal.isGlobal:
					t.handleGlobalSignal(signal)
				default:
					t.handleSignal(signal)
					// shouldn't reset, otherwise a frequent flushed collection will affect other collections
					// t.globalTrigger.Reset(Params.DataCoordCfg.GlobalCompactionInterval)
				}
			}
		}
	}()

	go t.startGlobalCompactionLoop()
}

func (t *compactionTrigger) startGlobalCompactionLoop() {
	defer logutil.LogPanic()
	defer t.wg.Done()

	// If AutoCompaction disabled, global loop will not start
	if !Params.DataCoordCfg.GetEnableAutoCompaction() {
		return
	}

	for {
		select {
		case <-t.quit:
			t.globalTrigger.Stop()
			log.Info("global compaction loop exit")
			return
		case <-t.globalTrigger.C:
			cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			ct, err := getCompactTime(cctx, t.allocator)
			if err != nil {
				log.Warn("unbale to get compaction time", zap.Error(err))
				cancel()
				continue
			}
			cancel()
			err = t.triggerCompaction(ct)
			if err != nil {
				log.Warn("unable to triggerCompaction", zap.Error(err))
			}
		}
	}
}

func (t *compactionTrigger) stop() {
	close(t.quit)
	t.wg.Wait()
}

// triggerCompaction trigger a compaction if any compaction condition satisfy.
func (t *compactionTrigger) triggerCompaction(compactTime *compactTime) error {
	id, err := t.allocSignalID()
	if err != nil {
		return err
	}
	signal := &compactionSignal{
		id:          id,
		isForce:     false,
		isGlobal:    true,
		compactTime: compactTime,
	}
	t.signals <- signal
	return nil
}

// triggerSingleCompaction triger a compaction bundled with collection-partiiton-channel-segment
func (t *compactionTrigger) triggerSingleCompaction(collectionID, partitionID, segmentID int64, channel string, compactTime *compactTime) error {
	// If AutoCompaction diabled, flush request will not trigger compaction
	if !Params.DataCoordCfg.GetEnableAutoCompaction() {
		return nil
	}

	id, err := t.allocSignalID()
	if err != nil {
		return err
	}
	signal := &compactionSignal{
		id:           id,
		isForce:      false,
		isGlobal:     false,
		collectionID: collectionID,
		partitionID:  partitionID,
		segmentID:    segmentID,
		channel:      channel,
		compactTime:  compactTime,
	}
	t.signals <- signal
	return nil
}

// forceTriggerCompaction force to start a compaction
// invoked by user `ManualCompaction` operation
func (t *compactionTrigger) forceTriggerCompaction(collectionID int64, compactTime *compactTime) (UniqueID, error) {
	id, err := t.allocSignalID()
	if err != nil {
		return -1, err
	}
	signal := &compactionSignal{
		id:           id,
		isForce:      true,
		isGlobal:     true,
		collectionID: collectionID,
		compactTime:  compactTime,
	}
	t.handleGlobalSignal(signal)
	return id, nil
}

func (t *compactionTrigger) allocSignalID() (UniqueID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return t.allocator.allocID(ctx)
}

func getPlanIDs(plans []*datapb.CompactionPlan) []int64 {
	ids := make([]int64, 0, len(plans))
	for _, p := range plans {
		ids = append(ids, p.GetPlanID())
	}
	return ids
}

func (t *compactionTrigger) handleGlobalSignal(signal *compactionSignal) {
	t.forceMu.Lock()
	defer t.forceMu.Unlock()

	m := t.meta.GetSegmentsChanPart(func(segment *SegmentInfo) bool {
		return (signal.collectionID == 0 || segment.CollectionID == signal.collectionID) &&
			isSegmentHealthy(segment) &&
			isFlush(segment) &&
			!segment.isCompacting // not compacting now
	}) // m is list of chanPartSegments, which is channel-partition organized segments
	for _, group := range m {
		if !signal.isForce && t.compactionHandler.isFull() {
			break
		}

		plans := t.generatePlans(group.segments, signal.isForce, signal.compactTime)
		for _, plan := range plans {
			if !signal.isForce && t.compactionHandler.isFull() {
				log.Warn("compaction plan skipped due to handler full", zap.Int64("collection", signal.collectionID), zap.Int64("planID", plan.PlanID))
				break
			}
			start := time.Now()
			if err := t.fillOriginPlan(plan); err != nil {
				log.Warn("failed to fill plan", zap.Error(err))
				continue
			}
			err := t.compactionHandler.execCompactionPlan(signal, plan)
			if err != nil {
				log.Warn("failed to execute compaction plan", zap.Int64("collection", signal.collectionID), zap.Int64("planID", plan.PlanID), zap.Error(err))
				continue
			}

			segIDs := make(map[int64][]*datapb.FieldBinlog, len(plan.SegmentBinlogs))
			for _, seg := range plan.SegmentBinlogs {
				segIDs[seg.SegmentID] = seg.Deltalogs
			}

			log.Info("time cost of generating global compaction", zap.Any("segID2DeltaLogs", segIDs), zap.Int64("planID", plan.PlanID), zap.Any("time cost", time.Since(start).Milliseconds()),
				zap.Int64("collectionID", signal.collectionID), zap.String("channel", group.channelName), zap.Int64("partitionID", group.partitionID))
		}
	}
}

// handleSignal processes segment flush caused partition-chan level compaction signal
func (t *compactionTrigger) handleSignal(signal *compactionSignal) {
	t.forceMu.Lock()
	defer t.forceMu.Unlock()

	// 1. check whether segment's binlogs should be compacted or not
	if t.compactionHandler.isFull() {
		return
	}

	segment := t.meta.GetSegment(signal.segmentID)
	if segment == nil {
		log.Warn("segment in compaction signal not found in meta", zap.Int64("segmentID", signal.segmentID))
		return
	}

	channel := segment.GetInsertChannel()
	partitionID := segment.GetPartitionID()
	segments := t.getCandidateSegments(channel, partitionID)
	plans := t.generatePlans(segments, signal.isForce, signal.compactTime)
	for _, plan := range plans {
		if t.compactionHandler.isFull() {
			log.Warn("compaction plan skipped due to handler full", zap.Int64("collection", signal.collectionID), zap.Int64("planID", plan.PlanID))
			break
		}
		start := time.Now()
		if err := t.fillOriginPlan(plan); err != nil {
			log.Warn("failed to fill plan", zap.Error(err))
			continue
		}
		t.compactionHandler.execCompactionPlan(signal, plan)

		log.Info("time cost of generating compaction", zap.Int64("planID", plan.PlanID), zap.Any("time cost", time.Since(start).Milliseconds()),
			zap.Int64("collectionID", signal.collectionID), zap.String("channel", channel), zap.Int64("partitionID", partitionID))
	}
}

func (t *compactionTrigger) generatePlans(segments []*SegmentInfo, force bool, compactTime *compactTime) []*datapb.CompactionPlan {
	// find segments need internal compaction
	// TODO add low priority candidates, for example if the segment is smaller than full 0.9 * max segment size but larger than small segment boundary, we only execute compaction when there are no compaction running actively
	var prioritizedCandidates []*SegmentInfo
	var smallCandidates []*SegmentInfo

	// TODO, currently we lack of the measurement of data distribution, there should be another compaction help on redistributing segment based on scalar/vector field distribution
	for _, segment := range segments {
		segment := segment.ShadowClone()
		// TODO should we trigger compaction periodically even if the segment has no obvious reason to be compacted?
		if force || t.ShouldDoSingleCompaction(segment, compactTime) {
			prioritizedCandidates = append(prioritizedCandidates, segment)
		} else if t.isSmallSegment(segment) {
			smallCandidates = append(smallCandidates, segment)
		}
	}

	var plans []*datapb.CompactionPlan
	// sort segment from large to small
	sort.Slice(prioritizedCandidates, func(i, j int) bool {
		if prioritizedCandidates[i].GetNumOfRows() != prioritizedCandidates[j].GetNumOfRows() {
			return prioritizedCandidates[i].GetNumOfRows() > prioritizedCandidates[j].GetNumOfRows()
		}
		return prioritizedCandidates[i].GetID() < prioritizedCandidates[j].GetID()
	})

	sort.Slice(smallCandidates, func(i, j int) bool {
		if smallCandidates[i].GetNumOfRows() != smallCandidates[j].GetNumOfRows() {
			return smallCandidates[i].GetNumOfRows() > smallCandidates[j].GetNumOfRows()
		}
		return smallCandidates[i].GetID() < smallCandidates[j].GetID()
	})

	// greedy pick from large segment to small, the goal is to fill each segment to reach 512M
	// we must ensure all prioritized candidates is in a plan
	//TODO the compaction policy should consider segment with similar timestamp together so timetravel and data expiration could work better.
	//TODO the compaction selection policy should consider if compaction workload is high
	for len(prioritizedCandidates) > 0 {
		var bucket []*SegmentInfo
		// pop out the first element
		segment := prioritizedCandidates[0]
		bucket = append(bucket, segment)
		prioritizedCandidates = prioritizedCandidates[1:]

		// only do single file compaction if segment is already large enough
		if segment.GetNumOfRows() < segment.GetMaxRowNum() {
			var result []*SegmentInfo
			free := segment.GetMaxRowNum() - segment.GetNumOfRows()
			maxNum := Params.DataCoordCfg.MaxSegmentToMerge - 1
			prioritizedCandidates, result, free = greedySelect(prioritizedCandidates, free, maxNum)
			bucket = append(bucket, result...)
			maxNum -= len(result)
			if maxNum > 0 {
				smallCandidates, result, _ = greedySelect(smallCandidates, free, maxNum)
				bucket = append(bucket, result...)
			}
		}
		// since this is priority compaction, we will execute even if there is only segment
		plan := segmentsToPlan(bucket, compactTime)
		var size int64
		var row int64
		for _, s := range bucket {
			size += s.getSegmentSize()
			row += s.GetNumOfRows()
		}
		log.Info("generate a plan for priority candidates", zap.Any("plan", plan),
			zap.Int64("target segment row", row), zap.Int64("target segment size", size))
		plans = append(plans, plan)
	}

	// check if there are small candidates left can be merged into large segments
	for len(smallCandidates) > 0 {
		var bucket []*SegmentInfo
		// pop out the first element
		segment := smallCandidates[0]
		bucket = append(bucket, segment)
		smallCandidates = smallCandidates[1:]

		var result []*SegmentInfo
		free := segment.GetMaxRowNum() - segment.GetNumOfRows()
		// for small segment merge, we pick one largest segment and merge as much as small segment together with it
		// Why reverse?	 try to merge as many segments as expected.
		// for instance, if a 255M and 255M is the largest small candidates, they will never be merged because of the MinSegmentToMerge limit.
		smallCandidates, result, _ = reverseGreedySelect(smallCandidates, free, Params.DataCoordCfg.MaxSegmentToMerge-1)
		bucket = append(bucket, result...)

		var size int64
		var targetRow int64
		for _, s := range bucket {
			size += s.getSegmentSize()
			targetRow += s.GetNumOfRows()
		}
		// only merge if candidate number is large than MinSegmentToMerge or if target row is large enough
		if len(bucket) >= Params.DataCoordCfg.MinSegmentToMerge || targetRow > int64(float64(segment.GetMaxRowNum())*Params.DataCoordCfg.SegmentSmallProportion) {
			plan := segmentsToPlan(bucket, compactTime)
			log.Info("generate a plan for small candidates", zap.Any("plan", plan),
				zap.Int64("target segment row", targetRow), zap.Int64("target segment size", size))
			plans = append(plans, plan)
		}
	}

	return plans
}

func segmentsToPlan(segments []*SegmentInfo, compactTime *compactTime) *datapb.CompactionPlan {
	plan := &datapb.CompactionPlan{
		Timetravel: compactTime.travelTime,
		Type:       datapb.CompactionType_MixCompaction,
		Channel:    segments[0].GetInsertChannel(),
	}

	for _, s := range segments {
		segmentBinlogs := &datapb.CompactionSegmentBinlogs{
			SegmentID:           s.GetID(),
			FieldBinlogs:        s.GetBinlogs(),
			Field2StatslogPaths: s.GetStatslogs(),
			Deltalogs:           s.GetDeltalogs(),
		}
		plan.SegmentBinlogs = append(plan.SegmentBinlogs, segmentBinlogs)
	}

	return plan
}

func greedySelect(candidates []*SegmentInfo, free int64, maxSegment int) ([]*SegmentInfo, []*SegmentInfo, int64) {
	var result []*SegmentInfo

	for i := 0; i < len(candidates); {
		candidate := candidates[i]
		if len(result) < maxSegment && candidate.GetNumOfRows() < free {
			result = append(result, candidate)
			free -= candidate.GetNumOfRows()
			candidates = append(candidates[:i], candidates[i+1:]...)
		} else {
			i++
		}
	}

	return candidates, result, free
}

func reverseGreedySelect(candidates []*SegmentInfo, free int64, maxSegment int) ([]*SegmentInfo, []*SegmentInfo, int64) {
	var result []*SegmentInfo

	for i := len(candidates) - 1; i >= 0; i-- {
		candidate := candidates[i]
		if (len(result) < maxSegment) && (candidate.GetNumOfRows() < free) {
			result = append(result, candidate)
			free -= candidate.GetNumOfRows()
			candidates = append(candidates[:i], candidates[i+1:]...)
		}
	}
	return candidates, result, free
}

func (t *compactionTrigger) getCandidateSegments(channel string, partitionID UniqueID) []*SegmentInfo {
	segments := t.meta.GetSegmentsByChannel(channel)
	var res []*SegmentInfo
	for _, s := range segments {
		if !isSegmentHealthy(s) || !isFlush(s) || s.GetInsertChannel() != channel ||
			s.GetPartitionID() != partitionID || s.isCompacting {
			continue
		}
		res = append(res, s)
	}
	return res
}

func (t *compactionTrigger) isSmallSegment(segment *SegmentInfo) bool {
	return segment.GetNumOfRows() < int64(float64(segment.GetMaxRowNum())*Params.DataCoordCfg.SegmentSmallProportion)
}

func (t *compactionTrigger) fillOriginPlan(plan *datapb.CompactionPlan) error {
	// TODO context
	id, err := t.allocator.allocID(context.TODO())
	if err != nil {
		return err
	}
	plan.PlanID = id
	plan.TimeoutInSeconds = Params.DataCoordCfg.CompactionTimeoutInSeconds
	return nil
}

func (t *compactionTrigger) ShouldDoSingleCompaction(segment *SegmentInfo, compactTime *compactTime) bool {
	// count all the binlog file count
	var totalLogNum int
	for _, binlogs := range segment.GetBinlogs() {
		totalLogNum += len(binlogs.GetBinlogs())
	}

	for _, deltaLogs := range segment.GetDeltalogs() {
		totalLogNum += len(deltaLogs.GetBinlogs())
	}

	for _, statsLogs := range segment.GetStatslogs() {
		totalLogNum += len(statsLogs.GetBinlogs())
	}
	// avoid segment has too many bin logs and the etcd meta is too large, force trigger compaction
	if totalLogNum > int(Params.DataCoordCfg.SingleCompactionBinlogMaxNum) {
		log.Info("total binlog number is too much, trigger compaction", zap.Int64("segment", segment.ID),
			zap.Int("Delta logs", len(segment.GetDeltalogs())), zap.Int("Bin Logs", len(segment.GetBinlogs())), zap.Int("Stat logs", len(segment.GetStatslogs())))
		return true
	}

	// if expire time is enabled, put segment into compaction candidate
	totalExpiredSize := int64(0)
	totalExpiredRows := 0
	for _, binlogs := range segment.GetBinlogs() {
		for _, l := range binlogs.GetBinlogs() {
			// TODO, we should probably estimate expired log entries by total rows in binlog and the ralationship of timeTo, timeFrom and expire time
			if l.TimestampTo < compactTime.expireTime {
				totalExpiredRows += int(l.GetEntriesNum())
				totalExpiredSize += l.GetLogSize()
			}
		}
	}

	if float32(totalExpiredRows)/float32(segment.GetNumOfRows()) >= Params.DataCoordCfg.SingleCompactionRatioThreshold || totalExpiredSize > Params.DataCoordCfg.SingleCompactionExpiredLogMaxSize {
		log.Info("total expired entities is too much, trigger compation", zap.Int64("segment", segment.ID),
			zap.Int("expired rows", totalExpiredRows), zap.Int64("expired log size", totalExpiredSize))
		return true
	}

	// single compaction only merge insert and delta log beyond the timetravel
	// segment's insert binlogs dont have time range info, so we wait until the segment's last expire time is less than timetravel
	// to ensure that all insert logs is beyond the timetravel.
	// TODO: add meta in insert binlog
	if segment.LastExpireTime >= compactTime.travelTime {
		return false
	}

	totalDeletedRows := 0
	totalDeleteLogSize := int64(0)
	for _, deltaLogs := range segment.GetDeltalogs() {
		for _, l := range deltaLogs.GetBinlogs() {
			if l.TimestampTo < compactTime.travelTime {
				totalDeletedRows += int(l.GetEntriesNum())
				totalDeleteLogSize += l.GetLogSize()
			}
		}
	}

	// currently delta log size and delete ratio policy is applied
	if float32(totalDeletedRows)/float32(segment.GetNumOfRows()) >= Params.DataCoordCfg.SingleCompactionRatioThreshold || totalDeleteLogSize > Params.DataCoordCfg.SingleCompactionDeltaLogMaxSize {
		log.Info("total delete entities is too much, trigger compation", zap.Int64("segment", segment.ID),
			zap.Int("deleted rows", totalDeletedRows), zap.Int64("delete log size", totalDeleteLogSize))
		return true
	}

	return false
}

func isFlush(segment *SegmentInfo) bool {
	return segment.GetState() == commonpb.SegmentState_Flushed || segment.GetState() == commonpb.SegmentState_Flushing
}
