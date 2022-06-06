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
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/logutil"
	"go.uber.org/zap"
)

const (
	signalBufferSize                = 100
	maxLittleSegmentNum             = 10
	maxCompactionTimeoutInSeconds   = 60
	singleCompactionRatioThreshold  = 0.2
	singleCompactionDeltaLogMaxSize = 10 * 1024 * 1024 //10MiB
	globalCompactionInterval        = 60 * time.Second
)

type timetravel struct {
	time Timestamp
}

type trigger interface {
	start()
	stop()
	// triggerCompaction triggers a compaction if any compaction condition satisfy.
	triggerCompaction(timetravel *timetravel) error
	// triggerSingleCompaction triggers a compaction bundled with collection-partition-channel-segment
	triggerSingleCompaction(collectionID, partitionID, segmentID int64, channel string, timetravel *timetravel) error
	// forceTriggerCompaction force to start a compaction
	forceTriggerCompaction(collectionID int64, timetravel *timetravel) (UniqueID, error)
}

type compactionSignal struct {
	id           UniqueID
	isForce      bool
	isGlobal     bool
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	channel      string
	timetravel   *timetravel
}

var _ trigger = (*compactionTrigger)(nil)

type compactionTrigger struct {
	meta                            *meta
	allocator                       allocator
	signals                         chan *compactionSignal
	singleCompactionPolicy          singleCompactionPolicy
	mergeCompactionPolicy           mergeCompactionPolicy
	compactionHandler               compactionPlanContext
	globalTrigger                   *time.Ticker
	forceMu                         sync.Mutex
	mergeCompactionSegmentThreshold int
	quit                            chan struct{}
	wg                              sync.WaitGroup
}

func newCompactionTrigger(meta *meta, compactionHandler compactionPlanContext, allocator allocator) *compactionTrigger {
	return &compactionTrigger{
		meta:                            meta,
		allocator:                       allocator,
		signals:                         make(chan *compactionSignal, signalBufferSize),
		singleCompactionPolicy:          (singleCompactionFunc)(chooseAllBinlogs),
		mergeCompactionPolicy:           (mergeCompactionFunc)(greedyMergeCompaction),
		compactionHandler:               compactionHandler,
		mergeCompactionSegmentThreshold: maxLittleSegmentNum,
	}
}

func (t *compactionTrigger) start() {
	t.quit = make(chan struct{})
	t.globalTrigger = time.NewTicker(globalCompactionInterval)
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
					t.globalTrigger.Reset(globalCompactionInterval)
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
			tt, err := getTimetravelReverseTime(cctx, t.allocator)
			if err != nil {
				log.Warn("unbale to get compaction timetravel", zap.Error(err))
				cancel()
				continue
			}
			cancel()
			err = t.triggerCompaction(tt)
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
func (t *compactionTrigger) triggerCompaction(timetravel *timetravel) error {
	id, err := t.allocSignalID()
	if err != nil {
		return err
	}
	signal := &compactionSignal{
		id:         id,
		isForce:    false,
		isGlobal:   true,
		timetravel: timetravel,
	}
	t.signals <- signal
	return nil
}

// triggerSingleCompaction triger a compaction bundled with collection-partiiton-channel-segment
func (t *compactionTrigger) triggerSingleCompaction(collectionID, partitionID, segmentID int64, channel string, timetravel *timetravel) error {
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
		timetravel:   timetravel,
	}
	t.signals <- signal
	return nil
}

// forceTriggerCompaction force to start a compaction
// invoked by user `ManualCompaction` operation
func (t *compactionTrigger) forceTriggerCompaction(collectionID int64, timetravel *timetravel) (UniqueID, error) {
	id, err := t.allocSignalID()
	if err != nil {
		return -1, err
	}
	signal := &compactionSignal{
		id:           id,
		isForce:      true,
		isGlobal:     true,
		collectionID: collectionID,
		timetravel:   timetravel,
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

		plans := t.generatePlans(group.segments, signal.isForce, signal.timetravel)
		log.Info("global generated plans", zap.Int64("collection", signal.collectionID), zap.Int("plan count", len(plans)))
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
			t.compactionHandler.execCompactionPlan(signal, plan)

			log.Info("time cost of generating global compaction", zap.Int64("planID", plan.PlanID), zap.Any("time cost", time.Since(start).Milliseconds()),
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
	plans := t.generatePlans(segments, signal.isForce, signal.timetravel)
	log.Info("single generated plans", zap.Int64("collection", signal.collectionID), zap.Int("plan count", len(plans)))
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

/*
func (t *compactionTrigger) globalMergeCompaction(signal *compactionSignal, isForce bool, collections ...UniqueID) []*datapb.CompactionPlan {
	colls := make(map[int64]struct{})
	for _, collID := range collections {
		colls[collID] = struct{}{}
	}
	m := t.meta.GetSegmentsChanPart(func(segment *SegmentInfo) bool {
		_, has := colls[segment.GetCollectionID()]
		return (has || len(collections) == 0) && // if filters collection
			isSegmentHealthy(segment) &&
			isFlush(segment) &&
			!segment.isCompacting // not compacting now
	}) // m is list of chanPartSegments, which is channel-partition organized segments
	plans := make([]*datapb.CompactionPlan, 0)
	for _, segments := range m {
		if !isForce && t.compactionHandler.isFull() {
			return plans
		}
		mplans := t.mergeCompaction(segments.segments, signal, isForce)
		plans = append(plans, mplans...)
	}

	return plans
}

func (t *compactionTrigger) mergeCompaction(segments []*SegmentInfo, signal *compactionSignal, isForce bool) []*datapb.CompactionPlan {
	if !isForce && !t.shouldDoMergeCompaction(segments) {
		return nil
	}

	plans := t.mergeCompactionPolicy.generatePlan(segments, signal.timetravel)
	if len(plans) == 0 {
		return nil
	}

	res := make([]*datapb.CompactionPlan, 0, len(plans))
	for _, plan := range plans {
		if !isForce && t.compactionHandler.isFull() {
			return nil
		}

		if err := t.fillOriginPlan(plan); err != nil {
			log.Warn("failed to fill plan", zap.Error(err))
			continue
		}

		log.Debug("exec merge compaction plan", zap.Any("plan", plan))
		if err := t.compactionHandler.execCompactionPlan(signal, plan); err != nil {
			log.Warn("failed to execute compaction plan", zap.Error(err))
			continue
		}
		res = append(res, plan)
	}
	return res
}*/

type SegmentHeap []*SegmentInfo

func (h *SegmentHeap) Len() int {
	return len(*h)
}

func (h *SegmentHeap) Less(i, j int) bool {
	return (*h)[i].GetNumOfRows() < (*h)[j].GetNumOfRows()
}

func (h *SegmentHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *SegmentHeap) Push(x interface{}) {
	*h = append(*h, x.(*SegmentInfo))
}

func (h *SegmentHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func (t *compactionTrigger) generatePlans(segments []*SegmentInfo, force bool, timetravel *timetravel) []*datapb.CompactionPlan {
	// find segments need internal compaction
	internalCandidates := &SegmentHeap{}
	mergeCandidates := &SegmentHeap{}
	for _, segment := range segments {
		segment := segment.ShadowClone()
		if (force && t.hasValidDeltaLogs(segment, timetravel)) || t.shouldDoSingleCompaction(segment, timetravel) {
			heap.Push(internalCandidates, segment)
			continue
		}
		if t.isSmallSegment(segment) {
			heap.Push(mergeCandidates, segment)
		}
	}

	var plans []*datapb.CompactionPlan

	generatePlan := func(segment *SegmentInfo) {
		var bucket []*SegmentInfo
		bucket = append(bucket, segment)
		free := segment.GetMaxRowNum()
		result, free := greedySelect(internalCandidates, free)
		bucket = append(bucket, result...)
		result, _ = greedySelect(mergeCandidates, free)
		bucket = append(bucket, result...)

		plans = append(plans, segmentsToPlan(bucket, timetravel))
	}

	var segment *SegmentInfo
	for internalCandidates.Len() > 0 {
		segment = heap.Pop(internalCandidates).(*SegmentInfo)
		generatePlan(segment)
	}

	// merge compaction need 2 or more segment candidates
	for mergeCandidates.Len() > 1 &&
		(mergeCandidates.Len() >= t.mergeCompactionSegmentThreshold || force) {
		segment = heap.Pop(mergeCandidates).(*SegmentInfo)
		generatePlan(segment)
	}

	return plans
}

func segmentsToPlan(segments []*SegmentInfo, timeTravel *timetravel) *datapb.CompactionPlan {
	plan := &datapb.CompactionPlan{
		Timetravel: timeTravel.time,
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

func greedySelect(candidates *SegmentHeap, free int64) ([]*SegmentInfo, int64) {
	var result []*SegmentInfo
	for candidates.Len() > 0 && (*candidates)[0].GetNumOfRows() < free {
		segment := heap.Pop(candidates).(*SegmentInfo)
		result = append(result, segment)
		free -= segment.GetNumOfRows()
	}
	return result, free
}

func (t *compactionTrigger) getCandidateSegments(channel string, partitionID UniqueID) []*SegmentInfo {
	segments := t.meta.GetSegmentsByChannel(channel)
	var res []*SegmentInfo
	for _, s := range segments {
		if !isFlush(s) || s.GetInsertChannel() != channel ||
			s.GetPartitionID() != partitionID || s.isCompacting {
			continue
		}
		res = append(res, s)
	}
	return res
}

func (t *compactionTrigger) isSmallSegment(segment *SegmentInfo) bool {
	return segment.GetNumOfRows() < segment.GetMaxRowNum()/2
}

func (t *compactionTrigger) shouldDoMergeCompaction(segments []*SegmentInfo) bool {
	littleSegmentNum := 0
	for _, s := range segments {
		if t.isSmallSegment(s) {
			littleSegmentNum++
		}
	}
	return littleSegmentNum >= t.mergeCompactionSegmentThreshold
}

func (t *compactionTrigger) fillOriginPlan(plan *datapb.CompactionPlan) error {
	// TODO context
	id, err := t.allocator.allocID(context.TODO())
	if err != nil {
		return err
	}
	ts, err := t.allocator.allocTimestamp(context.TODO())
	if err != nil {
		return err
	}
	plan.PlanID = id
	plan.StartTime = ts
	plan.TimeoutInSeconds = maxCompactionTimeoutInSeconds
	return nil
}

func (t *compactionTrigger) shouldDoSingleCompaction(segment *SegmentInfo, timetravel *timetravel) bool {
	// single compaction only merge insert and delta log beyond the timetravel
	// segment's insert binlogs dont have time range info, so we wait until the segment's last expire time is less than timetravel
	// to ensure that all insert logs is beyond the timetravel.
	// TODO: add meta in insert binlog
	if segment.LastExpireTime >= timetravel.time {
		return false
	}

	totalDeletedRows := 0
	totalDeleteLogSize := int64(0)
	for _, fbl := range segment.GetDeltalogs() {
		for _, l := range fbl.GetBinlogs() {
			if l.TimestampTo < timetravel.time {
				totalDeletedRows += int(l.GetEntriesNum())
				totalDeleteLogSize += l.GetLogSize()
			}
		}
	}

	// currently delta log size and delete ratio policy is applied
	return float32(totalDeletedRows)/float32(segment.NumOfRows) >= singleCompactionRatioThreshold || totalDeleteLogSize > singleCompactionDeltaLogMaxSize
}

func (t *compactionTrigger) hasValidDeltaLogs(segment *SegmentInfo, timetravel *timetravel) bool {
	if segment.LastExpireTime >= timetravel.time {
		return false
	}

	for _, fbl := range segment.GetDeltalogs() {
		for _, l := range fbl.GetBinlogs() {
			if l.TimestampTo < timetravel.time {
				return true
			}
		}
	}

	return false
}

/*
func (t *compactionTrigger) globalSingleCompaction(segments []*SegmentInfo, isForce bool, signal *compactionSignal) []*datapb.CompactionPlan {
	plans := make([]*datapb.CompactionPlan, 0)
	for _, segment := range segments {
		if !isForce && t.compactionHandler.isFull() {
			return plans
		}
		plan, err := t.singleCompaction(segment, isForce, signal)
		if err != nil {
			log.Warn("failed to exec single compaction", zap.Error(err))
			continue
		}
		if plan != nil {
			plans = append(plans, plan)
			log.Debug("exec single compaction plan", zap.Any("plan", plan))
		}
	}
	return plans
}

func (t *compactionTrigger) singleCompaction(segment *SegmentInfo, isForce bool, signal *compactionSignal) (*datapb.CompactionPlan, error) {
	if segment == nil {
		return nil, nil
	}

	if !isForce && !t.shouldDoSingleCompaction(segment, signal.timetravel) {
		return nil, nil
	}

	plan := t.singleCompactionPolicy.generatePlan(segment, signal.timetravel)
	if plan == nil {
		return nil, nil
	}

	if err := t.fillOriginPlan(plan); err != nil {
		return nil, err
	}
	return plan, t.compactionHandler.execCompactionPlan(signal, plan)
}*/

func isFlush(segment *SegmentInfo) bool {
	return segment.GetState() == commonpb.SegmentState_Flushed || segment.GetState() == commonpb.SegmentState_Flushing
}
