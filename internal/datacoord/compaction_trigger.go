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
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/milvus-io/milvus/pkg/util/tsoutil"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/util/logutil"
)

type compactTime struct {
	travelTime    Timestamp
	expireTime    Timestamp
	collectionTTL time.Duration
}

type trigger interface {
	start()
	stop()
	// triggerCompaction triggers a compaction if any compaction condition satisfy.
	triggerCompaction() error
	// triggerSingleCompaction triggers a compaction bundled with collection-partition-channel-segment
	triggerSingleCompaction(collectionID, partitionID, segmentID int64, channel string) error
	// forceTriggerCompaction force to start a compaction
	forceTriggerCompaction(collectionID int64) (UniqueID, error)
}

type compactionSignal struct {
	id           UniqueID
	isForce      bool
	isGlobal     bool
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	channel      string
}

var _ trigger = (*compactionTrigger)(nil)

type compactionTrigger struct {
	handler           Handler
	meta              *meta
	allocator         allocator
	signals           chan *compactionSignal
	compactionHandler compactionPlanContext
	globalTrigger     *time.Ticker
	forceMu           sync.Mutex
	quit              chan struct{}
	wg                sync.WaitGroup
	//segRefer                     *SegmentReferenceManager
	//indexCoord                   types.IndexCoord
	estimateNonDiskSegmentPolicy calUpperLimitPolicy
	estimateDiskSegmentPolicy    calUpperLimitPolicy
	// A sloopy hack, so we can test with different segment row count without worrying that
	// they are re-calculated in every compaction.
	testingOnly bool
}

func newCompactionTrigger(
	meta *meta,
	compactionHandler compactionPlanContext,
	allocator allocator,
	//segRefer *SegmentReferenceManager,
	//indexCoord types.IndexCoord,
	handler Handler,
) *compactionTrigger {
	return &compactionTrigger{
		meta:              meta,
		allocator:         allocator,
		signals:           make(chan *compactionSignal, 100),
		compactionHandler: compactionHandler,
		//segRefer:                     segRefer,
		//indexCoord:                   indexCoord,
		estimateDiskSegmentPolicy:    calBySchemaPolicyWithDiskIndex,
		estimateNonDiskSegmentPolicy: calBySchemaPolicy,
		handler:                      handler,
	}
}

func (t *compactionTrigger) start() {
	t.quit = make(chan struct{})
	t.globalTrigger = time.NewTicker(Params.DataCoordCfg.GlobalCompactionInterval.GetAsDuration(time.Second))
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
	if !Params.DataCoordCfg.EnableAutoCompaction.GetAsBool() {
		return
	}

	for {
		select {
		case <-t.quit:
			t.globalTrigger.Stop()
			log.Info("global compaction loop exit")
			return
		case <-t.globalTrigger.C:
			err := t.triggerCompaction()
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

func (t *compactionTrigger) allocTs() (Timestamp, error) {
	cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ts, err := t.allocator.allocTimestamp(cctx)
	if err != nil {
		return 0, err
	}

	return ts, nil
}

func (t *compactionTrigger) getCollection(collectionID UniqueID) (*collectionInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	coll, err := t.handler.GetCollection(ctx, collectionID)
	if err != nil {
		return nil, fmt.Errorf("collection ID %d not found, err: %w", collectionID, err)
	}
	return coll, nil
}

func (t *compactionTrigger) isCollectionAutoCompactionEnabled(coll *collectionInfo) bool {
	enabled, err := getCollectionAutoCompactionEnabled(coll.Properties)
	if err != nil {
		log.Warn("collection properties auto compaction not valid, returning false", zap.Error(err))
		return false
	}
	return enabled
}

func (t *compactionTrigger) getCompactTime(ts Timestamp, coll *collectionInfo) (*compactTime, error) {
	collectionTTL, err := getCollectionTTL(coll.Properties)
	if err != nil {
		return nil, err
	}

	pts, _ := tsoutil.ParseTS(ts)
	ttRetention := pts.Add(Params.CommonCfg.RetentionDuration.GetAsDuration(time.Second) * -1)
	ttRetentionLogic := tsoutil.ComposeTS(ttRetention.UnixNano()/int64(time.Millisecond), 0)

	if collectionTTL > 0 {
		ttexpired := pts.Add(-collectionTTL)
		ttexpiredLogic := tsoutil.ComposeTS(ttexpired.UnixNano()/int64(time.Millisecond), 0)
		return &compactTime{ttRetentionLogic, ttexpiredLogic, collectionTTL}, nil
	}

	// no expiration time
	return &compactTime{ttRetentionLogic, 0, 0}, nil
}

// triggerCompaction trigger a compaction if any compaction condition satisfy.
func (t *compactionTrigger) triggerCompaction() error {

	id, err := t.allocSignalID()
	if err != nil {
		return err
	}
	signal := &compactionSignal{
		id:       id,
		isForce:  false,
		isGlobal: true,
	}
	t.signals <- signal
	return nil
}

// triggerSingleCompaction triger a compaction bundled with collection-partition-channel-segment
func (t *compactionTrigger) triggerSingleCompaction(collectionID, partitionID, segmentID int64, channel string) error {
	// If AutoCompaction disabled, flush request will not trigger compaction
	if !Params.DataCoordCfg.EnableAutoCompaction.GetAsBool() {
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
	}
	t.signals <- signal
	return nil
}

// forceTriggerCompaction force to start a compaction
// invoked by user `ManualCompaction` operation
func (t *compactionTrigger) forceTriggerCompaction(collectionID int64) (UniqueID, error) {
	id, err := t.allocSignalID()
	if err != nil {
		return -1, err
	}
	signal := &compactionSignal{
		id:           id,
		isForce:      true,
		isGlobal:     true,
		collectionID: collectionID,
	}
	t.handleGlobalSignal(signal)
	return id, nil
}

func (t *compactionTrigger) allocSignalID() (UniqueID, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return t.allocator.allocID(ctx)
}

func (t *compactionTrigger) reCalcSegmentMaxNumOfRows(collectionID UniqueID, isDisk bool) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	collMeta, err := t.handler.GetCollection(ctx, collectionID)
	if err != nil {
		return -1, fmt.Errorf("failed to get collection %d", collectionID)
	}
	if isDisk {
		return t.estimateDiskSegmentPolicy(collMeta.Schema)
	}
	return t.estimateNonDiskSegmentPolicy(collMeta.Schema)
}

// TODO: Update segment info should be written back to Etcd.
func (t *compactionTrigger) updateSegmentMaxSize(segments []*SegmentInfo) (bool, error) {
	if len(segments) == 0 {
		return false, nil
	}

	collectionID := segments[0].GetCollectionID()
	indexInfos := t.meta.GetIndexesForCollection(segments[0].GetCollectionID(), "")

	isDiskANN := false
	for _, indexInfo := range indexInfos {
		indexType := getIndexType(indexInfo.IndexParams)
		if indexType == indexparamcheck.IndexDISKANN {
			// If index type is DiskANN, recalc segment max size here.
			isDiskANN = true
			newMaxRows, err := t.reCalcSegmentMaxNumOfRows(collectionID, true)
			if err != nil {
				return false, err
			}
			if len(segments) > 0 && int64(newMaxRows) != segments[0].GetMaxRowNum() {
				log.Info("segment max rows recalculated for DiskANN collection",
					zap.Int64("old max rows", segments[0].GetMaxRowNum()),
					zap.Int64("new max rows", int64(newMaxRows)))
				for _, segment := range segments {
					segment.MaxRowNum = int64(newMaxRows)
				}
			}
		}
	}
	// If index type is not DiskANN, recalc segment max size using default policy.
	if !isDiskANN && !t.testingOnly {
		newMaxRows, err := t.reCalcSegmentMaxNumOfRows(collectionID, false)
		if err != nil {
			return isDiskANN, err
		}
		if len(segments) > 0 && int64(newMaxRows) != segments[0].GetMaxRowNum() {
			log.Info("segment max rows recalculated for non-DiskANN collection",
				zap.Int64("old max rows", segments[0].GetMaxRowNum()),
				zap.Int64("new max rows", int64(newMaxRows)))
			for _, segment := range segments {
				segment.MaxRowNum = int64(newMaxRows)
			}
		}
	}
	return isDiskANN, nil
}

func (t *compactionTrigger) handleGlobalSignal(signal *compactionSignal) {
	t.forceMu.Lock()
	defer t.forceMu.Unlock()

	log := log.With(zap.Int64("compactionID", signal.id))
	m := t.meta.GetSegmentsChanPart(func(segment *SegmentInfo) bool {
		return (signal.collectionID == 0 || segment.CollectionID == signal.collectionID) &&
			isSegmentHealthy(segment) &&
			isFlush(segment) &&
			!segment.isCompacting && // not compacting now
			!segment.GetIsImporting() // not importing now
	}) // m is list of chanPartSegments, which is channel-partition organized segments

	if len(m) == 0 {
		return
	}

	ts, err := t.allocTs()
	if err != nil {
		log.Warn("allocate ts failed, skip to handle compaction",
			zap.Int64("collectionID", signal.collectionID),
			zap.Int64("partitionID", signal.partitionID),
			zap.Int64("segmentID", signal.segmentID))
		return
	}

	for _, group := range m {
		if !signal.isForce && t.compactionHandler.isFull() {
			break
		}

		isDiskIndex, err := t.updateSegmentMaxSize(group.segments)
		if err != nil {
			log.Warn("failed to update segment max size", zap.Error(err))
			continue
		}

		coll, err := t.getCollection(group.collectionID)
		if err != nil {
			log.Warn("get collection info failed, skip handling compaction",
				zap.Int64("collectionID", group.collectionID),
				zap.Int64("partitionID", group.partitionID),
				zap.String("channel", group.channelName),
				zap.Error(err),
			)
			return
		}

		if !signal.isForce && !t.isCollectionAutoCompactionEnabled(coll) {
			log.RatedInfo(20, "collection auto compaction disabled",
				zap.Int64("collectionID", group.collectionID),
			)
			return
		}

		ct, err := t.getCompactTime(ts, coll)
		if err != nil {
			log.Warn("get compact time failed, skip to handle compaction",
				zap.Int64("collectionID", group.collectionID),
				zap.Int64("partitionID", group.partitionID),
				zap.String("channel", group.channelName))
			return
		}

		plans := t.generatePlans(group.segments, signal.isForce, isDiskIndex, ct)
		for _, plan := range plans {
			segIDs := fetchSegIDs(plan.GetSegmentBinlogs())

			if !signal.isForce && t.compactionHandler.isFull() {
				log.Warn("compaction plan skipped due to handler full",
					zap.Int64("collectionID", signal.collectionID),
					zap.Int64s("segmentIDs", segIDs))
				break
			}
			start := time.Now()
			if err := t.fillOriginPlan(plan); err != nil {
				log.Warn("failed to fill plan",
					zap.Int64("collectionID", signal.collectionID),
					zap.Int64s("segmentIDs", segIDs),
					zap.Error(err))
				continue
			}
			err := t.compactionHandler.execCompactionPlan(signal, plan)
			if err != nil {
				log.Warn("failed to execute compaction plan",
					zap.Int64("collectionID", signal.collectionID),
					zap.Int64("planID", plan.PlanID),
					zap.Int64s("segmentIDs", segIDs),
					zap.Error(err))
				continue
			}

			segIDMap := make(map[int64][]*datapb.FieldBinlog, len(plan.SegmentBinlogs))
			for _, seg := range plan.SegmentBinlogs {
				segIDMap[seg.SegmentID] = seg.Deltalogs
			}

			log.Info("time cost of generating global compaction",
				zap.Any("segID2DeltaLogs", segIDMap),
				zap.Int64("planID", plan.PlanID),
				zap.Int64("time cost", time.Since(start).Milliseconds()),
				zap.Int64("collectionID", signal.collectionID),
				zap.String("channel", group.channelName),
				zap.Int64("partitionID", group.partitionID),
				zap.Int64s("segmentIDs", segIDs))
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

	segment := t.meta.GetHealthySegment(signal.segmentID)
	if segment == nil {
		log.Warn("segment in compaction signal not found in meta", zap.Int64("segmentID", signal.segmentID))
		return
	}

	channel := segment.GetInsertChannel()
	partitionID := segment.GetPartitionID()
	collectionID := segment.GetCollectionID()
	segments := t.getCandidateSegments(channel, partitionID)

	if len(segments) == 0 {
		return
	}

	isDiskIndex, err := t.updateSegmentMaxSize(segments)
	if err != nil {
		log.Warn("failed to update segment max size", zap.Error(err))
		return
	}

	ts, err := t.allocTs()
	if err != nil {
		log.Warn("allocate ts failed, skip to handle compaction", zap.Int64("collectionID", signal.collectionID),
			zap.Int64("partitionID", signal.partitionID), zap.Int64("segmentID", signal.segmentID))
		return
	}

	coll, err := t.getCollection(collectionID)
	if err != nil {
		log.Warn("get collection info failed, skip handling compaction",
			zap.Int64("collectionID", collectionID),
			zap.Int64("partitionID", partitionID),
			zap.String("channel", channel),
			zap.Error(err),
		)
		return
	}

	if !signal.isForce && !t.isCollectionAutoCompactionEnabled(coll) {
		log.RatedInfo(20, "collection auto compaction disabled",
			zap.Int64("collectionID", collectionID),
		)
		return
	}

	ct, err := t.getCompactTime(ts, coll)
	if err != nil {
		log.Warn("get compact time failed, skip to handle compaction", zap.Int64("collectionID", segment.GetCollectionID()),
			zap.Int64("partitionID", partitionID), zap.String("channel", channel))
		return
	}

	plans := t.generatePlans(segments, signal.isForce, isDiskIndex, ct)
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
		if err := t.compactionHandler.execCompactionPlan(signal, plan); err != nil {
			log.Warn("failed to execute compaction plan",
				zap.Int64("collection", signal.collectionID),
				zap.Int64("planID", plan.PlanID),
				zap.Int64s("segment IDs", fetchSegIDs(plan.GetSegmentBinlogs())),
				zap.Error(err))
			continue
		}
		log.Info("time cost of generating compaction",
			zap.Int64("plan ID", plan.PlanID),
			zap.Any("time cost", time.Since(start).Milliseconds()),
			zap.Int64("collectionID", signal.collectionID),
			zap.String("channel", channel),
			zap.Int64("partitionID", partitionID),
			zap.Int64s("segment IDs", fetchSegIDs(plan.GetSegmentBinlogs())))
	}
}

func (t *compactionTrigger) generatePlans(segments []*SegmentInfo, force bool, isDiskIndex bool, compactTime *compactTime) []*datapb.CompactionPlan {
	// find segments need internal compaction
	// TODO add low priority candidates, for example if the segment is smaller than full 0.9 * max segment size but larger than small segment boundary, we only execute compaction when there are no compaction running actively
	var prioritizedCandidates []*SegmentInfo
	var smallCandidates []*SegmentInfo
	var nonPlannedSegments []*SegmentInfo

	// TODO, currently we lack of the measurement of data distribution, there should be another compaction help on redistributing segment based on scalar/vector field distribution
	for _, segment := range segments {
		segment := segment.ShadowClone()
		// TODO should we trigger compaction periodically even if the segment has no obvious reason to be compacted?
		if force || t.ShouldDoSingleCompaction(segment, isDiskIndex, compactTime) {
			prioritizedCandidates = append(prioritizedCandidates, segment)
		} else if t.isSmallSegment(segment) {
			smallCandidates = append(smallCandidates, segment)
		} else {
			nonPlannedSegments = append(nonPlannedSegments, segment)
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

	// Sort non-planned from small to large.
	sort.Slice(nonPlannedSegments, func(i, j int) bool {
		if nonPlannedSegments[i].GetNumOfRows() != nonPlannedSegments[j].GetNumOfRows() {
			return nonPlannedSegments[i].GetNumOfRows() < nonPlannedSegments[j].GetNumOfRows()
		}
		return nonPlannedSegments[i].GetID() > nonPlannedSegments[j].GetID()
	})

	getSegmentIDs := func(segment *SegmentInfo, _ int) int64 {
		return segment.GetID()
	}
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
			maxNum := Params.DataCoordCfg.MaxSegmentToMerge.GetAsInt() - 1
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

	getSegIDsFromPlan := func(plan *datapb.CompactionPlan) []int64 {
		var segmentIDs []int64
		for _, binLog := range plan.GetSegmentBinlogs() {
			segmentIDs = append(segmentIDs, binLog.GetSegmentID())
		}
		return segmentIDs
	}
	var remainingSmallSegs []*SegmentInfo
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
		smallCandidates, result, _ = reverseGreedySelect(smallCandidates, free, Params.DataCoordCfg.MaxSegmentToMerge.GetAsInt()-1)
		bucket = append(bucket, result...)

		var size int64
		var targetRow int64
		for _, s := range bucket {
			size += s.getSegmentSize()
			targetRow += s.GetNumOfRows()
		}
		// only merge if candidate number is large than MinSegmentToMerge or if target row is large enough
		if len(bucket) >= Params.DataCoordCfg.MinSegmentToMerge.GetAsInt() ||
			len(bucket) > 1 &&
				targetRow > int64(float64(segment.GetMaxRowNum())*Params.DataCoordCfg.SegmentCompactableProportion.GetAsFloat()) {
			plan := segmentsToPlan(bucket, compactTime)
			log.Info("generate a plan for small candidates",
				zap.Int64s("plan segmentIDs", lo.Map(bucket, getSegmentIDs)),
				zap.Int64("target segment row", targetRow),
				zap.Int64("target segment size", size))
			plans = append(plans, plan)
		} else {
			remainingSmallSegs = append(remainingSmallSegs, bucket...)
		}
	}
	// Try adding remaining segments to existing plans.
	for i := len(remainingSmallSegs) - 1; i >= 0; i-- {
		s := remainingSmallSegs[i]
		if !isExpandableSmallSegment(s) {
			continue
		}
		// Try squeeze this segment into existing plans. This could cause segment size to exceed maxSize.
		for _, plan := range plans {
			if plan.TotalRows+s.GetNumOfRows() <= int64(Params.DataCoordCfg.SegmentExpansionRate.GetAsFloat()*float64(s.GetMaxRowNum())) {
				segmentBinLogs := &datapb.CompactionSegmentBinlogs{
					SegmentID:           s.GetID(),
					FieldBinlogs:        s.GetBinlogs(),
					Field2StatslogPaths: s.GetStatslogs(),
					Deltalogs:           s.GetDeltalogs(),
				}
				plan.TotalRows += s.GetNumOfRows()
				plan.SegmentBinlogs = append(plan.SegmentBinlogs, segmentBinLogs)
				log.Info("small segment appended on existing plan",
					zap.Int64("segmentID", s.GetID()),
					zap.Int64("target rows", plan.GetTotalRows()),
					zap.Int64s("plan segmentID", getSegIDsFromPlan(plan)),
				)

				remainingSmallSegs = append(remainingSmallSegs[:i], remainingSmallSegs[i+1:]...)
				break
			}
		}
	}
	// If there are still remaining small segments, try adding them to non-planned segments.
	for _, npSeg := range nonPlannedSegments {
		bucket := []*SegmentInfo{npSeg}
		targetRow := npSeg.GetNumOfRows()
		for i := len(remainingSmallSegs) - 1; i >= 0; i-- {
			// Note: could also simply use MaxRowNum as limit.
			if targetRow+remainingSmallSegs[i].GetNumOfRows() <=
				int64(Params.DataCoordCfg.SegmentExpansionRate.GetAsFloat()*float64(npSeg.GetMaxRowNum())) {
				bucket = append(bucket, remainingSmallSegs[i])
				targetRow += remainingSmallSegs[i].GetNumOfRows()
				remainingSmallSegs = append(remainingSmallSegs[:i], remainingSmallSegs[i+1:]...)
			}
		}
		if len(bucket) > 1 {
			plan := segmentsToPlan(bucket, compactTime)
			plans = append(plans, plan)
			log.Info("generate a plan for to squeeze small candidates into non-planned segment",
				zap.Int64s("plan segmentIDs", lo.Map(bucket, getSegmentIDs)),
				zap.Int64("target segment row", targetRow),
			)
		}
	}
	return plans
}

func segmentsToPlan(segments []*SegmentInfo, compactTime *compactTime) *datapb.CompactionPlan {
	plan := &datapb.CompactionPlan{
		Timetravel:    compactTime.travelTime,
		Type:          datapb.CompactionType_MixCompaction,
		Channel:       segments[0].GetInsertChannel(),
		CollectionTtl: compactTime.collectionTTL.Nanoseconds(),
	}

	for _, s := range segments {
		segmentBinlogs := &datapb.CompactionSegmentBinlogs{
			SegmentID:           s.GetID(),
			FieldBinlogs:        s.GetBinlogs(),
			Field2StatslogPaths: s.GetStatslogs(),
			Deltalogs:           s.GetDeltalogs(),
		}
		plan.TotalRows += s.GetNumOfRows()
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
		if !isSegmentHealthy(s) ||
			!isFlush(s) ||
			s.GetInsertChannel() != channel ||
			s.GetPartitionID() != partitionID ||
			s.isCompacting ||
			s.GetIsImporting() {
			continue
		}
		res = append(res, s)
	}

	return res
}

func (t *compactionTrigger) isSmallSegment(segment *SegmentInfo) bool {
	return segment.GetNumOfRows() < int64(float64(segment.GetMaxRowNum())*Params.DataCoordCfg.SegmentSmallProportion.GetAsFloat())
}

func isExpandableSmallSegment(segment *SegmentInfo) bool {
	return segment.GetNumOfRows() < int64(float64(segment.GetMaxRowNum())*(Params.DataCoordCfg.SegmentExpansionRate.GetAsFloat()-1))
}

func (t *compactionTrigger) fillOriginPlan(plan *datapb.CompactionPlan) error {
	// TODO context
	id, err := t.allocator.allocID(context.TODO())
	if err != nil {
		return err
	}
	plan.PlanID = id
	plan.TimeoutInSeconds = int32(Params.DataCoordCfg.CompactionTimeoutInSeconds.GetAsInt())
	return nil
}

func (t *compactionTrigger) isStaleSegment(segment *SegmentInfo) bool {
	return time.Since(segment.lastFlushTime).Minutes() >= segmentTimedFlushDuration
}

func (t *compactionTrigger) ShouldDoSingleCompaction(segment *SegmentInfo, isDiskIndex bool, compactTime *compactTime) bool {
	// no longer restricted binlog numbers because this is now related to field numbers
	var binLog int
	for _, binlogs := range segment.GetBinlogs() {
		binLog += len(binlogs.GetBinlogs())
	}

	// count all the statlog file count, only for flush generated segments
	if len(segment.CompactionFrom) == 0 {
		var statsLog int
		for _, statsLogs := range segment.GetStatslogs() {
			statsLog += len(statsLogs.GetBinlogs())
		}

		var maxSize int
		if isDiskIndex {
			maxSize = int(Params.DataCoordCfg.DiskSegmentMaxSize.GetAsInt64() * 1024 * 1024 / Params.DataNodeCfg.BinLogMaxSize.GetAsInt64())
		} else {
			maxSize = int(Params.DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024 / Params.DataNodeCfg.BinLogMaxSize.GetAsInt64())
		}

		// if stats log is more than expected, trigger compaction to reduce stats log size.
		// TODO maybe we want to compact to single statslog to reduce watch dml channel cost
		// TODO avoid rebuild index twice.
		if statsLog > maxSize*2.0 {
			log.Info("stats number is too much, trigger compaction", zap.Int64("segmentID", segment.ID), zap.Int("Bin logs", binLog), zap.Int("Stat logs", statsLog))
			return true
		}
	}

	var deltaLog int
	for _, deltaLogs := range segment.GetDeltalogs() {
		deltaLog += len(deltaLogs.GetBinlogs())
	}

	if deltaLog > Params.DataCoordCfg.SingleCompactionDeltalogMaxNum.GetAsInt() {
		log.Info("total delta number is too much, trigger compaction", zap.Int64("segmentID", segment.ID), zap.Int("Bin logs", binLog), zap.Int("Delta logs", deltaLog))
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

	if float64(totalExpiredRows)/float64(segment.GetNumOfRows()) >= Params.DataCoordCfg.SingleCompactionRatioThreshold.GetAsFloat() || totalExpiredSize > Params.DataCoordCfg.SingleCompactionExpiredLogMaxSize.GetAsInt64() {
		log.Info("total expired entities is too much, trigger compaction", zap.Int64("segmentID", segment.ID),
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
	if float64(totalDeletedRows)/float64(segment.GetNumOfRows()) >= Params.DataCoordCfg.SingleCompactionRatioThreshold.GetAsFloat() || totalDeleteLogSize > Params.DataCoordCfg.SingleCompactionDeltaLogMaxSize.GetAsInt64() {
		log.Info("total delete entities is too much, trigger compaction", zap.Int64("segmentID", segment.ID),
			zap.Int("deleted rows", totalDeletedRows), zap.Int64("delete log size", totalDeleteLogSize))
		return true
	}

	return false
}

func isFlush(segment *SegmentInfo) bool {
	return segment.GetState() == commonpb.SegmentState_Flushed || segment.GetState() == commonpb.SegmentState_Flushing
}

func fetchSegIDs(segBinLogs []*datapb.CompactionSegmentBinlogs) []int64 {
	var segIDs []int64
	for _, segBinLog := range segBinLogs {
		segIDs = append(segIDs, segBinLog.GetSegmentID())
	}
	return segIDs
}
