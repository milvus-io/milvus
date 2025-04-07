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
	"math"
	"sync"
	"time"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v2/util/logutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type compactTime struct {
	startTime     Timestamp
	expireTime    Timestamp
	collectionTTL time.Duration
}

// todo: migrate to compaction_trigger_v2
type trigger interface {
	start()
	stop()
	TriggerCompaction(ctx context.Context, signal *compactionSignal) (signalID UniqueID, err error)
}

type compactionSignal struct {
	id           UniqueID
	isForce      bool
	collectionID UniqueID
	partitionID  UniqueID
	channel      string
	segmentIDs   []UniqueID
	pos          *msgpb.MsgPosition
	resultCh     chan error
	waitResult   bool
}

func NewCompactionSignal() *compactionSignal {
	return &compactionSignal{
		resultCh:   make(chan error, 1),
		waitResult: true,
	}
}

func (cs *compactionSignal) WithID(id UniqueID) *compactionSignal {
	cs.id = id
	return cs
}

func (cs *compactionSignal) WithIsForce(isForce bool) *compactionSignal {
	cs.isForce = isForce
	return cs
}

func (cs *compactionSignal) WithCollectionID(collectionID UniqueID) *compactionSignal {
	cs.collectionID = collectionID
	return cs
}

func (cs *compactionSignal) WithPartitionID(partitionID UniqueID) *compactionSignal {
	cs.partitionID = partitionID
	return cs
}

func (cs *compactionSignal) WithChannel(channel string) *compactionSignal {
	cs.channel = channel
	return cs
}

func (cs *compactionSignal) WithSegmentIDs(segmentIDs ...UniqueID) *compactionSignal {
	cs.segmentIDs = segmentIDs
	return cs
}

func (cs *compactionSignal) WithWaitResult(waitResult bool) *compactionSignal {
	cs.waitResult = waitResult
	return cs
}

func (cs *compactionSignal) Notify(result error) {
	select {
	case cs.resultCh <- result:
	default:
	}
}

var _ trigger = (*compactionTrigger)(nil)

type compactionTrigger struct {
	handler           Handler
	meta              *meta
	allocator         allocator.Allocator
	signals           chan *compactionSignal
	manualSignals     chan *compactionSignal
	compactionHandler compactionPlanContext
	globalTrigger     *time.Ticker
	closeCh           lifetime.SafeChan
	closeWaiter       sync.WaitGroup

	indexEngineVersionManager IndexEngineVersionManager

	estimateNonDiskSegmentPolicy calUpperLimitPolicy
	estimateDiskSegmentPolicy    calUpperLimitPolicy
	// A sloopy hack, so we can test with different segment row count without worrying that
	// they are re-calculated in every compaction.
	testingOnly bool
}

func newCompactionTrigger(
	meta *meta,
	compactionHandler compactionPlanContext,
	allocator allocator.Allocator,
	handler Handler,
	indexVersionManager IndexEngineVersionManager,
) *compactionTrigger {
	return &compactionTrigger{
		meta:                         meta,
		allocator:                    allocator,
		signals:                      make(chan *compactionSignal, 100),
		manualSignals:                make(chan *compactionSignal, 100),
		compactionHandler:            compactionHandler,
		indexEngineVersionManager:    indexVersionManager,
		estimateDiskSegmentPolicy:    calBySchemaPolicyWithDiskIndex,
		estimateNonDiskSegmentPolicy: calBySchemaPolicy,
		handler:                      handler,
		closeCh:                      lifetime.NewSafeChan(),
	}
}

func (t *compactionTrigger) start() {
	t.globalTrigger = time.NewTicker(Params.DataCoordCfg.MixCompactionTriggerInterval.GetAsDuration(time.Second))
	t.closeWaiter.Add(2)
	go func() {
		defer t.closeWaiter.Done()
		t.work()
	}()

	go func() {
		defer t.closeWaiter.Done()
		t.schedule()
	}()
}

// schedule method triggers global signal by configured interval.
func (t *compactionTrigger) schedule() {
	defer logutil.LogPanic()

	// If AutoCompaction disabled, global loop will not start
	if !Params.DataCoordCfg.EnableAutoCompaction.GetAsBool() {
		return
	}

	for {
		select {
		case <-t.closeCh.CloseCh():
			t.globalTrigger.Stop()
			log.Info("global compaction loop exit")
			return
		case <-t.globalTrigger.C:
			// default signal, all collections withi isGlobal = true
			_, err := t.TriggerCompaction(context.Background(),
				NewCompactionSignal())
			if err != nil {
				log.Warn("unable to triggerCompaction", zap.Error(err))
			}
		}
	}
}

// work method listens the signal channels and generate plans from them.
func (t *compactionTrigger) work() {
	defer logutil.LogPanic()

	for {
		var signal *compactionSignal
		select {
		case <-t.closeCh.CloseCh():
			log.Info("compaction trigger quit")
			return
		case signal = <-t.signals:
		case signal = <-t.manualSignals:
		}
		err := t.handleSignal(signal)
		if err != nil {
			log.Warn("unable to handleSignal", zap.Int64("signalID", signal.id), zap.Error(err))
		}
		signal.Notify(err)
	}
}

func (t *compactionTrigger) stop() {
	t.closeCh.Close()
	t.closeWaiter.Wait()
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

func isCollectionAutoCompactionEnabled(coll *collectionInfo) bool {
	enabled, err := getCollectionAutoCompactionEnabled(coll.Properties)
	if err != nil {
		log.Warn("collection properties auto compaction not valid, returning false", zap.Error(err))
		return false
	}
	return enabled
}

func getCompactTime(ts Timestamp, coll *collectionInfo) (*compactTime, error) {
	collectionTTL, err := getCollectionTTL(coll.Properties)
	if err != nil {
		return nil, err
	}

	pts, _ := tsoutil.ParseTS(ts)

	if collectionTTL > 0 {
		ttexpired := pts.Add(-collectionTTL)
		ttexpiredLogic := tsoutil.ComposeTS(ttexpired.UnixNano()/int64(time.Millisecond), 0)
		return &compactTime{ts, ttexpiredLogic, collectionTTL}, nil
	}

	// no expiration time
	return &compactTime{ts, 0, 0}, nil
}

// TrigerCompaction is the public interface to send compaction signal to work queue.
// when waitResult = true, it waits until the result is returned from worker(via `signal.resultCh`)
// or the context is timeouted/canceled
// otherwise, it just try best to submit the signal to the channel, if the channel is full it just returns err
//
// by default, `signals` channel will be used to send compaction signal
// however, when the `isForce` flag is true, the `manualSignals` channel will be used to skip the queueing
// since manual signals shall have higher priority.
func (t *compactionTrigger) TriggerCompaction(ctx context.Context, signal *compactionSignal) (signalID UniqueID, err error) {
	// If AutoCompaction disabled, flush request will not trigger compaction
	if !paramtable.Get().DataCoordCfg.EnableAutoCompaction.GetAsBool() && !paramtable.Get().DataCoordCfg.EnableCompaction.GetAsBool() {
		return -1, nil
	}

	id, err := t.allocSignalID(ctx)
	if err != nil {
		return -1, err
	}

	signal.WithID(id)

	signalCh := t.signals
	// use force signal channel to skip non-force signal queue
	if signal.isForce {
		signalCh = t.manualSignals
	}

	// non force mode, try best to sent signal only
	if !signal.waitResult {
		select {
		case signalCh <- signal:
		default:
			log.Info("no space to send compaction signal",
				zap.Int64("collectionID", signal.collectionID),
				zap.Int64s("segmentID", signal.segmentIDs),
				zap.String("channel", signal.channel))
			return -1, merr.WrapErrServiceUnavailable("signal channel is full")
		}
		return id, nil
	}

	// force flag make sure signal is handle and returns error if any
	select {
	case signalCh <- signal:
	case <-ctx.Done():
		return -1, ctx.Err()
	}

	select {
	case err = <-signal.resultCh:
		return id, err
	case <-ctx.Done():
		return -1, ctx.Err()
	}
}

func (t *compactionTrigger) allocSignalID(ctx context.Context) (UniqueID, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return t.allocator.AllocID(ctx)
}

// handleSignal is the internal logic to convert compactionSignal into compaction tasks.
func (t *compactionTrigger) handleSignal(signal *compactionSignal) error {
	log := log.With(zap.Int64("compactionID", signal.id),
		zap.Int64("signal.collectionID", signal.collectionID),
		zap.Int64("signal.partitionID", signal.partitionID),
		zap.Int64s("signal.segmentIDs", signal.segmentIDs))

	if !signal.isForce && t.compactionHandler.isFull() {
		log.Warn("compaction plan skipped due to handler full")
		return merr.WrapErrServiceQuotaExceeded("compaction handler full")
	}

	groups, err := t.getCandidates(signal)
	if err != nil {
		log.Warn("handle signal failed, get candidates return error", zap.Error(err))
		return err
	}

	if len(groups) == 0 {
		log.Info("the length of candidate group is 0, skip to handle signal")
		return nil
	}

	for _, group := range groups {
		log := log.With(zap.Int64("collectionID", group.collectionID),
			zap.Int64("partitionID", group.partitionID),
			zap.String("channel", group.channelName))
		if !signal.isForce && t.compactionHandler.isFull() {
			log.Warn("compaction plan skipped due to handler full")
			break
		}

		if Params.DataCoordCfg.IndexBasedCompaction.GetAsBool() {
			group.segments = FilterInIndexedSegments(t.handler, t.meta, signal.isForce, group.segments...)
		}

		coll, err := t.getCollection(group.collectionID)
		if err != nil {
			log.Warn("get collection info failed, skip handling compaction", zap.Error(err))
			return err
		}

		if !signal.isForce && !isCollectionAutoCompactionEnabled(coll) {
			log.RatedInfo(20, "collection auto compaction disabled")
			return nil
		}

		ct, err := getCompactTime(tsoutil.ComposeTSByTime(time.Now(), 0), coll)
		if err != nil {
			log.Warn("get compact time failed, skip to handle compaction")
			return err
		}

		expectedSize := getExpectedSegmentSize(t.meta, coll)
		plans := t.generatePlans(group.segments, signal, ct, expectedSize)
		for _, plan := range plans {
			if !signal.isForce && t.compactionHandler.isFull() {
				log.Warn("compaction plan skipped due to handler full")
				break
			}
			totalRows, inputSegmentIDs := plan.A, plan.B

			// TODO[GOOSE], 11 = 1 planID + 10 segmentID, this is a hack need to be removed.
			// Any plan that output segment number greater than 10 will be marked as invalid plan for now.
			n := 11 * paramtable.Get().DataCoordCfg.CompactionPreAllocateIDExpansionFactor.GetAsInt64()
			startID, endID, err := t.allocator.AllocN(n)
			if err != nil {
				log.Warn("fail to allocate id", zap.Error(err))
				return err
			}
			start := time.Now()
			pts, _ := tsoutil.ParseTS(ct.startTime)
			task := &datapb.CompactionTask{
				PlanID:           startID,
				TriggerID:        signal.id,
				State:            datapb.CompactionTaskState_pipelining,
				StartTime:        pts.Unix(),
				TimeoutInSeconds: Params.DataCoordCfg.CompactionTimeoutInSeconds.GetAsInt32(),
				Type:             datapb.CompactionType_MixCompaction,
				CollectionTtl:    ct.collectionTTL.Nanoseconds(),
				CollectionID:     group.collectionID,
				PartitionID:      group.partitionID,
				Channel:          group.channelName,
				InputSegments:    inputSegmentIDs,
				ResultSegments:   []int64{},
				TotalRows:        totalRows,
				Schema:           coll.Schema,
				MaxSize:          getExpandedSize(expectedSize),
				PreAllocatedSegmentIDs: &datapb.IDRange{
					Begin: startID + 1,
					End:   endID,
				},
			}
			err = t.compactionHandler.enqueueCompaction(task)
			if err != nil {
				log.Warn("failed to execute compaction task",
					zap.Int64("collection", group.collectionID),
					zap.Int64("triggerID", signal.id),
					zap.Int64("planID", task.GetPlanID()),
					zap.Int64s("inputSegments", inputSegmentIDs),
					zap.Error(err))
				continue
			}

			log.Info("time cost of generating global compaction",
				zap.Int64("time cost", time.Since(start).Milliseconds()),
				zap.Int64s("segmentIDs", inputSegmentIDs))
		}
	}
	return nil
}

func (t *compactionTrigger) generatePlans(segments []*SegmentInfo, signal *compactionSignal, compactTime *compactTime, expectedSize int64) []*typeutil.Pair[int64, []int64] {
	if len(segments) == 0 {
		log.Warn("the number of candidate segments is 0, skip to generate compaction plan")
		return []*typeutil.Pair[int64, []int64]{}
	}

	// find segments need internal compaction
	// TODO add low priority candidates, for example if the segment is smaller than full 0.9 * max segment size but larger than small segment boundary, we only execute compaction when there are no compaction running actively
	var prioritizedCandidates []*SegmentInfo
	var smallCandidates []*SegmentInfo
	var nonPlannedSegments []*SegmentInfo

	// TODO, currently we lack of the measurement of data distribution, there should be another compaction help on redistributing segment based on scalar/vector field distribution
	for _, segment := range segments {
		segment := segment.ShadowClone()
		// TODO should we trigger compaction periodically even if the segment has no obvious reason to be compacted?
		if signal.isForce || t.ShouldDoSingleCompaction(segment, compactTime) {
			prioritizedCandidates = append(prioritizedCandidates, segment)
		} else if t.isSmallSegment(segment, expectedSize) {
			smallCandidates = append(smallCandidates, segment)
		} else {
			nonPlannedSegments = append(nonPlannedSegments, segment)
		}
	}

	buckets := [][]*SegmentInfo{}
	toUpdate := newSegmentPacker("update", prioritizedCandidates)
	toMerge := newSegmentPacker("merge", smallCandidates)
	toPack := newSegmentPacker("pack", nonPlannedSegments)

	maxSegs := int64(4096) // Deprecate the max segment limit since it is irrelevant in simple compactions.
	minSegs := Params.DataCoordCfg.MinSegmentToMerge.GetAsInt64()
	compactableProportion := Params.DataCoordCfg.SegmentCompactableProportion.GetAsFloat()
	satisfiedSize := int64(float64(expectedSize) * compactableProportion)
	expantionRate := Params.DataCoordCfg.SegmentExpansionRate.GetAsFloat()
	maxLeftSize := expectedSize - satisfiedSize
	expectedExpandedSize := int64(float64(expectedSize) * expantionRate)
	maxExpandedLeftSize := expectedExpandedSize - satisfiedSize
	reasons := make([]string, 0)
	// 1. Merge small segments if they can make a full bucket
	for {
		pack, left := toMerge.pack(expectedSize, maxLeftSize, minSegs, maxSegs)
		if len(pack) == 0 {
			break
		}
		reasons = append(reasons, fmt.Sprintf("merging %d small segments with left size %d", len(pack), left))
		buckets = append(buckets, pack)
	}

	// 2. Pack prioritized candidates with small segments
	// TODO the compaction selection policy should consider if compaction workload is high
	for {
		// No limit on the remaining size because we want to pack all prioritized candidates
		pack, _ := toUpdate.packWith(expectedSize, math.MaxInt64, 0, maxSegs, toMerge)
		if len(pack) == 0 {
			break
		}
		reasons = append(reasons, fmt.Sprintf("packing %d prioritized segments", len(pack)))
		buckets = append(buckets, pack)
	}
	// if there is any segment toUpdate left, its size must greater than expectedSize, add it to the buckets
	for _, s := range toUpdate.candidates {
		buckets = append(buckets, []*SegmentInfo{s})
		reasons = append(reasons, fmt.Sprintf("force packing prioritized segment %d", s.GetID()))
	}
	// 2.+ legacy: squeeze small segments
	// Try merge all small segments, and then squeeze
	for {
		pack, _ := toMerge.pack(expectedSize, math.MaxInt64, minSegs, maxSegs)
		if len(pack) == 0 {
			break
		}
		reasons = append(reasons, fmt.Sprintf("packing all %d small segments", len(pack)))
		buckets = append(buckets, pack)
	}
	remaining := t.squeezeSmallSegmentsToBuckets(toMerge.candidates, buckets, expectedSize)
	toMerge = newSegmentPacker("merge", remaining)

	// 3. pack remaining small segments with non-planned segments
	for {
		pack, _ := toMerge.packWith(expectedExpandedSize, maxExpandedLeftSize, minSegs, maxSegs, toPack)
		if len(pack) == 0 {
			break
		}
		reasons = append(reasons, fmt.Sprintf("packing %d small segments and non-planned segments", len(pack)))
		buckets = append(buckets, pack)
	}

	tasks := make([]*typeutil.Pair[int64, []int64], len(buckets))
	for i, b := range buckets {
		segmentIDs := make([]int64, 0)
		var totalRows int64
		for _, s := range b {
			totalRows += s.GetNumOfRows()
			segmentIDs = append(segmentIDs, s.GetID())
		}
		pair := typeutil.NewPair(totalRows, segmentIDs)
		tasks[i] = &pair
	}

	if len(tasks) > 0 {
		log.Info("generated nontrivial compaction tasks",
			zap.Int64("collectionID", signal.collectionID),
			zap.Int("prioritizedCandidates", len(prioritizedCandidates)),
			zap.Int("smallCandidates", len(smallCandidates)),
			zap.Int("nonPlannedSegments", len(nonPlannedSegments)),
			zap.Strings("reasons", reasons))
	}
	return tasks
}

// getCandidates converts signal criterion into corresponding compaction candidate groups
// since non-major compaction happens under channel+partition level
// the selected segments are grouped into these categories.
func (t *compactionTrigger) getCandidates(signal *compactionSignal) ([]chanPartSegments, error) {
	// default filter, select segments which could be compacted
	filters := []SegmentFilter{
		SegmentFilterFunc(func(segment *SegmentInfo) bool {
			return isSegmentHealthy(segment) &&
				isFlush(segment) &&
				!segment.isCompacting && // not compacting now
				!segment.GetIsImporting() && // not importing now
				segment.GetLevel() != datapb.SegmentLevel_L0 && // ignore level zero segments
				segment.GetLevel() != datapb.SegmentLevel_L2 && // ignore l2 segment
				!segment.GetIsInvisible()
		}),
	}

	// add segment filter if criterion provided
	if signal.collectionID > 0 {
		filters = append(filters, WithCollection(signal.collectionID))
	}
	if signal.channel != "" {
		filters = append(filters, WithChannel(signal.channel))
	}
	if signal.partitionID > 0 {
		filters = append(filters, SegmentFilterFunc(func(si *SegmentInfo) bool {
			return si.GetPartitionID() == signal.partitionID
		}))
	}
	// segment id provided
	// select these segments only
	if len(signal.segmentIDs) > 0 {
		idSet := typeutil.NewSet(signal.segmentIDs...)
		filters = append(filters, SegmentFilterFunc(func(si *SegmentInfo) bool {
			return idSet.Contain(si.GetID())
		}))
	}

	segments := t.meta.SelectSegments(context.TODO(), filters...)
	// some criterion not met or conflicted
	if len(signal.segmentIDs) > 0 && len(segments) != len(signal.segmentIDs) {
		return nil, merr.WrapErrServiceInternal("not all segment ids provided could be compacted")
	}

	type category struct {
		collectionID int64
		partitionID  int64
		channelName  string
	}
	groups := lo.GroupBy(segments, func(segment *SegmentInfo) category {
		return category{
			collectionID: segment.CollectionID,
			partitionID:  segment.PartitionID,
			channelName:  segment.InsertChannel,
		}
	})

	return lo.MapToSlice(groups, func(c category, segments []*SegmentInfo) chanPartSegments {
		return chanPartSegments{
			collectionID: c.collectionID,
			partitionID:  c.partitionID,
			channelName:  c.channelName,
			segments:     segments,
		}
	}), nil
}

func (t *compactionTrigger) isSmallSegment(segment *SegmentInfo, expectedSize int64) bool {
	return segment.getSegmentSize() < int64(float64(expectedSize)*Params.DataCoordCfg.SegmentSmallProportion.GetAsFloat())
}

func (t *compactionTrigger) isCompactableSegment(targetSize, expectedSize int64) bool {
	smallProportion := Params.DataCoordCfg.SegmentSmallProportion.GetAsFloat()
	compactableProportion := Params.DataCoordCfg.SegmentCompactableProportion.GetAsFloat()

	// avoid invalid single segment compaction
	if compactableProportion < smallProportion {
		compactableProportion = smallProportion
	}

	return targetSize > int64(float64(expectedSize)*compactableProportion)
}

func isExpandableSmallSegment(segment *SegmentInfo, expectedSize int64) bool {
	return segment.getSegmentSize() < int64(float64(expectedSize)*(Params.DataCoordCfg.SegmentExpansionRate.GetAsFloat()-1))
}

func isDeltalogTooManySegment(segment *SegmentInfo) bool {
	deltaLogCount := GetBinlogCount(segment.GetDeltalogs())
	return deltaLogCount > Params.DataCoordCfg.SingleCompactionDeltalogMaxNum.GetAsInt()
}

func isDeleteRowsTooManySegment(segment *SegmentInfo) bool {
	totalDeletedRows := 0
	totalDeleteLogSize := int64(0)
	for _, deltaLogs := range segment.GetDeltalogs() {
		for _, l := range deltaLogs.GetBinlogs() {
			totalDeletedRows += int(l.GetEntriesNum())
			totalDeleteLogSize += l.GetMemorySize()
		}
	}

	// currently delta log size and delete ratio policy is applied
	is := float64(totalDeletedRows)/float64(segment.GetNumOfRows()) >= Params.DataCoordCfg.SingleCompactionRatioThreshold.GetAsFloat() ||
		totalDeleteLogSize > Params.DataCoordCfg.SingleCompactionDeltaLogMaxSize.GetAsInt64()
	if is {
		log.Ctx(context.TODO()).Info("total delete entities is too much",
			zap.Int64("segmentID", segment.ID),
			zap.Int64("numRows", segment.GetNumOfRows()),
			zap.Int("deleted rows", totalDeletedRows),
			zap.Int64("delete log size", totalDeleteLogSize))
	}
	return is
}

func (t *compactionTrigger) ShouldDoSingleCompaction(segment *SegmentInfo, compactTime *compactTime) bool {
	// no longer restricted binlog numbers because this is now related to field numbers

	log := log.Ctx(context.TODO())
	binlogCount := GetBinlogCount(segment.GetBinlogs())
	deltaLogCount := GetBinlogCount(segment.GetDeltalogs())
	if isDeltalogTooManySegment(segment) {
		log.Info("total delta number is too much, trigger compaction", zap.Int64("segmentID", segment.ID), zap.Int("Bin logs", binlogCount), zap.Int("Delta logs", deltaLogCount))
		return true
	}

	// if expire time is enabled, put segment into compaction candidate
	totalExpiredSize := int64(0)
	totalExpiredRows := 0
	for _, binlogs := range segment.GetBinlogs() {
		for _, l := range binlogs.GetBinlogs() {
			// TODO, we should probably estimate expired log entries by total rows in binlog and the ralationship of timeTo, timeFrom and expire time
			if l.TimestampTo < compactTime.expireTime {
				log.RatedDebug(10, "mark binlog as expired",
					zap.Int64("segmentID", segment.ID),
					zap.Int64("binlogID", l.GetLogID()),
					zap.Uint64("binlogTimestampTo", l.TimestampTo),
					zap.Uint64("compactExpireTime", compactTime.expireTime))
				totalExpiredRows += int(l.GetEntriesNum())
				totalExpiredSize += l.GetMemorySize()
			}
		}
	}

	if float64(totalExpiredRows)/float64(segment.GetNumOfRows()) >= Params.DataCoordCfg.SingleCompactionRatioThreshold.GetAsFloat() ||
		totalExpiredSize > Params.DataCoordCfg.SingleCompactionExpiredLogMaxSize.GetAsInt64() {
		log.Info("total expired entities is too much, trigger compaction", zap.Int64("segmentID", segment.ID),
			zap.Int("expiredRows", totalExpiredRows), zap.Int64("expiredLogSize", totalExpiredSize),
			zap.Bool("createdByCompaction", segment.CreatedByCompaction), zap.Int64s("compactionFrom", segment.CompactionFrom))
		return true
	}

	// currently delta log size and delete ratio policy is applied
	if isDeleteRowsTooManySegment(segment) {
		return true
	}

	if Params.DataCoordCfg.AutoUpgradeSegmentIndex.GetAsBool() {
		// index version of segment lower than current version and IndexFileKeys should have value, trigger compaction
		indexIDToSegIdxes := t.meta.indexMeta.GetSegmentIndexes(segment.CollectionID, segment.ID)
		for _, index := range indexIDToSegIdxes {
			if index.CurrentIndexVersion < t.indexEngineVersionManager.GetCurrentIndexEngineVersion() &&
				len(index.IndexFileKeys) > 0 {
				log.Info("index version is too old, trigger compaction",
					zap.Int64("segmentID", segment.ID),
					zap.Int64("indexID", index.IndexID),
					zap.Strings("indexFileKeys", index.IndexFileKeys),
					zap.Int32("currentIndexVersion", index.CurrentIndexVersion),
					zap.Int32("currentEngineVersion", t.indexEngineVersionManager.GetCurrentIndexEngineVersion()))
				return true
			}
		}
	}

	return false
}

func isFlush(segment *SegmentInfo) bool {
	return segment.GetState() == commonpb.SegmentState_Flushed || segment.GetState() == commonpb.SegmentState_Flushing
}

func needSync(segment *SegmentInfo) bool {
	return segment.GetState() == commonpb.SegmentState_Flushed || segment.GetState() == commonpb.SegmentState_Flushing || segment.GetState() == commonpb.SegmentState_Sealed
}

// buckets will be updated inplace
func (t *compactionTrigger) squeezeSmallSegmentsToBuckets(small []*SegmentInfo, buckets [][]*SegmentInfo, expectedSize int64) (remaining []*SegmentInfo) {
	for i := len(small) - 1; i >= 0; i-- {
		s := small[i]
		if !isExpandableSmallSegment(s, expectedSize) {
			continue
		}
		// Try squeeze this segment into existing plans. This could cause segment size to exceed maxSize.
		for bidx, b := range buckets {
			totalSize := lo.SumBy(b, func(s *SegmentInfo) int64 { return s.getSegmentSize() })
			if totalSize+s.getSegmentSize() > int64(Params.DataCoordCfg.SegmentExpansionRate.GetAsFloat()*float64(expectedSize)) {
				continue
			}
			buckets[bidx] = append(buckets[bidx], s)

			small = append(small[:i], small[i+1:]...)
			break
		}
	}

	return small
}

func getExpandedSize(size int64) int64 {
	return int64(float64(size) * Params.DataCoordCfg.SegmentExpansionRate.GetAsFloat())
}
