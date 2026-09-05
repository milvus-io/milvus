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
	"math"
	"sync"
	"time"

	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/util/vecindexmgr"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v3/util/logutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	handler       Handler
	meta          *meta
	allocator     allocator.Allocator
	signals       chan *compactionSignal
	manualSignals chan *compactionSignal
	inspector     CompactionInspector
	globalTrigger *time.Ticker
	closeCh       lifetime.SafeChan
	closeWaiter   sync.WaitGroup

	indexEngineVersionManager IndexEngineVersionManager

	// A sloopy hack, so we can test with different segment row count without worrying that
	// they are re-calculated in every compaction.
	testingOnly bool
}

func newCompactionTrigger(
	meta *meta,
	inspector CompactionInspector,
	allocator allocator.Allocator,
	handler Handler,
	indexVersionManager IndexEngineVersionManager,
) *compactionTrigger {
	return &compactionTrigger{
		meta:                      meta,
		allocator:                 allocator,
		signals:                   make(chan *compactionSignal, 100),
		manualSignals:             make(chan *compactionSignal, 100),
		inspector:                 inspector,
		indexEngineVersionManager: indexVersionManager,
		handler:                   handler,
		closeCh:                   lifetime.NewSafeChan(),
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
			mlog.Info(context.TODO(), "global compaction loop exit")
			return
		case <-t.globalTrigger.C:
			// default signal, all collections withi isGlobal = true
			_, err := t.TriggerCompaction(context.Background(),
				NewCompactionSignal())
			if err != nil {
				mlog.Warn(context.TODO(), "unable to triggerCompaction", mlog.Err(err))
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
			mlog.Info(context.TODO(), "compaction trigger quit")
			return
		case signal = <-t.signals:
		case signal = <-t.manualSignals:
		}
		err := t.handleSignal(signal)
		if err != nil {
			mlog.Warn(context.TODO(), "unable to handleSignal", mlog.Int64("signalID", signal.id), mlog.Err(err))
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
		return nil, merr.Wrapf(err, "collection ID %d not found", collectionID)
	}
	return coll, nil
}

func isCollectionAutoCompactionEnabled(coll *collectionInfo) bool {
	if coll == nil {
		return false
	}
	if coll.IsExternal() {
		mlog.Debug(context.TODO(), "collection auto compaction disabled for external collection", mlog.FieldCollectionID(coll.ID))
		return false
	}
	enabled, err := getCollectionAutoCompactionEnabled(coll.Properties)
	if err != nil {
		mlog.Warn(context.TODO(), "collection properties auto compaction not valid, returning false", mlog.Err(err))
		return false
	}
	return enabled
}

func getCompactTime(ts Timestamp, coll *collectionInfo) (*compactTime, error) {
	collectionTTL, err := common.GetCollectionTTLFromMap(coll.Properties)
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
			mlog.Info(ctx, "no space to send compaction signal",
				mlog.FieldCollectionID(signal.collectionID),
				mlog.Int64s("segmentID", signal.segmentIDs),
				mlog.String("channel", signal.channel))
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
	log := mlog.With(mlog.Int64("compactionID", signal.id),
		mlog.Int64("signal.collectionID", signal.collectionID),
		mlog.Int64("signal.partitionID", signal.partitionID),
		mlog.Int64s("signal.segmentIDs", signal.segmentIDs))

	if !signal.isForce && t.inspector.isFull() {
		log.Warn(context.TODO(), "skip to generate compaction plan due to handler full")
		return merr.WrapErrServiceQuotaExceeded("compaction handler full")
	}

	log.Info(context.TODO(), "handleSignal receive")
	groups, err := t.getCandidates(signal)
	if err != nil {
		log.Warn(context.TODO(), "handle signal failed, get candidates return error", mlog.Err(err))
		return err
	}

	if len(groups) == 0 {
		log.Info(context.TODO(), "the length of candidate group is 0, skip to handle signal")
		return nil
	}

	for _, group := range groups {
		log := mlog.With(
			mlog.Int64("group.partitionID", group.partitionID),
			mlog.String("group.channel", group.channelName),
		)

		if !signal.isForce && t.inspector.isFull() {
			log.Warn(context.TODO(), "skip to generate compaction plan due to handler full")
			return merr.WrapErrServiceQuotaExceeded("compaction handler full")
		}

		if Params.DataCoordCfg.IndexBasedCompaction.GetAsBool() {
			group.segments = FilterInIndexedSegments(context.Background(), t.handler, t.meta, signal.isForce, group.segments...)
		}

		coll, err := t.getCollection(group.collectionID)
		if err != nil {
			log.Warn(context.TODO(), "get collection info failed, skip handling compaction", mlog.Err(err))
			if signal.collectionID != 0 {
				return err
			}
			continue
		}

		if !signal.isForce && !isCollectionAutoCompactionEnabled(coll) {
			log.RatedInfo(context.TODO(), rate.Limit(20), "collection auto compaction disabled")
			return nil
		}

		ct, err := getCompactTime(tsoutil.ComposeTSByTime(time.Now()), coll)
		if err != nil {
			log.Warn(context.TODO(), "get compact time failed, skip to handle compaction")
			return err
		}

		expectedSize := getExpectedSegmentSize(t.meta, coll.ID, coll.Schema)
		plans := t.generatePlans(group.segments, signal, ct, expectedSize)
		for _, bucket := range plans {
			if !signal.isForce && t.inspector.isFull() {
				log.Warn(context.TODO(), "skip to generate compaction plan due to handler full")
				return merr.WrapErrServiceQuotaExceeded("compaction handler full")
			}
			inputSegmentIDs := lo.Map(bucket.segments, func(s *SegmentInfo, _ int) int64 { return s.GetID() })

			inputs := typeutil.NewSet[int64](inputSegmentIDs...)
			totalSize := lo.SumBy(group.segments, func(s *SegmentInfo) int64 {
				if inputs.Contain(s.GetID()) {
					return s.getSegmentSize()
				}
				return 0
			})
			planID, preAllocatedSegmentIDs, err := allocCompactionPlanIDs(t.allocator, float64(totalSize), float64(expectedSize))
			if err != nil {
				log.Warn(context.TODO(), "fail to allocate id", mlog.Err(err))
				return err
			}
			start := time.Now()
			pts, _ := tsoutil.ParseTS(ct.startTime)
			task := &datapb.CompactionTask{
				PlanID:                 planID,
				TriggerID:              signal.id,
				State:                  datapb.CompactionTaskState_pipelining,
				StartTime:              pts.Unix(),
				Type:                   datapb.CompactionType_MixCompaction,
				CollectionTtl:          ct.collectionTTL.Nanoseconds(),
				CollectionID:           group.collectionID,
				PartitionID:            group.partitionID,
				Channel:                group.channelName,
				InputSegments:          inputSegmentIDs,
				ResultSegments:         []int64{},
				TotalRows:              bucket.totalRows,
				Schema:                 coll.Schema,
				MaxSize:                bucket.maxSize,
				PreAllocatedSegmentIDs: preAllocatedSegmentIDs,
			}
			err = t.inspector.enqueueCompaction(task)
			if err != nil {
				log.Warn(context.TODO(), "failed to execute compaction task",
					mlog.Int64("planID", task.GetPlanID()),
					mlog.Int64s("inputSegments", inputSegmentIDs),
					mlog.Err(err))
				continue
			}

			log.Info(context.TODO(), "time cost of generating compaction",
				mlog.Int64("planID", task.GetPlanID()),
				mlog.Int64("time cost", time.Since(start).Milliseconds()),
				mlog.Int64("target size", task.GetMaxSize()),
				mlog.Int64s("inputSegments", inputSegmentIDs))
		}
	}
	return nil
}

// compactionBucket groups the segments selected for a single compaction
// task, together with the row count and the target output size to use
// when building the task.
type compactionBucket struct {
	segments  []*SegmentInfo
	totalRows int64
	maxSize   int64
}

// generatePlans classifies candidate segments into prioritized (must
// compact) and compactable (fill-rate below the full threshold) sets,
// then composes compaction buckets with a two-tier bin-packing strategy:
//
//   - Full tier: pack compactable segments towards idealSize, only
//     emitting a bucket when the packed size clears the fill-rate gate.
//     This bounds each byte to at most one full-tier rewrite.
//   - Fragment tier: once leftover fragments (residual size below the
//     fragment threshold) exceed maxFragments, pack them towards
//     middleSize so they don't accumulate unbounded before ever being
//     compacted. Fragment-tier output feeds a later full-tier compaction,
//     bounding total write amplification to at most 2x on the happy path.
func (t *compactionTrigger) generatePlans(segments []*SegmentInfo, signal *compactionSignal, compactTime *compactTime, expectedSize int64) []*compactionBucket {
	if len(segments) == 0 {
		mlog.Warn(context.TODO(), "the number of candidate segments is 0, skip to generate compaction plan")
		return nil
	}

	maxFragments := Params.DataCoordCfg.MaxFragmentsPerGroup.GetAsInt64()
	middleSize := compactionMiddleSize(expectedSize)

	// Step 1: Classify segments
	var prioritized []*SegmentInfo
	var compactable []*SegmentInfo

	for _, segment := range segments {
		segment := segment.ShadowClone()
		if signal.isForce || t.ShouldDoSingleCompaction(segment, compactTime) {
			prioritized = append(prioritized, segment)
		} else if !isFullSegment(expectedSize, segment.GetResidualSegmentSize()) {
			compactable = append(compactable, segment)
		}
	}

	var buckets []*compactionBucket

	// Prioritized segments -> single-segment compaction tasks
	for _, s := range prioritized {
		buckets = append(buckets, &compactionBucket{
			segments:  []*SegmentInfo{s},
			totalRows: s.GetNumOfRows(),
			maxSize:   expectedSize,
		})
	}

	// Step 2: Full-tier composition
	packer := newSegmentPacker("full-tier", compactable, compactTime)
	fullMaxLeftSize := expectedSize - compactionFullThreshold(expectedSize)
	for {
		pack, _ := packer.pack(expectedSize, fullMaxLeftSize, 2, math.MaxInt64)
		if len(pack) == 0 {
			break
		}
		var rows int64
		for _, s := range pack {
			rows += s.GetNumOfRows()
		}
		buckets = append(buckets, &compactionBucket{
			segments:  pack,
			totalRows: rows,
			maxSize:   expectedSize,
		})
	}

	// Step 3: Fragment-tier composition
	var fragmentCount int64
	for _, s := range packer.candidates {
		if isFragmentSegment(expectedSize, s.GetResidualSegmentSize()) {
			fragmentCount++
		}
	}

	if fragmentCount > maxFragments {
		var fragments []*SegmentInfo
		for _, s := range packer.candidates {
			if isFragmentSegment(expectedSize, s.GetResidualSegmentSize()) {
				fragments = append(fragments, s)
			}
		}

		fragPacker := newSegmentPacker("fragment-tier", fragments, compactTime)
		for {
			pack, _ := fragPacker.pack(middleSize, math.MaxInt64, 2, math.MaxInt64)
			if len(pack) == 0 {
				break
			}
			var rows int64
			for _, s := range pack {
				rows += s.GetNumOfRows()
			}
			buckets = append(buckets, &compactionBucket{
				segments:  pack,
				totalRows: rows,
				maxSize:   middleSize,
			})
		}
	}

	if len(buckets) > 0 {
		mlog.Info(context.TODO(), "generated compaction plans",
			mlog.FieldCollectionID(signal.collectionID),
			mlog.Int("prioritized", len(prioritized)),
			mlog.Int("compactable", len(compactable)),
			mlog.Int("buckets", len(buckets)),
			mlog.Int64("fragmentCount", fragmentCount),
			mlog.Int64("maxFragments", maxFragments))
	}

	return buckets
}

// getCandidates converts signal criterion into corresponding compaction candidate groups
// since non-major compaction happens under channel+partition level
// the selected segments are grouped into these categories.
func (t *compactionTrigger) getCandidates(signal *compactionSignal) ([]chanPartSegments, error) {
	// Fail-closed: if any protected snapshot's RefIndex hasn't loaded yet,
	// block compaction for the entire collection.
	if signal.collectionID > 0 && t.meta.isCollectionCompactionBlocked(signal.collectionID) {
		mlog.Info(context.TODO(), "skip compaction candidates for collection due to unloaded protected snapshot RefIndex",
			mlog.FieldCollectionID(signal.collectionID))
		return nil, nil
	}

	// default filter, select segments which could be compacted
	filters := []SegmentFilter{
		SegmentFilterFunc(func(segment *SegmentInfo) bool {
			return isNormalManualCompactionCandidate(t.meta, segment)
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
		// SelectSegments also filters segments that are transiently mid-flush /
		// compacting / just dropped, so a count mismatch is usually server-side
		// state, not a bad id from the caller.
		return nil, merr.WrapErrServiceInternalMsg("not all segment ids provided could be compacted")
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

func hasTooManyDeletions(segment *SegmentInfo) bool {
	stats := segment.EnsureStats()
	deltaLogCount := int(stats.GetDeltaBinlogCount())
	totalDeletedRows := int(stats.GetDeleteNumRows())
	totalDeleteLogSize := stats.GetDeltaBinlogSize()

	// Too many deltalog files, accumulates IO count.
	if deltaLogCount > Params.DataCoordCfg.SingleCompactionDeltalogMaxNum.GetAsInt() {
		mlog.Info(context.TODO(), "delta logs file count exceeds threshold",
			mlog.FieldSegmentID(segment.ID),
			mlog.Int("delta log count", deltaLogCount),
			mlog.Int("file number threshold", Params.DataCoordCfg.SingleCompactionDeltalogMaxNum.GetAsInt()),
		)
		return true
	}

	// The proportion of deleted rows is too large, int64 PK tends to accumulates deleted row counts.
	if float64(totalDeletedRows)/float64(segment.GetNumOfRows()) >= Params.DataCoordCfg.SingleCompactionRatioThreshold.GetAsFloat() {
		mlog.Info(context.TODO(), "deleted entities rows proportion exceeds threshold",
			mlog.FieldSegmentID(segment.ID),
			mlog.Int64("number of rows", segment.GetNumOfRows()),
			mlog.Int("deleted rows", totalDeletedRows),
			mlog.Float64("proportion threshold", Params.DataCoordCfg.SingleCompactionRatioThreshold.GetAsFloat()),
		)
		return true
	}

	// Delete size is too large, varchar PK tends to accumulates deltalog size.
	if totalDeleteLogSize > Params.DataCoordCfg.SingleCompactionDeltaLogMaxSize.GetAsInt64() {
		mlog.Info(context.TODO(), "total delete entries size exceeds threshold",
			mlog.FieldSegmentID(segment.ID),
			mlog.Int64("numRows", segment.GetNumOfRows()),
			mlog.Int64("delete entries size", totalDeleteLogSize),
			mlog.Int64("size threshold", Params.DataCoordCfg.SingleCompactionDeltaLogMaxSize.GetAsInt64()),
		)
		return true
	}

	return false
}

func (t *compactionTrigger) ShouldCompactExpiry(fromTs uint64, compactTime *compactTime, segment *SegmentInfo) bool {
	if Params.DataCoordCfg.CompactionExpiryTolerance.GetAsInt() >= 0 {
		tolerantDuration := Params.DataCoordCfg.CompactionExpiryTolerance.GetAsDuration(time.Hour)
		expireTime, _ := tsoutil.ParseTS(compactTime.expireTime)
		earliestTolerance := expireTime.Add(-tolerantDuration)
		earliestFromTime, _ := tsoutil.ParseTS(fromTs)
		if earliestFromTime.Before(earliestTolerance) {
			mlog.Info(context.TODO(), "Trigger strict expiry compaction for segment",
				mlog.FieldSegmentID(segment.GetID()),
				mlog.FieldCollectionID(segment.GetCollectionID()),
				mlog.Int64("partition", segment.GetPartitionID()),
				mlog.String("channel", segment.GetInsertChannel()),
				mlog.Time("compaction expire time", expireTime),
				mlog.Time("earliest tolerance", earliestTolerance),
				mlog.Time("segment earliest from time", earliestFromTime),
			)
			return true
		}
	}
	return false
}

func getExpirQuantilesIndexByRatio(ratio float64, percentilesLen int) int {
	// expirQuantiles is [20%, 40%, 60%, 80%, 100%] (len = 5).
	// We map ratio to the nearest lower 20% bucket:
	// 0~0.39 -> 20%, 0.4~0.59 -> 40%, 0.6~0.79 -> 60%, 0.8~0.99 -> 80%, >=1.0 -> 100%
	if percentilesLen <= 0 {
		return 0
	}
	step := 0.2
	idx := int((ratio+0.01)/step) - 1 // add 0.01 to avoid rounding error
	if idx < 0 {
		idx = 0
	}
	if idx >= percentilesLen {
		idx = percentilesLen - 1
	}
	return idx
}

func (t *compactionTrigger) ShouldCompactExpiryWithTTLField(compactTime *compactTime, segment *SegmentInfo) bool {
	percentiles := segment.GetExpirQuantiles()
	if len(percentiles) == 0 {
		return false
	}

	ratio := Params.DataCoordCfg.SingleCompactionRatioThreshold.GetAsFloat()

	index := getExpirQuantilesIndexByRatio(ratio, len(percentiles))
	expirationTime := percentiles[index]
	// If current time (startTime) is greater than the expiration time at this percentile, trigger compaction
	startTs := tsoutil.PhysicalTime(compactTime.startTime)
	return startTs.UnixMicro() >= expirationTime && expirationTime > 0
}

func (t *compactionTrigger) ShouldDoSingleCompaction(segment *SegmentInfo, compactTime *compactTime) bool {
	// no longer restricted binlog numbers because this is now related to field numbers

	stats := segment.EnsureStats()
	commitTs := segment.GetCommitTimestamp()

	// Strict-tolerance path: exact min via Stats.TimestampFrom. For import
	// segments commit_timestamp overrides every row's effective timestamp.
	earliestFromTs := tsoutil.EffectiveTimestamp(stats.GetTimestampFrom(), commitTs)
	if t.ShouldCompactExpiry(earliestFromTs, compactTime, segment) {
		return true
	}

	// Ratio + size path: derive an expired-row fraction from the quantile
	// distribution (20%-bucket granularity). Approximate; the strict-
	// tolerance check above covers the precise edges.
	//
	// We deliberately UNDER-estimate. Q[i] < expireTime guarantees
	// percentiles[i] of rows are expired; that fraction times
	// InsertBinlogSize is the byte estimate under a uniform-per-row-size
	// assumption that does NOT hold when expired binlogs are smaller than
	// the segment-wide average. To prevent over-triggering on segments
	// whose precise expired-byte sum sits exactly at threshold, we shift
	// the fraction down one 20% bucket.
	ratio := Params.DataCoordCfg.SingleCompactionRatioThreshold.GetAsFloat()
	expiredFraction := 0.0
	if commitTs > 0 {
		if commitTs < compactTime.expireTime {
			expiredFraction = 1.0
		}
	} else {
		// Quantile i covers fraction (i+1)/len(quantiles) of rows. Count the
		// prefix of quantiles older than the expiration horizon, then shift
		// down one bucket (the deliberate under-estimate described above).
		quantiles := stats.GetTimestampQuantiles()
		qualifying := 0
		for _, q := range quantiles {
			if q <= 0 || uint64(q) >= compactTime.expireTime {
				break
			}
			qualifying++
		}
		if qualifying >= 2 {
			expiredFraction = float64(qualifying-1) / float64(len(quantiles))
		}
	}
	expiredApproxSize := int64(expiredFraction * float64(stats.GetInsertBinlogSize()))
	if expiredFraction >= ratio ||
		expiredApproxSize > Params.DataCoordCfg.SingleCompactionExpiredLogMaxSize.GetAsInt64() {
		mlog.Info(context.TODO(), "expired entities exceed ratio/size threshold, trigger compaction",
			mlog.Int64("segmentID", segment.ID),
			mlog.Float64("expiredFraction", expiredFraction),
			mlog.Int64("approxExpiredSize", expiredApproxSize),
			mlog.Bool("createdByCompaction", segment.CreatedByCompaction),
			mlog.Int64s("compactionFrom", segment.CompactionFrom))
		return true
	}

	// check if deltalog count, size, and deleted rowcount ratio exceeds threshold
	if hasTooManyDeletions(segment) {
		return true
	}

	if t.ShouldRebuildSegmentIndex(segment) {
		return true
	}

	if t.ShouldCompactExpiryWithTTLField(compactTime, segment) {
		mlog.Info(context.TODO(), "ttl field is expired, trigger compaction", mlog.FieldSegmentID(segment.ID),
			mlog.FieldCollectionID(segment.CollectionID),
			mlog.FieldPartitionID(segment.PartitionID),
			mlog.String("channel", segment.InsertChannel))
		return true
	}

	return false
}

func (t *compactionTrigger) ShouldRebuildSegmentIndex(segment *SegmentInfo) bool {
	if Params.DataCoordCfg.AutoUpgradeSegmentIndex.GetAsBool() {
		// index version of segment lower than resolved version and IndexFileKeys should have value, trigger compaction
		indexIDToSegIdxes := t.meta.indexMeta.GetSegmentIndexes(segment.CollectionID, segment.ID)
		for _, index := range indexIDToSegIdxes {
			if len(index.IndexFileKeys) == 0 {
				continue
			}

			indexParams := t.meta.indexMeta.GetIndexParams(segment.CollectionID, index.IndexID)
			indexType := GetIndexType(indexParams)
			isVectorIndex := vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType)

			var resolvedEngineVersion int32
			var segmentIndexVersion int32
			if isVectorIndex {
				resolvedEngineVersion = t.indexEngineVersionManager.ResolveVecIndexVersion()
				segmentIndexVersion = index.CurrentIndexVersion
			} else {
				resolvedEngineVersion = t.indexEngineVersionManager.ResolveScalarIndexVersion()
				segmentIndexVersion = index.CurrentScalarIndexVersion
			}

			if segmentIndexVersion < resolvedEngineVersion {
				mlog.Info(context.TODO(), "index version is too old, trigger compaction",
					mlog.FieldSegmentID(segment.ID),
					mlog.FieldIndexID(index.IndexID),
					mlog.String("indexType", indexType),
					mlog.Bool("isVectorIndex", isVectorIndex),
					mlog.Strings("indexFileKeys", index.IndexFileKeys),
					mlog.Int32("segmentIndexVersion", segmentIndexVersion),
					mlog.Int32("resolvedEngineVersion", resolvedEngineVersion))
				return true
			}
		}
	}

	// enable force rebuild index with target index version (only for vector index)
	if Params.DataCoordCfg.ForceRebuildSegmentIndex.GetAsBool() && Params.DataCoordCfg.TargetVecIndexVersion.GetAsInt64() != -1 {
		resolvedVecTarget := t.indexEngineVersionManager.ResolveVecIndexVersion()
		indexIDToSegIdxes := t.meta.indexMeta.GetSegmentIndexes(segment.CollectionID, segment.ID)
		for _, index := range indexIDToSegIdxes {
			if len(index.IndexFileKeys) == 0 {
				continue
			}

			indexParams := t.meta.indexMeta.GetIndexParams(segment.CollectionID, index.IndexID)
			indexType := GetIndexType(indexParams)
			isVectorIndex := vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType)

			// ForceRebuildSegmentIndex with TargetVecIndexVersion only applies to vector indexes
			if !isVectorIndex {
				continue
			}

			if index.CurrentIndexVersion != resolvedVecTarget {
				mlog.Info(context.TODO(), "index version is not equal to target vec index version, trigger compaction",
					mlog.FieldSegmentID(segment.ID),
					mlog.FieldIndexID(index.IndexID),
					mlog.String("indexType", indexType),
					mlog.Strings("indexFileKeys", index.IndexFileKeys),
					mlog.Int32("currentIndexVersion", index.CurrentIndexVersion),
					mlog.Int32("resolvedTargetVersion", resolvedVecTarget))
				return true
			}
		}
	}

	// enable force rebuild scalar index with target scalar index version
	if Params.DataCoordCfg.ForceRebuildScalarSegmentIndex.GetAsBool() && Params.DataCoordCfg.TargetScalarIndexVersion.GetAsInt64() != -1 {
		resolvedScalarTarget := t.indexEngineVersionManager.ResolveScalarIndexVersion()
		indexIDToSegIdxes := t.meta.indexMeta.GetSegmentIndexes(segment.CollectionID, segment.ID)
		for _, index := range indexIDToSegIdxes {
			if len(index.IndexFileKeys) == 0 {
				continue
			}

			indexParams := t.meta.indexMeta.GetIndexParams(segment.CollectionID, index.IndexID)
			indexType := GetIndexType(indexParams)
			isVectorIndex := vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType)

			if isVectorIndex {
				continue
			}

			if index.CurrentScalarIndexVersion != resolvedScalarTarget {
				mlog.Info(context.TODO(), "scalar index version != target, trigger compaction",
					mlog.FieldSegmentID(segment.ID),
					mlog.FieldIndexID(index.IndexID),
					mlog.String("indexType", indexType),
					mlog.Int32("currentScalarIndexVersion", index.CurrentScalarIndexVersion),
					mlog.Int32("resolvedTargetVersion", resolvedScalarTarget))
				return true
			}
		}
	}

	return false
}

func isFlushed(segment *SegmentInfo) bool {
	return segment.GetState() == commonpb.SegmentState_Flushed
}

func isFlush(segment *SegmentInfo) bool {
	return segment.GetState() == commonpb.SegmentState_Flushed || segment.GetState() == commonpb.SegmentState_Flushing
}

func canTriggerSortCompaction(segment *SegmentInfo) bool {
	return segment.GetState() == commonpb.SegmentState_Flushed &&
		segment.GetLevel() != datapb.SegmentLevel_L0 &&
		(!segment.GetIsSorted() && !segment.GetIsSortedByNamespace()) &&
		!segment.GetIsImporting() &&
		!segment.isCompacting
}
