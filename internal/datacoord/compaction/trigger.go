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

package compaction

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/samber/lo"
	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/policy"
	"github.com/milvus-io/milvus/internal/datacoord/compaction/view"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
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

// todo: migrate to compaction_trigger_v2
type Trigger interface {
	Start()
	Stop()
	TriggerCompaction(ctx context.Context, signal *CompactionSignal) (signalID UniqueID, err error)
}

type CompactionSignal struct {
	id           UniqueID
	IsForce      bool
	CollectionID UniqueID
	partitionID  UniqueID
	channel      string
	segmentIDs   []UniqueID
	pos          *msgpb.MsgPosition
	resultCh     chan error
	waitResult   bool
}

func NewCompactionSignal() *CompactionSignal {
	return &CompactionSignal{
		resultCh:   make(chan error, 1),
		waitResult: true,
	}
}

func (cs *CompactionSignal) WithID(id UniqueID) *CompactionSignal {
	cs.id = id
	return cs
}

func (cs *CompactionSignal) WithIsForce(isForce bool) *CompactionSignal {
	cs.IsForce = isForce
	return cs
}

func (cs *CompactionSignal) WithCollectionID(collectionID UniqueID) *CompactionSignal {
	cs.CollectionID = collectionID
	return cs
}

func (cs *CompactionSignal) WithPartitionID(partitionID UniqueID) *CompactionSignal {
	cs.partitionID = partitionID
	return cs
}

func (cs *CompactionSignal) WithChannel(channel string) *CompactionSignal {
	cs.channel = channel
	return cs
}

func (cs *CompactionSignal) WithSegmentIDs(segmentIDs ...UniqueID) *CompactionSignal {
	cs.segmentIDs = segmentIDs
	return cs
}

func (cs *CompactionSignal) WithWaitResult(waitResult bool) *CompactionSignal {
	cs.waitResult = waitResult
	return cs
}

func (cs *CompactionSignal) Notify(result error) {
	select {
	case cs.resultCh <- result:
	default:
	}
}

var _ Trigger = (*compactionTrigger)(nil)

type compactionTrigger struct {
	handler       meta.Handler
	meta          *meta.Meta
	indexMeta     *index.IndexMeta
	allocator     allocator.Allocator
	Signals       chan *CompactionSignal
	manualSignals chan *CompactionSignal
	inspector     CompactionInspector
	globalTrigger *time.Ticker
	closeCh       lifetime.SafeChan
	closeWaiter   sync.WaitGroup

	indexEngineVersionManager index.IndexEngineVersionManager

	// A sloopy hack, so we can test with different segment row count without worrying that
	// they are re-calculated in every compaction.
	testingOnly bool
}

func NewCompactionTrigger(
	mt *meta.Meta,
	indexMeta *index.IndexMeta,
	inspector CompactionInspector,
	allocator allocator.Allocator,
	handler meta.Handler,
	indexVersionManager index.IndexEngineVersionManager,
) *compactionTrigger {
	return &compactionTrigger{
		meta:                      mt,
		indexMeta:                 indexMeta,
		allocator:                 allocator,
		Signals:                   make(chan *CompactionSignal, 100),
		manualSignals:             make(chan *CompactionSignal, 100),
		inspector:                 inspector,
		indexEngineVersionManager: indexVersionManager,
		handler:                   handler,
		closeCh:                   lifetime.NewSafeChan(),
	}
}

func (t *compactionTrigger) Start() {
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
		var signal *CompactionSignal
		select {
		case <-t.closeCh.CloseCh():
			mlog.Info(context.TODO(), "compaction trigger quit")
			return
		case signal = <-t.Signals:
		case signal = <-t.manualSignals:
		}
		err := t.handleSignal(signal)
		if err != nil {
			mlog.Warn(context.TODO(), "unable to handleSignal", mlog.Int64("signalID", signal.id), mlog.Err(err))
		}
		signal.Notify(err)
	}
}

func (t *compactionTrigger) Stop() {
	t.closeCh.Close()
	t.closeWaiter.Wait()
}

func (t *compactionTrigger) getCollection(collectionID UniqueID) (*meta.CollectionInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	coll, err := t.handler.GetCollection(ctx, collectionID)
	if err != nil {
		return nil, merr.Wrapf(err, "collection ID %d not found", collectionID)
	}
	return coll, nil
}

func getCompactTime(ts Timestamp, coll *meta.CollectionInfo) (*view.CompactTime, error) {
	collectionTTL, err := common.GetCollectionTTLFromMap(coll.Properties)
	if err != nil {
		return nil, err
	}

	pts, _ := tsoutil.ParseTS(ts)

	if collectionTTL > 0 {
		ttexpired := pts.Add(-collectionTTL)
		ttexpiredLogic := tsoutil.ComposeTS(ttexpired.UnixNano()/int64(time.Millisecond), 0)
		return &view.CompactTime{StartTime: ts, ExpireTime: ttexpiredLogic, CollectionTTL: collectionTTL}, nil
	}

	// no expiration time
	return &view.CompactTime{StartTime: ts, ExpireTime: 0, CollectionTTL: 0}, nil
}

// TrigerCompaction is the public interface to send compaction signal to work queue.
// when waitResult = true, it waits until the result is returned from worker(via `signal.resultCh`)
// or the context is timeouted/canceled
// otherwise, it just try best to submit the signal to the channel, if the channel is full it just returns err
//
// by default, `signals` channel will be used to send compaction signal
// however, when the `isForce` flag is true, the `manualSignals` channel will be used to skip the queueing
// since manual signals shall have higher priority.
func (t *compactionTrigger) TriggerCompaction(ctx context.Context, signal *CompactionSignal) (signalID UniqueID, err error) {
	// If AutoCompaction disabled, flush request will not trigger compaction
	if !paramtable.Get().DataCoordCfg.EnableAutoCompaction.GetAsBool() && !paramtable.Get().DataCoordCfg.EnableCompaction.GetAsBool() {
		return -1, nil
	}

	id, err := t.allocSignalID(ctx)
	if err != nil {
		return -1, err
	}

	signal.WithID(id)

	signalCh := t.Signals
	// use force signal channel to skip non-force signal queue
	if signal.IsForce {
		signalCh = t.manualSignals
	}

	// non force mode, try best to sent signal only
	if !signal.waitResult {
		select {
		case signalCh <- signal:
		default:
			mlog.Info(ctx, "no space to send compaction signal",
				mlog.FieldCollectionID(signal.CollectionID),
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
func (t *compactionTrigger) handleSignal(signal *CompactionSignal) error {
	log := mlog.With(mlog.Int64("compactionID", signal.id),
		mlog.Int64("signal.collectionID", signal.CollectionID),
		mlog.Int64("signal.partitionID", signal.partitionID),
		mlog.Int64s("signal.segmentIDs", signal.segmentIDs))

	if !signal.IsForce && t.inspector.isFull() {
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
			mlog.Int64("group.partitionID", group.PartitionID),
			mlog.String("group.channel", group.ChannelName),
		)

		if !signal.IsForce && t.inspector.isFull() {
			log.Warn(context.TODO(), "skip to generate compaction plan due to handler full")
			return merr.WrapErrServiceQuotaExceeded("compaction handler full")
		}

		if Params.DataCoordCfg.IndexBasedCompaction.GetAsBool() {
			group.Segments = index.FilterInIndexedSegments(context.Background(), t.handler, t.indexMeta, signal.IsForce, group.Segments...)
		}

		coll, err := t.getCollection(group.CollectionID)
		if err != nil {
			log.Warn(context.TODO(), "get collection info failed, skip handling compaction", mlog.Err(err))
			if signal.CollectionID != 0 {
				return err
			}
			continue
		}

		if !signal.IsForce && !policy.IsCollectionAutoCompactionEnabled(coll) {
			log.RatedInfo(context.TODO(), rate.Limit(20), "collection auto compaction disabled")
			if signal.CollectionID != 0 {
				return nil
			}
			// A global signal spans every collection, so one collection opting
			// out must only skip its own group, not end the whole tick.
			continue
		}

		ct, err := getCompactTime(tsoutil.ComposeTSByTime(time.Now()), coll)
		if err != nil {
			log.Warn(context.TODO(), "get compact time failed, skip to handle compaction", mlog.Err(err))
			if signal.CollectionID != 0 {
				return err
			}
			continue
		}

		expectedSize := policy.GetExpectedSegmentSize(t.indexMeta, coll.ID, coll.Schema)
		plans := t.generatePlans(group.Segments, signal, ct, expectedSize)
		for _, bucket := range plans {
			if !signal.IsForce && t.inspector.isFull() {
				log.Warn(context.TODO(), "skip to generate compaction plan due to handler full")
				return merr.WrapErrServiceQuotaExceeded("compaction handler full")
			}
			inputSegmentIDs := lo.Map(bucket.segments, func(s *meta.SegmentInfo, _ int) int64 { return s.GetID() })

			inputs := typeutil.NewSet[int64](inputSegmentIDs...)
			totalSize := lo.SumBy(group.Segments, func(s *meta.SegmentInfo) int64 {
				if inputs.Contain(s.GetID()) {
					return s.GetSegmentSize()
				}
				return 0
			})
			planID, preAllocatedSegmentIDs, err := allocCompactionPlanIDs(t.allocator, float64(totalSize), float64(expectedSize))
			if err != nil {
				log.Warn(context.TODO(), "fail to allocate id", mlog.Err(err))
				return err
			}
			start := time.Now()
			pts, _ := tsoutil.ParseTS(ct.StartTime)
			task := &datapb.CompactionTask{
				PlanID:                 planID,
				TriggerID:              signal.id,
				State:                  datapb.CompactionTaskState_pipelining,
				StartTime:              pts.Unix(),
				Type:                   datapb.CompactionType_MixCompaction,
				CollectionTtl:          ct.CollectionTTL.Nanoseconds(),
				CollectionID:           group.CollectionID,
				PartitionID:            group.PartitionID,
				Channel:                group.ChannelName,
				InputSegments:          inputSegmentIDs,
				ResultSegments:         []int64{},
				TotalRows:              bucket.totalRows,
				Schema:                 coll.Schema,
				MaxSize:                bucket.maxSize,
				PreAllocatedSegmentIDs: preAllocatedSegmentIDs,
			}
			err = t.inspector.EnqueueCompaction(task)
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
	segments  []*meta.SegmentInfo
	totalRows int64
	maxSize   int64
}

func (t *compactionTrigger) generatePlans(segments []*meta.SegmentInfo, signal *CompactionSignal, compactTime *view.CompactTime, expectedSize int64) []*compactionBucket {
	if Params.DataCoordCfg.TwoTierCompaction.GetAsBool() {
		return t.generatePlansTwoTier(segments, signal, compactTime, expectedSize)
	}
	return t.generatePlansLegacy(segments, signal, compactTime, expectedSize)
}

func (t *compactionTrigger) generatePlansTwoTier(segments []*meta.SegmentInfo, signal *CompactionSignal, compactTime *view.CompactTime, expectedSize int64) []*compactionBucket {
	if len(segments) == 0 {
		mlog.Warn(context.TODO(), "the number of candidate segments is 0, skip to generate compaction plan")
		return nil
	}

	maxFragments := Params.DataCoordCfg.MaxFragmentsPerGroup.GetAsInt64()
	middleSize := compactionMiddleSize(expectedSize)

	// Step 1: Classify segments.
	// Force compaction (manual trigger) bypasses the single-compaction
	// classification so all segments are packed together.
	var prioritized []*meta.SegmentInfo
	var compactable []*meta.SegmentInfo

	for _, segment := range segments {
		segment := segment.ShadowClone()
		if signal.IsForce {
			compactable = append(compactable, segment)
		} else if t.ShouldDoSingleCompaction(segment, compactTime) {
			prioritized = append(prioritized, segment)
		} else if !isFullSegment(expectedSize, segment.GetResidualSegmentSize()) {
			compactable = append(compactable, segment)
		}
	}

	var buckets []*compactionBucket

	// Prioritized segments -> single-segment compaction tasks
	for _, s := range prioritized {
		buckets = append(buckets, &compactionBucket{
			segments:  []*meta.SegmentInfo{s},
			totalRows: s.GetNumOfRows(),
			maxSize:   expectedSize,
		})
	}

	// Step 2: Full-tier composition.
	// Force compaction bypasses the fill-rate gate and minimum-segment
	// count so every candidate is packed.
	packer := view.NewSegmentPacker("full-tier", compactable, compactTime)
	fullMaxLeftSize := expectedSize - compactionFullThreshold(expectedSize)
	minSegs := int64(2)
	if signal.IsForce {
		fullMaxLeftSize = math.MaxInt64
		minSegs = 1
	}
	for {
		pack, _ := packer.PackSliding(expectedSize, fullMaxLeftSize, minSegs, math.MaxInt64)
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

	// Force compaction: segments too large for the target size still
	// need individual compaction tasks (e.g. to apply deletes).
	if signal.IsForce {
		for _, s := range packer.Candidates {
			buckets = append(buckets, &compactionBucket{
				segments:  []*meta.SegmentInfo{s},
				totalRows: s.GetNumOfRows(),
				maxSize:   expectedSize,
			})
		}
		packer.Candidates = nil
	}

	// Step 3: Fragment-tier composition
	var fragmentCount int64
	for _, s := range packer.Candidates {
		if isFragmentSegment(expectedSize, s.GetResidualSegmentSize()) {
			fragmentCount++
		}
	}

	if fragmentCount > maxFragments {
		var fragments []*meta.SegmentInfo
		for _, s := range packer.Candidates {
			if isFragmentSegment(expectedSize, s.GetResidualSegmentSize()) {
				fragments = append(fragments, s)
			}
		}

		fragPacker := view.NewSegmentPacker("fragment-tier", fragments, compactTime)
		for {
			pack, _ := fragPacker.Pack(middleSize, math.MaxInt64, 2, math.MaxInt64)
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
			mlog.FieldCollectionID(signal.CollectionID),
			mlog.Int("prioritized", len(prioritized)),
			mlog.Int("compactable", len(compactable)),
			mlog.Int("buckets", len(buckets)),
			mlog.Int64("fragmentCount", fragmentCount),
			mlog.Int64("maxFragments", maxFragments))
	}

	return buckets
}

func (t *compactionTrigger) generatePlansLegacy(segments []*meta.SegmentInfo, signal *CompactionSignal, compactTime *view.CompactTime, expectedSize int64) []*compactionBucket {
	if len(segments) == 0 {
		mlog.Warn(context.TODO(), "the number of candidate segments is 0, skip to generate compaction plan")
		return nil
	}

	var prioritizedCandidates []*meta.SegmentInfo
	var smallCandidates []*meta.SegmentInfo
	var nonPlannedSegments []*meta.SegmentInfo

	for _, segment := range segments {
		segment := segment.ShadowClone()
		if signal.IsForce || t.ShouldDoSingleCompaction(segment, compactTime) {
			prioritizedCandidates = append(prioritizedCandidates, segment)
		} else if t.isSmallSegment(segment, expectedSize) {
			smallCandidates = append(smallCandidates, segment)
		} else {
			nonPlannedSegments = append(nonPlannedSegments, segment)
		}
	}

	buckets := [][]*meta.SegmentInfo{}
	toUpdate := view.NewSegmentPacker("update", prioritizedCandidates, compactTime)
	toMerge := view.NewSegmentPacker("merge", smallCandidates, compactTime)

	maxSegs := int64(4096)
	minSegs := Params.DataCoordCfg.MinSegmentToMerge.GetAsInt64()
	compactableProportion := Params.DataCoordCfg.SegmentCompactableProportion.GetAsFloat()
	satisfiedSize := int64(float64(expectedSize) * compactableProportion)
	maxLeftSize := expectedSize - satisfiedSize
	reasons := make([]string, 0)

	for {
		pack, left := toMerge.Pack(expectedSize, maxLeftSize, minSegs, maxSegs)
		if len(pack) == 0 {
			break
		}
		reasons = append(reasons, fmt.Sprintf("merging %d small segments with left size %d", len(pack), left))
		buckets = append(buckets, pack)
	}

	for {
		pack, _ := toUpdate.PackWith(expectedSize, math.MaxInt64, 0, maxSegs, toMerge)
		if len(pack) == 0 {
			break
		}
		reasons = append(reasons, fmt.Sprintf("packing %d prioritized segments", len(pack)))
		buckets = append(buckets, pack)
	}
	for _, s := range toUpdate.Candidates {
		buckets = append(buckets, []*meta.SegmentInfo{s})
		reasons = append(reasons, fmt.Sprintf("force packing prioritized segment %d", s.GetID()))
	}

	for {
		pack, _ := toMerge.Pack(expectedSize, math.MaxInt64, minSegs, maxSegs)
		if len(pack) == 0 {
			break
		}
		reasons = append(reasons, fmt.Sprintf("packing all %d small segments", len(pack)))
		buckets = append(buckets, pack)
	}
	smallRemaining := t.squeezeSmallSegmentsToBuckets(toMerge.Candidates, buckets, expectedSize)

	result := make([]*compactionBucket, 0, len(buckets))
	for _, b := range buckets {
		var totalRows int64
		for _, s := range b {
			totalRows += s.GetNumOfRows()
		}
		result = append(result, &compactionBucket{
			segments:  b,
			totalRows: totalRows,
			maxSize:   expectedSize,
		})
	}

	if len(result) > 0 {
		mlog.Info(context.TODO(), "generated nontrivial compaction tasks",
			mlog.FieldCollectionID(signal.CollectionID),
			mlog.Int("prioritizedCandidates", len(prioritizedCandidates)),
			mlog.Int("smallCandidates", len(smallCandidates)),
			mlog.Int("nonPlannedSegments", len(nonPlannedSegments)),
			mlog.Strings("reasons", reasons))
	}
	if len(smallRemaining) > 0 {
		mlog.RatedInfo(context.TODO(), rate.Limit(300), "remain small segments",
			mlog.FieldCollectionID(signal.CollectionID),
			mlog.FieldPartitionID(signal.partitionID),
			mlog.String("channel", signal.channel),
			mlog.Int("smallRemainingCount", len(smallRemaining)))
	}
	return result
}

func (t *compactionTrigger) isSmallSegment(segment *meta.SegmentInfo, expectedSize int64) bool {
	return segment.GetSegmentSize() < int64(float64(expectedSize)*Params.DataCoordCfg.SegmentSmallProportion.GetAsFloat())
}

func isExpandableSmallSegment(segment *meta.SegmentInfo, expectedSize int64) bool {
	return segment.GetSegmentSize() < int64(float64(expectedSize)*(Params.DataCoordCfg.SegmentExpansionRate.GetAsFloat()-1))
}

func (t *compactionTrigger) squeezeSmallSegmentsToBuckets(small []*meta.SegmentInfo, buckets [][]*meta.SegmentInfo, expectedSize int64) (remaining []*meta.SegmentInfo) {
	for i := len(small) - 1; i >= 0; i-- {
		s := small[i]
		if !isExpandableSmallSegment(s, expectedSize) {
			continue
		}
		for bidx, b := range buckets {
			totalSize := lo.SumBy(b, func(s *meta.SegmentInfo) int64 { return s.GetSegmentSize() })
			if totalSize+s.GetSegmentSize() > int64(Params.DataCoordCfg.SegmentExpansionRate.GetAsFloat()*float64(expectedSize)) {
				continue
			}
			buckets[bidx] = append(buckets[bidx], s)
			small = append(small[:i], small[i+1:]...)
			break
		}
	}
	return small
}

// getCandidates converts signal criterion into corresponding compaction candidate groups
// since non-major compaction happens under channel+partition level
// the selected segments are grouped into these categories.
func (t *compactionTrigger) getCandidates(signal *CompactionSignal) ([]meta.ChanPartSegments, error) {
	// Fail-closed: if any protected snapshot's RefIndex hasn't loaded yet,
	// block compaction for the entire collection.
	if signal.CollectionID > 0 && t.meta.IsCollectionCompactionBlocked(signal.CollectionID) {
		mlog.Info(context.TODO(), "skip compaction candidates for collection due to unloaded protected snapshot RefIndex",
			mlog.FieldCollectionID(signal.CollectionID))
		return nil, nil
	}

	// default filter, select segments which could be compacted
	filters := []meta.SegmentFilter{
		meta.SegmentFilterFunc(func(segment *meta.SegmentInfo) bool {
			return policy.IsNormalManualCompactionCandidate(t.meta, segment)
		}),
	}

	// add segment filter if criterion provided
	if signal.CollectionID > 0 {
		filters = append(filters, meta.WithCollection(signal.CollectionID))
	}
	if signal.channel != "" {
		filters = append(filters, meta.WithChannel(signal.channel))
	}
	if signal.partitionID > 0 {
		filters = append(filters, meta.SegmentFilterFunc(func(si *meta.SegmentInfo) bool {
			return si.GetPartitionID() == signal.partitionID
		}))
	}
	// segment id provided
	// select these segments only
	if len(signal.segmentIDs) > 0 {
		idSet := typeutil.NewSet(signal.segmentIDs...)
		filters = append(filters, meta.SegmentFilterFunc(func(si *meta.SegmentInfo) bool {
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
	groups := lo.GroupBy(segments, func(segment *meta.SegmentInfo) category {
		return category{
			collectionID: segment.CollectionID,
			partitionID:  segment.PartitionID,
			channelName:  segment.InsertChannel,
		}
	})

	return lo.MapToSlice(groups, func(c category, segments []*meta.SegmentInfo) meta.ChanPartSegments {
		return meta.ChanPartSegments{
			CollectionID: c.collectionID,
			PartitionID:  c.partitionID,
			ChannelName:  c.channelName,
			Segments:     segments,
		}
	}), nil
}

func (t *compactionTrigger) ShouldCompactExpiry(fromTs uint64, compactTime *view.CompactTime, segment *meta.SegmentInfo) bool {
	if Params.DataCoordCfg.CompactionExpiryTolerance.GetAsInt() >= 0 {
		tolerantDuration := Params.DataCoordCfg.CompactionExpiryTolerance.GetAsDuration(time.Hour)
		expireTime, _ := tsoutil.ParseTS(compactTime.ExpireTime)
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

func (t *compactionTrigger) ShouldCompactExpiryWithTTLField(compactTime *view.CompactTime, segment *meta.SegmentInfo) bool {
	percentiles := segment.GetExpirQuantiles()
	if len(percentiles) == 0 {
		return false
	}

	ratio := Params.DataCoordCfg.SingleCompactionRatioThreshold.GetAsFloat()

	idx := getExpirQuantilesIndexByRatio(ratio, len(percentiles))
	expirationTime := percentiles[idx]
	// If current time (startTime) is greater than the expiration time at this percentile, trigger compaction
	startTs := tsoutil.PhysicalTime(compactTime.StartTime)
	return startTs.UnixMicro() >= expirationTime && expirationTime > 0
}

func (t *compactionTrigger) ShouldDoSingleCompaction(segment *meta.SegmentInfo, compactTime *view.CompactTime) bool {
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
		if commitTs < compactTime.ExpireTime {
			expiredFraction = 1.0
		}
	} else {
		// Quantile i covers fraction (i+1)/len(quantiles) of rows. Count the
		// prefix of quantiles older than the expiration horizon, then shift
		// down one bucket (the deliberate under-estimate described above).
		quantiles := stats.GetTimestampQuantiles()
		qualifying := 0
		for _, q := range quantiles {
			if q <= 0 || uint64(q) >= compactTime.ExpireTime {
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
	if policy.HasTooManyDeletions(segment) {
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

func (t *compactionTrigger) ShouldRebuildSegmentIndex(segment *meta.SegmentInfo) bool {
	if Params.DataCoordCfg.AutoUpgradeSegmentIndex.GetAsBool() {
		// index version of segment lower than resolved version and IndexFileKeys should have value, trigger compaction
		indexIDToSegIdxes := t.indexMeta.GetSegmentIndexes(segment.CollectionID, segment.ID)
		for _, idx := range indexIDToSegIdxes {
			if len(idx.IndexFileKeys) == 0 {
				continue
			}

			indexParams := t.indexMeta.GetIndexParams(segment.CollectionID, idx.IndexID)
			indexType := meta.GetIndexType(indexParams)
			isVectorIndex := vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType)

			var resolvedEngineVersion int32
			var segmentIndexVersion int32
			if isVectorIndex {
				resolvedEngineVersion = t.indexEngineVersionManager.ResolveVecIndexVersion()
				segmentIndexVersion = idx.CurrentIndexVersion
			} else {
				resolvedEngineVersion = t.indexEngineVersionManager.ResolveScalarIndexVersion()
				segmentIndexVersion = idx.CurrentScalarIndexVersion
			}

			if segmentIndexVersion < resolvedEngineVersion {
				mlog.Info(context.TODO(), "index version is too old, trigger compaction",
					mlog.FieldSegmentID(segment.ID),
					mlog.FieldIndexID(idx.IndexID),
					mlog.String("indexType", indexType),
					mlog.Bool("isVectorIndex", isVectorIndex),
					mlog.Strings("indexFileKeys", idx.IndexFileKeys),
					mlog.Int32("segmentIndexVersion", segmentIndexVersion),
					mlog.Int32("resolvedEngineVersion", resolvedEngineVersion))
				return true
			}
		}
	}

	// enable force rebuild index with target index version (only for vector index)
	if Params.DataCoordCfg.ForceRebuildSegmentIndex.GetAsBool() && Params.DataCoordCfg.TargetVecIndexVersion.GetAsInt64() != -1 {
		resolvedVecTarget := t.indexEngineVersionManager.ResolveVecIndexVersion()
		indexIDToSegIdxes := t.indexMeta.GetSegmentIndexes(segment.CollectionID, segment.ID)
		for _, idx := range indexIDToSegIdxes {
			if len(idx.IndexFileKeys) == 0 {
				continue
			}

			indexParams := t.indexMeta.GetIndexParams(segment.CollectionID, idx.IndexID)
			indexType := meta.GetIndexType(indexParams)
			isVectorIndex := vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType)

			// ForceRebuildSegmentIndex with TargetVecIndexVersion only applies to vector indexes
			if !isVectorIndex {
				continue
			}

			if idx.CurrentIndexVersion != resolvedVecTarget {
				mlog.Info(context.TODO(), "index version is not equal to target vec index version, trigger compaction",
					mlog.FieldSegmentID(segment.ID),
					mlog.FieldIndexID(idx.IndexID),
					mlog.String("indexType", indexType),
					mlog.Strings("indexFileKeys", idx.IndexFileKeys),
					mlog.Int32("currentIndexVersion", idx.CurrentIndexVersion),
					mlog.Int32("resolvedTargetVersion", resolvedVecTarget))
				return true
			}
		}
	}

	// enable force rebuild scalar index with target scalar index version
	if Params.DataCoordCfg.ForceRebuildScalarSegmentIndex.GetAsBool() && Params.DataCoordCfg.TargetScalarIndexVersion.GetAsInt64() != -1 {
		resolvedScalarTarget := t.indexEngineVersionManager.ResolveScalarIndexVersion()
		indexIDToSegIdxes := t.indexMeta.GetSegmentIndexes(segment.CollectionID, segment.ID)
		for _, idx := range indexIDToSegIdxes {
			if len(idx.IndexFileKeys) == 0 {
				continue
			}

			indexParams := t.indexMeta.GetIndexParams(segment.CollectionID, idx.IndexID)
			indexType := meta.GetIndexType(indexParams)
			isVectorIndex := vecindexmgr.GetVecIndexMgrInstance().IsVecIndex(indexType)

			if isVectorIndex {
				continue
			}

			if idx.CurrentScalarIndexVersion != resolvedScalarTarget {
				mlog.Info(context.TODO(), "scalar index version != target, trigger compaction",
					mlog.FieldSegmentID(segment.ID),
					mlog.FieldIndexID(idx.IndexID),
					mlog.String("indexType", indexType),
					mlog.Int32("currentScalarIndexVersion", idx.CurrentScalarIndexVersion),
					mlog.Int32("resolvedTargetVersion", resolvedScalarTarget))
				return true
			}
		}
	}

	return false
}
