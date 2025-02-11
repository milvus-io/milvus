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

package segments

/*
#cgo pkg-config: milvus_core

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
*/
import "C"

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments/metricsutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/eventlog"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/cache"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// TODO maybe move to manager and change segment constructor
var channelMapper = metautil.NewDynChannelMapper()

type SegmentAction func(segment Segment) bool

func IncreaseVersion(version int64) SegmentAction {
	return func(segment Segment) bool {
		log := log.Ctx(context.Background()).With(
			zap.Int64("segmentID", segment.ID()),
			zap.String("type", segment.Type().String()),
			zap.Int64("segmentVersion", segment.Version()),
			zap.Int64("updateVersion", version),
		)
		for oldVersion := segment.Version(); oldVersion < version; {
			if segment.CASVersion(oldVersion, version) {
				return true
			}
		}
		log.Warn("segment version cannot go backwards, skip update")
		return false
	}
}

type Manager struct {
	Collection CollectionManager
	Segment    SegmentManager
	DiskCache  cache.Cache[int64, Segment]
	Loader     Loader
}

func NewManager() *Manager {
	diskCap := paramtable.Get().QueryNodeCfg.DiskCacheCapacityLimit.GetAsSize()

	segMgr := NewSegmentManager()
	sf := singleflight.Group{}
	manager := &Manager{
		Collection: NewCollectionManager(),
		Segment:    segMgr,
	}

	manager.DiskCache = cache.NewCacheBuilder[int64, Segment]().WithLazyScavenger(func(key int64) int64 {
		segment := segMgr.GetWithType(key, SegmentTypeSealed)
		if segment == nil {
			return 0
		}
		return int64(segment.ResourceUsageEstimate().DiskSize)
	}, diskCap).WithLoader(func(ctx context.Context, key int64) (Segment, error) {
		log := log.Ctx(ctx)
		log.Debug("cache missed segment", zap.Int64("segmentID", key))
		segment := segMgr.GetWithType(key, SegmentTypeSealed)
		if segment == nil {
			// the segment has been released, just ignore it
			log.Warn("segment is not found when loading", zap.Int64("segmentID", key))
			return nil, merr.ErrSegmentNotFound
		}
		info := segment.LoadInfo()
		_, err, _ := sf.Do(fmt.Sprint(segment.ID()), func() (nop interface{}, err error) {
			cacheLoadRecord := metricsutil.NewCacheLoadRecord(getSegmentMetricLabel(segment))
			cacheLoadRecord.WithBytes(segment.ResourceUsageEstimate().DiskSize)
			defer func() {
				cacheLoadRecord.Finish(err)
			}()

			collection := manager.Collection.Get(segment.Collection())
			if collection == nil {
				return nil, merr.WrapErrCollectionNotLoaded(segment.Collection(), "failed to load segment fields")
			}

			err = manager.Loader.LoadLazySegment(ctx, segment.(*LocalSegment), info)
			return nil, err
		})
		if err != nil {
			log.Warn("cache sealed segment failed", zap.Error(err))
			return nil, err
		}
		return segment, nil
	}).WithFinalizer(func(ctx context.Context, key int64, segment Segment) error {
		log := log.Ctx(ctx)
		log.Debug("evict segment from cache", zap.Int64("segmentID", key))
		cacheEvictRecord := metricsutil.NewCacheEvictRecord(getSegmentMetricLabel(segment))
		cacheEvictRecord.WithBytes(segment.ResourceUsageEstimate().DiskSize)
		defer cacheEvictRecord.Finish(nil)
		segment.Release(ctx, WithReleaseScope(ReleaseScopeData))
		return nil
	}).WithReloader(func(ctx context.Context, key int64) (Segment, error) {
		log := log.Ctx(ctx)
		segment := segMgr.GetWithType(key, SegmentTypeSealed)
		if segment == nil {
			// the segment has been released, just ignore it
			log.Debug("segment is not found when reloading", zap.Int64("segmentID", key))
			return nil, merr.ErrSegmentNotFound
		}

		localSegment := segment.(*LocalSegment)
		err := manager.Loader.LoadIndex(ctx, localSegment, segment.LoadInfo(), segment.NeedUpdatedVersion())
		if err != nil {
			log.Warn("reload segment failed", zap.Int64("segmentID", key), zap.Error(err))
			return nil, merr.ErrSegmentLoadFailed
		}
		if err := localSegment.RemoveUnusedFieldFiles(); err != nil {
			log.Warn("remove unused field files failed", zap.Int64("segmentID", key), zap.Error(err))
			return nil, merr.ErrSegmentReduplicate
		}

		return segment, nil
	}).Build()

	segMgr.registerReleaseCallback(func(s Segment) {
		if s.Type() == SegmentTypeSealed {
			// !!! We cannot use ctx of request to call Remove,
			// Once context canceled, the segment will be leak in cache forever.
			// Because it has been cleaned from segment manager.
			manager.DiskCache.Remove(context.Background(), s.ID())
		}
	})

	return manager
}

func (mgr *Manager) SetLoader(loader Loader) {
	mgr.Loader = loader
}

type SegmentManager interface {
	// Put puts the given segments in,
	// and increases the ref count of the corresponding collection,
	// dup segments will not increase the ref count
	Put(ctx context.Context, segmentType SegmentType, segments ...Segment)
	UpdateBy(action SegmentAction, filters ...SegmentFilter) int
	Get(segmentID typeutil.UniqueID) Segment
	GetWithType(segmentID typeutil.UniqueID, typ SegmentType) Segment
	GetBy(filters ...SegmentFilter) []Segment
	// Get segments and acquire the read locks
	GetAndPinBy(filters ...SegmentFilter) ([]Segment, error)
	GetAndPin(segments []int64, filters ...SegmentFilter) ([]Segment, error)
	Unpin(segments []Segment)

	GetSealed(segmentID typeutil.UniqueID) Segment
	GetGrowing(segmentID typeutil.UniqueID) Segment
	Empty() bool

	// Remove removes the given segment,
	// and decreases the ref count of the corresponding collection,
	// will not decrease the ref count if the given segment not exists
	Remove(ctx context.Context, segmentID typeutil.UniqueID, scope querypb.DataScope) (int, int)
	RemoveBy(ctx context.Context, filters ...SegmentFilter) (int, int)
	Clear(ctx context.Context)

	// Deprecated: quick fix critical issue: #30857
	// TODO: All Segment assigned to querynode should be managed by SegmentManager, including loading or releasing to perform a transaction.
	Exist(segmentID typeutil.UniqueID, typ SegmentType) bool
}

var _ SegmentManager = (*segmentManager)(nil)

type secondarySegmentIndex struct {
	shardSegments map[metautil.Channel]segments
}

func newSecondarySegmentIndex() secondarySegmentIndex {
	return secondarySegmentIndex{
		shardSegments: make(map[metautil.Channel]segments),
	}
}

func (si secondarySegmentIndex) Put(ctx context.Context, segmentType SegmentType, segment Segment) {
	shard := segment.Shard()
	segments, ok := si.shardSegments[shard]
	if !ok {
		segments = newSegments()
		si.shardSegments[shard] = segments
	}
	segments.Put(ctx, segmentType, segment)
}

func (si secondarySegmentIndex) Remove(s Segment) {
	shard := s.Shard()
	segments, ok := si.shardSegments[shard]
	if !ok {
		return
	}
	segments.Remove(s)
	if segments.Empty() {
		delete(si.shardSegments, shard)
	}
}

type segments struct {
	growingSegments map[typeutil.UniqueID]Segment
	sealedSegments  map[typeutil.UniqueID]Segment
}

func (segments segments) Put(_ context.Context, segmentType SegmentType, segment Segment) {
	switch segmentType {
	case SegmentTypeGrowing:
		segments.growingSegments[segment.ID()] = segment
	case SegmentTypeSealed:
		segments.sealedSegments[segment.ID()] = segment
	}
}

func (segments segments) Get(segmentID int64) (growing Segment, sealed Segment) {
	return segments.growingSegments[segmentID], segments.sealedSegments[segmentID]
}

func (segments segments) GetWithType(segmentID int64, segmentType SegmentType) (Segment, bool) {
	// var targetMap map[int64]Segment
	var segment Segment
	var ok bool
	switch segmentType {
	case SegmentTypeGrowing:
		segment, ok = segments.growingSegments[segmentID]
	case SegmentTypeSealed:
		segment, ok = segments.sealedSegments[segmentID]
	}
	return segment, ok
}

func (segments segments) RemoveWithType(segmentID int64, segmentType SegmentType) (Segment, bool) {
	var segment Segment
	var ok bool
	switch segmentType {
	case SegmentTypeGrowing:
		segment, ok = segments.growingSegments[segmentID]
		delete(segments.growingSegments, segmentID)
	case SegmentTypeSealed:
		segment, ok = segments.sealedSegments[segmentID]
		delete(segments.sealedSegments, segmentID)
	}
	return segment, ok
}

func (segments segments) Remove(segment Segment) {
	switch segment.Type() {
	case SegmentTypeGrowing:
		delete(segments.growingSegments, segment.ID())
	case SegmentTypeSealed:
		delete(segments.sealedSegments, segment.ID())
	}
}

func (segments segments) RangeWithFilter(criterion *segmentCriterion, process func(id int64, segType SegmentType, segment Segment) bool) {
	if criterion.segmentIDs != nil {
		for id := range criterion.segmentIDs {
			// var segment Segment
			// var ok bool
			var segs []Segment
			if criterion.segmentType == commonpb.SegmentState_SegmentStateNone {
				growing, sealed := segments.Get(id)
				if growing != nil {
					segs = append(segs, growing)
				}
				if sealed != nil {
					segs = append(segs, sealed)
				}
			} else {
				segment, ok := segments.GetWithType(id, criterion.segmentType)
				if ok {
					segs = append(segs, segment)
				}
			}

			for _, segment := range segs {
				if criterion.Match(segment) {
					if !process(id, segment.Type(), segment) {
						return
					}
				}
			}
		}
		return
	}

	var candidates []map[int64]Segment
	switch criterion.segmentType {
	case SegmentTypeGrowing:
		candidates = []map[int64]Segment{segments.growingSegments}
	case SegmentTypeSealed:
		candidates = []map[int64]Segment{segments.sealedSegments}
	default:
		candidates = []map[int64]Segment{segments.growingSegments, segments.sealedSegments}
	}

	for _, candidate := range candidates {
		for id, segment := range candidate {
			if criterion.Match(segment) {
				if !process(id, segment.Type(), segment) {
					return
				}
			}
		}
	}
}

func (segments segments) Empty() bool {
	return len(segments.growingSegments) == 0 && len(segments.sealedSegments) == 0
}

func newSegments() segments {
	return segments{
		growingSegments: make(map[int64]Segment),
		sealedSegments:  make(map[int64]Segment),
	}
}

// Manager manages all collections and segments
type segmentManager struct {
	mu sync.RWMutex // guards all

	globalSegments segments
	secondaryIndex secondarySegmentIndex

	// releaseCallback is the callback function when a segment is released.
	releaseCallback func(s Segment)

	growingOnReleasingSegments typeutil.UniqueSet
	sealedOnReleasingSegments  typeutil.UniqueSet
}

func NewSegmentManager() *segmentManager {
	return &segmentManager{
		globalSegments:             newSegments(),
		secondaryIndex:             newSecondarySegmentIndex(),
		growingOnReleasingSegments: typeutil.NewUniqueSet(),
		sealedOnReleasingSegments:  typeutil.NewUniqueSet(),
	}
}

// put is the internal put method updating both global segments and secondary index.
func (mgr *segmentManager) put(ctx context.Context, segmentType SegmentType, segment Segment) {
	mgr.globalSegments.Put(ctx, segmentType, segment)
	mgr.secondaryIndex.Put(ctx, segmentType, segment)
}

func (mgr *segmentManager) Put(ctx context.Context, segmentType SegmentType, segments ...Segment) {
	var replacedSegment []Segment
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	log := log.Ctx(ctx)
	for _, segment := range segments {
		oldSegment, ok := mgr.globalSegments.GetWithType(segment.ID(), segmentType)
		if ok {
			if oldSegment.Version() >= segment.Version() {
				log.Warn("Invalid segment distribution changed, skip it",
					zap.Int64("segmentID", segment.ID()),
					zap.Int64("oldVersion", oldSegment.Version()),
					zap.Int64("newVersion", segment.Version()),
				)
				// delete redundant segment
				segment.Release(ctx)
				continue
			}
			replacedSegment = append(replacedSegment, oldSegment)
		}

		mgr.put(ctx, segmentType, segment)

		eventlog.Record(eventlog.NewRawEvt(eventlog.Level_Info, fmt.Sprintf("Segment %d[%d] loaded", segment.ID(), segment.Collection())))
		metrics.QueryNodeNumSegments.WithLabelValues(
			fmt.Sprint(paramtable.GetNodeID()),
			fmt.Sprint(segment.Collection()),
			fmt.Sprint(segment.Partition()),
			segment.Type().String(),
			fmt.Sprint(len(segment.Indexes())),
			segment.Level().String(),
		).Inc()
	}
	mgr.updateMetric()

	// release replaced segment
	if len(replacedSegment) > 0 {
		go func() {
			for _, segment := range replacedSegment {
				mgr.release(ctx, segment)
			}
		}()
	}
}

func (mgr *segmentManager) UpdateBy(action SegmentAction, filters ...SegmentFilter) int {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	updated := 0
	mgr.rangeWithFilter(func(_ int64, _ SegmentType, segment Segment) bool {
		if action(segment) {
			updated++
		}
		return true
	}, filters...)
	return updated
}

// Deprecated:
// TODO: All Segment assigned to querynode should be managed by SegmentManager, including loading or releasing to perform a transaction.
func (mgr *segmentManager) Exist(segmentID typeutil.UniqueID, typ SegmentType) bool {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	_, ok := mgr.globalSegments.GetWithType(segmentID, typ)
	if ok {
		return true
	}
	switch typ {
	case SegmentTypeGrowing:
		if mgr.growingOnReleasingSegments.Contain(segmentID) {
			return true
		}
	case SegmentTypeSealed:
		if mgr.sealedOnReleasingSegments.Contain(segmentID) {
			return true
		}
	}

	return false
}

func (mgr *segmentManager) Get(segmentID typeutil.UniqueID) Segment {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	growing, sealed := mgr.globalSegments.Get(segmentID)
	if growing != nil {
		return growing
	}
	return sealed
}

func (mgr *segmentManager) GetWithType(segmentID typeutil.UniqueID, typ SegmentType) Segment {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	segment, _ := mgr.globalSegments.GetWithType(segmentID, typ)
	return segment
}

func (mgr *segmentManager) GetBy(filters ...SegmentFilter) []Segment {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	var ret []Segment
	mgr.rangeWithFilter(func(id int64, _ SegmentType, segment Segment) bool {
		ret = append(ret, segment)
		return true
	}, filters...)
	return ret
}

func (mgr *segmentManager) GetAndPinBy(filters ...SegmentFilter) ([]Segment, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	var ret []Segment
	var err error
	defer func() {
		if err != nil {
			for _, segment := range ret {
				segment.Unpin()
			}
			ret = nil
		}
	}()

	mgr.rangeWithFilter(func(id int64, _ SegmentType, segment Segment) bool {
		if segment.Level() == datapb.SegmentLevel_L0 {
			return true
		}
		err = segment.PinIfNotReleased()
		if err != nil {
			return false
		}
		ret = append(ret, segment)
		return true
	}, filters...)

	return ret, err
}

func (mgr *segmentManager) GetAndPin(segments []int64, filters ...SegmentFilter) ([]Segment, error) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	lockedSegments := make([]Segment, 0, len(segments))
	var err error
	defer func() {
		if err != nil {
			for _, segment := range lockedSegments {
				segment.Unpin()
			}
			lockedSegments = nil
		}
	}()

	criterion := &segmentCriterion{}
	for _, filter := range filters {
		filter.AddFilter(criterion)
	}

	for _, id := range segments {
		var segments []Segment
		if criterion.segmentType == commonpb.SegmentState_SegmentStateNone {
			growing, sealed := mgr.globalSegments.Get(id)

			if growing == nil && sealed == nil {
				err = merr.WrapErrSegmentNotLoaded(id, "segment not found")
				return nil, err
			}

			segments = []Segment{growing, sealed}
		} else {
			segment, ok := mgr.globalSegments.GetWithType(id, criterion.segmentType)
			if !ok {
				err = merr.WrapErrSegmentNotLoaded(id, "segment not found")
				return nil, err
			}
			segments = []Segment{segment}
		}

		for _, segment := range segments {
			if segment == nil {
				continue
			}
			// L0 Segment should not be queryable.
			if segment.Level() == datapb.SegmentLevel_L0 {
				continue
			}

			if !filter(segment, filters...) {
				continue
			}

			err = segment.PinIfNotReleased()
			if err != nil {
				return nil, err
			}
			lockedSegments = append(lockedSegments, segment)
		}
	}

	return lockedSegments, nil
}

func (mgr *segmentManager) Unpin(segments []Segment) {
	for _, segment := range segments {
		segment.Unpin()
	}
}

func (mgr *segmentManager) rangeWithFilter(process func(id int64, segType SegmentType, segment Segment) bool, filters ...SegmentFilter) {
	criterion := &segmentCriterion{}
	for _, filter := range filters {
		filter.AddFilter(criterion)
	}

	target := mgr.globalSegments
	var ok bool
	if !criterion.channel.IsZero() {
		target, ok = mgr.secondaryIndex.shardSegments[criterion.channel]
		if !ok {
			return
		}
	}

	target.RangeWithFilter(criterion, process)
}

func filter(segment Segment, filters ...SegmentFilter) bool {
	for _, filter := range filters {
		if !filter.Match(segment) {
			return false
		}
	}
	return true
}

func (mgr *segmentManager) GetSealed(segmentID typeutil.UniqueID) Segment {
	return mgr.GetWithType(segmentID, SegmentTypeSealed)
}

func (mgr *segmentManager) GetGrowing(segmentID typeutil.UniqueID) Segment {
	return mgr.GetWithType(segmentID, SegmentTypeGrowing)
}

func (mgr *segmentManager) Empty() bool {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	return len(mgr.globalSegments.growingSegments)+len(mgr.globalSegments.sealedSegments) == 0
}

// returns true if the segment exists,
// false otherwise
func (mgr *segmentManager) Remove(ctx context.Context, segmentID typeutil.UniqueID, scope querypb.DataScope) (int, int) {
	mgr.mu.Lock()

	var removeGrowing, removeSealed int
	var growing, sealed Segment
	switch scope {
	case querypb.DataScope_Streaming:
		growing = mgr.removeSegmentWithType(SegmentTypeGrowing, segmentID)
		if growing != nil {
			removeGrowing = 1
		}

	case querypb.DataScope_Historical:
		sealed = mgr.removeSegmentWithType(SegmentTypeSealed, segmentID)
		if sealed != nil {
			removeSealed = 1
		}

	case querypb.DataScope_All:
		growing = mgr.removeSegmentWithType(SegmentTypeGrowing, segmentID)
		if growing != nil {
			removeGrowing = 1
		}

		sealed = mgr.removeSegmentWithType(SegmentTypeSealed, segmentID)
		if sealed != nil {
			removeSealed = 1
		}
	}
	mgr.updateMetric()
	mgr.mu.Unlock()

	if growing != nil {
		mgr.release(ctx, growing)
	}

	if sealed != nil {
		mgr.release(ctx, sealed)
	}

	return removeGrowing, removeSealed
}

func (mgr *segmentManager) removeSegmentWithType(typ SegmentType, segmentID typeutil.UniqueID) Segment {
	segment, ok := mgr.globalSegments.RemoveWithType(segmentID, typ)
	if !ok {
		return nil
	}

	switch typ {
	case SegmentTypeGrowing:
		mgr.growingOnReleasingSegments.Insert(segmentID)
	case SegmentTypeSealed:
		mgr.sealedOnReleasingSegments.Insert(segmentID)
	}

	mgr.secondaryIndex.Remove(segment)

	return segment
}

func (mgr *segmentManager) RemoveBy(ctx context.Context, filters ...SegmentFilter) (int, int) {
	mgr.mu.Lock()

	var removeSegments []Segment
	var removeGrowing, removeSealed int

	mgr.rangeWithFilter(func(id int64, segType SegmentType, segment Segment) bool {
		s := mgr.removeSegmentWithType(segType, id)
		if s != nil {
			removeSegments = append(removeSegments, s)
			switch segType {
			case SegmentTypeGrowing:
				removeGrowing++
			case SegmentTypeSealed:
				removeSealed++
			}
		}
		return true
	}, filters...)
	mgr.updateMetric()
	mgr.mu.Unlock()

	for _, s := range removeSegments {
		mgr.release(ctx, s)
	}
	return removeGrowing, removeSealed
}

func (mgr *segmentManager) Clear(ctx context.Context) {
	mgr.mu.Lock()

	for id := range mgr.globalSegments.growingSegments {
		mgr.growingOnReleasingSegments.Insert(id)
	}
	growingWaitForRelease := mgr.globalSegments.growingSegments

	for id := range mgr.globalSegments.sealedSegments {
		mgr.sealedOnReleasingSegments.Insert(id)
	}
	sealedWaitForRelease := mgr.globalSegments.sealedSegments
	mgr.globalSegments = newSegments()
	mgr.secondaryIndex = newSecondarySegmentIndex()
	mgr.updateMetric()
	mgr.mu.Unlock()

	for _, segment := range growingWaitForRelease {
		mgr.release(ctx, segment)
	}
	for _, segment := range sealedWaitForRelease {
		mgr.release(ctx, segment)
	}
}

// registerReleaseCallback registers the callback function when a segment is released.
// TODO: bad implementation for keep consistency with DiskCache, need to be refactor.
func (mgr *segmentManager) registerReleaseCallback(callback func(s Segment)) {
	mgr.releaseCallback = callback
}

func (mgr *segmentManager) updateMetric() {
	// update collection and partiation metric
	collections, partitions := make(typeutil.Set[int64]), make(typeutil.Set[int64])
	for _, seg := range mgr.globalSegments.growingSegments {
		collections.Insert(seg.Collection())
		if seg.Partition() != common.AllPartitionsID {
			partitions.Insert(seg.Partition())
		}
	}
	for _, seg := range mgr.globalSegments.sealedSegments {
		collections.Insert(seg.Collection())
		if seg.Partition() != common.AllPartitionsID {
			partitions.Insert(seg.Partition())
		}
	}
	metrics.QueryNodeNumCollections.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Set(float64(collections.Len()))
	metrics.QueryNodeNumPartitions.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Set(float64(partitions.Len()))
}

func (mgr *segmentManager) release(ctx context.Context, segment Segment) {
	if mgr.releaseCallback != nil {
		mgr.releaseCallback(segment)
		log.Ctx(ctx).Info("remove segment from cache", zap.Int64("segmentID", segment.ID()))
	}
	segment.Release(ctx)

	metrics.QueryNodeNumSegments.WithLabelValues(
		fmt.Sprint(paramtable.GetNodeID()),
		fmt.Sprint(segment.Collection()),
		fmt.Sprint(segment.Partition()),
		segment.Type().String(),
		fmt.Sprint(len(segment.Indexes())),
		segment.Level().String(),
	).Dec()

	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	switch segment.Type() {
	case SegmentTypeGrowing:
		mgr.growingOnReleasingSegments.Remove(segment.ID())
	case SegmentTypeSealed:
		mgr.sealedOnReleasingSegments.Remove(segment.ID())
	}
}
