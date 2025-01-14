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

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments/metricsutil"
	"github.com/milvus-io/milvus/pkg/eventlog"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/cache"
	"github.com/milvus-io/milvus/pkg/util/lock"
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

			err = manager.Loader.LoadLazySegment(ctx, segment, info)
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
	keyLock       *lock.KeyLock[metautil.Channel]
	shardSegments *typeutil.ConcurrentMap[metautil.Channel, segments]
}

func newSecondarySegmentIndex() secondarySegmentIndex {
	return secondarySegmentIndex{
		keyLock:       lock.NewKeyLock[metautil.Channel](),
		shardSegments: typeutil.NewConcurrentMap[metautil.Channel, segments](),
	}
}

func (si secondarySegmentIndex) Put(ctx context.Context, segmentType SegmentType, segment Segment) {
	shard := segment.Shard()
	si.keyLock.Lock(shard)
	defer si.keyLock.Unlock(shard)

	segments, _ := si.shardSegments.GetOrInsert(shard, newSegments())
	segments.Put(ctx, segmentType, segment)
}

func (si secondarySegmentIndex) Remove(s Segment) {
	shard := s.Shard()
	si.keyLock.Lock(shard)
	defer si.keyLock.Unlock(shard)

	segments, ok := si.shardSegments.Get(shard)
	if !ok {
		return
	}
	segments.Remove(s)
	if segments.Empty() {
		si.shardSegments.Remove(shard)
	}
}

type segments struct {
	growingSegments *typeutil.ConcurrentMap[typeutil.UniqueID, Segment]
	sealedSegments  *typeutil.ConcurrentMap[typeutil.UniqueID, Segment]
}

func (segments segments) Put(_ context.Context, segmentType SegmentType, segment Segment) {
	switch segmentType {
	case SegmentTypeGrowing:
		segments.growingSegments.Insert(segment.ID(), segment)
	case SegmentTypeSealed:
		segments.sealedSegments.Insert(segment.ID(), segment)
	}
}

func (segments segments) Get(segmentID int64) (Segment, bool) {
	if segment, ok := segments.growingSegments.Get(segmentID); ok {
		return segment, true
	} else if segment, ok = segments.sealedSegments.Get(segmentID); ok {
		return segment, true
	}

	return nil, false
}

func (segments segments) GetWithType(segmentID int64, segmentType SegmentType) (Segment, bool) {
	// var targetMap map[int64]Segment
	var segment Segment
	var ok bool
	switch segmentType {
	case SegmentTypeGrowing:
		segment, ok = segments.growingSegments.Get(segmentID)
	case SegmentTypeSealed:
		segment, ok = segments.sealedSegments.Get(segmentID)
	}
	return segment, ok
}

func (segments segments) RemoveWithType(segmentID int64, segmentType SegmentType) (Segment, bool) {
	var segment Segment
	var ok bool
	switch segmentType {
	case SegmentTypeGrowing:
		segment, ok = segments.growingSegments.GetAndRemove(segmentID)
	case SegmentTypeSealed:
		segment, ok = segments.sealedSegments.GetAndRemove(segmentID)
	}
	return segment, ok
}

func (segments segments) Remove(segment Segment) {
	switch segment.Type() {
	case SegmentTypeGrowing:
		segments.growingSegments.Remove(segment.ID())
	case SegmentTypeSealed:
		segments.sealedSegments.Remove(segment.ID())
	}
}

func (segments segments) RangeWithFilter(criterion *segmentCriterion, process func(id int64, segType SegmentType, segment Segment) bool) {
	if criterion.segmentIDs != nil {
		for id := range criterion.segmentIDs {
			var segment Segment
			var ok bool
			if criterion.segmentType == commonpb.SegmentState_SegmentStateNone {
				segment, ok = segments.Get(id)
			} else {
				segment, ok = segments.GetWithType(id, criterion.segmentType)
			}

			if ok && criterion.Match(segment) {
				if !process(id, segment.Type(), segment) {
					return
				}
			}
		}
		return
	}

	var candidates []*typeutil.ConcurrentMap[typeutil.UniqueID, Segment]
	switch criterion.segmentType {
	case SegmentTypeGrowing:
		candidates = []*typeutil.ConcurrentMap[typeutil.UniqueID, Segment]{segments.growingSegments}
	case SegmentTypeSealed:
		candidates = []*typeutil.ConcurrentMap[typeutil.UniqueID, Segment]{segments.sealedSegments}
	default:
		candidates = []*typeutil.ConcurrentMap[typeutil.UniqueID, Segment]{segments.growingSegments, segments.sealedSegments}
	}

	for _, candidate := range candidates {
		candidate.Range(func(id typeutil.UniqueID, segment Segment) bool {
			if criterion.Match(segment) {
				if !process(id, segment.Type(), segment) {
					return false
				}
			}
			return true
		})
	}
}

func (segments segments) Empty() bool {
	return segments.growingSegments.Len() == 0 && segments.sealedSegments.Len() == 0
}

func newSegments() segments {
	return segments{
		growingSegments: typeutil.NewConcurrentMap[typeutil.UniqueID, Segment](),
		sealedSegments:  typeutil.NewConcurrentMap[typeutil.UniqueID, Segment](),
	}
}

// Manager manages all collections and segments
type segmentManager struct {
	globalSegments segments
	secondaryIndex secondarySegmentIndex

	// releaseCallback is the callback function when a segment is released.
	releaseCallback func(s Segment)

	growingOnReleasingSegments *typeutil.ConcurrentSet[int64]
	sealedOnReleasingSegments  *typeutil.ConcurrentSet[int64]
}

func NewSegmentManager() *segmentManager {
	return &segmentManager{
		globalSegments:             newSegments(),
		secondaryIndex:             newSecondarySegmentIndex(),
		growingOnReleasingSegments: typeutil.NewConcurrentSet[int64](),
		sealedOnReleasingSegments:  typeutil.NewConcurrentSet[int64](),
	}
}

// put is the internal put method updating both global segments and secondary index.
func (mgr *segmentManager) put(ctx context.Context, segmentType SegmentType, segment Segment) {
	mgr.globalSegments.Put(ctx, segmentType, segment)
	mgr.secondaryIndex.Put(ctx, segmentType, segment)
}

func (mgr *segmentManager) Put(ctx context.Context, segmentType SegmentType, segments ...Segment) {
	var replacedSegment []Segment

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
	segment, _ := mgr.globalSegments.Get(segmentID)
	return segment
}

func (mgr *segmentManager) GetWithType(segmentID typeutil.UniqueID, typ SegmentType) Segment {
	segment, _ := mgr.globalSegments.GetWithType(segmentID, typ)
	return segment
}

func (mgr *segmentManager) GetBy(filters ...SegmentFilter) []Segment {
	var ret []Segment
	mgr.rangeWithFilter(func(id int64, _ SegmentType, segment Segment) bool {
		ret = append(ret, segment)
		return true
	}, filters...)
	return ret
}

func (mgr *segmentManager) GetAndPinBy(filters ...SegmentFilter) ([]Segment, error) {
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

	for _, id := range segments {
		// growing, growingExist := mgr.growingSegments[id]
		// sealed, sealedExist := mgr.sealedSegments[id]
		segment, ok := mgr.globalSegments.Get(id)

		if !ok {
			err = merr.WrapErrSegmentNotLoaded(id, "segment not found")
			return nil, err
		}

		// L0 Segment should not be queryable.
		if segment.Level() == datapb.SegmentLevel_L0 {
			continue
		}

		// growingExist = growingExist && filter(growing, filters...)
		// sealedExist = sealedExist && filter(sealed, filters...)
		if !filter(segment, filters...) {
			continue
		}

		err = segment.PinIfNotReleased()
		if err != nil {
			return nil, err
		}
		lockedSegments = append(lockedSegments, segment)
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
		target, ok = mgr.secondaryIndex.shardSegments.Get(criterion.channel)
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
	return mgr.globalSegments.growingSegments.Len()+mgr.globalSegments.sealedSegments.Len() == 0
}

// returns true if the segment exists,
// false otherwise
func (mgr *segmentManager) Remove(ctx context.Context, segmentID typeutil.UniqueID, scope querypb.DataScope) (int, int) {
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
	for _, s := range removeSegments {
		mgr.release(ctx, s)
	}
	return removeGrowing, removeSealed
}

func (mgr *segmentManager) Clear(ctx context.Context) {
	mgr.globalSegments.growingSegments.Range(func(id typeutil.UniqueID, _ Segment) bool {
		mgr.growingOnReleasingSegments.Insert(id)
		return true
	})
	growingWaitForRelease := mgr.globalSegments.growingSegments

	mgr.globalSegments.sealedSegments.Range(func(id typeutil.UniqueID, _ Segment) bool {
		mgr.sealedOnReleasingSegments.Insert(id)
		return true
	})
	sealedWaitForRelease := mgr.globalSegments.sealedSegments
	mgr.globalSegments = newSegments()
	mgr.secondaryIndex = newSecondarySegmentIndex()

	growingWaitForRelease.Range(func(_ typeutil.UniqueID, segment Segment) bool {
		mgr.release(ctx, segment)
		return true
	})
	sealedWaitForRelease.Range(func(_ typeutil.UniqueID, segment Segment) bool {
		mgr.release(ctx, segment)
		return true
	})
}

// registerReleaseCallback registers the callback function when a segment is released.
// TODO: bad implementation for keep consistency with DiskCache, need to be refactor.
func (mgr *segmentManager) registerReleaseCallback(callback func(s Segment)) {
	mgr.releaseCallback = callback
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

	switch segment.Type() {
	case SegmentTypeGrowing:
		mgr.growingOnReleasingSegments.Remove(segment.ID())
	case SegmentTypeSealed:
		mgr.sealedOnReleasingSegments.Remove(segment.ID())
	}
}
