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

// SegmentFilter is the interface for segment selection criteria.
type SegmentFilter interface {
	Filter(segment Segment) bool
	SegmentType() (SegmentType, bool)
	SegmentIDs() ([]int64, bool)
}

// SegmentFilterFunc is a type wrapper for `func(Segment) bool` to SegmentFilter.
type SegmentFilterFunc func(segment Segment) bool

func (f SegmentFilterFunc) Filter(segment Segment) bool {
	return f(segment)
}

func (f SegmentFilterFunc) SegmentType() (SegmentType, bool) {
	return commonpb.SegmentState_SegmentStateNone, false
}

func (s SegmentFilterFunc) SegmentIDs() ([]int64, bool) {
	return nil, false
}

// SegmentIDFilter is the specific segment filter for SegmentID only.
type SegmentIDFilter int64

func (f SegmentIDFilter) Filter(segment Segment) bool {
	return segment.ID() == int64(f)
}

func (f SegmentIDFilter) SegmentType() (SegmentType, bool) {
	return commonpb.SegmentState_SegmentStateNone, false
}

func (f SegmentIDFilter) SegmentIDs() ([]int64, bool) {
	return []int64{int64(f)}, true
}

type SegmentIDsFilter struct {
	segmentIDs typeutil.Set[int64]
}

func (f SegmentIDsFilter) Filter(segment Segment) bool {
	return f.segmentIDs.Contain(segment.ID())
}

func (f SegmentIDsFilter) SegmentType() (SegmentType, bool) {
	return commonpb.SegmentState_SegmentStateNone, false
}

func (f SegmentIDsFilter) SegmentIDs() ([]int64, bool) {
	return f.segmentIDs.Collect(), true
}

type SegmentTypeFilter SegmentType

func (f SegmentTypeFilter) Filter(segment Segment) bool {
	return segment.Type() == SegmentType(f)
}

func (f SegmentTypeFilter) SegmentType() (SegmentType, bool) {
	return SegmentType(f), true
}

func (f SegmentTypeFilter) SegmentIDs() ([]int64, bool) {
	return nil, false
}

func WithSkipEmpty() SegmentFilter {
	return SegmentFilterFunc(func(segment Segment) bool {
		return segment.InsertCount() > 0
	})
}

func WithPartition(partitionID typeutil.UniqueID) SegmentFilter {
	return SegmentFilterFunc(func(segment Segment) bool {
		return segment.Partition() == partitionID
	})
}

func WithChannel(channel string) SegmentFilter {
	ac, err := metautil.ParseChannel(channel, channelMapper)
	if err != nil {
		return SegmentFilterFunc(func(segment Segment) bool {
			return false
		})
	}
	return SegmentFilterFunc(func(segment Segment) bool {
		return segment.Shard().Equal(ac)
	})
}

func WithType(typ SegmentType) SegmentFilter {
	return SegmentTypeFilter(typ)
}

func WithID(id int64) SegmentFilter {
	return SegmentIDFilter(id)
}

func WithIDs(ids ...int64) SegmentFilter {
	return SegmentIDsFilter{
		segmentIDs: typeutil.NewSet(ids...),
	}
}

func WithLevel(level datapb.SegmentLevel) SegmentFilter {
	return SegmentFilterFunc(func(segment Segment) bool {
		return segment.Level() == level
	})
}

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

// Manager manages all collections and segments
type segmentManager struct {
	mu sync.RWMutex // guards all

	growingSegments map[typeutil.UniqueID]Segment
	sealedSegments  map[typeutil.UniqueID]Segment

	// releaseCallback is the callback function when a segment is released.
	releaseCallback func(s Segment)

	growingOnReleasingSegments typeutil.UniqueSet
	sealedOnReleasingSegments  typeutil.UniqueSet
}

func NewSegmentManager() *segmentManager {
	return &segmentManager{
		growingSegments:            make(map[int64]Segment),
		sealedSegments:             make(map[int64]Segment),
		growingOnReleasingSegments: typeutil.NewUniqueSet(),
		sealedOnReleasingSegments:  typeutil.NewUniqueSet(),
	}
}

func (mgr *segmentManager) Put(ctx context.Context, segmentType SegmentType, segments ...Segment) {
	var replacedSegment []Segment
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	var targetMap map[int64]Segment
	switch segmentType {
	case SegmentTypeGrowing:
		targetMap = mgr.growingSegments
	case SegmentTypeSealed:
		targetMap = mgr.sealedSegments
	default:
		panic("unexpected segment type")
	}
	log := log.Ctx(ctx)
	for _, segment := range segments {
		oldSegment, ok := targetMap[segment.ID()]

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
		targetMap[segment.ID()] = segment

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
	switch typ {
	case SegmentTypeGrowing:
		if _, ok := mgr.growingSegments[segmentID]; ok {
			return true
		} else if mgr.growingOnReleasingSegments.Contain(segmentID) {
			return true
		}
	case SegmentTypeSealed:
		if _, ok := mgr.sealedSegments[segmentID]; ok {
			return true
		} else if mgr.sealedOnReleasingSegments.Contain(segmentID) {
			return true
		}
	}

	return false
}

func (mgr *segmentManager) Get(segmentID typeutil.UniqueID) Segment {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	if segment, ok := mgr.growingSegments[segmentID]; ok {
		return segment
	} else if segment, ok = mgr.sealedSegments[segmentID]; ok {
		return segment
	}

	return nil
}

func (mgr *segmentManager) GetWithType(segmentID typeutil.UniqueID, typ SegmentType) Segment {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	switch typ {
	case SegmentTypeSealed:
		return mgr.sealedSegments[segmentID]
	case SegmentTypeGrowing:
		return mgr.growingSegments[segmentID]
	default:
		return nil
	}
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

	for _, id := range segments {
		growing, growingExist := mgr.growingSegments[id]
		sealed, sealedExist := mgr.sealedSegments[id]

		// L0 Segment should not be queryable.
		if sealedExist && sealed.Level() == datapb.SegmentLevel_L0 {
			continue
		}

		growingExist = growingExist && filter(growing, filters...)
		sealedExist = sealedExist && filter(sealed, filters...)

		if growingExist {
			err = growing.PinIfNotReleased()
			if err != nil {
				return nil, err
			}
			lockedSegments = append(lockedSegments, growing)
		}
		if sealedExist {
			err = sealed.PinIfNotReleased()
			if err != nil {
				return nil, err
			}
			lockedSegments = append(lockedSegments, sealed)
		}

		if !growingExist && !sealedExist {
			err = merr.WrapErrSegmentNotLoaded(id, "segment not found")
			return nil, err
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
	var segType SegmentType
	var hasSegType, hasSegIDs bool
	segmentIDs := typeutil.NewSet[int64]()

	otherFilters := make([]SegmentFilter, 0, len(filters))
	for _, filter := range filters {
		if sType, ok := filter.SegmentType(); ok {
			segType = sType
			hasSegType = true
			continue
		}
		if segIDs, ok := filter.SegmentIDs(); ok {
			hasSegIDs = true
			segmentIDs.Insert(segIDs...)
			continue
		}
		otherFilters = append(otherFilters, filter)
	}

	mergedFilter := func(info Segment) bool {
		for _, filter := range otherFilters {
			if !filter.Filter(info) {
				return false
			}
		}
		return true
	}

	var candidates map[SegmentType]map[int64]Segment
	switch segType {
	case SegmentTypeSealed:
		candidates = map[SegmentType]map[int64]Segment{SegmentTypeSealed: mgr.sealedSegments}
	case SegmentTypeGrowing:
		candidates = map[SegmentType]map[int64]Segment{SegmentTypeGrowing: mgr.growingSegments}
	default:
		if !hasSegType {
			candidates = map[SegmentType]map[int64]Segment{
				SegmentTypeSealed:  mgr.sealedSegments,
				SegmentTypeGrowing: mgr.growingSegments,
			}
		}
	}

	for segType, candidate := range candidates {
		if hasSegIDs {
			for id := range segmentIDs {
				segment, has := candidate[id]
				if has && mergedFilter(segment) {
					if !process(id, segType, segment) {
						break
					}
				}
			}
		} else {
			for id, segment := range candidate {
				if mergedFilter(segment) {
					if !process(id, segType, segment) {
						break
					}
				}
			}
		}
	}
}

func filter(segment Segment, filters ...SegmentFilter) bool {
	for _, filter := range filters {
		if !filter.Filter(segment) {
			return false
		}
	}
	return true
}

func (mgr *segmentManager) GetSealed(segmentID typeutil.UniqueID) Segment {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	if segment, ok := mgr.sealedSegments[segmentID]; ok {
		return segment
	}

	return nil
}

func (mgr *segmentManager) GetGrowing(segmentID typeutil.UniqueID) Segment {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	if segment, ok := mgr.growingSegments[segmentID]; ok {
		return segment
	}

	return nil
}

func (mgr *segmentManager) Empty() bool {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	return len(mgr.growingSegments)+len(mgr.sealedSegments) == 0
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
	switch typ {
	case SegmentTypeGrowing:
		s, ok := mgr.growingSegments[segmentID]
		if ok {
			delete(mgr.growingSegments, segmentID)
			mgr.growingOnReleasingSegments.Insert(segmentID)
			return s
		}

	case SegmentTypeSealed:
		s, ok := mgr.sealedSegments[segmentID]
		if ok {
			delete(mgr.sealedSegments, segmentID)
			mgr.sealedOnReleasingSegments.Insert(segmentID)
			return s
		}
	default:
		return nil
	}

	return nil
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

	for id := range mgr.growingSegments {
		mgr.growingOnReleasingSegments.Insert(id)
	}
	growingWaitForRelease := mgr.growingSegments
	mgr.growingSegments = make(map[int64]Segment)

	for id := range mgr.sealedSegments {
		mgr.sealedOnReleasingSegments.Insert(id)
	}
	sealedWaitForRelease := mgr.sealedSegments
	mgr.sealedSegments = make(map[int64]Segment)
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
	collections, partiations := make(typeutil.Set[int64]), make(typeutil.Set[int64])
	for _, seg := range mgr.growingSegments {
		collections.Insert(seg.Collection())
		if seg.Partition() != common.AllPartitionsID {
			partiations.Insert(seg.Partition())
		}
	}
	for _, seg := range mgr.sealedSegments {
		collections.Insert(seg.Collection())
		if seg.Partition() != common.AllPartitionsID {
			partiations.Insert(seg.Partition())
		}
	}
	metrics.QueryNodeNumCollections.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Set(float64(collections.Len()))
	metrics.QueryNodeNumPartitions.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Set(float64(partiations.Len()))
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
