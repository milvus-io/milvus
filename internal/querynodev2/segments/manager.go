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
#cgo pkg-config: milvus_segcore

#include "segcore/collection_c.h"
#include "segcore/segment_c.h"
*/
import "C"
import (
	"sync"

	"github.com/milvus-io/milvus/internal/proto/querypb"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
)

type SegmentFilter func(segment Segment) bool

func WithPartition(partitionID UniqueID) SegmentFilter {
	return func(segment Segment) bool {
		return segment.Partition() == partitionID
	}
}

func WithChannel(channel string) SegmentFilter {
	return func(segment Segment) bool {
		return segment.Shard() == channel
	}
}

func WithType(typ SegmentType) SegmentFilter {
	return func(segment Segment) bool {
		return segment.Type() == typ
	}
}

func WithID(id int64) SegmentFilter {
	return func(segment Segment) bool {
		return segment.ID() == id
	}
}

type Manager struct {
	Collection CollectionManager
	Segment    SegmentManager
}

func NewManager() *Manager {
	return &Manager{
		Collection: NewCollectionManager(),
		Segment:    NewSegmentManager(),
	}
}

type SegmentManager interface {
	// Put puts the given segments in,
	// and increases the ref count of the corresponding collection,
	// dup segments will not increase the ref count
	Put(segmentType SegmentType, segments ...Segment)
	Get(segmentID UniqueID) Segment
	GetWithType(segmentID UniqueID, typ SegmentType) Segment
	GetBy(filters ...SegmentFilter) []Segment
	GetSealed(segmentID UniqueID) Segment
	GetGrowing(segmentID UniqueID) Segment
	// Remove removes the given segment,
	// and decreases the ref count of the corresponding collection,
	// will not decrease the ref count if the given segment not exists
	Remove(segmentID UniqueID, scope querypb.DataScope)
	RemoveBy(filters ...SegmentFilter)
}

var _ SegmentManager = (*segmentManager)(nil)

// Manager manages all collections and segments
type segmentManager struct {
	mu sync.RWMutex // guards all

	growingSegments map[UniqueID]Segment
	sealedSegments  map[UniqueID]Segment
}

func NewSegmentManager() *segmentManager {
	return &segmentManager{
		growingSegments: make(map[int64]Segment),
		sealedSegments:  make(map[int64]Segment),
	}
}

func (mgr *segmentManager) Put(segmentType SegmentType, segments ...Segment) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	targetMap := mgr.growingSegments
	switch segmentType {
	case SegmentTypeGrowing:
		targetMap = mgr.growingSegments
	case SegmentTypeSealed:
		targetMap = mgr.sealedSegments
	default:
		panic("unexpected segment type")
	}

	for _, segment := range segments {
		targetMap[segment.ID()] = segment
	}
}

func (mgr *segmentManager) Get(segmentID UniqueID) Segment {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	if segment, ok := mgr.growingSegments[segmentID]; ok {
		return segment
	} else if segment, ok = mgr.sealedSegments[segmentID]; ok {
		return segment
	}

	return nil
}

func (mgr *segmentManager) GetWithType(segmentID UniqueID, typ SegmentType) Segment {
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

	ret := make([]Segment, 0)
	for _, segment := range mgr.growingSegments {
		if filter(segment, filters...) {
			ret = append(ret, segment)
		}
	}

	for _, segment := range mgr.sealedSegments {
		if filter(segment, filters...) {
			ret = append(ret, segment)
		}
	}
	return ret
}

func filter(segment Segment, filters ...SegmentFilter) bool {
	for _, filter := range filters {
		if !filter(segment) {
			return false
		}
	}
	return true
}

func (mgr *segmentManager) GetSealed(segmentID UniqueID) Segment {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	if segment, ok := mgr.sealedSegments[segmentID]; ok {
		return segment
	}

	return nil
}

func (mgr *segmentManager) GetGrowing(segmentID UniqueID) Segment {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	if segment, ok := mgr.growingSegments[segmentID]; ok {
		return segment
	}

	return nil
}

func (mgr *segmentManager) Remove(segmentID UniqueID, scope querypb.DataScope) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	switch scope {
	case querypb.DataScope_Streaming:
		remove(segmentID, mgr.growingSegments)

	case querypb.DataScope_Historical:
		remove(segmentID, mgr.sealedSegments)

	case querypb.DataScope_All:
		remove(segmentID, mgr.growingSegments)
		remove(segmentID, mgr.sealedSegments)
	}
}

func (mgr *segmentManager) RemoveBy(filters ...SegmentFilter) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	for id, segment := range mgr.growingSegments {
		if filter(segment, filters...) {
			remove(id, mgr.growingSegments)
		}
	}

	for id, segment := range mgr.sealedSegments {
		if filter(segment, filters...) {
			remove(id, mgr.sealedSegments)
		}
	}
}

func remove(segmentID int64, container map[int64]Segment) {
	segment, ok := container[segmentID]
	if !ok {
		return
	}
	delete(container, segmentID)
	DeleteSegment(segment.(*LocalSegment))
}
