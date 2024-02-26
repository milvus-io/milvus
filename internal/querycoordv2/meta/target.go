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

package meta

import (
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/util/memory"
)

// CollectionTarget collection target is immutable,
type CollectionTarget struct {
	segments   map[int64]*datapb.SegmentInfo
	dmChannels map[string]*DmChannel
	version    int64
}

func NewCollectionTarget(segments map[int64]*datapb.SegmentInfo, dmChannels map[string]*DmChannel) *CollectionTarget {
	return &CollectionTarget{
		segments:   segments,
		dmChannels: dmChannels,
		version:    time.Now().UnixNano(),
	}
}

func (p *CollectionTarget) GetAllSegments() map[int64]*datapb.SegmentInfo {
	return p.segments
}

func (p *CollectionTarget) GetTargetVersion() int64 {
	return p.version
}

func (p *CollectionTarget) GetAllDmChannels() map[string]*DmChannel {
	return p.dmChannels
}

func (p *CollectionTarget) GetAllSegmentIDs() []int64 {
	return lo.Keys(p.segments)
}

func (p *CollectionTarget) GetAllDmChannelNames() []string {
	return lo.Keys(p.dmChannels)
}

func (p *CollectionTarget) IsEmpty() bool {
	return len(p.dmChannels)+len(p.segments) == 0
}

func (p *CollectionTarget) Size() int64 {
	size := 0
	for _, segment := range p.segments {
		size += proto.Size(segment) + 8
	}
	for name, channel := range p.dmChannels {
		size += int(channel.Size()) + len(name)
	}
	return int64(size) + 8
}

type target struct {
	// just maintain target at collection level
	collectionTargetMap map[int64]*CollectionTarget
	tracker             memory.MemoryTracker
}

func newTarget() *target {
	return &target{
		collectionTargetMap: make(map[int64]*CollectionTarget),
	}
}

func (t *target) setTracker(tracker memory.MemoryTracker) {
	t.tracker = tracker
}

func (t *target) updateCollectionTarget(collectionID int64, target *CollectionTarget) {
	if t.collectionTargetMap[collectionID] != nil && target.GetTargetVersion() <= t.collectionTargetMap[collectionID].GetTargetVersion() {
		return
	}

	if t.tracker != nil {
		if _, ok := t.collectionTargetMap[collectionID]; ok {
			t.tracker.Return(t.collectionTargetMap[collectionID].Size())
		}
		t.tracker.Consume(target.Size())
	}
	t.collectionTargetMap[collectionID] = target
}

func (t *target) removeCollectionTarget(collectionID int64) {
	if t.tracker != nil {
		if _, ok := t.collectionTargetMap[collectionID]; ok {
			t.tracker.Return(t.collectionTargetMap[collectionID].Size())
		}
	}
	delete(t.collectionTargetMap, collectionID)
}

func (t *target) getCollectionTarget(collectionID int64) *CollectionTarget {
	return t.collectionTargetMap[collectionID]
}
