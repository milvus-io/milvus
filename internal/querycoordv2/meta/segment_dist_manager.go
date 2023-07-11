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
	"sync"

	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	. "github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Segment struct {
	*datapb.SegmentInfo
	Node               int64                             // Node the segment is in
	Version            int64                             // Version is the timestamp of loading segment
	LastDeltaTimestamp uint64                            // The timestamp of the last delta record
	IndexInfo          map[int64]*querypb.FieldIndexInfo // index info of loaded segment
}

func SegmentFromInfo(info *datapb.SegmentInfo) *Segment {
	return &Segment{
		SegmentInfo: info,
	}
}

func (segment *Segment) Clone() *Segment {
	return &Segment{
		SegmentInfo: proto.Clone(segment.SegmentInfo).(*datapb.SegmentInfo),
		Node:        segment.Node,
		Version:     segment.Version,
	}
}

type SegmentDistManager struct {
	rwmutex sync.RWMutex

	// nodeID -> []*Segment
	segments map[UniqueID][]*Segment
}

func NewSegmentDistManager() *SegmentDistManager {
	return &SegmentDistManager{
		segments: make(map[UniqueID][]*Segment),
	}
}

func (m *SegmentDistManager) Update(nodeID UniqueID, segments ...*Segment) {
	m.rwmutex.Lock()
	defer m.rwmutex.Unlock()

	for _, segment := range segments {
		segment.Node = nodeID
	}
	m.segments[nodeID] = segments
}

func (m *SegmentDistManager) Get(id UniqueID) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ret := make([]*Segment, 0)
	for _, segments := range m.segments {
		for _, segment := range segments {
			if segment.GetID() == id {
				ret = append(ret, segment)
			}
		}
	}
	return ret
}

// GetAll returns all segments
func (m *SegmentDistManager) GetAll() []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ret := make([]*Segment, 0)
	for _, segments := range m.segments {
		ret = append(ret, segments...)
	}
	return ret
}

// func (m *SegmentDistManager) Remove(ids ...UniqueID) {
// 	m.rwmutex.Lock()
// 	defer m.rwmutex.Unlock()

// 	for _, id := range ids {
// 		delete(m.segments, id)
// 	}
// }

// GetByNode returns all segments of the given node.
func (m *SegmentDistManager) GetByNode(nodeID UniqueID) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	return m.segments[nodeID]
}

// GetByCollection returns all segments of the given collection.
func (m *SegmentDistManager) GetByCollection(collectionID UniqueID) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ret := make([]*Segment, 0)
	for _, segments := range m.segments {
		for _, segment := range segments {
			if segment.CollectionID == collectionID {
				ret = append(ret, segment)
			}
		}
	}
	return ret
}

// GetByShard returns all segments of the given collection.
func (m *SegmentDistManager) GetByShard(shard string) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ret := make([]*Segment, 0)
	for _, segments := range m.segments {
		for _, segment := range segments {
			if segment.GetInsertChannel() == shard {
				ret = append(ret, segment)
			}
		}
	}
	return ret
}

// GetByShard returns all segments of the given collection.
func (m *SegmentDistManager) GetByShardWithReplica(shard string, replica *Replica) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ret := make([]*Segment, 0)
	for nodeID, segments := range m.segments {
		if !replica.Contains(nodeID) {
			continue
		}
		for _, segment := range segments {
			if segment.GetInsertChannel() == shard {
				ret = append(ret, segment)
			}
		}
	}
	return ret
}

// GetByCollectionAndNode returns all segments of the given collection and node.
func (m *SegmentDistManager) GetByCollectionAndNode(collectionID, nodeID UniqueID) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ret := make([]*Segment, 0)
	for _, segment := range m.segments[nodeID] {
		if segment.CollectionID == collectionID {
			ret = append(ret, segment)
		}
	}
	return ret
}
