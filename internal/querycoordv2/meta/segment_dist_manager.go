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

type SegmentDistFilter func(s *Segment) bool

func WithSegmentID(segmentID int64) SegmentDistFilter {
	return func(s *Segment) bool {
		return s.GetID() == segmentID
	}
}

func WithReplica(replica *Replica) SegmentDistFilter {
	return func(s *Segment) bool {
		return replica.GetCollectionID() == s.GetCollectionID() && replica.Contains(s.Node)
	}
}

func WithNodeID(nodeID int64) SegmentDistFilter {
	return func(s *Segment) bool {
		return s.Node == nodeID
	}
}

func WithCollectionID(collectionID UniqueID) SegmentDistFilter {
	return func(s *Segment) bool {
		return s.CollectionID == collectionID
	}
}

func WithChannel(channelName string) SegmentDistFilter {
	return func(s *Segment) bool {
		return s.GetInsertChannel() == channelName
	}
}

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

// GetByFilter return segment list which match all given filters
func (m *SegmentDistManager) GetByFilter(filters ...SegmentDistFilter) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	mergedFilters := func(s *Segment) bool {
		for _, f := range filters {
			if f != nil && !f(s) {
				return false
			}
		}
		return true
	}

	ret := make([]*Segment, 0)
	for _, segments := range m.segments {
		for _, segment := range segments {
			if mergedFilters(segment) {
				ret = append(ret, segment)
			}
		}
	}
	return ret
}

// GetByFilterForNodes return segment list which match all given filters for given nodes
func (m *SegmentDistManager) GetByFilterForNodes(nodes []int64, filters ...SegmentDistFilter) []*Segment {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	mergedFilters := func(s *Segment) bool {
		for _, f := range filters {
			if f != nil && !f(s) {
				return false
			}
		}
		return true
	}

	ret := make([]*Segment, 0)
	for _, nodeid := range nodes {
		for _, segment := range m.segments[nodeid] {
			if mergedFilters(segment) {
				ret = append(ret, segment)
			}
		}
	}
	return ret
}

// return node list which contains the given segmentID
func (m *SegmentDistManager) GetSegmentDist(segmentID int64) []int64 {
	m.rwmutex.RLock()
	defer m.rwmutex.RUnlock()

	ret := make([]int64, 0)
	for nodeID, segments := range m.segments {
		for _, segment := range segments {
			if segment.GetID() == segmentID {
				ret = append(ret, nodeID)
				break
			}
		}
	}
	return ret
}
