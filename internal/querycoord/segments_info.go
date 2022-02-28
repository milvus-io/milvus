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

package querycoord

import (
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util"
)

// segmentsInfo provides interfaces to do persistence/retrieve for segments with an in-memory cache
type segmentsInfo struct {
	mu           sync.RWMutex
	loadOnce     sync.Once
	segmentIDMap map[int64]*querypb.SegmentInfo
	kv           kv.TxnKV
}

func newSegmentsInfo(kv kv.TxnKV) *segmentsInfo {
	return &segmentsInfo{
		kv:           kv,
		segmentIDMap: make(map[int64]*querypb.SegmentInfo),
	}
}

func (s *segmentsInfo) loadSegments() error {
	var err error
	s.loadOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		var values []string
		_, values, err = s.kv.LoadWithPrefix(util.SegmentMetaPrefix)
		if err != nil {
			return
		}
		for _, v := range values {
			segment := &querypb.SegmentInfo{}
			if err = proto.Unmarshal([]byte(v), segment); err != nil {
				return
			}
			s.segmentIDMap[segment.GetSegmentID()] = segment
			metrics.QueryCoordNumEntities.WithLabelValues(fmt.Sprint(segment.CollectionID)).Add(float64(segment.NumRows))
		}
	})
	return err
}

func (s *segmentsInfo) saveSegment(segment *querypb.SegmentInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := getSegmentKey(segment)
	v, err := proto.Marshal(segment)
	if err != nil {
		return err
	}
	if err = s.kv.Save(k, string(v)); err != nil {
		return err
	}
	s.segmentIDMap[segment.GetSegmentID()] = segment
	metrics.QueryCoordNumEntities.WithLabelValues(fmt.Sprint(segment.CollectionID)).Add(float64(segment.NumRows))
	return nil
}

func (s *segmentsInfo) removeSegment(segment *querypb.SegmentInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := getSegmentKey(segment)
	if err := s.kv.Remove(k); err != nil {
		return err
	}
	delete(s.segmentIDMap, segment.GetSegmentID())
	metrics.QueryCoordNumEntities.WithLabelValues(fmt.Sprint(segment.CollectionID)).Sub(float64(segment.NumRows))
	return nil
}

func (s *segmentsInfo) getSegment(ID int64) *querypb.SegmentInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.segmentIDMap[ID]
}

func (s *segmentsInfo) getSegments() []*querypb.SegmentInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make([]*querypb.SegmentInfo, 0, len(s.segmentIDMap))
	for _, segment := range s.segmentIDMap {
		res = append(res, segment)
	}
	return res
}

func getSegmentKey(segment *querypb.SegmentInfo) string {
	return fmt.Sprintf("%s/%d/%d/%d", util.SegmentMetaPrefix, segment.GetCollectionID(), segment.GetPartitionID(),
		segment.GetSegmentID())
}
