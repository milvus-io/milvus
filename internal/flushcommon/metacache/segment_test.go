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

package metacache

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
)

type SegmentSuite struct {
	suite.Suite

	info *datapb.SegmentInfo
}

func (s *SegmentSuite) TestBasic() {
	bfs := pkoracle.NewBloomFilterSet()
	stats := NewEmptySegmentBM25Stats()
	segment := NewSegmentInfo(s.info, bfs, stats)
	s.Equal(s.info.GetID(), segment.SegmentID())
	s.Equal(s.info.GetPartitionID(), segment.PartitionID())
	s.Equal(s.info.GetNumOfRows(), segment.NumOfRows())
	s.Equal(s.info.GetStartPosition(), segment.StartPosition())
	s.Equal(s.info.GetDmlPosition(), segment.Checkpoint())
	s.Equal(bfs.GetHistory(), segment.GetHistory())
	s.True(segment.startPosRecorded)
}

func (s *SegmentSuite) TestClone() {
	bfs := pkoracle.NewBloomFilterSet()
	stats := NewEmptySegmentBM25Stats()
	segment := NewSegmentInfo(s.info, bfs, stats)
	cloned := segment.Clone()
	s.Equal(segment.SegmentID(), cloned.SegmentID())
	s.Equal(segment.PartitionID(), cloned.PartitionID())
	s.Equal(segment.NumOfRows(), cloned.NumOfRows())
	s.Equal(segment.StartPosition(), cloned.StartPosition())
	s.Equal(segment.Checkpoint(), cloned.Checkpoint())
	s.Equal(segment.GetHistory(), cloned.GetHistory())
	s.Equal(segment.startPosRecorded, cloned.startPosRecorded)
}

func TestSegment(t *testing.T) {
	suite.Run(t, new(SegmentSuite))
}
