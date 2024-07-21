// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package datacoord

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestClusteringCompactionPolicySuite(t *testing.T) {
	suite.Run(t, new(ClusteringCompactionPolicySuite))
}

type ClusteringCompactionPolicySuite struct {
	suite.Suite

	mockAlloc          *NMockAllocator
	mockTriggerManager *MockTriggerManager
	testLabel          *CompactionGroupLabel
	handler            Handler
	mockPlanContext    *MockCompactionPlanContext

	clusteringCompactionPolicy *clusteringCompactionPolicy
}

func (s *ClusteringCompactionPolicySuite) SetupTest() {
	s.testLabel = &CompactionGroupLabel{
		CollectionID: 1,
		PartitionID:  10,
		Channel:      "ch-1",
	}

	segments := genSegmentsForMeta(s.testLabel)
	meta := &meta{segments: NewSegmentsInfo()}
	for id, segment := range segments {
		meta.segments.SetSegment(id, segment)
	}
	mockAllocator := newMockAllocator()
	mockHandler := NewNMockHandler(s.T())
	s.clusteringCompactionPolicy = newClusteringCompactionPolicy(meta, mockAllocator, mockHandler)
}

func (s *ClusteringCompactionPolicySuite) TestTrigger() {
	events, err := s.clusteringCompactionPolicy.Trigger()
	s.NoError(err)
	gotViews, ok := events[TriggerTypeClustering]
	s.True(ok)
	s.NotNil(gotViews)
	s.Equal(0, len(gotViews))
}
