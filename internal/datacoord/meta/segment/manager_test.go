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

package segment

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type ManagerSuite struct {
	suite.Suite

	catalog *mocks.DataCoordCatalog
}

func (s *ManagerSuite) SetupTest() {
	catalog := mocks.NewDataCoordCatalog(s.T())

	s.catalog = catalog
}

// getManager is utility method to initailize segmentManager
// it gives the segment manager with predefined segment info
func (s *ManagerSuite) getManager(ctx context.Context, initialSegments []*datapb.SegmentInfo) *segmentManager {
	s.catalog.EXPECT().ListSegments(ctx).Return(initialSegments, nil)

	manager, err := NewSegmentManager(ctx, s.catalog)
	s.Require().NoError(err)

	return manager
}

func (s *ManagerSuite) TestInitialization() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type testCase struct {
		tag       string
		input     []*datapb.SegmentInfo
		err       error
		expectErr bool
		ids       []int64
	}

	testCases := []testCase{
		{
			tag:   "empty_segments",
			input: []*datapb.SegmentInfo{},
		},
		{
			tag: "normal_segments",
			input: []*datapb.SegmentInfo{
				{ID: 10001, CollectionID: 100, PartitionID: 1000, InsertChannel: "by-dev-rootcoord-dml_0-100v0"},
			},
			ids: []int64{10001},
		},
		{
			tag:       "catalog_error",
			err:       merr.WrapErrServiceInternal("mocked"),
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.tag, func() {
			s.catalog.EXPECT().ListSegments(ctx).Return(tc.input, tc.err).Once()
			m, err := NewSegmentManager(ctx, s.catalog)
			if tc.expectErr {
				s.Error(err)
				return
			}

			for _, id := range tc.ids {
				seg := m.GetSegmentByID(id)
				s.NotNil(seg)
			}
		})
	}
}

func (s *ManagerSuite) TestAddSegment() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("normal_add", func() {
		manager := s.getManager(ctx, nil)

		s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(nil).Once()

		err := manager.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
			ID:            10001,
			CollectionID:  100,
			PartitionID:   1000,
			InsertChannel: "by-dev-rootcoord-dml_0-100v0",
		}))

		s.NoError(err)

		info := manager.GetSegmentByID(10001)
		s.NotNil(info)
	})

	s.Run("catalog_error", func() {
		manager := s.getManager(ctx, nil)

		s.catalog.EXPECT().AddSegment(mock.Anything, mock.Anything).Return(merr.WrapErrServiceInternal("mocked")).Once()

		err := manager.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
			ID:            10001,
			CollectionID:  100,
			PartitionID:   1000,
			InsertChannel: "by-dev-rootcoord-dml_0-100v0",
		}))

		s.Error(err)
	})
}

func TestManager(t *testing.T) {
	suite.Run(t, new(ManagerSuite))
}
