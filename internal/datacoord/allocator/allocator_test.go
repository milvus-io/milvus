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

package allocator

import (
	"context"
	"math/rand"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type RootCoordAllocatorSuite struct {
	suite.Suite
	ms        *mocks.MockRootCoordClient
	allocator Allocator
}

func (s *RootCoordAllocatorSuite) SetupTest() {
	s.ms = mocks.NewMockRootCoordClient(s.T())
	s.allocator = NewRootCoordAllocator(s.ms)
}

func (s *RootCoordAllocatorSuite) TestAllocTimestamp() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("normal", func() {
		ts := rand.Uint64()
		s.ms.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, atr *rootcoordpb.AllocTimestampRequest, co ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
			s.EqualValues(1, atr.GetCount())
			return &rootcoordpb.AllocTimestampResponse{
				Status:    merr.Success(),
				Timestamp: ts,
			}, nil
		}).Once()
		result, err := s.allocator.AllocTimestamp(ctx)
		s.NoError(err)
		s.EqualValues(ts, result)
	})

	s.Run("error", func() {
		s.ms.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).Return(nil, errors.New("mock")).Once()
		_, err := s.allocator.AllocTimestamp(ctx)
		s.Error(err)
	})
}

func (s *RootCoordAllocatorSuite) TestAllocID() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("normal", func() {
		id := rand.Int63n(1000000)
		s.ms.EXPECT().AllocID(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ai *rootcoordpb.AllocIDRequest, co ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
			s.EqualValues(1, ai.GetCount())
			return &rootcoordpb.AllocIDResponse{
				Status: merr.Success(),
				ID:     id,
			}, nil
		}).Once()
		result, err := s.allocator.AllocID(ctx)
		s.NoError(err)
		s.EqualValues(id, result)
	})

	s.Run("error", func() {
		s.ms.EXPECT().AllocID(mock.Anything, mock.Anything).Return(nil, errors.New("mock")).Once()
		_, err := s.allocator.AllocID(ctx)
		s.Error(err)
	})
}

func (s *RootCoordAllocatorSuite) TestAllocN() {
	s.Run("normal", func() {
		n := rand.Int63n(100) + 1
		id := rand.Int63n(1000000)
		s.ms.EXPECT().AllocID(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ai *rootcoordpb.AllocIDRequest, co ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
			s.EqualValues(n, ai.GetCount())
			return &rootcoordpb.AllocIDResponse{
				Status: merr.Success(),
				ID:     id,
				Count:  uint32(n),
			}, nil
		}).Once()
		start, end, err := s.allocator.AllocN(n)
		s.NoError(err)
		s.EqualValues(id, start)
		s.EqualValues(id+n, end)
	})

	s.Run("zero_n", func() {
		id := rand.Int63n(1000000)
		s.ms.EXPECT().AllocID(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ai *rootcoordpb.AllocIDRequest, co ...grpc.CallOption) (*rootcoordpb.AllocIDResponse, error) {
			s.EqualValues(1, ai.GetCount())
			return &rootcoordpb.AllocIDResponse{
				Status: merr.Success(),
				ID:     id,
				Count:  uint32(1),
			}, nil
		}).Once()
		start, end, err := s.allocator.AllocN(0)
		s.NoError(err)
		s.EqualValues(id, start)
		s.EqualValues(id+1, end)
	})

	s.Run("error", func() {
		s.ms.EXPECT().AllocID(mock.Anything, mock.Anything).Return(nil, errors.New("mock")).Once()
		_, _, err := s.allocator.AllocN(10)
		s.Error(err)
	})
}

func TestRootCoordAllocator(t *testing.T) {
	suite.Run(t, new(RootCoordAllocatorSuite))
}
