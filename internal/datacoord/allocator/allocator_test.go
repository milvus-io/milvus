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
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type RootCoordAllocatorSuite struct {
	suite.Suite
	ms        *mocks.MixCoord
	allocator Allocator
}

func (s *RootCoordAllocatorSuite) SetupTest() {
	s.ms = mocks.NewMixCoord(s.T())
	s.allocator = NewRootCoordAllocator(s.ms)
}

func (s *RootCoordAllocatorSuite) TestAllocTimestamp() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("normal", func() {
		ts := rand.Uint64()
		s.ms.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, atr *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
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
		s.ms.EXPECT().AllocID(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ai *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
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
	s.Run("too_large", func() {
		ms := mocks.NewMixCoord(s.T())
		allocator := NewRootCoordAllocator(ms)
		ms.EXPECT().AllocID(mock.Anything, mock.Anything).Return(&rootcoordpb.AllocIDResponse{
			Status: merr.Success(),
		}, nil).Maybe()

		_, _, err := allocator.AllocN(math.MaxUint32 + 1)
		s.Require().Error(err)
		s.ErrorIs(err, merr.ErrParameterInvalid)
		s.Contains(err.Error(), "too large to allocate IDs in a single batch: requested 4294967296, max 4294967295")
		ms.AssertNotCalled(s.T(), "AllocID", mock.Anything, mock.Anything)
	})

	s.Run("normal", func() {
		n := rand.Int63n(100) + 1
		id := rand.Int63n(1000000)
		s.ms.EXPECT().AllocID(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ai *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
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
		s.ms.EXPECT().AllocID(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, ai *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
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

	s.Run("cancel_in_flight_with_context", func() {
		ms := mocks.NewMixCoord(s.T())
		alloc := NewRootCoordAllocator(ms)

		entered := make(chan struct{})
		observedErr := make(chan error, 1)
		ms.EXPECT().AllocID(mock.Anything, mock.Anything).RunAndReturn(func(rpcCtx context.Context, _ *rootcoordpb.AllocIDRequest) (*rootcoordpb.AllocIDResponse, error) {
			close(entered)
			<-rpcCtx.Done()
			err := rpcCtx.Err()
			observedErr <- err
			return nil, err
		}).Once()

		callerCtx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error, 1)
		go func() {
			_, _, err := AllocNWithContext(callerCtx, alloc, 5)
			done <- err
		}()

		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			s.FailNow("timed out waiting for AllocID entry barrier")
		}

		cancel()

		select {
		case err := <-observedErr:
			s.ErrorIs(err, context.Canceled)
		case <-time.After(2 * time.Second):
			s.FailNow("inner AllocID context did not observe cancellation")
		}

		select {
		case err := <-done:
			s.ErrorIs(err, context.Canceled)
		case <-time.After(2 * time.Second):
			s.FailNow("AllocNWithContext did not return after cancellation")
		}
	})

	s.Run("already_canceled_with_context", func() {
		ms := mocks.NewMixCoord(s.T())
		alloc := NewRootCoordAllocator(ms)

		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := AllocNWithContext(canceledCtx, alloc, 5)
		s.ErrorIs(err, context.Canceled)
		ms.AssertNotCalled(s.T(), "AllocID", mock.Anything, mock.Anything)
	})
}

func TestRootCoordAllocator(t *testing.T) {
	suite.Run(t, new(RootCoordAllocatorSuite))
}
