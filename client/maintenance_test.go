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

package client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type MaintenanceSuite struct {
	MockSuiteBase
}

func (s *MaintenanceSuite) TestLoadCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("success", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		done := atomic.NewBool(false)
		s.mock.EXPECT().LoadCollection(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, lcr *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
			s.Equal(collectionName, lcr.GetCollectionName())
			return merr.Success(), nil
		}).Once()
		s.mock.EXPECT().GetLoadingProgress(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, glpr *milvuspb.GetLoadingProgressRequest) (*milvuspb.GetLoadingProgressResponse, error) {
			s.Equal(collectionName, glpr.GetCollectionName())

			progress := int64(50)
			if done.Load() {
				progress = 100
			}

			return &milvuspb.GetLoadingProgressResponse{
				Status:   merr.Success(),
				Progress: progress,
			}, nil
		})
		defer s.mock.EXPECT().GetLoadingProgress(mock.Anything, mock.Anything).Unset()

		task, err := s.client.LoadCollection(ctx, NewLoadCollectionOption(collectionName))
		s.NoError(err)

		ch := make(chan struct{})
		go func() {
			defer close(ch)
			err := task.Await(ctx)
			s.NoError(err)
		}()

		select {
		case <-ch:
			s.FailNow("task done before index state set to finish")
		case <-time.After(time.Second):
		}

		done.Store(true)

		select {
		case <-ch:
		case <-time.After(time.Second):
			s.FailNow("task not done after index set finished")
		}
	})

	s.Run("failure", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		s.mock.EXPECT().LoadCollection(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.LoadCollection(ctx, NewLoadCollectionOption(collectionName))
		s.Error(err)
	})
}

func (s *MaintenanceSuite) TestLoadPartitions() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("success", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))

		done := atomic.NewBool(false)
		s.mock.EXPECT().LoadPartitions(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, lpr *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
			s.Equal(collectionName, lpr.GetCollectionName())
			s.ElementsMatch([]string{partitionName}, lpr.GetPartitionNames())
			return merr.Success(), nil
		}).Once()
		s.mock.EXPECT().GetLoadingProgress(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, glpr *milvuspb.GetLoadingProgressRequest) (*milvuspb.GetLoadingProgressResponse, error) {
			s.Equal(collectionName, glpr.GetCollectionName())
			s.ElementsMatch([]string{partitionName}, glpr.GetPartitionNames())

			progress := int64(50)
			if done.Load() {
				progress = 100
			}

			return &milvuspb.GetLoadingProgressResponse{
				Status:   merr.Success(),
				Progress: progress,
			}, nil
		})
		defer s.mock.EXPECT().GetLoadingProgress(mock.Anything, mock.Anything).Unset()

		task, err := s.client.LoadPartitions(ctx, NewLoadPartitionsOption(collectionName, []string{partitionName}))
		s.NoError(err)

		ch := make(chan struct{})
		go func() {
			defer close(ch)
			err := task.Await(ctx)
			s.NoError(err)
		}()

		select {
		case <-ch:
			s.FailNow("task done before index state set to finish")
		case <-time.After(time.Second):
		}

		done.Store(true)

		select {
		case <-ch:
		case <-time.After(time.Second):
			s.FailNow("task not done after index set finished")
		}
	})

	s.Run("failure", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))

		s.mock.EXPECT().LoadPartitions(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.LoadPartitions(ctx, NewLoadPartitionsOption(collectionName, []string{partitionName}))
		s.Error(err)
	})
}

func (s *MaintenanceSuite) TestFlush() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("success", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		done := atomic.NewBool(false)
		s.mock.EXPECT().Flush(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, fr *milvuspb.FlushRequest) (*milvuspb.FlushResponse, error) {
			s.ElementsMatch([]string{collectionName}, fr.GetCollectionNames())
			return &milvuspb.FlushResponse{
				Status: merr.Success(),
				CollSegIDs: map[string]*schemapb.LongArray{
					collectionName: {Data: []int64{1, 2, 3}},
				},
				CollFlushTs: map[string]uint64{collectionName: 321},
			}, nil
		}).Once()
		s.mock.EXPECT().GetFlushState(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, gfsr *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
			s.Equal(collectionName, gfsr.GetCollectionName())
			s.ElementsMatch([]int64{1, 2, 3}, gfsr.GetSegmentIDs())
			s.EqualValues(321, gfsr.GetFlushTs())
			return &milvuspb.GetFlushStateResponse{
				Status:  merr.Success(),
				Flushed: done.Load(),
			}, nil
		})
		defer s.mock.EXPECT().GetFlushState(mock.Anything, mock.Anything).Unset()

		task, err := s.client.Flush(ctx, NewFlushOption(collectionName))
		s.NoError(err)

		ch := make(chan struct{})
		go func() {
			defer close(ch)
			err := task.Await(ctx)
			s.NoError(err)
		}()

		select {
		case <-ch:
			s.FailNow("task done before index state set to finish")
		case <-time.After(time.Second):
		}

		done.Store(true)

		select {
		case <-ch:
		case <-time.After(time.Second):
			s.FailNow("task not done after index set finished")
		}
	})

	s.Run("failure", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		s.mock.EXPECT().Flush(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.Flush(ctx, NewFlushOption(collectionName))
		s.Error(err)
	})
}

func TestMaintenance(t *testing.T) {
	suite.Run(t, new(MaintenanceSuite))
}
