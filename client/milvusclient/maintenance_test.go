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

package milvusclient

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type MaintenanceSuite struct {
	MockSuiteBase
}

func (s *MaintenanceSuite) TestLoadCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		fieldNames := []string{"id", "part", "vector"}
		replicaNum := rand.Intn(3) + 1
		rgs := []string{"rg1", "rg2"}

		done := atomic.NewBool(false)
		mockLoadCollection := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).LoadCollection).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, lcr *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
			s.Equal(collectionName, lcr.GetCollectionName())
			s.ElementsMatch(fieldNames, lcr.GetLoadFields())
			s.True(lcr.SkipLoadDynamicField)
			s.EqualValues(replicaNum, lcr.GetReplicaNumber())
			s.ElementsMatch(rgs, lcr.GetResourceGroups())
			return merr.Success(), nil
		}).Build()
		defer mockLoadCollection.UnPatch()
		mockGetLoadingProgress := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).GetLoadingProgress).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, glpr *milvuspb.GetLoadingProgressRequest) (*milvuspb.GetLoadingProgressResponse, error) {
			s.Equal(collectionName, glpr.GetCollectionName())

			progress := int64(50)
			if done.Load() {
				progress = 100
			}

			return &milvuspb.GetLoadingProgressResponse{
				Status:   merr.Success(),
				Progress: progress,
			}, nil
		}).Build()
		defer mockGetLoadingProgress.UnPatch()

		task, err := s.client.LoadCollection(ctx, NewLoadCollectionOption(collectionName).
			WithReplica(replicaNum).
			WithResourceGroup(rgs...).
			WithLoadFields(fieldNames...).
			WithSkipLoadDynamicField(true))
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
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		mockLoadCollection := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).LoadCollection).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockLoadCollection.UnPatch()

		_, err := s.client.LoadCollection(ctx, NewLoadCollectionOption(collectionName))
		s.Error(err)
	})
}

func (s *MaintenanceSuite) TestLoadPartitions() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))
		fieldNames := []string{"id", "part", "vector"}
		replicaNum := rand.Intn(3) + 1
		rgs := []string{"rg1", "rg2"}

		done := atomic.NewBool(false)
		mockLoadPartitions := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).LoadPartitions).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, lpr *milvuspb.LoadPartitionsRequest) (*commonpb.Status, error) {
			s.Equal(collectionName, lpr.GetCollectionName())
			s.ElementsMatch([]string{partitionName}, lpr.GetPartitionNames())
			s.ElementsMatch(fieldNames, lpr.GetLoadFields())
			s.True(lpr.SkipLoadDynamicField)
			s.EqualValues(replicaNum, lpr.GetReplicaNumber())
			s.ElementsMatch(rgs, lpr.GetResourceGroups())
			return merr.Success(), nil
		}).Build()
		defer mockLoadPartitions.UnPatch()
		mockGetLoadingProgress := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).GetLoadingProgress).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, glpr *milvuspb.GetLoadingProgressRequest) (*milvuspb.GetLoadingProgressResponse, error) {
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
		}).Build()
		defer mockGetLoadingProgress.UnPatch()

		task, err := s.client.LoadPartitions(ctx, NewLoadPartitionsOption(collectionName, partitionName).
			WithReplica(replicaNum).
			WithResourceGroup(rgs...).
			WithLoadFields(fieldNames...).
			WithSkipLoadDynamicField(true))
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
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))

		mockLoadPartitions := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).LoadPartitions).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockLoadPartitions.UnPatch()

		_, err := s.client.LoadPartitions(ctx, NewLoadPartitionsOption(collectionName, partitionName))
		s.Error(err)
	})
}

func (s *MaintenanceSuite) TestReleaseCollection() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		mockReleaseCollection := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).ReleaseCollection).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, rcr *milvuspb.ReleaseCollectionRequest) (*commonpb.Status, error) {
			s.Equal(collectionName, rcr.GetCollectionName())
			return merr.Success(), nil
		}).Build()
		defer mockReleaseCollection.UnPatch()

		err := s.client.ReleaseCollection(ctx, NewReleaseCollectionOption(collectionName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		mockReleaseCollection := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).ReleaseCollection).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockReleaseCollection.UnPatch()

		err := s.client.ReleaseCollection(ctx, NewReleaseCollectionOption(collectionName))
		s.Error(err)
	})
}

func (s *MaintenanceSuite) TestReleasePartitions() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))
		mockReleasePartitions := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).ReleasePartitions).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, rpr *milvuspb.ReleasePartitionsRequest) (*commonpb.Status, error) {
			s.Equal(collectionName, rpr.GetCollectionName())
			s.ElementsMatch([]string{partitionName}, rpr.GetPartitionNames())
			return merr.Success(), nil
		}).Build()
		defer mockReleasePartitions.UnPatch()

		err := s.client.ReleasePartitions(ctx, NewReleasePartitionsOptions(collectionName, partitionName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))
		mockReleasePartitions := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).ReleasePartitions).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockReleasePartitions.UnPatch()

		err := s.client.ReleasePartitions(ctx, NewReleasePartitionsOptions(collectionName, partitionName))
		s.Error(err)
	})
}

func (s *MaintenanceSuite) TestFlush() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		done := atomic.NewBool(false)
		mockFlush := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).Flush).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, fr *milvuspb.FlushRequest) (*milvuspb.FlushResponse, error) {
			s.ElementsMatch([]string{collectionName}, fr.GetCollectionNames())
			return &milvuspb.FlushResponse{
				Status: merr.Success(),
				CollSegIDs: map[string]*schemapb.LongArray{
					collectionName: {Data: []int64{1, 2, 3}},
				},
				CollFlushTs: map[string]uint64{collectionName: 321},
			}, nil
		}).Build()
		defer mockFlush.UnPatch()
		mockGetFlushState := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).GetFlushState).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, gfsr *milvuspb.GetFlushStateRequest) (*milvuspb.GetFlushStateResponse, error) {
			s.Equal(collectionName, gfsr.GetCollectionName())
			s.ElementsMatch([]int64{1, 2, 3}, gfsr.GetSegmentIDs())
			s.EqualValues(321, gfsr.GetFlushTs())
			return &milvuspb.GetFlushStateResponse{
				Status:  merr.Success(),
				Flushed: done.Load(),
			}, nil
		}).Build()
		defer mockGetFlushState.UnPatch()

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
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		mockFlush := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).Flush).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockFlush.UnPatch()

		_, err := s.client.Flush(ctx, NewFlushOption(collectionName))
		s.Error(err)
	})
}

func (s *MaintenanceSuite) TestRefreshLoad() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		done := atomic.NewBool(false)
		mockLoadCollection := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).LoadCollection).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, lcr *milvuspb.LoadCollectionRequest) (*commonpb.Status, error) {
			s.Equal(collectionName, lcr.GetCollectionName())
			s.True(lcr.GetRefresh())
			return merr.Success(), nil
		}).Build()
		defer mockLoadCollection.UnPatch()
		mockGetLoadingProgress := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).GetLoadingProgress).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, glpr *milvuspb.GetLoadingProgressRequest) (*milvuspb.GetLoadingProgressResponse, error) {
			s.Equal(collectionName, glpr.GetCollectionName())

			progress := int64(50)
			if done.Load() {
				progress = 100
			}

			return &milvuspb.GetLoadingProgressResponse{
				Status:          merr.Success(),
				RefreshProgress: progress,
			}, nil
		}).Build()
		defer mockGetLoadingProgress.UnPatch()

		task, err := s.client.RefreshLoad(ctx, NewRefreshLoadOption(collectionName))
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
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		mockLoadCollection := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).LoadCollection).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockLoadCollection.UnPatch()

		_, err := s.client.RefreshLoad(ctx, NewRefreshLoadOption(collectionName))
		s.Error(err)
	})
}

func (s *MaintenanceSuite) TestCompact() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		compactID := rand.Int63()

		mockManualCompaction := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).ManualCompaction).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, cr *milvuspb.ManualCompactionRequest) (*milvuspb.ManualCompactionResponse, error) {
			s.Equal(collectionName, cr.GetCollectionName())
			return &milvuspb.ManualCompactionResponse{
				CompactionID: compactID,
			}, nil
		}).Build()
		defer mockManualCompaction.UnPatch()

		id, err := s.client.Compact(ctx, NewCompactOption(collectionName))
		s.NoError(err)
		s.Equal(compactID, id)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		mockManualCompaction := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).ManualCompaction).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockManualCompaction.UnPatch()

		_, err := s.client.Compact(ctx, NewCompactOption(collectionName))
		s.Error(err)
	})
}

func (s *MaintenanceSuite) TestGetCompactionState() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		compactID := rand.Int63()

		mockGetCompactionState := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).GetCompactionState).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, gcsr *milvuspb.GetCompactionStateRequest) (*milvuspb.GetCompactionStateResponse, error) {
			s.Equal(compactID, gcsr.GetCompactionID())
			return &milvuspb.GetCompactionStateResponse{
				Status: merr.Success(),
				State:  commonpb.CompactionState_Completed,
			}, nil
		}).Build()
		defer mockGetCompactionState.UnPatch()

		state, err := s.client.GetCompactionState(ctx, NewGetCompactionStateOption(compactID))
		s.NoError(err)
		s.Equal(entity.CompactionStateCompleted, state)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		compactID := rand.Int63()

		mockGetCompactionState := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).GetCompactionState).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockGetCompactionState.UnPatch()

		_, err := s.client.GetCompactionState(ctx, NewGetCompactionStateOption(compactID))
		s.Error(err)
	})
}

func (s *MaintenanceSuite) TestGetLoadState() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		progress := rand.Int63n(100)

		mockGetLoadState := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).GetLoadState).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, glsr *milvuspb.GetLoadStateRequest) (*milvuspb.GetLoadStateResponse, error) {
			s.Equal(collectionName, glsr.GetCollectionName())
			return &milvuspb.GetLoadStateResponse{
				Status: merr.Success(),
				State:  commonpb.LoadState_LoadStateLoading,
			}, nil
		}).Build()
		defer mockGetLoadState.UnPatch()
		mockGetLoadingProgress := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).GetLoadingProgress).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, glpr *milvuspb.GetLoadingProgressRequest) (*milvuspb.GetLoadingProgressResponse, error) {
			s.Equal(collectionName, glpr.GetCollectionName())
			return &milvuspb.GetLoadingProgressResponse{
				Status:   merr.Success(),
				Progress: progress,
			}, nil
		}).Build()
		defer mockGetLoadingProgress.UnPatch()

		state, err := s.client.GetLoadState(ctx, NewGetLoadStateOption(collectionName))
		s.NoError(err)
		s.Equal(entity.LoadStateLoading, state.State)
		s.Equal(progress, state.Progress)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		mockGetLoadState := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).GetLoadState).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockGetLoadState.UnPatch()

		_, err := s.client.GetLoadState(ctx, NewGetLoadStateOption(collectionName))
		s.Error(err)
	})
}

func TestMaintenance(t *testing.T) {
	suite.Run(t, new(MaintenanceSuite))
}
