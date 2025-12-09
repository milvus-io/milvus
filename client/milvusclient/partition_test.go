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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type PartitionSuite struct {
	MockSuiteBase
}

func (s *PartitionSuite) TestListPartitions() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		mockShowPartitions := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).ShowPartitions).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, spr *milvuspb.ShowPartitionsRequest) (*milvuspb.ShowPartitionsResponse, error) {
			s.Equal(collectionName, spr.GetCollectionName())
			return &milvuspb.ShowPartitionsResponse{
				Status:         merr.Success(),
				PartitionNames: []string{"_default", "part_1"},
				PartitionIDs:   []int64{100, 101},
			}, nil
		}).Build()
		defer mockShowPartitions.UnPatch()

		names, err := s.client.ListPartitions(ctx, NewListPartitionOption(collectionName))
		s.NoError(err)
		s.ElementsMatch([]string{"_default", "part_1"}, names)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))

		mockShowPartitions := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).ShowPartitions).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockShowPartitions.UnPatch()

		_, err := s.client.ListPartitions(ctx, NewListPartitionOption(collectionName))
		s.Error(err)
	})
}

func (s *PartitionSuite) TestCreatePartition() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))

		mockCreatePartition := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).CreatePartition).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, cpr *milvuspb.CreatePartitionRequest) (*commonpb.Status, error) {
			s.Equal(collectionName, cpr.GetCollectionName())
			s.Equal(partitionName, cpr.GetPartitionName())
			return merr.Success(), nil
		}).Build()
		defer mockCreatePartition.UnPatch()

		err := s.client.CreatePartition(ctx, NewCreatePartitionOption(collectionName, partitionName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))

		mockCreatePartition := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).CreatePartition).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockCreatePartition.UnPatch()

		err := s.client.CreatePartition(ctx, NewCreatePartitionOption(collectionName, partitionName))
		s.Error(err)
	})
}

func (s *PartitionSuite) TestHasPartition() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))

		mockHasPartition := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).HasPartition).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, hpr *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
			s.Equal(collectionName, hpr.GetCollectionName())
			s.Equal(partitionName, hpr.GetPartitionName())
			return &milvuspb.BoolResponse{Status: merr.Success()}, nil
		}).Build()
		defer mockHasPartition.UnPatch()

		has, err := s.client.HasPartition(ctx, NewHasPartitionOption(collectionName, partitionName))
		s.NoError(err)
		s.False(has)
	})

	s.Run("success_true", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))

		mockHasPartition := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).HasPartition).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, hpr *milvuspb.HasPartitionRequest) (*milvuspb.BoolResponse, error) {
			s.Equal(collectionName, hpr.GetCollectionName())
			s.Equal(partitionName, hpr.GetPartitionName())
			return &milvuspb.BoolResponse{
				Status: merr.Success(),
				Value:  true,
			}, nil
		}).Build()
		defer mockHasPartition.UnPatch()

		has, err := s.client.HasPartition(ctx, NewHasPartitionOption(collectionName, partitionName))
		s.NoError(err)
		s.True(has)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))
		mockHasPartition := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).HasPartition).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockHasPartition.UnPatch()

		_, err := s.client.HasPartition(ctx, NewHasPartitionOption(collectionName, partitionName))
		s.Error(err)
	})
}

func (s *PartitionSuite) TestDropPartition() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))
		mockDropPartition := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DropPartition).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, dpr *milvuspb.DropPartitionRequest) (*commonpb.Status, error) {
			s.Equal(collectionName, dpr.GetCollectionName())
			s.Equal(partitionName, dpr.GetPartitionName())
			return merr.Success(), nil
		}).Build()
		defer mockDropPartition.UnPatch()

		err := s.client.DropPartition(ctx, NewDropPartitionOption(collectionName, partitionName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))
		mockDropPartition := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).DropPartition).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockDropPartition.UnPatch()

		err := s.client.DropPartition(ctx, NewDropPartitionOption(collectionName, partitionName))
		s.Error(err)
	})
}

func (s *PartitionSuite) TestGetPartitionStats() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))
		mockGetPartitionStatistics := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).GetPartitionStatistics).To(func(_ *milvuspb.UnimplementedMilvusServiceServer, ctx context.Context, gpsr *milvuspb.GetPartitionStatisticsRequest) (*milvuspb.GetPartitionStatisticsResponse, error) {
			s.Equal(collectionName, gpsr.GetCollectionName())
			s.Equal(partitionName, gpsr.GetPartitionName())
			return &milvuspb.GetPartitionStatisticsResponse{
				Status: merr.Success(),
				Stats: []*commonpb.KeyValuePair{
					{Key: "rows", Value: "100"},
				},
			}, nil
		}).Build()
		defer mockGetPartitionStatistics.UnPatch()

		stats, err := s.client.GetPartitionStats(ctx, NewGetPartitionStatsOption(collectionName, partitionName))
		s.NoError(err)
		s.Equal("100", stats["rows"])
	})

	s.Run("failure", func() {
		defer mockey.UnPatchAll()
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		partitionName := fmt.Sprintf("part_%s", s.randString(6))
		mockGetPartitionStatistics := mockey.Mock((*milvuspb.UnimplementedMilvusServiceServer).GetPartitionStatistics).Return(nil, merr.WrapErrServiceInternal("mocked")).Build()
		defer mockGetPartitionStatistics.UnPatch()

		_, err := s.client.GetPartitionStats(ctx, NewGetPartitionStatsOption(collectionName, partitionName))
		s.Error(err)
	})
}

func TestPartition(t *testing.T) {
	suite.Run(t, new(PartitionSuite))
}
