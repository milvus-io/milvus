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

package proxy

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type LoadInfoCacheSuite struct {
	suite.Suite

	mockQueryCoord *mocks.MockQueryCoordClient
}

func (s *LoadInfoCacheSuite) SetupTest() {
	s.mockQueryCoord = mocks.NewMockQueryCoordClient(s.T())
}

func (s *LoadInfoCacheSuite) TestGetLoadInfo() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("normal_run", func() {
		s.mockQueryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, scr *querypb.ShowCollectionsRequest, co ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
			return &querypb.ShowCollectionsResponse{
				Status:        merr.Success(),
				CollectionIDs: scr.GetCollectionIDs(),
				LoadFields: []*schemapb.LongArray{
					{Data: []int64{100, 101, 102}},
				},
			}, nil
		}).Once()
		cache := NewLoadInfoCache(s.mockQueryCoord)

		info, err := cache.GetLoadInfo(ctx, 1000)
		s.NoError(err)
		s.Equal(int64(1000), info.collectionID)
		s.ElementsMatch([]int64{100, 101, 102}, info.GetLoadedFields())

		for _, fieldID := range []int64{100, 101, 102} {
			s.True(info.IsFieldLoaded(fieldID))
		}

		// cached
		info, err = cache.GetLoadInfo(ctx, 1000)
		s.NoError(err)
		s.Equal(int64(1000), info.collectionID)
		s.ElementsMatch([]int64{100, 101, 102}, info.GetLoadedFields())
	})

	s.Run("return_error", func() {
		s.mockQueryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, scr *querypb.ShowCollectionsRequest, co ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
			return nil, merr.WrapErrCollectionLoaded(fmt.Sprintf("%v", scr.GetCollectionIDs()))
		}).Once()

		cache := NewLoadInfoCache(s.mockQueryCoord)

		_, err := cache.GetLoadInfo(ctx, 1000)
		s.Error(err)
	})

	s.Run("legacy_querycoord", func() {
		s.mockQueryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, scr *querypb.ShowCollectionsRequest, co ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
			return &querypb.ShowCollectionsResponse{
				Status:        merr.Success(),
				CollectionIDs: scr.GetCollectionIDs(),
			}, nil
		}).Once()
		cache := NewLoadInfoCache(s.mockQueryCoord)

		info, err := cache.GetLoadInfo(ctx, 1000)
		s.NoError(err)
		s.Equal(int64(1000), info.collectionID)
		s.Len(info.GetLoadedFields(), 0)

		// legacy shall treat all field as loaded
		for _, fieldID := range []int64{100, 101, 102} {
			s.True(info.IsFieldLoaded(fieldID))
		}
	})
}

func (s *LoadInfoCacheSuite) TestExpire() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("normal_run", func() {
		s.mockQueryCoord.EXPECT().ShowCollections(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, scr *querypb.ShowCollectionsRequest, co ...grpc.CallOption) (*querypb.ShowCollectionsResponse, error) {
			return &querypb.ShowCollectionsResponse{
				Status:        merr.Success(),
				CollectionIDs: scr.GetCollectionIDs(),
				LoadFields: []*schemapb.LongArray{
					{Data: []int64{100, 101, 102}},
				},
			}, nil
		}).Twice()
		cache := NewLoadInfoCache(s.mockQueryCoord)

		cache.Expire(ctx, 1000)

		info, err := cache.GetLoadInfo(ctx, 1000)
		s.NoError(err)
		s.Equal(int64(1000), info.collectionID)
		s.ElementsMatch([]int64{100, 101, 102}, info.GetLoadedFields())

		for _, fieldID := range []int64{100, 101, 102} {
			s.True(info.IsFieldLoaded(fieldID))
		}

		cache.Expire(ctx, 1000)
		// trigger another read
		info, err = cache.GetLoadInfo(ctx, 1000)
		s.NoError(err)
		s.Equal(int64(1000), info.collectionID)
		s.ElementsMatch([]int64{100, 101, 102}, info.GetLoadedFields())
	})
}

func TestLoadInfoCache(t *testing.T) {
	suite.Run(t, new(LoadInfoCacheSuite))
}
