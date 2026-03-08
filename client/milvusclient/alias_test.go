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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type AliasSuite struct {
	MockSuiteBase
}

func (s *AliasSuite) TestCreateAlias() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aliasName := fmt.Sprintf("test_alias_%s", s.randString(6))
	collectionName := fmt.Sprintf("test_collection_%s", s.randString(6))

	s.Run("success", func() {
		s.mock.EXPECT().CreateAlias(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, car *milvuspb.CreateAliasRequest) (*commonpb.Status, error) {
			s.Equal(aliasName, car.GetAlias())
			s.Equal(collectionName, car.GetCollectionName())
			return merr.Success(), nil
		}).Once()

		err := s.client.CreateAlias(ctx, NewCreateAliasOption(collectionName, aliasName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().CreateAlias(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.CreateAlias(ctx, NewCreateAliasOption(collectionName, aliasName))
		s.Error(err)
	})
}

func (s *AliasSuite) TestDropAlias() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aliasName := fmt.Sprintf("test_alias_%s", s.randString(6))

	s.Run("success", func() {
		s.mock.EXPECT().DropAlias(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, dar *milvuspb.DropAliasRequest) (*commonpb.Status, error) {
			s.Equal(aliasName, dar.GetAlias())
			return merr.Success(), nil
		}).Once()

		err := s.client.DropAlias(ctx, NewDropAliasOption(aliasName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().DropAlias(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.DropAlias(ctx, NewDropAliasOption(aliasName))
		s.Error(err)
	})
}

func (s *AliasSuite) TestDescribeAlias() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aliasName := fmt.Sprintf("test_alias_%s", s.randString(6))
	collectionName := fmt.Sprintf("test_collection_%s", s.randString(6))

	s.Run("success", func() {
		s.mock.EXPECT().DescribeAlias(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, car *milvuspb.DescribeAliasRequest) (*milvuspb.DescribeAliasResponse, error) {
			s.Equal(aliasName, car.GetAlias())
			return &milvuspb.DescribeAliasResponse{
				Alias:      aliasName,
				Collection: collectionName,
			}, nil
		}).Once()

		alias, err := s.client.DescribeAlias(ctx, NewDescribeAliasOption(aliasName))
		s.NoError(err)
		s.Equal(aliasName, alias.Alias)
		s.Equal(collectionName, alias.CollectionName)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().DescribeAlias(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.DescribeAlias(ctx, NewDescribeAliasOption(aliasName))
		s.Error(err)
	})
}

func (s *AliasSuite) TestAlterAlias() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aliasName := fmt.Sprintf("test_alias_%s", s.randString(6))
	collectionName := fmt.Sprintf("test_collection_%s", s.randString(6))

	s.Run("success", func() {
		s.mock.EXPECT().AlterAlias(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, dar *milvuspb.AlterAliasRequest) (*commonpb.Status, error) {
			s.Equal(aliasName, dar.GetAlias())
			s.Equal(collectionName, dar.GetCollectionName())
			return merr.Success(), nil
		}).Once()

		err := s.client.AlterAlias(ctx, NewAlterAliasOption(aliasName, collectionName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().AlterAlias(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.AlterAlias(ctx, NewAlterAliasOption(aliasName, collectionName))
		s.Error(err)
	})
}

func (s *AliasSuite) TestListAliases() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	collectionName := fmt.Sprintf("test_collection_%s", s.randString(6))

	s.Run("success", func() {
		s.mock.EXPECT().ListAliases(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, lar *milvuspb.ListAliasesRequest) (*milvuspb.ListAliasesResponse, error) {
			s.Equal(collectionName, lar.GetCollectionName())
			return &milvuspb.ListAliasesResponse{
				Aliases: []string{"test1", "test2", "test3"},
			}, nil
		}).Once()

		names, err := s.client.ListAliases(ctx, NewListAliasesOption(collectionName))
		s.NoError(err)
		s.ElementsMatch([]string{"test1", "test2", "test3"}, names)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().ListAliases(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.ListAliases(ctx, NewListAliasesOption(collectionName))
		s.Error(err)
	})
}

func TestAlias(t *testing.T) {
	suite.Run(t, new(AliasSuite))
}
