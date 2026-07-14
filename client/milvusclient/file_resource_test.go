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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type FileResourceSuite struct {
	MockSuiteBase
}

func (s *FileResourceSuite) TestOptions() {
	addReq := NewAddFileResourceOption("synonyms", "analyzers/synonyms.txt").Request()
	s.NotNil(addReq.GetBase())
	s.Equal("synonyms", addReq.GetName())
	s.Equal("analyzers/synonyms.txt", addReq.GetPath())

	removeReq := NewRemoveFileResourceOption("synonyms").Request()
	s.NotNil(removeReq.GetBase())
	s.Equal("synonyms", removeReq.GetName())

	listReq := NewListFileResourcesOption().Request()
	s.NotNil(listReq.GetBase())
}

func (s *FileResourceSuite) TestAddFileResource() {
	ctx := context.Background()

	s.Run("success", func() {
		s.mock.EXPECT().AddFileResource(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, req *milvuspb.AddFileResourceRequest) (*commonpb.Status, error) {
				s.Equal("synonyms", req.GetName())
				s.Equal("analyzers/synonyms.txt", req.GetPath())
				return merr.Success(), nil
			}).Once()

		s.NoError(s.client.AddFileResource(ctx, NewAddFileResourceOption("synonyms", "analyzers/synonyms.txt")))
	})

	s.Run("rpc error", func() {
		s.mock.EXPECT().AddFileResource(mock.Anything, mock.Anything).Return(nil, errors.New("mocked")).Once()
		s.Error(s.client.AddFileResource(ctx, NewAddFileResourceOption("synonyms", "analyzers/synonyms.txt")))
	})

	s.Run("status error", func() {
		s.mock.EXPECT().AddFileResource(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrParameterInvalid), nil).Once()
		s.Error(s.client.AddFileResource(ctx, NewAddFileResourceOption("synonyms", "analyzers/synonyms.txt")))
	})

	s.Run("nil option", func() {
		err := s.client.AddFileResource(ctx, nil)
		s.ErrorIs(err, merr.ErrParameterInvalid)
	})
}

func (s *FileResourceSuite) TestRemoveFileResource() {
	ctx := context.Background()

	s.Run("success", func() {
		s.mock.EXPECT().RemoveFileResource(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, req *milvuspb.RemoveFileResourceRequest) (*commonpb.Status, error) {
				s.Equal("synonyms", req.GetName())
				return merr.Success(), nil
			}).Once()

		s.NoError(s.client.RemoveFileResource(ctx, NewRemoveFileResourceOption("synonyms")))
	})

	s.Run("failure", func() {
		s.mock.EXPECT().RemoveFileResource(mock.Anything, mock.Anything).Return(nil, errors.New("mocked")).Once()
		s.Error(s.client.RemoveFileResource(ctx, NewRemoveFileResourceOption("synonyms")))
	})

	s.Run("status error", func() {
		s.mock.EXPECT().RemoveFileResource(mock.Anything, mock.Anything).Return(merr.Status(merr.ErrParameterInvalid), nil).Once()
		s.Error(s.client.RemoveFileResource(ctx, NewRemoveFileResourceOption("synonyms")))
	})

	s.Run("nil option", func() {
		err := s.client.RemoveFileResource(ctx, nil)
		s.ErrorIs(err, merr.ErrParameterInvalid)
	})
}

func (s *FileResourceSuite) TestListFileResources() {
	ctx := context.Background()

	s.Run("success", func() {
		s.mock.EXPECT().ListFileResources(mock.Anything, mock.Anything).Return(&milvuspb.ListFileResourcesResponse{
			Status: merr.Success(),
			Resources: []*milvuspb.FileResourceInfo{
				{Id: 100, Name: "synonyms", Path: "analyzers/synonyms.txt"},
			},
		}, nil).Once()

		resources, err := s.client.ListFileResources(ctx, NewListFileResourcesOption())
		s.NoError(err)
		s.Require().Len(resources, 1)
		s.EqualValues(100, resources[0].ID)
		s.Equal("synonyms", resources[0].Name)
		s.Equal("analyzers/synonyms.txt", resources[0].Path)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().ListFileResources(mock.Anything, mock.Anything).Return(nil, errors.New("mocked")).Once()
		resources, err := s.client.ListFileResources(ctx, NewListFileResourcesOption())
		s.Error(err)
		s.Nil(resources)
	})

	s.Run("status error", func() {
		s.mock.EXPECT().ListFileResources(mock.Anything, mock.Anything).Return(&milvuspb.ListFileResourcesResponse{
			Status: merr.Status(merr.ErrParameterInvalid),
		}, nil).Once()
		resources, err := s.client.ListFileResources(ctx, NewListFileResourcesOption())
		s.Error(err)
		s.Nil(resources)
	})

	s.Run("nil option", func() {
		resources, err := s.client.ListFileResources(ctx, nil)
		s.ErrorIs(err, merr.ErrParameterInvalid)
		s.Nil(resources)
	})
}

func TestFileResource(t *testing.T) {
	suite.Run(t, new(FileResourceSuite))
}
