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
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type PrivilgeGroupSuite struct {
	MockSuiteBase
}

func (s *PrivilgeGroupSuite) TestGrantV2() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	roleName := fmt.Sprintf("test_role_%s", s.randString(6))
	privilegeName := "Insert"
	dbName := fmt.Sprintf("test_db_%s", s.randString(6))
	collectionName := fmt.Sprintf("test_collection_%s", s.randString(6))

	s.Run("success", func() {
		s.mock.EXPECT().OperatePrivilegeV2(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.OperatePrivilegeV2Request) (*commonpb.Status, error) {
			s.Equal(roleName, r.GetRole().GetName())
			s.Equal(privilegeName, r.GetGrantor().GetPrivilege().GetName())
			s.Equal(dbName, r.GetDbName())
			s.Equal(collectionName, r.GetCollectionName())
			return merr.Success(), nil
		}).Once()

		err := s.client.GrantV2(ctx, NewGrantV2Option(roleName, privilegeName, dbName, collectionName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().OperatePrivilegeV2(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.GrantV2(ctx, NewGrantV2Option(roleName, privilegeName, dbName, collectionName))
		s.Error(err)
	})
}

func (s *PrivilgeGroupSuite) TestRevokeV2() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	roleName := fmt.Sprintf("test_role_%s", s.randString(6))
	privilegeName := "Insert"
	dbName := fmt.Sprintf("test_db_%s", s.randString(6))
	collectionName := fmt.Sprintf("test_collection_%s", s.randString(6))

	s.Run("success", func() {
		s.mock.EXPECT().OperatePrivilegeV2(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.OperatePrivilegeV2Request) (*commonpb.Status, error) {
			s.Equal(roleName, r.GetRole().GetName())
			s.Equal(privilegeName, r.GetGrantor().GetPrivilege().GetName())
			s.Equal(dbName, r.GetDbName())
			s.Equal(collectionName, r.GetCollectionName())
			return merr.Success(), nil
		}).Once()

		err := s.client.RevokeV2(ctx, NewRevokeV2Option(roleName, privilegeName, dbName, collectionName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().OperatePrivilegeV2(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.RevokeV2(ctx, NewRevokeV2Option(roleName, privilegeName, dbName, collectionName))
		s.Error(err)
	})
}

func (s *PrivilgeGroupSuite) TestCreatePrivilegeGroup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	groupName := fmt.Sprintf("test_pg_%s", s.randString(6))

	s.Run("success", func() {
		s.mock.EXPECT().CreatePrivilegeGroup(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.CreatePrivilegeGroupRequest) (*commonpb.Status, error) {
			s.Equal(groupName, r.GetGroupName())
			return merr.Success(), nil
		}).Once()

		err := s.client.CreatePrivilegeGroup(ctx, NewCreatePrivilegeGroupOption(groupName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().CreatePrivilegeGroup(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.CreatePrivilegeGroup(ctx, NewCreatePrivilegeGroupOption(groupName))
		s.Error(err)
	})
}

func (s *PrivilgeGroupSuite) TestDropPrivilegeGroup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	groupName := fmt.Sprintf("test_pg_%s", s.randString(6))

	s.Run("success", func() {
		s.mock.EXPECT().DropPrivilegeGroup(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.DropPrivilegeGroupRequest) (*commonpb.Status, error) {
			s.Equal(groupName, r.GetGroupName())
			return merr.Success(), nil
		}).Once()

		err := s.client.DropPrivilegeGroup(ctx, NewDropPrivilegeGroupOption(groupName))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().DropPrivilegeGroup(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.DropPrivilegeGroup(ctx, NewDropPrivilegeGroupOption(groupName))
		s.Error(err)
	})
}

func (s *PrivilgeGroupSuite) TestListPrivilegeGroups() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		s.mock.EXPECT().ListPrivilegeGroups(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.ListPrivilegeGroupsRequest) (*milvuspb.ListPrivilegeGroupsResponse, error) {
			return &milvuspb.ListPrivilegeGroupsResponse{
				PrivilegeGroups: []*milvuspb.PrivilegeGroupInfo{
					{
						GroupName:  "pg1",
						Privileges: []*milvuspb.PrivilegeEntity{{Name: "Insert"}, {Name: "Query"}},
					},
					{
						GroupName:  "pg2",
						Privileges: []*milvuspb.PrivilegeEntity{{Name: "Delete"}, {Name: "Query"}},
					},
				},
			}, nil
		}).Once()

		pgs, err := s.client.ListPrivilegeGroups(ctx, NewListPrivilegeGroupsOption())
		s.NoError(err)
		s.Equal(2, len(pgs))
		s.Equal("pg1", pgs[0].GroupName)
		s.Equal([]string{"Insert", "Query"}, pgs[0].Privileges)
		s.Equal("pg2", pgs[1].GroupName)
		s.Equal([]string{"Delete", "Query"}, pgs[1].Privileges)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().ListPrivilegeGroups(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.ListPrivilegeGroups(ctx, NewListPrivilegeGroupsOption())
		s.Error(err)
	})
}

func (s *PrivilgeGroupSuite) TestOperatePrivilegeGroup() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	groupName := fmt.Sprintf("test_pg_%s", s.randString(6))
	privileges := []*milvuspb.PrivilegeEntity{{Name: "Insert"}, {Name: "Query"}}
	operateType := milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup

	s.Run("success", func() {
		s.mock.EXPECT().OperatePrivilegeGroup(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, r *milvuspb.OperatePrivilegeGroupRequest) (*commonpb.Status, error) {
			s.Equal(groupName, r.GetGroupName())
			return merr.Success(), nil
		}).Once()

		err := s.client.OperatePrivilegeGroup(ctx, NewOperatePrivilegeGroupOption(groupName, privileges, operateType))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().OperatePrivilegeGroup(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.OperatePrivilegeGroup(ctx, NewOperatePrivilegeGroupOption(groupName, privileges, operateType))
		s.Error(err)
	})
}

func TestPrivilegeGroup(t *testing.T) {
	suite.Run(t, new(PrivilgeGroupSuite))
}
