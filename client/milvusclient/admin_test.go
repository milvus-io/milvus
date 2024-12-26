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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type AdminSuite struct {
	MockSuiteBase
}

func (s *AdminSuite) TestGetServerVersion() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		version := fmt.Sprintf("v%s", s.randString(6))

		s.mock.EXPECT().GetVersion(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, gvr *milvuspb.GetVersionRequest) (*milvuspb.GetVersionResponse, error) {
			return &milvuspb.GetVersionResponse{
				Status:  merr.Success(),
				Version: version,
			}, nil
		}).Once()

		v, err := s.client.GetServerVersion(ctx, NewGetServerVersionOption())
		s.NoError(err)
		s.Equal(version, v)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().GetVersion(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.GetServerVersion(ctx, NewGetServerVersionOption())
		s.Error(err)
	})
}

func (s *AdminSuite) TestGetPersistentSegmentInfo() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		collectionName := fmt.Sprintf("coll_%s", s.randString(6))
		segments := []*entity.Segment{
			{ID: rand.Int63(), CollectionID: rand.Int63(), ParititionID: rand.Int63(), NumRows: rand.Int63(), State: commonpb.SegmentState_Flushed},
			{ID: rand.Int63(), CollectionID: rand.Int63(), ParititionID: rand.Int63(), NumRows: rand.Int63(), State: commonpb.SegmentState_Flushed},
		}

		s.mock.EXPECT().GetPersistentSegmentInfo(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, gpsi *milvuspb.GetPersistentSegmentInfoRequest) (*milvuspb.GetPersistentSegmentInfoResponse, error) {
			return &milvuspb.GetPersistentSegmentInfoResponse{
				Status: merr.Success(),
				Infos: lo.Map(segments, func(segment *entity.Segment, _ int) *milvuspb.PersistentSegmentInfo {
					return &milvuspb.PersistentSegmentInfo{
						SegmentID:    segment.ID,
						CollectionID: segment.CollectionID,
						PartitionID:  segment.ParititionID,
						NumRows:      segment.NumRows,
						State:        segment.State,
					}
				}),
			}, nil
		}).Once()

		segments, err := s.client.GetPersistentSegmentInfo(ctx, NewGetPersistentSegmentInfoOption(collectionName))
		s.NoError(err)
		s.Equal(segments, segments)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().GetPersistentSegmentInfo(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.GetPersistentSegmentInfo(ctx, &getPersistentSegmentInfoOption{collectionName: "coll"})
		s.Error(err)
	})
}

func (s *AdminSuite) TestBackupRBAC() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		s.mock.EXPECT().BackupRBAC(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, brr *milvuspb.BackupRBACMetaRequest) (*milvuspb.BackupRBACMetaResponse, error) {
			return &milvuspb.BackupRBACMetaResponse{
				Status: merr.Success(),
				RBACMeta: &milvuspb.RBACMeta{
					Users: []*milvuspb.UserInfo{
						{
							User:     "user1",
							Password: "passwd",
							Roles: []*milvuspb.RoleEntity{
								{Name: "role1"},
							},
						},
					},
					Roles: []*milvuspb.RoleEntity{
						{Name: "role1"},
					},
					Grants: []*milvuspb.GrantEntity{
						{
							Object: &milvuspb.ObjectEntity{
								Name: "testObject",
							},
							ObjectName: "testObjectName",
							Role: &milvuspb.RoleEntity{
								Name: "testRole",
							},
							Grantor: &milvuspb.GrantorEntity{
								User: &milvuspb.UserEntity{
									Name: "grantorUser",
								},
								Privilege: &milvuspb.PrivilegeEntity{
									Name: "testPrivilege",
								},
							},
							DbName: "testDB",
						},
					},
					PrivilegeGroups: []*milvuspb.PrivilegeGroupInfo{
						{
							GroupName: "testGroup",
							Privileges: []*milvuspb.PrivilegeEntity{
								{Name: "testPrivilege"},
							},
						},
					},
				},
			}, nil
		}).Once()

		meta, err := s.client.BackupRBAC(ctx, NewBackupRBACOption())
		s.NoError(err)
		s.Len(meta.Users, 1)
		s.Len(meta.Roles, 1)
		s.Len(meta.RoleGrants, 1)
		s.Len(meta.PrivilegeGroups, 1)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().BackupRBAC(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		_, err := s.client.BackupRBAC(ctx, NewBackupRBACOption())
		s.Error(err)
	})
}

func (s *AdminSuite) TestRestoreRBAC() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s.Run("success", func() {
		meta := &entity.RBACMeta{
			Users: []*entity.UserInfo{
				{
					UserDescription: entity.UserDescription{
						Name:  "user1",
						Roles: []string{"role1"},
					},
					Password: "passwd",
				},
			},
			Roles: []*entity.Role{
				{RoleName: "role1"},
			},
			RoleGrants: []*entity.RoleGrants{
				{
					Object:        "testObject",
					ObjectName:    "testObjectName",
					RoleName:      "testRole",
					GrantorName:   "grantorUser",
					PrivilegeName: "testPrivilege",
					DbName:        "testDB",
				},
			},
			PrivilegeGroups: []*entity.PrivilegeGroup{
				{
					GroupName:  "testGroup",
					Privileges: []string{"testPrivilege"},
				},
			},
		}

		s.mock.EXPECT().RestoreRBAC(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, rrr *milvuspb.RestoreRBACMetaRequest) (*commonpb.Status, error) {
			return merr.Success(), nil
		}).Once()

		err := s.client.RestoreRBAC(ctx, NewRestoreRBACOption(meta))
		s.NoError(err)
	})

	s.Run("failure", func() {
		s.mock.EXPECT().RestoreRBAC(mock.Anything, mock.Anything).Return(nil, merr.WrapErrServiceInternal("mocked")).Once()

		err := s.client.RestoreRBAC(ctx, NewRestoreRBACOption(&entity.RBACMeta{}))
		s.Error(err)
	})
}

func TestAdminAPIs(t *testing.T) {
	suite.Run(t, new(AdminSuite))
}
