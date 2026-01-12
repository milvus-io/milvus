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

package privilege

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type PrivilegeCacheTestSuite struct {
	suite.Suite
	mockMixCoord *mocks.MockMixCoordClient
	cache        *privilegeCache
	ctx          context.Context
}

func (s *PrivilegeCacheTestSuite) SetupTest() {
	s.mockMixCoord = mocks.NewMockMixCoordClient(s.T())
	s.cache = NewPrivilegeCache(s.mockMixCoord)
	s.ctx = context.Background()
}

func (s *PrivilegeCacheTestSuite) TestGetCredentialInfo() {
	s.Run("cache_hit", func() {
		username := "testuser"
		expectedCredInfo := &internalpb.CredentialInfo{
			Username:          username,
			EncryptedPassword: "encrypted_password",
		}

		s.cache.credMut.Lock()
		s.cache.credMap[username] = expectedCredInfo
		s.cache.credMut.Unlock()

		credInfo, err := s.cache.GetCredentialInfo(s.ctx, username)
		s.NoError(err)
		s.NotNil(credInfo)
		s.Equal(username, credInfo.Username)
		s.Equal("encrypted_password", credInfo.EncryptedPassword)
	})

	s.Run("cache_miss_success", func() {
		username := "newuser"
		expectedPassword := "encrypted_password_from_rpc"
		s.mockMixCoord.EXPECT().GetCredential(
			mock.Anything,
			mock.MatchedBy(func(req *rootcoordpb.GetCredentialRequest) bool {
				return req.Username == username &&
					req.Base.MsgType == commonpb.MsgType_GetCredential
			}),
			mock.Anything,
		).Return(&rootcoordpb.GetCredentialResponse{
			Status:   merr.Success(),
			Username: username,
			Password: expectedPassword,
		}, nil).Once()

		credInfo, err := s.cache.GetCredentialInfo(s.ctx, username)

		s.NoError(err)
		s.NotNil(credInfo)
		s.Equal(username, credInfo.Username)
		s.Equal(expectedPassword, credInfo.EncryptedPassword)
	})

	s.Run("cache_miss_rpc_error", func() {
		username := "erroruser"
		expectedError := errors.New("RPC call failed")
		s.mockMixCoord.EXPECT().GetCredential(
			mock.Anything,
			mock.MatchedBy(func(req *rootcoordpb.GetCredentialRequest) bool {
				return req.Username == username
			}),
			mock.Anything,
		).Return(nil, expectedError)

		credInfo, err := s.cache.GetCredentialInfo(s.ctx, username)
		s.Error(err)
		s.NotNil(credInfo)
		s.Equal(&internalpb.CredentialInfo{}, credInfo)
	})

	s.Run("cache_miss_response_error", func() {
		username := "badstatususer"
		expectedError := merr.WrapErrServiceInternal("internal error")
		s.mockMixCoord.EXPECT().GetCredential(
			mock.Anything,
			mock.MatchedBy(func(req *rootcoordpb.GetCredentialRequest) bool {
				return req.Username == username
			}),
			mock.Anything,
		).Return(&rootcoordpb.GetCredentialResponse{
			Status: merr.Status(expectedError),
		}, nil)

		credInfo, err := s.cache.GetCredentialInfo(s.ctx, username)
		s.Error(err)
		s.NotNil(credInfo)
		s.Equal(&internalpb.CredentialInfo{}, credInfo)
	})
}

func TestPrivilegeCache(t *testing.T) {
	suite.Run(t, new(PrivilegeCacheTestSuite))
}
