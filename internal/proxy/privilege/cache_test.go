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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

func (s *PrivilegeCacheTestSuite) TestRevokeCombinedKeyRemovesBothFormats() {
	// Verify that revoking with a combined "|"-delimited key removes both
	// name-based and ID-based entries from privilegeInfos.
	// This is the core behavior that the impl.go fix relies on.
	namePolicy := `{"PType":"p","V0":"role1","V1":"Collection-default.myCol","V2":"Insert"}`
	idPolicy := `{"PType":"p","V0":"role1","V1":"Collection-dbID:1.colID:123","V2":"Insert"}`

	// Simulate CacheRefresh loading dual-write entries
	s.cache.mu.Lock()
	s.cache.privilegeInfos[namePolicy] = struct{}{}
	s.cache.privilegeInfos[idPolicy] = struct{}{}
	s.cache.mu.Unlock()

	// Simulate revoke with combined key (what the fixed impl.go sends)
	combinedKey := namePolicy + "|" + idPolicy
	keys := funcutil.PrivilegesForPolicy(combinedKey)
	s.cache.mu.Lock()
	for _, key := range keys {
		delete(s.cache.privilegeInfos, key)
	}
	s.cache.mu.Unlock()

	s.cache.mu.RLock()
	_, hasName := s.cache.privilegeInfos[namePolicy]
	_, hasID := s.cache.privilegeInfos[idPolicy]
	s.cache.mu.RUnlock()

	s.False(hasName, "name-based policy should be removed")
	s.False(hasID, "ID-based policy should be removed")
}

func (s *PrivilegeCacheTestSuite) TestRevokeSingleKeyLeavesStale() {
	// Documents the bug: revoking only one format leaves the other stale.
	namePolicy := `{"PType":"p","V0":"role1","V1":"Collection-default.myCol","V2":"Insert"}`
	idPolicy := `{"PType":"p","V0":"role1","V1":"Collection-dbID:1.colID:123","V2":"Insert"}`

	s.cache.mu.Lock()
	s.cache.privilegeInfos[namePolicy] = struct{}{}
	s.cache.privilegeInfos[idPolicy] = struct{}{}
	s.cache.mu.Unlock()

	// Old behavior: only ID-based key sent
	keys := funcutil.PrivilegesForPolicy(idPolicy)
	s.cache.mu.Lock()
	for _, key := range keys {
		delete(s.cache.privilegeInfos, key)
	}
	s.cache.mu.Unlock()

	s.cache.mu.RLock()
	_, hasName := s.cache.privilegeInfos[namePolicy]
	_, hasID := s.cache.privilegeInfos[idPolicy]
	s.cache.mu.RUnlock()

	s.True(hasName, "name-based policy remains when only ID-based key is revoked")
	s.False(hasID, "ID-based policy should be removed")
}

func TestPrivilegeCache(t *testing.T) {
	suite.Run(t, new(PrivilegeCacheTestSuite))
}
