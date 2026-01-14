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

package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/proxy/privilege"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestParseHTTPAuth(t *testing.T) {
	t.Run("basic_auth", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/expr", nil)
		req.SetBasicAuth("testuser", "testpass")

		username, password, ok := parseHTTPAuth(req)
		assert.True(t, ok)
		assert.Equal(t, "testuser", username)
		assert.Equal(t, "testpass", password)
	})

	t.Run("no_auth", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/expr", nil)

		username, password, ok := parseHTTPAuth(req)
		assert.False(t, ok)
		assert.Empty(t, username)
		assert.Empty(t, password)
	})

	t.Run("unsupported_auth_format", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/expr", nil)
		req.Header.Set("Authorization", "Bearer some_token")

		username, password, ok := parseHTTPAuth(req)
		assert.False(t, ok)
		assert.Empty(t, username)
		assert.Empty(t, password)
	})
}

func TestIsAuthenticationError(t *testing.T) {
	authErr := &ErrAuthentication{msg: "test error"}
	permErr := &ErrPermissionDenied{msg: "test error"}

	assert.True(t, IsAuthenticationError(authErr))
	assert.False(t, IsAuthenticationError(permErr))
	assert.False(t, IsAuthenticationError(nil))
}

func TestIsPermissionDeniedError(t *testing.T) {
	authErr := &ErrAuthentication{msg: "test error"}
	permErr := &ErrPermissionDenied{msg: "test error"}

	assert.False(t, IsPermissionDeniedError(authErr))
	assert.True(t, IsPermissionDeniedError(permErr))
	assert.False(t, IsPermissionDeniedError(nil))
}

func TestErrorMessages(t *testing.T) {
	authErr := &ErrAuthentication{msg: "auth failed"}
	permErr := &ErrPermissionDenied{msg: "permission denied"}

	assert.Equal(t, "auth failed", authErr.Error())
	assert.Equal(t, "permission denied", permErr.Error())
}

// CheckPrivilegeTestSuite tests the CheckPrivilege function
type CheckPrivilegeTestSuite struct {
	suite.Suite
	ctx                    context.Context
	originalPasswordVerify func(ctx context.Context, username, password string) bool
	originalGetUserRole    func(username string) ([]string, error)
}

func (s *CheckPrivilegeTestSuite) SetupSuite() {
	paramtable.Init()
}

func (s *CheckPrivilegeTestSuite) SetupTest() {
	s.ctx = context.Background()
	// Save original functions to restore later
	s.originalPasswordVerify = passwordVerifyFunc
	s.originalGetUserRole = getUserRoleFunc
}

func (s *CheckPrivilegeTestSuite) TearDownTest() {
	// Restore original functions
	passwordVerifyFunc = s.originalPasswordVerify
	getUserRoleFunc = s.originalGetUserRole
	// Reset paramtable settings
	paramtable.Get().Reset(paramtable.Get().CommonCfg.AuthorizationEnabled.Key)
	paramtable.Get().Reset(paramtable.Get().CommonCfg.RootShouldBindRole.Key)
}

func (s *CheckPrivilegeTestSuite) TestAuthorizationDisabled() {
	// When authorization is disabled, CheckPrivilege should return nil
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "false")

	req := httptest.NewRequest(http.MethodGet, "/expr", nil)
	// No auth header needed when authorization is disabled

	err := CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)

	s.NoError(err)
}

func (s *CheckPrivilegeTestSuite) TestMissingAuthHeader() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")

	req := httptest.NewRequest(http.MethodGet, "/expr", nil)
	// No auth header

	err := CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)

	s.Error(err)
	s.True(IsAuthenticationError(err))
	s.Contains(err.Error(), "authentication required")
}

func (s *CheckPrivilegeTestSuite) TestEmptyUsername() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")

	req := httptest.NewRequest(http.MethodGet, "/expr", nil)
	req.SetBasicAuth("", "password")

	err := CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)

	s.Error(err)
	s.True(IsAuthenticationError(err))
	s.Contains(err.Error(), "authentication required")
}

func (s *CheckPrivilegeTestSuite) TestEmptyPassword() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")

	req := httptest.NewRequest(http.MethodGet, "/expr", nil)
	req.SetBasicAuth("username", "")

	err := CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)

	s.Error(err)
	s.True(IsAuthenticationError(err))
	s.Contains(err.Error(), "authentication required")
}

func (s *CheckPrivilegeTestSuite) TestPasswordVerifyFuncNotSet() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")

	// Ensure passwordVerifyFunc is nil
	passwordVerifyFunc = nil

	req := httptest.NewRequest(http.MethodGet, "/expr", nil)
	req.SetBasicAuth("testuser", "testpass")

	err := CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)

	s.Error(err)
	s.True(IsAuthenticationError(err))
	s.Contains(err.Error(), "password verification not available")
}

func (s *CheckPrivilegeTestSuite) TestPasswordVerificationFailure() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")

	// Register a password verify function that always fails
	RegisterPasswordVerifyFunc(func(ctx context.Context, username, password string) bool {
		return false
	})

	req := httptest.NewRequest(http.MethodGet, "/expr", nil)
	req.SetBasicAuth("testuser", "wrongpassword")

	err := CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)

	s.Error(err)
	s.True(IsAuthenticationError(err))
	s.Contains(err.Error(), "invalid credentials")
}

func (s *CheckPrivilegeTestSuite) TestRootUserBypassWhenRootShouldBindRoleIsFalse() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().CommonCfg.RootShouldBindRole.Key, "false")

	// Register a password verify function that accepts root
	RegisterPasswordVerifyFunc(func(ctx context.Context, username, password string) bool {
		return username == util.UserRoot && password == "Milvus"
	})

	req := httptest.NewRequest(http.MethodGet, "/expr", nil)
	req.SetBasicAuth(util.UserRoot, "Milvus")

	err := CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)

	// Root user should bypass privilege check
	s.NoError(err)
}

func (s *CheckPrivilegeTestSuite) TestRootUserNoBypassWhenRootShouldBindRoleIsTrue() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().CommonCfg.RootShouldBindRole.Key, "true")

	// Register a password verify function that accepts root
	RegisterPasswordVerifyFunc(func(ctx context.Context, username, password string) bool {
		return username == util.UserRoot && password == "Milvus"
	})

	// getUserRoleFunc not set, should fail
	getUserRoleFunc = nil

	req := httptest.NewRequest(http.MethodGet, "/expr", nil)
	req.SetBasicAuth(util.UserRoot, "Milvus")

	err := CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)

	// Root user should NOT bypass when RootShouldBindRole is true
	// It will fail because getUserRoleFunc is nil
	s.Error(err)
	s.True(IsAuthenticationError(err))
	s.Contains(err.Error(), "role lookup not available")
}

func (s *CheckPrivilegeTestSuite) TestRoleLookupFailure() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().CommonCfg.RootShouldBindRole.Key, "false")

	// Register a password verify function
	RegisterPasswordVerifyFunc(func(ctx context.Context, username, password string) bool {
		return username == "testuser" && password == "testpass"
	})

	// Register a getUserRoleFunc that returns an error
	RegisterGetUserRoleFunc(func(username string) ([]string, error) {
		return nil, errors.New("role lookup failed")
	})

	req := httptest.NewRequest(http.MethodGet, "/expr", nil)
	req.SetBasicAuth("testuser", "testpass")

	err := CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)

	s.Error(err)
	s.True(IsAuthenticationError(err))
	s.Contains(err.Error(), "failed to get user roles")
}

func TestCheckPrivilegeSuite(t *testing.T) {
	suite.Run(t, new(CheckPrivilegeTestSuite))
}

// CheckPrivilegeWithEnforcerTestSuite tests CheckPrivilege with Casbin enforcer
// These tests require setting up the privilege cache and enforcer
type CheckPrivilegeWithEnforcerTestSuite struct {
	suite.Suite
	ctx                    context.Context
	mockMixCoord           *mocks.MockMixCoordClient
	originalPasswordVerify func(ctx context.Context, username, password string) bool
	originalGetUserRole    func(username string) ([]string, error)
}

func (s *CheckPrivilegeWithEnforcerTestSuite) SetupSuite() {
	paramtable.Init()
}

func (s *CheckPrivilegeWithEnforcerTestSuite) SetupTest() {
	s.ctx = context.Background()
	s.originalPasswordVerify = passwordVerifyFunc
	s.originalGetUserRole = getUserRoleFunc
}

func (s *CheckPrivilegeWithEnforcerTestSuite) TearDownTest() {
	passwordVerifyFunc = s.originalPasswordVerify
	getUserRoleFunc = s.originalGetUserRole
	paramtable.Get().Reset(paramtable.Get().CommonCfg.AuthorizationEnabled.Key)
	paramtable.Get().Reset(paramtable.Get().CommonCfg.RootShouldBindRole.Key)
	privilege.CleanPrivilegeCache()
}

func (s *CheckPrivilegeWithEnforcerTestSuite) initPrivilegeCacheWithPolicies(policies []string, userRoles []string) {
	s.mockMixCoord = mocks.NewMockMixCoordClient(s.T())
	s.mockMixCoord.EXPECT().ListPolicy(mock.Anything, mock.Anything, mock.Anything).Return(&internalpb.ListPolicyResponse{
		Status:      merr.Success(),
		PolicyInfos: policies,
		UserRoles:   userRoles,
	}, nil).Maybe()

	err := privilege.InitPrivilegeCache(s.ctx, s.mockMixCoord)
	s.Require().NoError(err)
}

func (s *CheckPrivilegeWithEnforcerTestSuite) TestPermissionGrantedByRole() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().CommonCfg.RootShouldBindRole.Key, "false")
	privilege.InitPrivilegeGroups()

	// Set up policies: role1 has PrivilegeAll on Global.*
	policies := []string{
		funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeAll.String(), "default"),
	}
	userRoles := []string{
		funcutil.EncodeUserRoleCache("testuser", "role1"),
	}
	s.initPrivilegeCacheWithPolicies(policies, userRoles)

	// Register password verify function
	RegisterPasswordVerifyFunc(func(ctx context.Context, username, password string) bool {
		return username == "testuser" && password == "testpass"
	})

	// Register getUserRoleFunc to return role1
	RegisterGetUserRoleFunc(func(username string) ([]string, error) {
		if username == "testuser" {
			return []string{"role1"}, nil
		}
		return nil, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/expr", nil)
	req.SetBasicAuth("testuser", "testpass")

	err := CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)

	s.NoError(err)
}

func (s *CheckPrivilegeWithEnforcerTestSuite) TestPermissionDeniedForAllRoles() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().CommonCfg.RootShouldBindRole.Key, "false")
	privilege.InitPrivilegeGroups()

	// Set up policies: role1 has only PrivilegeLoad on Collection
	policies := []string{
		funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Collection.String(), "col1", commonpb.ObjectPrivilege_PrivilegeLoad.String(), "default"),
	}
	userRoles := []string{
		funcutil.EncodeUserRoleCache("testuser", "role1"),
	}
	s.initPrivilegeCacheWithPolicies(policies, userRoles)

	// Register password verify function
	RegisterPasswordVerifyFunc(func(ctx context.Context, username, password string) bool {
		return username == "testuser" && password == "testpass"
	})

	// Register getUserRoleFunc to return role1
	RegisterGetUserRoleFunc(func(username string) ([]string, error) {
		if username == "testuser" {
			return []string{"role1"}, nil
		}
		return nil, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/expr", nil)
	req.SetBasicAuth("testuser", "testpass")

	// Request a privilege that role1 doesn't have
	err := CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)

	s.Error(err)
	s.True(IsPermissionDeniedError(err))
	s.Contains(err.Error(), "permission denied")
}

func (s *CheckPrivilegeWithEnforcerTestSuite) TestCacheHitPermissionGranted() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().CommonCfg.RootShouldBindRole.Key, "false")
	privilege.InitPrivilegeGroups()

	// Set up policies with permission granted
	policies := []string{
		funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Global.String(), "*", commonpb.ObjectPrivilege_PrivilegeAll.String(), "default"),
	}
	userRoles := []string{
		funcutil.EncodeUserRoleCache("testuser", "role1"),
	}
	s.initPrivilegeCacheWithPolicies(policies, userRoles)

	// Register password verify function
	RegisterPasswordVerifyFunc(func(ctx context.Context, username, password string) bool {
		return username == "testuser" && password == "testpass"
	})

	// Register getUserRoleFunc
	RegisterGetUserRoleFunc(func(username string) ([]string, error) {
		if username == "testuser" {
			return []string{"role1"}, nil
		}
		return nil, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/expr", nil)
	req.SetBasicAuth("testuser", "testpass")

	// First call - cache miss, will populate cache
	err := CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)
	s.NoError(err)

	// Second call - should hit cache
	err = CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)
	s.NoError(err)
}

func (s *CheckPrivilegeWithEnforcerTestSuite) TestCacheHitPermissionDenied() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().CommonCfg.RootShouldBindRole.Key, "false")
	privilege.InitPrivilegeGroups()

	// Set up policies without the requested permission
	policies := []string{
		funcutil.PolicyForPrivilege("role1", commonpb.ObjectType_Collection.String(), "col1", commonpb.ObjectPrivilege_PrivilegeLoad.String(), "default"),
	}
	userRoles := []string{
		funcutil.EncodeUserRoleCache("testuser", "role1"),
	}
	s.initPrivilegeCacheWithPolicies(policies, userRoles)

	// Register password verify function
	RegisterPasswordVerifyFunc(func(ctx context.Context, username, password string) bool {
		return username == "testuser" && password == "testpass"
	})

	// Register getUserRoleFunc
	RegisterGetUserRoleFunc(func(username string) ([]string, error) {
		if username == "testuser" {
			return []string{"role1"}, nil
		}
		return nil, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/expr", nil)
	req.SetBasicAuth("testuser", "testpass")

	// First call - cache miss, will populate cache with denied result
	err := CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)
	s.Error(err)
	s.True(IsPermissionDeniedError(err))

	// Second call - should hit cache and still be denied
	err = CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)
	s.Error(err)
	s.True(IsPermissionDeniedError(err))
}

func (s *CheckPrivilegeWithEnforcerTestSuite) TestRootUserWithRootShouldBindRoleTrueAndAdminRole() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.AuthorizationEnabled.Key, "true")
	paramtable.Get().Save(paramtable.Get().CommonCfg.RootShouldBindRole.Key, "true")
	privilege.InitPrivilegeGroups()

	// Set up policies: root user is assigned to admin role
	// admin role bypasses privilege checks in Casbin model
	policies := []string{}
	userRoles := []string{
		funcutil.EncodeUserRoleCache(util.UserRoot, "admin"),
	}
	s.initPrivilegeCacheWithPolicies(policies, userRoles)

	// Register password verify function
	RegisterPasswordVerifyFunc(func(ctx context.Context, username, password string) bool {
		return username == util.UserRoot && password == "Milvus"
	})

	// Register getUserRoleFunc
	RegisterGetUserRoleFunc(func(username string) ([]string, error) {
		if username == util.UserRoot {
			return []string{"admin"}, nil
		}
		return nil, nil
	})

	req := httptest.NewRequest(http.MethodGet, "/expr", nil)
	req.SetBasicAuth(util.UserRoot, "Milvus")

	err := CheckPrivilege(
		s.ctx,
		req,
		commonpb.ObjectType_Global,
		commonpb.ObjectPrivilege_PrivilegeAll,
		util.AnyWord,
		util.DefaultDBName,
	)

	// Should succeed because admin role bypasses privilege checks
	s.NoError(err)
}

func TestCheckPrivilegeWithEnforcerSuite(t *testing.T) {
	suite.Run(t, new(CheckPrivilegeWithEnforcerTestSuite))
}