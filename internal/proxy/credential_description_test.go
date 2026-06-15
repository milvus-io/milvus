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

package proxy

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/crypto"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestCreateCredentialPassesDescription(t *testing.T) {
	paramtable.Init()

	description := "created user description"
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.EXPECT().
		CreateCredential(mock.Anything, mock.MatchedBy(func(cred *internalpb.CredentialInfo) bool {
			return cred.GetUsername() == "desc_user" &&
				cred.Description != nil &&
				cred.GetDescription() == description &&
				cred.GetEncryptedPassword() != "" &&
				cred.GetSha256Password() != ""
		})).
		Return(merr.Success(), nil)

	node := &Proxy{mixCoord: mixCoord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	status, err := node.CreateCredential(context.Background(), &milvuspb.CreateCredentialRequest{
		Username:    "desc_user",
		Password:    crypto.Base64Encode("password"),
		Description: &description,
	})

	require.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
}

func TestCreateCredentialRejectsOverLimitDescription(t *testing.T) {
	paramtable.Init()

	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.On("CreateCredential", mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()
	node := &Proxy{mixCoord: mixCoord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	description := strings.Repeat("x", 1025)
	status, err := node.CreateCredential(context.Background(), &milvuspb.CreateCredentialRequest{
		Username:    "desc_user",
		Password:    crypto.Base64Encode("password"),
		Description: &description,
	})

	require.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, status.GetErrorCode())
	mixCoord.AssertNotCalled(t, "CreateCredential", mock.Anything, mock.Anything)
}

func TestUpdateCredentialDescriptionOnlySkipsPasswordVerification(t *testing.T) {
	paramtable.Init()

	description := "updated user description"
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.EXPECT().
		UpdateCredential(mock.Anything, mock.MatchedBy(func(cred *internalpb.CredentialInfo) bool {
			return cred.GetUsername() == "desc_user" &&
				cred.Description != nil &&
				cred.GetDescription() == description &&
				cred.GetEncryptedPassword() == "" &&
				cred.GetSha256Password() == ""
		})).
		Return(merr.Success(), nil)

	node := &Proxy{mixCoord: mixCoord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	status, err := node.UpdateCredential(context.Background(), &milvuspb.UpdateCredentialRequest{
		Username:    "desc_user",
		Description: &description,
	})

	require.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
}

func TestUpdateCredentialClearsDescriptionOnly(t *testing.T) {
	paramtable.Init()

	description := ""
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.EXPECT().
		UpdateCredential(mock.Anything, mock.MatchedBy(func(cred *internalpb.CredentialInfo) bool {
			return cred.GetUsername() == "desc_user" &&
				cred.Description != nil &&
				cred.GetDescription() == description &&
				cred.GetEncryptedPassword() == "" &&
				cred.GetSha256Password() == ""
		})).
		Return(merr.Success(), nil)

	node := &Proxy{mixCoord: mixCoord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	status, err := node.UpdateCredential(context.Background(), &milvuspb.UpdateCredentialRequest{
		Username:    "desc_user",
		Description: &description,
	})

	require.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
}

func TestUpdateCredentialRejectsEmptyUpdate(t *testing.T) {
	paramtable.Init()

	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.On("UpdateCredential", mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()
	node := &Proxy{mixCoord: mixCoord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	status, err := node.UpdateCredential(context.Background(), &milvuspb.UpdateCredentialRequest{
		Username: "desc_user",
	})

	require.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, status.GetErrorCode())
	assert.Contains(t, status.GetReason(), "must update either password or description")
	mixCoord.AssertNotCalled(t, "UpdateCredential", mock.Anything, mock.Anything)
}

func TestUpdateCredentialPasswordAndDescription(t *testing.T) {
	paramtable.Init()
	require.NoError(t, paramtable.Get().Save(Params.CommonCfg.SuperUsers.Key, "root"))
	defer paramtable.Get().Reset(Params.CommonCfg.SuperUsers.Key)

	description := "updated user description"
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.EXPECT().
		UpdateCredential(mock.Anything, mock.MatchedBy(func(cred *internalpb.CredentialInfo) bool {
			return cred.GetUsername() == "desc_user" &&
				cred.Description != nil &&
				cred.GetDescription() == description &&
				cred.GetEncryptedPassword() != "" &&
				cred.GetSha256Password() == crypto.SHA256("new_password", "desc_user")
		})).
		Return(merr.Success(), nil)

	node := &Proxy{mixCoord: mixCoord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	status, err := node.UpdateCredential(GetContext(context.Background(), "root:password"), &milvuspb.UpdateCredentialRequest{
		Username:    "desc_user",
		OldPassword: crypto.Base64Encode("old_password"),
		NewPassword: crypto.Base64Encode("new_password"),
		Description: &description,
	})

	require.NoError(t, err)
	assert.Equal(t, commonpb.ErrorCode_Success, status.GetErrorCode())
}

func TestUpdateCredentialRejectsOverLimitDescription(t *testing.T) {
	paramtable.Init()
	require.NoError(t, paramtable.Get().Save(Params.CommonCfg.SuperUsers.Key, "root"))
	defer paramtable.Get().Reset(Params.CommonCfg.SuperUsers.Key)

	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.On("UpdateCredential", mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()
	node := &Proxy{mixCoord: mixCoord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)

	description := strings.Repeat("x", 1025)
	status, err := node.UpdateCredential(GetContext(context.Background(), "root:password"), &milvuspb.UpdateCredentialRequest{
		Username:    "desc_user",
		OldPassword: crypto.Base64Encode("old_password"),
		NewPassword: crypto.Base64Encode("new_password"),
		Description: &description,
	})

	require.NoError(t, err)
	assert.NotEqual(t, commonpb.ErrorCode_Success, status.GetErrorCode())
	mixCoord.AssertNotCalled(t, "UpdateCredential", mock.Anything, mock.Anything)
}

func TestUpdateCredentialPasswordValidationErrors(t *testing.T) {
	paramtable.Init()

	tests := []struct {
		name        string
		oldPassword string
		newPassword string
		reason      string
	}{
		{
			name:        "invalid old password base64",
			oldPassword: "not-base64!!",
			newPassword: crypto.Base64Encode("new_password"),
			reason:      "decode old password failed",
		},
		{
			name:        "invalid new password base64",
			oldPassword: crypto.Base64Encode("old_password"),
			newPassword: "not-base64!!",
			reason:      "decode password failed",
		},
		{
			name:        "invalid new password length",
			oldPassword: crypto.Base64Encode("old_password"),
			newPassword: crypto.Base64Encode("short"),
			reason:      "invalid password length",
		},
		{
			name:        "old password verification failed",
			oldPassword: crypto.Base64Encode("old_password"),
			newPassword: crypto.Base64Encode("new_password"),
			reason:      "old password not correct",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mixCoord := mocks.NewMockMixCoordClient(t)
			mixCoord.On("UpdateCredential", mock.Anything, mock.Anything).Return(merr.Success(), nil).Maybe()
			node := &Proxy{mixCoord: mixCoord}
			node.UpdateStateCode(commonpb.StateCode_Healthy)

			status, err := node.UpdateCredential(context.Background(), &milvuspb.UpdateCredentialRequest{
				Username:    "desc_user",
				OldPassword: test.oldPassword,
				NewPassword: test.newPassword,
			})

			require.NoError(t, err)
			assert.NotEqual(t, commonpb.ErrorCode_Success, status.GetErrorCode())
			assert.Contains(t, status.GetReason(), test.reason)
			mixCoord.AssertNotCalled(t, "UpdateCredential", mock.Anything, mock.Anything)
		})
	}
}
