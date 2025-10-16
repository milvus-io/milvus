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

package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestDDLCallbacksRBACCredential(t *testing.T) {
	core := initStreamingSystemAndCore(t)
	testUserName := "user" + funcutil.RandomString(10)

	// Delete a not existed credential should succeed
	status, err := core.DeleteCredential(context.Background(), &milvuspb.DeleteCredentialRequest{
		Username: testUserName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))

	// Update a not existed credential should return error.
	status, err = core.UpdateCredential(context.Background(), &internalpb.CredentialInfo{
		Username:          testUserName,
		EncryptedPassword: "123456",
	})
	require.Error(t, merr.CheckRPCCall(status, err))

	// Create a new credential.
	status, err = core.CreateCredential(context.Background(), &internalpb.CredentialInfo{
		Username:          testUserName,
		EncryptedPassword: "123456",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	getCredentialResp, err := core.GetCredential(context.Background(), &rootcoordpb.GetCredentialRequest{
		Username: testUserName,
	})
	require.NoError(t, merr.CheckRPCCall(getCredentialResp.Status, err))
	assert.Equal(t, "123456", getCredentialResp.Password)

	// Create a new credential with same username should return error.
	status, err = core.CreateCredential(context.Background(), &internalpb.CredentialInfo{
		Username:          testUserName,
		EncryptedPassword: "123456",
	})
	require.Error(t, merr.CheckRPCCall(status, err))

	// Update the created credential.
	status, err = core.UpdateCredential(context.Background(), &internalpb.CredentialInfo{
		Username:          testUserName,
		EncryptedPassword: "1234567",
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	getCredentialResp, err = core.GetCredential(context.Background(), &rootcoordpb.GetCredentialRequest{
		Username: testUserName,
	})
	require.NoError(t, merr.CheckRPCCall(getCredentialResp.Status, err))
	assert.Equal(t, "1234567", getCredentialResp.Password)

	// Delete the created credential.
	status, err = core.DeleteCredential(context.Background(), &milvuspb.DeleteCredentialRequest{
		Username: testUserName,
	})
	require.NoError(t, merr.CheckRPCCall(status, err))
	getCredentialResp, err = core.GetCredential(context.Background(), &rootcoordpb.GetCredentialRequest{
		Username: testUserName,
	})
	require.Error(t, merr.CheckRPCCall(getCredentialResp.Status, err))
}
