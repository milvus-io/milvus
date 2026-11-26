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
//go:build nobcrypt

package rootcoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/crypto"
)

func captureNoBcryptLogs(t *testing.T) *mlog.TestSink {
	t.Helper()
	return mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug", Format: "text", DisableCaller: true, DisableTimestamp: true, DisableStacktrace: true})
}

// A nobcrypt build names the users it cannot verify at startup, and flags root.
func TestInitCredentialReportsBcryptUsersWithoutBcrypt(t *testing.T) {
	pbkdf2Hash, err := crypto.PasswordEncrypt("a-password")
	require.NoError(t, err)
	require.True(t, crypto.IsPBKDF2SHA512Hash(pbkdf2Hash))
	const bcryptHash = "$2a$10$abcdefghijklmnopqrstuuJ6U3Fv0Mi9Lra2Jq5oJxk7xS0wGx1pO"
	creds := map[string]string{util.UserRoot: bcryptHash, "alice": bcryptHash, "bob": pbkdf2Hash}

	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().ListCredentials(mock.Anything).Return([]string{util.UserRoot, "alice", "bob"}, nil)
	catalog.EXPECT().GetCredential(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, username string) (*model.Credential, error) {
		return &model.Credential{Username: username, EncryptedPassword: creds[username]}, nil
	})
	logs := captureNoBcryptLogs(t)

	mt := &MetaTable{catalog: catalog}
	require.NoError(t, mt.InitCredential(context.Background()))
	out := logs.String()
	assert.Contains(t, out, "cannot log in on this build until their passwords are reset")
	assert.Contains(t, out, "alice")
	assert.NotContains(t, out, "bob")
	assert.Contains(t, out, "the root password hash uses bcrypt")
	assert.NotContains(t, out, bcryptHash)
}

// With only PBKDF2 hashes there is nothing to report, and a missing root is
// still created, with a PBKDF2 hash.
func TestInitCredentialCreatesPBKDF2RootWithoutBcrypt(t *testing.T) {
	catalog := mocks.NewRootCoordCatalog(t)
	catalog.EXPECT().ListCredentials(mock.Anything).Return(nil, nil)
	catalog.EXPECT().GetCredential(mock.Anything, util.UserRoot).Return(nil, nil)
	var stored *model.Credential
	catalog.EXPECT().AlterCredential(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, c *model.Credential) error {
		stored = c
		return nil
	})
	logs := captureNoBcryptLogs(t)

	mt := &MetaTable{catalog: catalog}
	require.NoError(t, mt.InitCredential(context.Background()))
	require.NotNil(t, stored)
	assert.True(t, crypto.IsPBKDF2SHA512Hash(stored.EncryptedPassword))
	assert.NotContains(t, logs.String(), "cannot log in")
}
