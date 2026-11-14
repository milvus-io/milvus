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

package crypto

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// A bcrypt hash in the stored format. A nobcrypt build never verifies it, so
// any well-formed value works.
const storedBcryptHash = "$2a$04$abcdefghijklmnopqrstuuJ6U3Fv0Mi9Lra2Jq5oJxk7xS0wGx1pO"

func TestNoBcryptDefaultsToPBKDF2SHA512(t *testing.T) {
	paramtable.Get().Reset(passwordHashAlgorithmKey)
	assert.False(t, BcryptAvailable)
	assert.Equal(t, PasswordHashPBKDF2SHA512, paramtable.Get().CommonCfg.PasswordHashAlgorithm.GetValue())
	hash, err := PasswordEncrypt("a-password")
	require.NoError(t, err)
	assert.True(t, IsPBKDF2SHA512Hash(hash))
	require.NoError(t, VerifyStoredPassword(context.Background(), hash, "a-password"))
	require.ErrorIs(t, VerifyStoredPassword(context.Background(), hash, "wrong"), merr.ErrPrivilegeNotAuthenticated)
}

// configs/milvus.yaml sets bcrypt explicitly, so a nobcrypt build must still
// start with it and hash with pbkdf2-sha512.
func TestNoBcryptMapsConfiguredBcryptToPBKDF2SHA512(t *testing.T) {
	usePasswordHashAlgorithm(t, PasswordHashBcrypt)
	assert.Equal(t, PasswordHashPBKDF2SHA512, paramtable.Get().CommonCfg.PasswordHashAlgorithm.GetValue())
	hash, err := PasswordEncrypt("a-password")
	require.NoError(t, err)
	assert.True(t, IsPBKDF2SHA512Hash(hash))
}

func TestNoBcryptUnknownAlgorithmStillRejected(t *testing.T) {
	usePasswordHashAlgorithm(t, "pbkdf2-sha256")
	_, err := PasswordEncrypt("a-password")
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestNoBcryptHashHelperRefuses(t *testing.T) {
	hash, err := bcryptHash("a-password")
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.Empty(t, hash)
}

// A stored bcrypt hash is a credential-store problem in this build, not a wrong
// password, whatever the candidate is, including one over the bcrypt limit.
func TestNoBcryptStoredBcryptHashIsSystemError(t *testing.T) {
	for _, candidate := range []string{"the-right-password", "wrong", strings.Repeat("x", BcryptMaxPasswordBytes+1)} {
		err := VerifyStoredPassword(context.Background(), storedBcryptHash, candidate)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		assert.NotErrorIs(t, err, merr.ErrPrivilegeNotAuthenticated)
		assert.Contains(t, err.Error(), "nobcrypt")
	}
}

func TestNoBcryptKeepsPasswordLimit(t *testing.T) {
	_, err := PasswordEncrypt(strings.Repeat("x", BcryptMaxPasswordBytes+1))
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}
