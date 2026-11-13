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

//go:build !nobcrypt

package crypto

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestPasswordEncryptUsesDefaultBcryptCost(t *testing.T) {
	encryptedPassword, err := PasswordEncrypt("test-password")
	require.NoError(t, err)

	cost, err := bcrypt.Cost([]byte(encryptedPassword))
	require.NoError(t, err)
	assert.Equal(t, bcrypt.DefaultCost, cost)
	assert.GreaterOrEqual(t, cost, 10)
}

func TestPasswordEncryptVerification(t *testing.T) {
	password := "test-password"
	encryptedPassword, err := PasswordEncrypt(password)
	require.NoError(t, err)

	assert.NoError(t, bcrypt.CompareHashAndPassword([]byte(encryptedPassword), []byte(password)))
	assert.Error(t, bcrypt.CompareHashAndPassword([]byte(encryptedPassword), []byte("wrong-password")))
}

func TestPasswordEncryptSupportsLegacyBcryptHash(t *testing.T) {
	const legacyHash = "$2a$04$FDqkwIegT7SfTGRcPyQjk.reG7poJ99Two1jsl2euNBy0Z.e1DiR."

	cost, err := bcrypt.Cost([]byte(legacyHash))
	require.NoError(t, err)
	assert.Equal(t, bcrypt.MinCost, cost)
	assert.NoError(t, bcrypt.CompareHashAndPassword([]byte(legacyHash), []byte("legacy-password")))
}

func BenchmarkPasswordHashGeneration(b *testing.B) {
	benchmarkBcryptCosts(b, func(b *testing.B, cost int) {
		for i := 0; i < b.N; i++ {
			if _, err := bcrypt.GenerateFromPassword([]byte("benchmark-password"), cost); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPasswordHashComparison(b *testing.B) {
	benchmarkBcryptCosts(b, func(b *testing.B, cost int) {
		hash, err := bcrypt.GenerateFromPassword([]byte("benchmark-password"), cost)
		require.NoError(b, err)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if err := bcrypt.CompareHashAndPassword(hash, []byte("benchmark-password")); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkPasswordHashComparisonParallel(b *testing.B) {
	benchmarkBcryptCosts(b, func(b *testing.B, cost int) {
		hash, err := bcrypt.GenerateFromPassword([]byte("benchmark-password"), cost)
		require.NoError(b, err)
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := bcrypt.CompareHashAndPassword(hash, []byte("benchmark-password")); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}

func benchmarkBcryptCosts(b *testing.B, benchmark func(b *testing.B, cost int)) {
	b.Helper()
	for _, cost := range []int{bcrypt.MinCost, bcrypt.DefaultCost} {
		b.Run(fmt.Sprintf("cost_%d", cost), func(b *testing.B) {
			b.ReportAllocs()
			benchmark(b, cost)
		})
	}
}

func TestPasswordHashAlgorithmDefaultsToBcrypt(t *testing.T) {
	assert.Equal(t, PasswordHashBcrypt, paramtable.Get().CommonCfg.PasswordHashAlgorithm.GetValue())

	hash, err := PasswordEncrypt("a-password")
	require.NoError(t, err)
	cost, err := bcrypt.Cost([]byte(hash))
	require.NoError(t, err)
	assert.Equal(t, bcrypt.DefaultCost, cost)

	logs := capturePasswordLogs(t)
	require.NoError(t, VerifyStoredPassword(context.Background(), hash, "a-password"))
	assert.NotContains(t, logs.String(), "bcrypt password hash")
}

// The warning's limiter is per call site and process-wide, so this must stay the
// only test that verifies a matching bcrypt hash with pbkdf2-sha512 configured.
func TestVerifyStoredPasswordWarnsOnLegacyBcrypt(t *testing.T) {
	const password = "PASSWORD_SENTINEL_DO_NOT_LOG"
	legacy, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	require.NoError(t, err)
	stored := encodePBKDF2SHA512(t, password, []byte("0123456789abcdef"), pbkdf2SHA512MinIterations)

	usePasswordHashAlgorithm(t, PasswordHashPBKDF2SHA512)
	logs := capturePasswordLogs(t)

	require.ErrorIs(t, VerifyStoredPassword(context.Background(), string(legacy), "wrong"), merr.ErrPrivilegeNotAuthenticated)
	require.NoError(t, VerifyStoredPassword(context.Background(), stored, password))
	assert.NotContains(t, logs.String(), "bcrypt password hash", "only a verified bcrypt hash needs rotating")

	for range 3 {
		require.NoError(t, VerifyStoredPassword(context.Background(), string(legacy), password))
	}
	output := logs.String()
	assert.Equal(t, 1, strings.Count(output, "bcrypt password hash"), "the warning must be rate limited")
	assert.NotContains(t, output, password)
	assert.NotContains(t, output, string(legacy))
}
