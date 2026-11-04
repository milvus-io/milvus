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

package crypto

import (
	"context"
	"crypto/pbkdf2"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const passwordHashAlgorithmKey = "common.security.passwordHashAlgorithm"

func usePasswordHashAlgorithm(t *testing.T, algorithm string) {
	t.Helper()
	require.NoError(t, paramtable.Get().Save(passwordHashAlgorithmKey, algorithm))
	t.Cleanup(func() { paramtable.Get().Reset(passwordHashAlgorithmKey) })
}

func capturePasswordLogs(t *testing.T) *mlog.TestSink {
	t.Helper()
	return mlog.CaptureGlobalLogs(t, &mlog.Config{
		Level:             "debug",
		Format:            "text",
		DisableCaller:     true,
		DisableTimestamp:  true,
		DisableStacktrace: true,
	})
}

// encodePBKDF2SHA512 builds a stored hash with an independent derivation.
func encodePBKDF2SHA512(t *testing.T, password string, salt []byte, iterations int) string {
	t.Helper()
	key, err := pbkdf2.Key(sha512.New, password, salt, iterations, pbkdf2SHA512KeyBytes)
	require.NoError(t, err)
	return fmt.Sprintf("%si=%d$%s$%s", pbkdf2SHA512Prefix, iterations,
		pbkdf2Encoding.EncodeToString(salt), pbkdf2Encoding.EncodeToString(key))
}

// Expected values were produced with Python's hashlib.pbkdf2_hmac, which runs on
// OpenSSL, so they are independent of Go's implementation.
func TestPBKDF2SHA512KnownAnswers(t *testing.T) {
	for _, test := range []struct {
		password, salt string
		iterations     int
		keyBytes       int
		expected       string
	}{
		{
			"password", "salt", 1, 64,
			"867f70cf1ade02cff3752599a3a53dc4af34c7a669815ae5d513554e1c8cf252c02d470a285a0501bad999bfe943c08f050235d7d68b1da55e63f73b60a57fce",
		},
		{
			"password", "salt", 2, 64,
			"e1d9c16aa681708a45f5c7c4e215ceb66e011a2e9f0040713f18aefdb866d53cf76cab2868a39b9f7840edce4fef5a82be67335c77a6068e04112754f27ccf4e",
		},
		{
			"password", "salt", 4096, 64,
			"d197b1b33db0143e018b12f3d1d1479e6cdebdcc97c5c0f87f6902e072f457b5143f30602641b3d55cd335988cb36b84376060ecd532e039b742a239434af2d5",
		},
		{
			"passwordPASSWORDpassword", "saltSALTsaltSALTsaltSALTsaltSALTsalt", 4096, 64,
			"8c0511f4c6e597c6ac6315d8f0362e225f3c501495ba23b868c005174dc4ee71115b59f9e60cd9532fa33e0f75aefe30225c583a186cd82bd4daea9724a3d3b8",
		},
		{"pass\x00word", "sa\x00lt", 4096, 16, "9d9e9c4cd21fe4be24d5b8244c759665"},
		// Longer than one SHA-512 block, so the block index matters.
		{
			"password", "salt", 1000, 100,
			"afe6c5530785b6cc6b1c6453384731bd5ee432ee549fd42fb6695779ad8a1c5bf59de69c48f774efc4007d5298f9033c0241d5ab69305e7b64eceeb8d834cfec6afdec3c1c23982a121f2d4be008889378a49a0dfb104f0d2856e38f44271cdaf6de4341",
		},
	} {
		derived := pbkdf2SHA512([]byte(test.password), []byte(test.salt), test.iterations, test.keyBytes)
		assert.Equal(t, test.expected, hex.EncodeToString(derived), "%q %q %d", test.password, test.salt, test.iterations)
	}
}

func TestPBKDF2SHA512MatchesStandardLibrary(t *testing.T) {
	for _, password := range []string{"", "x", "a-password", strings.Repeat("long", 100)} {
		for _, keyBytes := range []int{1, 20, 63, 64, 65, 128, 200} {
			for _, iterations := range []int{1, 2, 3, 1000} {
				salt := []byte("0123456789abcdef")
				expected, err := pbkdf2.Key(sha512.New, password, salt, iterations, keyBytes)
				require.NoError(t, err)
				assert.Equal(t, expected, pbkdf2SHA512([]byte(password), salt, iterations, keyBytes))
			}
		}
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

func TestPasswordEncryptPBKDF2SHA512(t *testing.T) {
	usePasswordHashAlgorithm(t, PasswordHashPBKDF2SHA512)
	const password = "a-business-user-password"

	hash, err := PasswordEncrypt(password)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(hash, "$pbkdf2-sha512$i=210000$"), hash)

	iterations, salt, key, reason := parsePBKDF2SHA512(hash)
	require.Empty(t, reason)
	assert.Equal(t, 210000, iterations)
	assert.Len(t, salt, 16)
	assert.Len(t, key, 64)
	expected, err := pbkdf2.Key(sha512.New, password, salt, iterations, len(key))
	require.NoError(t, err)
	assert.Equal(t, expected, key)

	require.NoError(t, VerifyStoredPassword(context.Background(), hash, password))
	mismatch := VerifyStoredPassword(context.Background(), hash, "wrong-password")
	require.ErrorIs(t, mismatch, merr.ErrPrivilegeNotAuthenticated)
	require.NotErrorIs(t, mismatch, merr.ErrServiceInternal)

	other, err := PasswordEncrypt(password)
	require.NoError(t, err)
	assert.NotEqual(t, hash, other, "each hash must get its own salt")
}

func TestPasswordEncryptPBKDF2SHA512KeepsPasswordLimit(t *testing.T) {
	usePasswordHashAlgorithm(t, PasswordHashPBKDF2SHA512)
	longest := strings.Repeat("x", BcryptMaxPasswordBytes)

	hash, err := PasswordEncrypt(longest)
	require.NoError(t, err)
	require.NoError(t, VerifyStoredPassword(context.Background(), hash, longest))
	require.ErrorIs(t, VerifyStoredPassword(context.Background(), hash, longest+"x"), merr.ErrPrivilegeNotAuthenticated)

	_, err = PasswordEncrypt(longest + "x")
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestPasswordEncryptRejectsUnknownAlgorithm(t *testing.T) {
	usePasswordHashAlgorithm(t, "pbkdf2-sha256")
	hash, err := PasswordEncrypt("a-password")
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	assert.Empty(t, hash)
}

// The algorithm is read from the hash, so a PBKDF2 credential must verify on a
// node whose configuration differs, which is the CDC secondary and rollback case.
func TestVerifyStoredPasswordFollowsHashFormat(t *testing.T) {
	salt := []byte("0123456789abcdef")
	// Produced with Python's hashlib.pbkdf2_hmac('sha512', ...) at the default
	// iteration count, pinning the stored format as well as the derivation.
	stored := fmt.Sprintf("$pbkdf2-sha512$i=210000$%s$%s", pbkdf2Encoding.EncodeToString(salt),
		pbkdf2Encoding.EncodeToString(mustDecodeHex(t, "15e8993cac9b1ec07aa1568bb0d294b93ed9e3a723886390563d8b46734329889698b8e841324244cef89a4621b9d9269df6fa89254e512ed169a7d3766bc324")))

	for _, algorithm := range []string{PasswordHashBcrypt, PasswordHashPBKDF2SHA512} {
		t.Run(algorithm, func(t *testing.T) {
			usePasswordHashAlgorithm(t, algorithm)
			require.NoError(t, VerifyStoredPassword(context.Background(), stored, "a-password"))
			require.ErrorIs(t, VerifyStoredPassword(context.Background(), stored, "wrong"), merr.ErrPrivilegeNotAuthenticated)
		})
	}
}

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

// The bcrypt limit is a property of bcrypt hashes and must not reach this format.
func TestVerifyStoredPasswordPBKDF2SHA512HasNoBcryptCap(t *testing.T) {
	long := strings.Repeat("x", BcryptMaxPasswordBytes+1)
	stored := encodePBKDF2SHA512(t, long, []byte("0123456789abcdef"), pbkdf2SHA512MinIterations)
	require.NoError(t, VerifyStoredPassword(context.Background(), stored, long))
}

func TestVerifyStoredPasswordMalformedPBKDF2SHA512(t *testing.T) {
	const password = "a-password"
	salt := []byte("0123456789abcdef")
	valid := encodePBKDF2SHA512(t, password, salt, pbkdf2SHA512MinIterations)
	require.NoError(t, VerifyStoredPassword(context.Background(), valid, password))

	fields := strings.Split(valid, "$")
	encodedSalt, encodedKey := fields[3], fields[4]
	build := func(iterations, s, k string) string {
		return pbkdf2SHA512Prefix + iterations + "$" + s + "$" + k
	}
	tests := map[string]string{
		"prefix only":        pbkdf2SHA512Prefix,
		"missing key":        pbkdf2SHA512Prefix + "i=1000$" + encodedSalt,
		"extra field":        valid + "$extra",
		"no iteration label": build("1000", encodedSalt, encodedKey),
		"empty iterations":   build("i=", encodedSalt, encodedKey),
		"signed iterations":  build("i=+1000", encodedSalt, encodedKey),
		"padded iterations":  build("i=01000", encodedSalt, encodedKey),
		"text iterations":    build("i=SENTINELabc", encodedSalt, encodedKey),
		"overflow iterations": build("i=99999999999999999999999",
			encodedSalt, encodedKey),
		"too few iterations":  build("i=999", encodedSalt, encodedKey),
		"too many iterations": build("i=10000001", encodedSalt, encodedKey),
		"empty salt":          build("i=1000", "", encodedKey),
		"short salt":          build("i=1000", pbkdf2Encoding.EncodeToString(salt[:15]), encodedKey),
		"salt not base64":     build("i=1000", "not*base64", encodedKey),
		"salt padded":         build("i=1000", encodedSalt+"==", encodedKey),
		"empty key":           build("i=1000", encodedSalt, ""),
		"short key":           build("i=1000", encodedSalt, encodedKey[:len(encodedKey)-4]),
		"key not base64":      build("i=1000", encodedSalt, "not*base64"),
	}
	for name, stored := range tests {
		t.Run(name, func(t *testing.T) {
			err := VerifyStoredPassword(context.Background(), stored, password)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.NotErrorIs(t, err, merr.ErrPrivilegeNotAuthenticated)
			assert.NotContains(t, err.Error(), "SENTINEL")
			assert.NotContains(t, err.Error(), encodedKey)
		})
	}

	// Only this exact prefix selects the new format; anything else keeps the
	// behavior bcrypt gives it.
	for _, stored := range []string{"$pbkdf2-sha256$i=1000$c2FsdA$a2V5", "pbkdf2-sha512$i=1000$c2FsdA$a2V5"} {
		require.ErrorIs(t, VerifyStoredPassword(context.Background(), stored, password), merr.ErrServiceInternal)
	}
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

func BenchmarkVerifyStoredPasswordPBKDF2SHA512(b *testing.B) {
	key := pbkdf2SHA512([]byte("benchmark-password"), []byte("0123456789abcdef"), pbkdf2SHA512Iterations, pbkdf2SHA512KeyBytes)
	stored := fmt.Sprintf("%si=%d$%s$%s", pbkdf2SHA512Prefix, pbkdf2SHA512Iterations,
		pbkdf2Encoding.EncodeToString([]byte("0123456789abcdef")), pbkdf2Encoding.EncodeToString(key))
	b.ResetTimer()
	for b.Loop() {
		if err := VerifyStoredPassword(context.Background(), stored, "benchmark-password"); err != nil {
			b.Fatal(err)
		}
	}
}
