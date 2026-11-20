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

package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestFixtureBackupImportsReceivedSourceKey(t *testing.T) {
	var sender fixtureCipher
	require.NoError(t, sender.Init(map[string]string{"cipher.milvusRoleName": "mixcoord"}))
	provider, ok := any(&sender).(interface{ Backup(int64) (string, error) })
	require.True(t, ok, "fixture must implement the host's backup interface")
	backup, err := provider.Backup(17)
	require.NoError(t, err)
	decoded, err := base64.StdEncoding.DecodeString(backup)
	require.NoError(t, err)
	var payload struct {
		EZID int64  `json:"ez_id"`
		Key  []byte `json:"key"`
	}
	require.NoError(t, json.Unmarshal(decoded, &payload))
	require.Equal(t, int64(17), payload.EZID)
	require.Equal(t, sender.GetUnsafeKey(17, 0), payload.Key)

	var receiver fixtureCipher
	require.NoError(t, receiver.Init(map[string]string{"cipher.milvusRoleName": "datanode"}))
	require.Empty(t, receiver.GetUnsafeKey(17, 0))
	require.NoError(t, receiver.Init(map[string]string{"cipher.ez.import": backup}))
	require.Equal(t, payload.Key, receiver.GetUnsafeKey(17, 0))
	require.Empty(t, receiver.GetUnsafeKey(18, 0))
	payload.Key[0] ^= 1
	invalid, err := json.Marshal(payload)
	require.NoError(t, err)
	require.Error(t, receiver.Init(map[string]string{"cipher.ez.import": base64.StdEncoding.EncodeToString(invalid)}))
}

func TestFixtureDataNodeRequiresReceivedKey(t *testing.T) {
	var receiver fixtureCipher
	require.NoError(t, receiver.Init(map[string]string{"cipher.milvusRoleName": "datanode"}))
	require.Empty(t, receiver.GetUnsafeKey(17, 23))
	_, _, err := receiver.GetEncryptor(17, 23)
	require.Error(t, err)

	var sender fixtureCipher
	require.NoError(t, sender.Init(map[string]string{"cipher.milvusRoleName": "mixcoord"}))
	encryptor, edek, err := sender.GetEncryptor(17, 23)
	require.NoError(t, err)
	_, err = receiver.GetDecryptor(17, 23, edek)
	require.Error(t, err)

	require.NoError(t, receiver.Init(map[string]string{
		createEZKey: "17",
		unsafeEZK:   base64.StdEncoding.EncodeToString(sender.GetUnsafeKey(17, 23)),
	}))
	require.Equal(t, sender.GetUnsafeKey(17, 23), receiver.GetUnsafeKey(17, 23))
	decryptor, err := receiver.GetDecryptor(17, 23, edek)
	require.NoError(t, err)
	ciphertext, err := encryptor.Encrypt([]byte("received task key"))
	require.NoError(t, err)
	plain, err := decryptor.Decrypt(ciphertext)
	require.NoError(t, err)
	require.Equal(t, []byte("received task key"), plain)

	var restarted fixtureCipher
	require.NoError(t, restarted.Init(map[string]string{"cipher.milvusRoleName": "datanode"}))
	require.Empty(t, restarted.GetUnsafeKey(17, 23))
}

func TestFixtureWriterKeyFaultRecovery(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("MILVUS_CMEK_FIXTURE_CONTROL", dir)
	var writer, otherRole fixtureCipher
	require.NoError(t, writer.Init(map[string]string{"cipher.milvusRoleName": "streamingnode"}))
	require.NoError(t, otherRole.Init(map[string]string{"cipher.milvusRoleName": "proxy"}))
	key := writer.GetUnsafeKey(17, 23)
	require.NotEmpty(t, key)

	// Only the writer-key lookup is unavailable. WAL encryption/decryption and
	// other roles, collections and zones retain their own key access.
	fault := []byte(`{"role":"streamingnode","ez_id":17,"collection_id":23,"operation":"unsafe"}`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "fault.json"), fault, 0o600))
	for range 2 {
		require.Empty(t, writer.GetUnsafeKey(17, 23))
	}
	require.Equal(t, key, otherRole.GetUnsafeKey(17, 23))
	require.NotEmpty(t, writer.GetUnsafeKey(18, 23))
	require.NotEmpty(t, writer.GetUnsafeKey(17, 24))
	encryptor, edek, err := writer.GetEncryptor(17, 23)
	require.NoError(t, err)
	require.NotNil(t, encryptor)
	_, err = writer.GetDecryptor(17, 23, edek)
	require.NoError(t, err)

	require.NoError(t, os.Remove(filepath.Join(dir, "fault.json")))
	require.Equal(t, key, writer.GetUnsafeKey(17, 23))
	records, err := filepath.Glob(filepath.Join(dir, "go-*.jsonl"))
	require.NoError(t, err)
	require.NotEmpty(t, records)
	contents, err := os.ReadFile(records[0])
	require.NoError(t, err)
	require.Contains(t, string(contents), `"outcome":"unavailable"`)
	require.NotContains(t, string(contents), base64.StdEncoding.EncodeToString(key))
}

func TestFixtureCipherDerivesDistinctEZKeysWithoutState(t *testing.T) {
	cipher := fixtureCipher{}

	first := cipher.GetUnsafeKey(17, 23)
	second := cipher.GetUnsafeKey(18, 23)

	require.Len(t, first, sha256.Size)
	require.Len(t, second, sha256.Size)
	require.NotEqual(t, first, second)
}

func TestFixtureCipherDerivesSameEZKeyAcrossInstances(t *testing.T) {
	first := fixtureCipher{}
	second := fixtureCipher{}

	require.Equal(t, first.GetUnsafeKey(17, 23), second.GetUnsafeKey(17, 23))
}

func TestFixtureCipherRejectsUnexpectedRootKey(t *testing.T) {
	cipher := fixtureCipher{}

	err := cipher.Init(map[string]string{
		createEZKey: "17",
		kmsKeyARN:   "unexpected-root-key",
	})
	require.ErrorContains(t, err, "unexpected root key")
}

func TestFixtureCipherValidatesImportedEZKey(t *testing.T) {
	cipher := fixtureCipher{}
	encoded := base64.StdEncoding.EncodeToString(deriveEZKey(17))

	require.NoError(t, cipher.Init(map[string]string{
		createEZKey: "17",
		unsafeEZK:   "17:" + encoded,
	}))

	wrongKey := base64.StdEncoding.EncodeToString(deriveEZKey(18))
	err := cipher.Init(map[string]string{
		createEZKey: "17",
		unsafeEZK:   "17:" + wrongKey,
	})
	require.ErrorContains(t, err, "unexpected EZ key")
}

func TestFixtureCipherUsesFreshDEKAcrossInstances(t *testing.T) {
	first := fixtureCipher{}
	second := fixtureCipher{}

	firstEncryptor, firstEDEK, err := first.GetEncryptor(17, 23)
	require.NoError(t, err)
	secondEncryptor, secondEDEK, err := second.GetEncryptor(17, 23)
	require.NoError(t, err)

	require.NotEqual(t, firstEDEK, secondEDEK)
	require.NotEqual(t, firstEncryptor.(*fixtureEncryptor).key, secondEncryptor.(*fixtureEncryptor).key)
}

func TestFixtureDataKeyMatchesCppFixture(t *testing.T) {
	nonce, err := hex.DecodeString("000102030405060708090a0b0c0d0e0f")
	require.NoError(t, err)
	ezk := deriveEZKey(17)
	dek := deriveDataKey(ezk, nonce, 17, 23)
	tag := deriveEDEKTag(ezk, nonce, 17, 23)

	require.Equal(t, "aae4017d22a80fd5beb5489b8db3bf7a2dce66c39e8e21f20bc77433a054d387", hex.EncodeToString(ezk))
	require.Equal(t, "bcdbeac6f09eb42ea81144b43a0ccc75cba5455134dda7ff6dcc5790da012327", hex.EncodeToString(dek))
	require.Equal(t, "d9cb9e34022d55b0e61b0c37e4561ad255400a285b27f74cbe31c2574dd10e4d", hex.EncodeToString(tag))
}

func TestFixtureCipherEdekSurvivesJSONRoundTrip(t *testing.T) {
	encryptingCipher := fixtureCipher{}
	decryptingCipher := fixtureCipher{}

	encryptor, edek, err := encryptingCipher.GetEncryptor(17, 23)
	require.NoError(t, err)
	require.Regexp(t, `^v1:[0-9a-f]{32}:[0-9a-f]{64}$`, string(edek))
	require.True(t, utf8.Valid(edek))

	serialized, err := json.Marshal(string(edek))
	require.NoError(t, err)
	var persisted string
	require.NoError(t, json.Unmarshal(serialized, &persisted))

	decryptor, err := decryptingCipher.GetDecryptor(17, 23, []byte(persisted))
	require.NoError(t, err)
	ciphertext, err := encryptor.Encrypt([]byte("fixture payload"))
	require.NoError(t, err)
	plaintext, err := decryptor.Decrypt(ciphertext)
	require.NoError(t, err)
	require.Equal(t, []byte("fixture payload"), plaintext)
}

func TestFixtureCipherRejectsWrongOrTamperedEDEKContext(t *testing.T) {
	cipher := fixtureCipher{}
	_, edek, err := cipher.GetEncryptor(17, 23)
	require.NoError(t, err)

	_, err = cipher.GetDecryptor(18, 23, edek)
	require.ErrorContains(t, err, "authentication failed")
	_, err = cipher.GetDecryptor(17, 24, edek)
	require.ErrorContains(t, err, "authentication failed")

	tampered := append([]byte(nil), edek...)
	if tampered[len(tampered)-1] == '0' {
		tampered[len(tampered)-1] = '1'
	} else {
		tampered[len(tampered)-1] = '0'
	}
	_, err = cipher.GetDecryptor(17, 23, tampered)
	require.ErrorContains(t, err, "authentication failed")
}

func TestFixtureCipherRejectsMalformedEDEK(t *testing.T) {
	cipher := fixtureCipher{}

	for _, edek := range []string{
		"",
		"v2:000102030405060708090a0b0c0d0e0f:d9cb9e34022d55b0e61b0c37e4561ad255400a285b27f74cbe31c2574dd10e4d",
		"v1:00:d9cb9e34022d55b0e61b0c37e4561ad255400a285b27f74cbe31c2574dd10e4d",
		"v1:000102030405060708090A0B0C0D0E0F:d9cb9e34022d55b0e61b0c37e4561ad255400a285b27f74cbe31c2574dd10e4d",
	} {
		_, err := cipher.GetDecryptor(17, 23, []byte(edek))
		require.Error(t, err, edek)
	}
}
