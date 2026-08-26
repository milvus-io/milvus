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
	"encoding/hex"
	"encoding/json"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestFixtureCipherRequiresInitializedEZ(t *testing.T) {
	cipher := fixtureCipher{keys: make(map[int64][]byte)}

	_, _, err := cipher.GetEncryptor(17, 23)
	require.ErrorContains(t, err, "EZ 17 is not initialized")
}

func TestFixtureCipherRejectsUnexpectedRootKey(t *testing.T) {
	cipher := fixtureCipher{keys: make(map[int64][]byte)}

	err := cipher.Init(map[string]string{
		createEZKey: "17",
		kmsKeyARN:   "unexpected-root-key",
	})
	require.ErrorContains(t, err, "unexpected root key")
}

func TestFixtureDataKeyMatchesCppFixture(t *testing.T) {
	key := deriveDataKey(
		[]byte("fixture-root/fixture-root-key"),
		[]byte("fixture-edek"),
	)

	require.Equal(t, "0da22159f596deb7593ba6cb73ad0825653d81431468d0483076d66e7d217066", hex.EncodeToString(key))
}

func TestFixtureCipherEdekSurvivesJSONRoundTrip(t *testing.T) {
	cipher := fixtureCipher{keys: map[int64][]byte{17: []byte("fixture-root")}}

	encryptor, edek, err := cipher.GetEncryptor(17, 23)
	require.NoError(t, err)
	require.Len(t, edek, 64)
	require.True(t, utf8.Valid(edek))

	serialized, err := json.Marshal(string(edek))
	require.NoError(t, err)
	var persisted string
	require.NoError(t, json.Unmarshal(serialized, &persisted))

	decryptor, err := cipher.GetDecryptor(17, 23, []byte(persisted))
	require.NoError(t, err)
	ciphertext, err := encryptor.Encrypt([]byte("fixture payload"))
	require.NoError(t, err)
	plaintext, err := decryptor.Decrypt(ciphertext)
	require.NoError(t, err)
	require.Equal(t, []byte("fixture payload"), plaintext)
}
