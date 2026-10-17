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

package cmek

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// fixtureParquetKey resolves an already inspected EDEK using the fixture protocol.
func fixtureParquetKey(t *testing.T, encoded string, ezID, collectionID int64) []byte {
	t.Helper()
	edek := strings.Split(encoded, ":")
	require.Len(t, edek, 3)
	require.Equal(t, "v1", edek[0])
	nonce, err := hex.DecodeString(edek[1])
	require.NoError(t, err)
	require.Len(t, nonce, 16)
	tag, err := hex.DecodeString(edek[2])
	require.NoError(t, err)
	ezKey := fixtureHMAC([]byte("milvus-cmek-fixture-master-v1"), "ezk-v1\x00", nil, ezID)
	require.True(t, hmac.Equal(tag, fixtureHMAC(ezKey, "edek-v1\x00", nonce, ezID, collectionID)), "invalid fixture EDEK authentication")
	return fixtureHMAC(ezKey, "dek-v1\x00", nonce, ezID, collectionID)
}

func fixtureHMAC(key []byte, domain string, nonce []byte, ids ...int64) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(domain))
	_, _ = h.Write(nonce)
	for _, id := range ids {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(id))
		_, _ = h.Write(encoded[:])
	}
	return h.Sum(nil)
}
