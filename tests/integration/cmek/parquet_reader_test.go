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
	"encoding/hex"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
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

// The representative object comes from the independently inspected DataNode
// manifest. Read all row groups using the same entry point and immutable bytes
// in each key mode; never obtain the DEK from a production plugin or reader.
func (s *RawDataV3Suite) assertParquetKeyModes(sample parquetKeySample, collectionID int64) {
	raw, rows, objectPath := sample.raw, sample.object.Rows, sample.object.Path
	key := fixtureParquetKey(s.T(), sample.edek, s.ezID, collectionID)
	digest := sha256.Sum256(raw)
	table, err := readParquetWithFooterKey(raw, key)
	s.Require().NoError(err, "correct key must read the complete representative object")
	func() {
		defer table.Release()
		s.Require().Positive(rows)
		s.Require().Equal(rows, table.NumRows())
		columns := table.Schema().FieldIndices(sample.column)
		s.Require().Len(columns, 1, "representative object must contain the pre-generated payload")
		var count int64
		for _, chunk := range table.Column(columns[0]).Data().Chunks() {
			values, ok := chunk.(*array.Int64)
			s.Require().True(ok)
			s.Require().Zero(values.NullN())
			for _, value := range values.Int64Values() {
				s.Require().Equal(parquetBaselineValue, value)
				count++
			}
		}
		s.Require().Equal(rows, count)
	}()
	s.T().Logf("stage=format-key object=%s sha256=%x mode=correct rows=%d result=exact-read", objectPath, digest, rows)

	missingTable, err := readParquetWithFooterKey(raw, nil)
	if missingTable != nil {
		missingTable.Release()
	}
	s.Require().Nil(missingTable)
	s.Require().ErrorContains(err, "could not read encrypted metadata, no decryption found in reader's properties")
	s.T().Logf("stage=format-key object=%s sha256=%x mode=missing result=no-decryption-config", objectPath, digest)

	wrongKey := append([]byte(nil), key...)
	wrongKey[0] ^= 1
	wrongTable, err := readParquetWithFooterKey(raw, wrongKey)
	if wrongTable != nil {
		wrongTable.Release()
	}
	s.Require().Nil(wrongTable)
	s.Require().EqualError(err, "cipher: message authentication failed")
	s.Require().Equal(digest, sha256.Sum256(raw), "key modes must consume the same object bytes")
	s.T().Logf("stage=format-key object=%s sha256=%x mode=wrong result=authentication-failure", objectPath, digest)
}
