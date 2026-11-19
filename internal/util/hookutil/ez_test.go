// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hookutil

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type failingInitCipher struct {
	testCipher
}

func (failingInitCipher) Init(map[string]string) error {
	return errors.New("kms unavailable")
}

// TestCreateLocalEZReturnsParameterError pins the plugin-context boundary: a
// failed local KMS init carries a Milvus code instead of being serialized as
// an unclassified status (code 65535) by the worker.
func (s *CipherSuite) TestCreateLocalEZReturnsParameterError() {
	// Consume the one-time plugin init first: it stores nil when encryption is
	// disabled and would otherwise overwrite the fake cipher.
	InitOnceCipher()
	storeCipher(failingInitCipher{})

	err := CreateLocalEZ(1, "key")
	s.ErrorIs(err, merr.ErrParameterInvalid)
}

type importResolverCipher struct {
	testCipher
	resolve func(string) (int64, []byte, error)
}

func (c importResolverCipher) ResolveImportEZ(artifact string) (int64, []byte, error) {
	return c.resolve(artifact)
}

func TestBackupSourcePluginContext(t *testing.T) {
	paramtable.Init()
	previous := GetCipher()
	t.Cleanup(func() { storeCipher(previous) })
	denied := errors.New("source KMS denied Decrypt")
	for _, tc := range []struct {
		name   string
		cipher hook.Cipher
		err    error
	}{
		{"missing cipher", nil, merr.ErrServiceUnavailable},
		{"cipher without resolver", testCipher{}, merr.ErrServiceUnimplemented},
		{"empty resolved keyset", importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 17, nil, nil
		}}, merr.ErrServiceInternal},
		{"provider error survives", importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 0, nil, denied
		}}, denied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storeCipher(tc.cipher)
			ctx, err := GetReadStoragePluginContext("artifact")
			require.ErrorIs(t, err, tc.err)
			require.Nil(t, ctx)
		})
	}

	t.Run("same EZ returns the bytes of each artifact", func(t *testing.T) {
		storeCipher(importResolverCipher{resolve: func(artifact string) (int64, []byte, error) {
			if artifact == "before rotation" {
				return 17, []byte{0xff, 0x00}, nil
			}
			return 17, []byte{0x01, 0xff}, nil
		}})
		for _, backup := range []struct{ artifact, wire string }{
			{"before rotation", "17:/wA="},
			{"after rotation", "17:Af8="},
			{"before rotation", "17:/wA="},
		} {
			ctx, err := GetReadStoragePluginContext(backup.artifact)
			require.NoError(t, err)
			require.Len(t, ctx, 1)
			require.Equal(t, CipherConfigUnsafeEZK, ctx[0].GetKey())
			require.Equal(t, backup.wire, ctx[0].GetValue())
		}
	})
}
