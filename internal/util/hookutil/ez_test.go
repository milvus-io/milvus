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
	"context"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus/pkg/v3/common"
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

func (c importResolverCipher) Backup(int64) (string, error) {
	return "", nil
}

func (c importResolverCipher) ResolveImportEZ(artifact string) (int64, []byte, error) {
	return c.resolve(artifact)
}

type temporaryTestError struct{ msg string }

func (e temporaryTestError) Error() string   { return e.msg }
func (e temporaryTestError) Temporary() bool { return true }

type causeOnlyError struct{ cause error }

func (e causeOnlyError) Error() string { return "caused by: " + e.cause.Error() }
func (e causeOnlyError) Cause() error  { return e.cause }

func TestBackupSourcePluginContext(t *testing.T) {
	paramtable.Init()
	previous := GetCipher()
	t.Cleanup(func() { storeCipher(previous) })
	validArtifact := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":17}`))
	denied := errors.New("source KMS denied Decrypt")
	for _, tc := range []struct {
		name     string
		artifact string
		cipher   hook.Cipher
		err      error
	}{
		{"invalid base64 artifact", "!", importResolverCipher{}, merr.ErrParameterInvalid},
		{"invalid json artifact", base64.StdEncoding.EncodeToString([]byte("not-json")), importResolverCipher{}, merr.ErrParameterInvalid},
		{"non-positive ez_id artifact", base64.StdEncoding.EncodeToString([]byte(`{"ez_id":0}`)), importResolverCipher{}, merr.ErrParameterInvalid},
		{"missing cipher", validArtifact, nil, merr.ErrServiceUnavailable},
		{"cipher without resolver", validArtifact, testCipher{}, merr.ErrServiceUnimplemented},
		{"empty resolved keyset", validArtifact, importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 17, nil, nil
		}}, merr.ErrServiceInternal},
		{"mismatched ezID", validArtifact, importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 18, []byte("key"), nil
		}}, merr.ErrServiceInternal},
		{"provider error survives", validArtifact, importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 0, nil, denied
		}}, denied},
		{"temporary error maps to service unavailable", validArtifact, importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 0, nil, fmt.Errorf("kms throttled: %w", temporaryTestError{msg: "try again"})
		}}, merr.ErrServiceUnavailable},
		{"joined temporary error maps to service unavailable", validArtifact, importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 0, nil, errors.Join(errors.New("outer"), causeOnlyError{cause: temporaryTestError{msg: "retry"}})
		}}, merr.ErrServiceUnavailable},
		{"deadline exceeded stays retryable", validArtifact, importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 0, nil, context.DeadlineExceeded
		}}, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storeCipher(tc.cipher)
			ctx, err := ResolveImportEZ(tc.artifact)
			require.ErrorIs(t, err, tc.err)
			require.Nil(t, ctx)
		})
	}

	t.Run("same EZ returns the bytes of each artifact", func(t *testing.T) {
		beforeArtifact := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":17,"v":1}`))
		afterArtifact := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":17,"v":2}`))
		storeCipher(importResolverCipher{resolve: func(artifact string) (int64, []byte, error) {
			if artifact == beforeArtifact {
				return 17, []byte{0xff, 0x00}, nil
			}
			return 17, []byte{0x01, 0xff}, nil
		}})
		for _, backup := range []struct{ artifact, wire string }{
			{beforeArtifact, "17:/wA="},
			{afterArtifact, "17:Af8="},
			{beforeArtifact, "17:/wA="},
		} {
			ctx, err := ResolveImportEZ(backup.artifact)
			require.NoError(t, err)
			require.Len(t, ctx, 1)
			require.Equal(t, CipherConfigUnsafeEZK, ctx[0].GetKey())
			require.Equal(t, backup.wire, ctx[0].GetValue())
		}
	})

	t.Run("decodeEZContext does not echo raw secret", func(t *testing.T) {
		secret := "super-secret-ezk-value"
		_, _, err := decodeEZContext(secret)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.NotContains(t, err.Error(), secret)
	})

	t.Run("reject duplicate and invalid target ezID", func(t *testing.T) {
		storeCipher(testCipher{})
		for _, props := range [][]*commonpb.KeyValuePair{
			{{Key: common.EncryptionEzIDKey, Value: "17"}, {Key: common.EncryptionEzIDKey, Value: "18"}},
			{{Key: common.EncryptionEzIDKey, Value: "0"}},
			{{Key: common.EncryptionEzIDKey, Value: "-5"}},
			{{Key: common.EncryptionEzIDKey, Value: "abc"}},
		} {
			_, err := GetStoragePluginContext(props, 23)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			_, err = GetCollectionPluginContext(props, 23)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		}
	})
}
