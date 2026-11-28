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

func (c importResolverCipher) ResolveImportEZ(artifact string) (int64, []byte, error) {
	return c.resolve(artifact)
}

type backupOnlyCipher struct {
	testCipher
	backup func(int64) (string, error)
}

func (c backupOnlyCipher) Backup(ezID int64) (string, error) {
	return c.backup(ezID)
}

type temporaryError struct {
	msg       string
	temporary bool
}

func (e temporaryError) Error() string   { return e.msg }
func (e temporaryError) Temporary() bool { return e.temporary }

func TestBackupSourcePluginContext(t *testing.T) {
	paramtable.Init()
	previous := GetCipher()
	t.Cleanup(func() { storeCipher(previous) })
	denied := errors.New("source KMS denied Decrypt")
	artifact := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":17}`))
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
		{"mismatched resolved ezID", importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 18, []byte("k"), nil
		}}, merr.ErrServiceInternal},
		{"provider error survives", importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 0, nil, denied
		}}, denied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storeCipher(tc.cipher)
			ctx, err := ResolveImportEZ(artifact)
			require.ErrorIs(t, err, tc.err)
			if tc.cipher == nil {
				require.ErrorIs(t, err, errCipherPluginMissing)
			}
			if tc.name == "provider error survives" || tc.name == "cipher without resolver" {
				require.False(t, merr.IsRetryableErr(err))
				require.False(t, merr.IsCanceledOrTimeout(err))
			}
			require.Nil(t, ctx)
		})
	}

	t.Run("narrow BackupInterface and ImportEZResolver are independent", func(t *testing.T) {
		storeCipher(backupOnlyCipher{backup: func(ezID int64) (string, error) {
			return fmt.Sprintf("backup-%d", ezID), nil
		}})
		exported, err := BackupEZ(17)
		require.NoError(t, err)
		require.Equal(t, "backup-17", exported)
		ctx, err := ResolveImportEZ(artifact)
		require.Nil(t, ctx)
		require.ErrorIs(t, err, merr.ErrServiceUnimplemented)

		storeCipher(importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 17, []byte("key"), nil
		}})
		_, err = BackupEZ(17)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		ctx, err = ResolveImportEZ(artifact)
		require.NoError(t, err)
		require.Len(t, ctx, 1)
	})

	t.Run("rejects malformed artifact before calling resolver", func(t *testing.T) {
		called := false
		storeCipher(importResolverCipher{resolve: func(string) (int64, []byte, error) {
			called = true
			return 17, []byte("k"), nil
		}})
		for _, invalid := range []string{"%%", base64.StdEncoding.EncodeToString([]byte(`not-json`)), base64.StdEncoding.EncodeToString([]byte(`{"ez_id":0}`))} {
			ctx, err := ResolveImportEZ(invalid)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.Nil(t, ctx)
		}
		require.False(t, called)
	})

	t.Run("transient non-merr error maps to retryable service unavailable", func(t *testing.T) {
		transient := temporaryError{msg: "source KMS throttled", temporary: true}
		storeCipher(importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 0, nil, errors.Wrap(transient, "kms call failed")
		}})
		ctx, err := ResolveImportEZ(artifact)
		require.Nil(t, ctx)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.ErrorIs(t, err, transient)
		require.True(t, merr.IsRetryableErr(err))

		// Joined error with non-retryable first and temporary second must map to ServiceUnavailable.
		joined := fmt.Errorf("%w: %w", merr.ErrParameterInvalid, transient)
		storeCipher(importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 0, nil, joined
		}})
		ctx, err = ResolveImportEZ(artifact)
		require.Nil(t, ctx)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.True(t, merr.IsRetryableErr(err))

		// Non-temporary error stays non-retryable.
		nonTransient := temporaryError{msg: "permanent error", temporary: false}
		storeCipher(importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 0, nil, nonTransient
		}})
		ctx, err = ResolveImportEZ(artifact)
		require.Nil(t, ctx)
		require.ErrorIs(t, err, nonTransient)
		require.NotErrorIs(t, err, merr.ErrServiceUnavailable)
		require.False(t, merr.IsRetryableErr(err))

		// Existing retryable merr code and context timeout are preserved as-is.
		storeCipher(importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 0, nil, merr.WrapErrTooManyRequests(10)
		}})
		_, err = ResolveImportEZ(artifact)
		require.ErrorIs(t, err, merr.ErrServiceTooManyRequests)
		require.NotErrorIs(t, err, merr.ErrServiceUnavailable)
		require.True(t, merr.IsRetryableErr(err))

		// DeadlineExceeded maps to retryable ServiceUnavailable while preserving its cause.
		storeCipher(importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 0, nil, fmt.Errorf("rpc call: %w", context.DeadlineExceeded)
		}})
		_, err = ResolveImportEZ(artifact)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.True(t, merr.IsRetryableErr(err))
		require.True(t, merr.IsCanceledOrTimeout(err))

		// Canceled passes through without wrapping as ServiceUnavailable.
		storeCipher(importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 0, nil, context.Canceled
		}})
		_, err = ResolveImportEZ(artifact)
		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, merr.ErrServiceUnavailable)
		require.False(t, merr.IsRetryableErr(err))
		require.True(t, merr.IsCanceledOrTimeout(err))
	})

	t.Run("same EZ returns the bytes of each artifact", func(t *testing.T) {
		beforeArtifact := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":17,"v":1}`))
		afterArtifact := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":17,"v":2}`))
		storeCipher(importResolverCipher{resolve: func(a string) (int64, []byte, error) {
			if a == beforeArtifact {
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

	t.Run("decodeEZContext does not echo raw context", func(t *testing.T) {
		_, _, err := decodeEZContext("super-secret-value")
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.NotContains(t, err.Error(), "super-secret-value")

		_, _, err = decodeEZContext("not-an-int:super-secret-key")
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.NotContains(t, err.Error(), "super-secret-key")
	})

	t.Run("GetImportStoragePluginContext validates target properties", func(t *testing.T) {
		storeCipher(importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return 17, []byte("src"), nil
		}})
		_, err := GetImportStoragePluginContext(artifact, []*commonpb.KeyValuePair{
			{Key: common.EncryptionEzIDKey, Value: "17"},
			{Key: common.EncryptionEzIDKey, Value: "18"},
		}, 23)
		require.ErrorContains(t, err, "duplicate")

		_, err = GetImportStoragePluginContext(artifact, []*commonpb.KeyValuePair{
			{Key: common.EncryptionEzIDKey, Value: "0"},
		}, 23)
		require.ErrorContains(t, err, "invalid target")
	})
}
