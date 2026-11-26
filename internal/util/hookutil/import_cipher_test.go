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

package hookutil

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type importReadCipher struct {
	testCipher
	calls []map[string]string
	err   error
}

func (c *importReadCipher) Init(config map[string]string) error {
	c.calls = append(c.calls, config)
	return c.err
}

func TestPrepareImportCipher(t *testing.T) {
	paramtable.Init()
	previous := GetCipher()
	t.Cleanup(func() { storeCipher(previous) })
	sourceKey := []byte{0, 0xff, 1}
	targetKey := []byte{0xff, 2, 0}
	source := &commonpb.KeyValuePair{Key: CipherConfigUnsafeEZK, Value: encodeEZContext(17, sourceKey)}
	target := &commonpb.KeyValuePair{Key: CipherConfigUnsafeEZK, Value: encodeEZContext(17, targetKey)}
	artifact := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":17}`))
	properties := []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "17"}}
	for _, tc := range []struct {
		name       string
		artifact   string
		properties []*commonpb.KeyValuePair
		entries    []*commonpb.KeyValuePair
	}{
		{"plaintext", "", nil, nil},
		{"source only", artifact, nil, []*commonpb.KeyValuePair{source}},
		{"target only", "", properties, []*commonpb.KeyValuePair{target}},
		{"same zone", artifact, properties, []*commonpb.KeyValuePair{source, {Key: "other"}, target}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cipher := &importReadCipher{}
			storeCipher(cipher)
			ctx, err := PrepareImportCipher(tc.artifact, tc.properties, tc.entries, 23)
			require.NoError(t, err)
			var calls []map[string]string
			if tc.properties != nil {
				calls = append(calls, map[string]string{CipherConfigCreateEZ: "17", CipherConfigUnsafeEZK: base64.StdEncoding.EncodeToString(targetKey)})
				require.Equal(t, base64.StdEncoding.EncodeToString(targetKey), ctx.Target.GetEncryptionKey())
				require.Equal(t, int64(23), ctx.Target.GetCollectionId())
			} else {
				require.Nil(t, ctx.Target)
			}
			if tc.artifact != "" {
				calls = append(calls, map[string]string{CipherConfigImportEZ: "17", CipherConfigUnsafeEZK: base64.StdEncoding.EncodeToString(sourceKey)})
				require.Equal(t, base64.StdEncoding.EncodeToString(sourceKey), ctx.Source.GetEncryptionKey())
				require.Zero(t, ctx.Source.GetCollectionId())
			} else {
				require.Nil(t, ctx.Source)
			}
			require.Equal(t, calls, cipher.calls)
		})
	}
	t.Run("target registration failure stops before source registration", func(t *testing.T) {
		cipher := &importReadCipher{err: merr.ErrServiceInternal}
		storeCipher(cipher)
		_, err := PrepareImportCipher(artifact, properties, []*commonpb.KeyValuePair{source, target}, 23)
		require.ErrorIs(t, err, merr.ErrImportSysFailed)
		require.Equal(t, []map[string]string{
			{CipherConfigCreateEZ: "17", CipherConfigUnsafeEZK: base64.StdEncoding.EncodeToString(targetKey)},
		}, cipher.calls)
	})
	t.Run("validate all wire entries before registration", func(t *testing.T) {
		cipher := &importReadCipher{}
		storeCipher(cipher)
		for _, entries := range [][]*commonpb.KeyValuePair{
			{source},
			{source, target, target},
			{source, {Key: CipherConfigUnsafeEZK, Value: encodeEZContext(18, targetKey)}},
			{{Key: CipherConfigUnsafeEZK, Value: encodeEZContext(12, sourceKey)}, target},
			{source, {Key: CipherConfigUnsafeEZK, Value: "17:not-base64"}},
			{source, {Key: CipherConfigUnsafeEZK, Value: "17:"}},
		} {
			_, err := PrepareImportCipher(artifact, properties, entries, 23)
			require.ErrorIs(t, err, merr.ErrImportSysFailed)
		}
		_, err := PrepareImportCipher(artifact, []*commonpb.KeyValuePair{
			{Key: common.EncryptionEzIDKey, Value: "17"},
			{Key: common.EncryptionEzIDKey, Value: "18"},
		}, []*commonpb.KeyValuePair{source, target}, 23)
		require.ErrorIs(t, err, merr.ErrImportSysFailed)
		require.Empty(t, cipher.calls)
	})
	t.Run("source requires read-only registration", func(t *testing.T) {
		storeCipher(nil)
		_, err := PrepareImportCipher(artifact, nil, []*commonpb.KeyValuePair{source}, 23)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		storeCipher(&importReadCipher{err: merr.ErrServiceInternal})
		_, err = PrepareImportCipher(artifact, nil, []*commonpb.KeyValuePair{source}, 23)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.Equal(t, merr.Code(merr.ErrImportSysFailed), merr.Status(err).GetCode())
		storeCipher(&importReadCipher{err: merr.ErrServiceUnavailable})
		_, err = PrepareImportCipher(artifact, nil, []*commonpb.KeyValuePair{source}, 23)
		require.Equal(t, merr.Code(merr.ErrServiceUnavailable), merr.Status(err).GetCode())
	})
}
