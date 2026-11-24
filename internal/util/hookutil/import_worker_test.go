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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type importReadCipher struct {
	testCipher
	calls []map[string]string
	err   error
}

func TestMalformedEZContextDoesNotEchoInput(t *testing.T) {
	for _, wire := range []string{"private-context", "private-context:key"} {
		_, _, err := decodeEZContext(wire)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "private-context")
	}
}

func (c *importReadCipher) Init(cfg map[string]string) error {
	c.calls = append(c.calls, cfg)
	return c.err
}

func (c *importReadCipher) GetUnsafeKey(int64, int64) []byte {
	panic("import must use key material from the request")
}

func TestRegisterImportCipherContexts(t *testing.T) {
	paramtable.Init()
	const collectionID = 35
	artifact := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":7}`))
	schema := []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "7"}}
	source := &commonpb.KeyValuePair{Key: CipherConfigUnsafeEZK, Value: "7:b2xk"}
	target := &commonpb.KeyValuePair{Key: CipherConfigUnsafeEZK, Value: "7:bmV3"}
	other := &commonpb.KeyValuePair{Key: "other.plugin", Value: "context"}

	for _, tc := range []struct {
		name                   string
		artifact               string
		props                  []*commonpb.KeyValuePair
		wire                   []*commonpb.KeyValuePair
		wantSource, wantTarget bool
	}{
		{"plaintext", "", nil, []*commonpb.KeyValuePair{other}, false, false},
		{"encrypted source", artifact, nil, []*commonpb.KeyValuePair{other, source}, true, false},
		{"encrypted target", "", schema, []*commonpb.KeyValuePair{target, other}, false, true},
		{"same zone on both sides", artifact, schema, []*commonpb.KeyValuePair{source, other, target}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &importReadCipher{}
			t.Cleanup(SetTestCipher(c))
			source, target, err := ParseImportCipherContexts(tc.artifact, tc.props, tc.wire, collectionID)
			require.NoError(t, err)
			var want []map[string]string
			if tc.wantSource {
				require.Equal(t, "b2xk", source.GetEncryptionKey())
			} else {
				require.Nil(t, source)
			}
			if tc.wantTarget {
				want = append(want, map[string]string{CipherConfigCreateEZ: "7", CipherConfigUnsafeEZK: "bmV3"})
				require.EqualValues(t, 7, target.GetEncryptionZoneId())
				require.EqualValues(t, collectionID, target.GetCollectionId())
				require.Equal(t, "bmV3", target.GetEncryptionKey())
			} else {
				require.Nil(t, target)
			}
			if tc.wantSource {
				want = append(want, map[string]string{CipherConfigImportEZ: "7", CipherConfigUnsafeEZK: "b2xk"})
			}
			require.Empty(t, c.calls)
			err = RegisterImportCipherContexts(tc.artifact, tc.props, tc.wire, collectionID)
			require.NoError(t, err)
			require.Equal(t, want, c.calls)
		})
	}
}

func TestRegisterImportCipherContextsRejectsInvalidRequest(t *testing.T) {
	paramtable.Init()
	const collectionID = 35
	artifact := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":7}`))
	missingID := base64.StdEncoding.EncodeToString([]byte(`{}`))
	zone := func(s string) []*commonpb.KeyValuePair {
		return []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: s}}
	}
	wire := func(values ...string) []*commonpb.KeyValuePair {
		var kvs []*commonpb.KeyValuePair
		for _, v := range values {
			kvs = append(kvs, &commonpb.KeyValuePair{Key: CipherConfigUnsafeEZK, Value: v})
		}
		return kvs
	}
	for _, tc := range []struct {
		name, artifact string
		props, entries []*commonpb.KeyValuePair
	}{
		{"missing source", artifact, nil, nil},
		{"missing target", "", zone("8"), nil},
		{"too few entries for equal IDs", artifact, zone("7"), wire("7:b2xk")},
		{"extra entry", "", nil, wire("7:b2xk")},
		{"reversed roles", artifact, zone("8"), wire("8:bmV3", "7:b2xk")},
		{"wrong target", artifact, zone("8"), wire("7:b2xk", "7:bmV3")},
		{"invalid schema zone", "", zone("abc"), nil},
		{"duplicate schema zone", "", append(zone("8"), zone("8")...), wire("8:bmV3", "8:bmV3")},
		{"invalid artifact", "%%", nil, nil},
		{"missing artifact ID", missingID, nil, wire("0:b2xk")},
		{"invalid wire format", artifact, nil, wire("7:b2xk:secret")},
		{"invalid target encoding", artifact, zone("8"), wire("7:b2xk", "8:secret!")},
		{"empty keyset", artifact, nil, wire("7:")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &importReadCipher{}
			t.Cleanup(SetTestCipher(c))
			err := RegisterImportCipherContexts(tc.artifact, tc.props, tc.entries, collectionID)
			require.ErrorIs(t, err, merr.ErrImportFailed)
			require.NotContains(t, err.Error(), "secret")
			require.Empty(t, c.calls)
		})
	}
	t.Run("disabled cipher", func(t *testing.T) {
		t.Cleanup(SetTestCipher(nil))
		err := RegisterImportCipherContexts(artifact, nil, wire("7:b2xk"), collectionID)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		err = RegisterImportCipherContexts("", zone("8"), wire("8:bmV3"), collectionID)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		err = RegisterImportCipherContexts("", nil, nil, collectionID)
		require.NoError(t, err)
	})
	t.Run("incompatible cached read key", func(t *testing.T) {
		conflict := errors.New("CipherPlugin: cannot register ezk, ezID=7, err=conflicting key ID 24 for ezID=7")
		t.Cleanup(SetTestCipher(&importReadCipher{err: conflict}))
		err := RegisterImportCipherContexts(artifact, nil, wire("7:b2xk"), collectionID)
		require.ErrorIs(t, err, merr.ErrImportFailed)
	})
	t.Run("target failure stops before source", func(t *testing.T) {
		targetErr := merr.WrapErrServiceUnavailable("target key cache unavailable")
		c := &importReadCipher{err: targetErr}
		t.Cleanup(SetTestCipher(c))
		err := RegisterImportCipherContexts(artifact, zone("7"), wire("7:b2xk", "7:bmV3"), collectionID)
		require.ErrorIs(t, err, targetErr)
		require.Equal(t, []map[string]string{{CipherConfigCreateEZ: "7", CipherConfigUnsafeEZK: "bmV3"}}, c.calls)
	})
}
