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

package syncmgr

import (
	"bytes"
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

type retryingWriterCipher struct {
	hook.Cipher
	remainingFailures int
	lookups           int
	key               []byte
}

func (c *retryingWriterCipher) GetUnsafeKey(int64, int64) []byte {
	c.lookups++
	if c.remainingFailures > 0 {
		c.remainingFailures--
		return nil
	}
	return c.key
}

func TestPackedWriterKeyLookupRetry(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failures int
		cancel   bool
		lookups  int
	}{
		{name: "recovers within write budget", failures: 2, lookups: 3},
		{name: "preserves unavailable error after budget", failures: 3, lookups: 3},
		{name: "canceled sync does not fetch key", cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := bytes.Repeat([]byte{0x51}, 32)
			key[16] = 0
			cipher := &retryingWriterCipher{remainingFailures: tc.failures, key: key}
			patch := mockey.Mock(hookutil.GetCipher).Return(cipher).Build()
			t.Cleanup(func() { patch.UnPatch() })
			bw := &BulkPackWriterV2{BulkPackWriter: &BulkPackWriter{
				schema: &schemapb.CollectionSchema{Properties: []*commonpb.KeyValuePair{
					{Key: common.EncryptionEzIDKey, Value: "17"},
				}},
				writeRetryOpts: []retry.Option{retry.Attempts(3), retry.Sleep(time.Millisecond)},
			}}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			if tc.cancel {
				cancel()
			}
			pluginContext, err := bw.getPluginContext(ctx, 23)
			require.Equal(t, tc.lookups, cipher.lookups)
			switch {
			case tc.cancel:
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, pluginContext)
			case tc.failures == 3:
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				require.Nil(t, pluginContext)
			default:
				require.NoError(t, err)
				require.NotNil(t, pluginContext)
				require.EqualValues(t, 17, pluginContext.GetEncryptionZoneId())
				require.EqualValues(t, 23, pluginContext.GetCollectionId())
				require.Equal(t, base64.StdEncoding.EncodeToString(key), pluginContext.GetEncryptionKey())
			}
		})
	}
}
