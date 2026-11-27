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

package meta

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type metaTestBackupCipher struct {
	hook.Cipher
	targetKey []byte
	sourceKey []byte
	sourceErr error
}

func (c *metaTestBackupCipher) GetUnsafeKey(int64, int64) []byte { return c.targetKey }
func (c *metaTestBackupCipher) Backup(int64) (string, error)     { return "", nil }
func (c *metaTestBackupCipher) ResolveImportEZ(string) (int64, []byte, error) {
	return 17, c.sourceKey, c.sourceErr
}

func (c *metaTestBackupCipher) GetDecryptorForKeyset(int64, int64, []byte, []byte) (hook.Decryptor, error) {
	panic("dispatch does not decrypt binlogs")
}

func TestWrapPluginContextImportV2AndV3(t *testing.T) {
	artifact := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":17}`))
	options := []*commonpb.KeyValuePair{{Key: importutilv2.EZK, Value: artifact}}
	props := []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "17"}}
	cipher := &metaTestBackupCipher{
		sourceKey: []byte("source"),
		targetKey: []byte("target"),
	}
	cleanup := hookutil.SetTestCipher(cipher)
	defer cleanup()

	want := []*commonpb.KeyValuePair{
		{Key: hookutil.CipherConfigUnsafeEZK, Value: "17:" + base64.StdEncoding.EncodeToString([]byte("source"))},
		{Key: hookutil.CipherConfigUnsafeEZK, Value: "17:" + base64.StdEncoding.EncodeToString([]byte("target"))},
	}
	wantTargetOnly := []*commonpb.KeyValuePair{
		{Key: hookutil.CipherConfigUnsafeEZK, Value: "17:" + base64.StdEncoding.EncodeToString([]byte("target"))},
	}

	preReq := &datapb.PreImportRequest{Options: options}
	require.NoError(t, WrapPluginContext(23, props, preReq))
	require.Equal(t, want, preReq.GetPluginContext())

	impReq := &datapb.ImportRequest{Options: options}
	require.NoError(t, WrapPluginContext(23, props, impReq))
	require.Equal(t, want, impReq.GetPluginContext())

	reshardReq := &datapb.ReshardTaskRequest{Plan: &importv3pb.ReshardTaskPlan{Options: options}}
	require.NoError(t, WrapPluginContext(23, props, reshardReq))
	require.Equal(t, want, reshardReq.GetPluginContext())

	// Import V3 final merge only writes target segments; reshard already decrypted the backup source.
	impV3Req := &datapb.ImportTaskV3Request{}
	require.NoError(t, WrapPluginContext(23, props, impV3Req))
	require.Equal(t, wantTargetOnly, impV3Req.GetPluginContext())

	// Missing target key must propagate retryable ErrServiceUnavailable.
	cipher.targetKey = nil
	err := WrapPluginContext(23, props, &datapb.CompactionPlan{})
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.True(t, merr.IsRetryableErr(err))
}
