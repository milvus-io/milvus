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

package initcore

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestInitPluginLoaderLogsProtectConfig(t *testing.T) {
	paramtable.Init()
	params := paramtable.GetCipherParams()
	original := params.SoPathCpp.GetValue()
	t.Cleanup(func() { require.NoError(t, params.Save(params.SoPathCpp.Key, original)) })
	const canary = "native-plugin-path-secret-canary"
	path := filepath.Join(t.TempDir(), canary+".so")
	require.NoError(t, params.Save(params.SoPathCpp.Key, path))
	patch := mockey.Mock(hookutil.IsClusterEncryptionEnabled).Return(true).Build()
	defer patch.UnPatch()
	sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "debug"})

	// The real native loader fails at dlopen. Its status goes through the
	// ordinary HandleCStatus and is then printed by startup callers again.
	err := InitPluginLoader()
	require.Error(t, err)
	assert.Equal(t, int32(merr.CodeUnexpectedError), merr.Code(err))
	assert.Equal(t, merr.SystemError, merr.GetErrorType(err))
	assert.False(t, merr.IsRetryableErr(err))
	mlog.Error(context.TODO(), "startup failed", mlog.Err(err))
	assert.NotContains(t, fmt.Sprintf("init query node segcore failed, %+v", err), canary)
	assert.NotContains(t, sink.String(), canary)
	assert.Contains(t, sink.String(), "Init PluginLoader")
	assert.Contains(t, sink.String(), "C runtime exception")
	assert.Equal(t, path, params.SoPathCpp.GetValue(), "runtime input remains raw")
}
