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

package adaptor_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/registry"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestUpsertGenerationGate(t *testing.T) {
	paramtable.Init()
	initIdempotencyResourceForTest(t)
	previousWAL := message.GetDefaultWALName()
	message.RegisterDefaultWALName(message.WALNameTest)
	defer message.RegisterDefaultWALName(previousWAL)
	params := paramtable.Get()
	const key = "streaming.wal.enableUpsertMessage"
	defer params.Reset(key)
	require.NoError(t, params.Save(key, "false"))
	opener, err := registry.MustGetBuilder(message.WALNameTest, timetick.NewInterceptorBuilder()).Build()
	require.NoError(t, err)
	defer opener.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	w, err := opener.Open(ctx, &wal.OpenOption{
		Channel: types.PChannelInfo{Name: "upsert-gate-channel", Term: 1}, DisableFlusher: true,
	})
	require.NoError(t, err)
	defer w.Close()
	build := func() message.MutableMessage {
		return message.NewUpsertMessageBuilderV2().WithVChannel("upsert-gate-channel_1v0").
			WithHeader(&message.UpsertMessageHeader{}).WithBody(&message.UpsertMessageBody{}).MustBuildMutable()
	}
	for _, configured := range []string{"false", "auto"} {
		require.NoError(t, params.Save(key, configured))
		for _, replicated := range []bool{false, true} {
			msg := build()
			if replicated {
				msg.WithReplicateHeader(&message.ReplicateHeader{ClusterID: "source", MessageID: walimplstest.NewTestMessageID(1), LastConfirmedMessageID: walimplstest.NewTestMessageID(0), TimeTick: 10})
			}
			result, err := w.Append(ctx, msg)
			require.Nil(t, result)
			require.Error(t, err)
			require.True(t, status.AsStreamingError(err).IsUnrecoverable())
			require.False(t, msg.Properties().Exist("_tt"), "the disabled message must not reach TimeTick allocation")
		}
	}
}

func TestUpsertEnvelopeAsyncAdmission(t *testing.T) {
	paramtable.Init()
	initIdempotencyResourceForTest(t)
	previousWAL := message.GetDefaultWALName()
	message.RegisterDefaultWALName(message.WALNameTest)
	defer message.RegisterDefaultWALName(previousWAL)
	gate := &paramtable.Get().StreamingCfg.EnableUpsertMessage
	old := gate.SwapTempValue("true")
	defer gate.SwapTempValue(old)
	opener, err := registry.MustGetBuilder(message.WALNameTest, timetick.NewInterceptorBuilder()).Build()
	require.NoError(t, err)
	defer opener.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	w, err := opener.Open(ctx, &wal.OpenOption{Channel: types.PChannelInfo{Name: "upsert-envelope-async", Term: 1}, DisableFlusher: true})
	require.NoError(t, err)
	defer w.Close()
	msg := message.NewUpsertMessageBuilderV2().WithVChannel("upsert-envelope-async_1v0").WithProperty("_np", "").WithHeader(&message.UpsertMessageHeader{}).WithBody(&message.UpsertMessageBody{}).MustBuildMutable()
	done := make(chan struct{})
	w.AppendAsync(ctx, msg, func(result *wal.AppendResult, err error) {
		defer close(done)
		assert.Nil(t, result)
		assert.Error(t, err)
		assert.True(t, status.AsStreamingError(err).IsUnrecoverable())
	})
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.False(t, msg.Properties().Exist("_tt"), "invalid envelope must not reach TimeTick")
}
