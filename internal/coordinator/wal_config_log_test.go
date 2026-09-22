// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// Keep the handler, broadcast helper, and message builder real. Only replace
// cluster discovery and the broadcast transport so a valid request reaches all
// pre-broadcast diagnostics without a running cluster.
func TestHandleAlterWALLogsProtectConfig(t *testing.T) {
	manager := config.NewManager()
	t.Cleanup(manager.Close)
	params := &paramtable.ComponentParam{}
	params.MQCfg.Type = paramtable.ParamItem{Key: "mq.type", DefaultValue: "pulsar"}
	params.MQCfg.Type.Init(manager)
	paramsPatch := mockey.Mock(paramtable.Get).Return(params).Build()
	t.Cleanup(func() { paramsPatch.UnPatch() })
	clusterPatch := mockey.Mock(channel.GetClusterChannels).Return(message.ClusterChannels{
		Channels: []string{"pchannel0"}, ControlChannel: "pchannel0_vcchan",
	}).Build()
	t.Cleanup(func() { clusterPatch.UnPatch() })
	balancerPatch := mockey.Mock(balance.GetWithContext).Return(&walConfigLogBalancer{}, nil).Build()
	t.Cleanup(func() { balancerPatch.UnPatch() })
	walPatch := mockey.Mock(streaming.WAL).Return(&walConfigLogWAL{}).Build()
	t.Cleanup(func() { walPatch.UnPatch() })

	for _, test := range []struct {
		name         string
		broadcastErr error
		wantStatus   int
	}{
		{name: "success", wantStatus: http.StatusOK},
		{name: "broadcast failure", broadcastErr: merr.ErrServiceUnavailable, wantStatus: http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &walConfigLogBroadcaster{err: test.broadcastErr}
			patch := mockey.Mock(broadcast.StartBroadcastWithResourceKeys).Return(transport, nil).Build()
			t.Cleanup(func() { patch.UnPatch() })
			sink := log.CaptureGlobalLogs(t, &log.Config{Level: "debug"})
			// Both names and values are request-controlled. Include a credential whose
			// name is not covered by a password-only classifier as well as a secret key.
			configs := map[string]string{
				"sasl.password":          "wal-password-value-canary", // #nosec G101 -- synthetic redaction canary.
				"ssl.key.pem":            "wal-private-key-value-canary",
				"wal-request-key-canary": "wal-arbitrary-value-canary",
			}
			payload, err := json.Marshal(map[string]any{"target_wal_name": "kafka", "config": configs})
			require.NoError(t, err)
			request := httptest.NewRequest(http.MethodPost, "/management/wal/alter", bytes.NewReader(payload))
			response := httptest.NewRecorder()
			(&mixCoordImpl{}).HandleAlterWAL(response, request)

			require.Equal(t, test.wantStatus, response.Code, response.Body.String())
			require.NotNil(t, transport.received, "must reach the real broadcast helper")
			header := message.MustAsBroadcastAlterWALMessageV2(transport.received).Header()
			assert.Equal(t, commonpb.WALName_Kafka, header.TargetWalName)
			assert.Equal(t, configs, header.Config, "redaction must not alter the actual WAL configuration")
			assert.True(t, transport.closed, "broadcast resources must be released on success and failure")
			output := sink.String()
			assert.Contains(t, output, "configCount")
			assert.Contains(t, output, "broadcastAlterWALMessage preparing")
			for key, value := range configs {
				assert.NotContains(t, output, key)
				assert.NotContains(t, output, value)
				assert.NotContains(t, response.Body.String(), key)
				assert.NotContains(t, response.Body.String(), value)
			}
		})
	}
}

type walConfigLogBroadcaster struct {
	received message.BroadcastMutableMessage
	err      error
	closed   bool
}

func (b *walConfigLogBroadcaster) Broadcast(_ context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
	b.received = msg
	if b.err != nil {
		return nil, b.err
	}
	return &types.BroadcastAppendResult{
		BroadcastID:   1,
		AppendResults: map[string]*types.AppendResult{"pchannel0_vcchan": {}},
	}, nil
}

func (b *walConfigLogBroadcaster) Close() { b.closed = true }

type walConfigLogBalancer struct{ balancer.Balancer }

func (b *walConfigLogBalancer) GetLatestChannelAssignment() (*balancer.WatchChannelAssignmentsCallbackParam, error) {
	pchannel := channel.NewPChannelMeta("pchannel0", types.AccessModeRW)
	return &balancer.WatchChannelAssignmentsCallbackParam{
		PChannelView: &channel.PChannelView{Channels: map[channel.ChannelID]*channel.PChannelMeta{pchannel.ChannelID(): pchannel}},
	}, nil
}

type walConfigLogWAL struct{ streaming.WALAccesser }

func (w *walConfigLogWAL) ControlChannel() string { return "pchannel0_vcchan" }
