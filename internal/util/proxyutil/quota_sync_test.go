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

package proxyutil

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/quotasync"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type quotaTransport struct {
	types.ProxyClient
	receiver     *proxy.SimpleLimiter
	legacy       bool
	failMetrics  bool
	loseRequest  bool
	loseReply    bool
	restartOnSet bool
	probeRouted  bool
	sent         []*proxypb.SetRatesRequest
}

func (p *quotaTransport) GetProxyMetrics(_ context.Context, request *milvuspb.GetMetricsRequest, _ ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	if p.failMetrics {
		return nil, context.DeadlineExceeded
	}
	isProbe := request.GetRequest() == `{"metric_type":"quota_sync"}`
	if isProbe {
		p.probeRouted = request.GetBase() != nil
	}
	if p.legacy {
		if isProbe {
			return &milvuspb.GetMetricsResponse{Status: merr.Status(merr.ErrServiceUnimplemented)}, nil
		}
		return &milvuspb.GetMetricsResponse{Status: merr.Success()}, nil
	}
	return &milvuspb.GetMetricsResponse{Status: p.receiver.QuotaSyncStatus()}, nil
}

func (p *quotaTransport) SetRates(_ context.Context, r *proxypb.SetRatesRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	p.sent = append(p.sent, proto.Clone(r).(*proxypb.SetRatesRequest))
	if p.loseRequest {
		return nil, context.DeadlineExceeded
	}
	if p.legacy {
		return merr.Success(), nil
	}
	if p.restartOnSet {
		p.restartOnSet = false
		p.receiver = proxy.NewSimpleLimiter(0, 0)
	}
	result := p.receiver.ApplyQuotaRequest(r)
	if p.loseReply {
		return nil, context.DeadlineExceeded
	}
	return result, nil
}

func TestQuotaSyncManagerLossIdleAndLegacy(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QuotaConfig.VersionedRatesEnabled.Key, "true"))
	t.Cleanup(func() { _ = params.Reset(params.QuotaConfig.VersionedRatesEnabled.Key) })
	receiver := proxy.NewSimpleLimiter(0, 0)
	client := &quotaTransport{receiver: receiver}
	old := &quotaTransport{legacy: true}
	manager := NewProxyClientManager(nil)
	manager.proxyClient.Insert(1, client)
	manager.proxyClient.Insert(2, old)
	defaults := quotasync.ScopeDefaults()
	publisher := &quotasync.Publisher{}
	root := quotasync.Key{Scope: internalpb.RateScope_Cluster}
	publisher.Publish(1, map[quotasync.Key]*proxypb.Limiter{root: {}}, defaults)
	sync := func() map[int64]QuotaSyncResult {
		return manager.SyncQuota(context.Background(), publisher, true, func() *proxypb.SetRatesRequest {
			return &proxypb.SetRatesRequest{RootLimiter: publisher.Full().RootLimiter}
		})
	}
	results := sync()
	require.NoError(t, results[1].Err)
	require.NoError(t, results[2].Err)
	require.True(t, client.probeRouted)
	require.True(t, old.probeRouted)
	require.Equal(t, "full", results[1].Mode)
	require.Zero(t, old.sent[0].QuotaProtocol)
	require.NotNil(t, old.sent[0].RootLimiter)
	client.sent = nil
	sync()
	require.Empty(t, client.sent) // Periodic reconciliation without payload.
	publisher.PublishPatch(1, map[quotasync.Key]*proxypb.Limiter{root: {Rates: []*internalpb.Rate{{Rt: internalpb.RateType_DMLInsert, R: 1}}}}, nil, defaults)
	client.loseRequest = true
	results = sync()
	require.Error(t, results[1].Err)
	require.NoError(t, results[2].Err)
	client.loseRequest = false
	results = sync()
	require.NoError(t, results[1].Err)
	require.Equal(t, "delta", results[1].Mode) // Last update recovered without new inputs.
	publisher.PublishPatch(1, map[quotasync.Key]*proxypb.Limiter{root: {Rates: []*internalpb.Rate{{Rt: internalpb.RateType_DMLInsert, R: 2}}}}, nil, defaults)
	client.loseReply = true
	require.Error(t, sync()[1].Err)
	client.loseReply = false
	client.sent = nil
	require.NoError(t, sync()[1].Err)
	require.Empty(t, client.sent) // Applied response was lost.
	publisher.PublishPatch(1, map[quotasync.Key]*proxypb.Limiter{root: {Rates: []*internalpb.Rate{{Rt: internalpb.RateType_DMLInsert, R: 3}}}}, nil, defaults)
	client.loseRequest = true
	sync()
	publisher.PublishPatch(1, map[quotasync.Key]*proxypb.Limiter{root: {Rates: []*internalpb.Rate{{Rt: internalpb.RateType_DMLInsert, R: 4}}}}, nil, defaults)
	client.loseRequest = false
	require.Equal(t, "full", sync()[1].Mode)
	// A metric error after negotiation can only send versioned FULL.
	client.failMetrics = true
	results = sync()
	require.NoError(t, results[1].Err)
	require.Equal(t, "full", results[1].Mode)
	require.EqualValues(t, 1, client.sent[len(client.sent)-1].QuotaProtocol)
	client.failMetrics = false
	// Same client/address, new process identity: new full baseline.
	client.receiver = proxy.NewSimpleLimiter(0, 0)
	require.Equal(t, "full", sync()[1].Mode)

	// Replacing the process after sampling invalidates the selected delta.
	publisher.PublishPatch(1, map[quotasync.Key]*proxypb.Limiter{root: {Rates: []*internalpb.Rate{{Rt: internalpb.RateType_DMLInsert, R: 5}}}}, nil, defaults)
	client.restartOnSet = true
	client.sent = nil
	result := sync()[1]
	require.NoError(t, result.Err)
	require.Equal(t, "full", result.Mode)
	require.Len(t, client.sent, 2)
	require.Equal(t, proxypb.QuotaSyncMode_QUOTA_DELTA, client.sent[0].GetQuotaMode())
	require.Equal(t, proxypb.QuotaSyncMode_QUOTA_FULL, client.sent[1].GetQuotaMode())
	require.NotEqual(t, client.sent[0].GetQuotaSession(), client.sent[1].GetQuotaSession())
}

// TestQuotaSyncManagerUnversionedProxy covers a partial rollout or a rollback
// of the Proxy side: the coordinator publishes versioned rules while the
// Proxy runs without the protocol. The Proxy receives and installs the legacy
// FULL, and it refuses a versioned request instead of misreading it.
func TestQuotaSyncManagerUnversionedProxy(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.QuotaConfig.VersionedRatesEnabled.Key, "false"))
	require.NoError(t, params.Save(params.QuotaConfig.QuotaAndLimitsEnabled.Key, "true"))
	t.Cleanup(func() {
		_ = params.Reset(params.QuotaConfig.VersionedRatesEnabled.Key)
		_ = params.Reset(params.QuotaConfig.QuotaAndLimitsEnabled.Key)
	})
	receiver := proxy.NewSimpleLimiter(0, 0)
	client := &quotaTransport{receiver: receiver}
	manager := NewProxyClientManager(nil)
	manager.proxyClient.Insert(1, client)
	defaults := quotasync.ScopeDefaults()
	root := quotasync.Key{Scope: internalpb.RateScope_Cluster}
	deny := &proxypb.Limiter{
		Rates:  []*internalpb.Rate{{Rt: internalpb.RateType_DMLInsert, R: 0}},
		States: []milvuspb.QuotaState{milvuspb.QuotaState_DenyToWrite},
		Codes:  []commonpb.ErrorCode{commonpb.ErrorCode_DiskQuotaExhausted},
	}
	publisher := &quotasync.Publisher{}
	publisher.Publish(1, map[quotasync.Key]*proxypb.Limiter{root: deny}, defaults)
	results := manager.SyncQuota(context.Background(), publisher, true, func() *proxypb.SetRatesRequest {
		return &proxypb.SetRatesRequest{RootLimiter: publisher.Full().RootLimiter}
	})
	require.NoError(t, results[1].Err)
	require.Equal(t, "legacy", results[1].Mode)
	require.Len(t, client.sent, 1)
	require.Zero(t, client.sent[0].QuotaProtocol)
	require.ErrorIs(t, receiver.Check(1, map[int64][]int64{100: nil}, internalpb.RateType_DMLInsert, 1), merr.ErrServiceQuotaExceeded)

	versioned := publisher.Full()
	versioned.QuotaSession = "session"
	require.ErrorIs(t, merr.Error(receiver.ApplyQuotaRequest(versioned)), merr.ErrServiceUnimplemented)
	require.ErrorIs(t, receiver.Check(1, map[int64][]int64{100: nil}, internalpb.RateType_DMLInsert, 1), merr.ErrServiceQuotaExceeded)
}
