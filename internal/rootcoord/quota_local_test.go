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

package rootcoord

import (
	"context"
	"maps"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/internal/util/quotasync"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type localQuotaTestSource struct {
	types.MixCoord
	values, changes map[int64]*metricsinfo.CollectionQuotaSnapshot
	forceFull       bool
}

func (s *localQuotaTestSource) ReadQuotaSnapshot(_ context.Context, full bool) (*metricsinfo.QuotaSnapshot, error) {
	full = full || s.forceFull
	values := s.changes
	if full {
		values = s.values
	}
	total := int64(0)
	for _, v := range s.values {
		total += v.Bytes
	}
	return &metricsinfo.QuotaSnapshot{Full: full, TotalBytes: total, Collections: maps.Clone(values)}, nil
}

func (s *localQuotaTestSource) AcknowledgeQuotaSnapshot(_ *metricsinfo.QuotaSnapshot) {
	clear(s.changes)
	s.forceFull = false
}

func (s *localQuotaTestSource) GetConnectedDataNodeMetrics(context.Context, *milvuspb.GetMetricsRequest) ([]metricsinfo.DataNodeInfos, error) {
	return nil, nil
}

func (s *localQuotaTestSource) put(id, bytes int64) {
	value := &metricsinfo.CollectionQuotaSnapshot{Present: true, HasQuota: true, HasRows: true, Bytes: bytes, Rows: 12, PartitionBytes: map[int64]int64{id * 10: bytes}}
	s.values[id] = value
	s.changes[id] = value
}

func TestQuotaLocalCalculationMatchesReference(t *testing.T) {
	setQuotaTestParam(t, &Params.QuotaConfig.CollectionStatsCacheEnabled, "true")
	setQuotaTestParam(t, &Params.QuotaConfig.LocalQuotaCalculationEnabled, "true")
	setQuotaTestParam(t, &Params.QuotaConfig.DiskProtectionEnabled, "true")
	setQuotaTestParam(t, &Params.QuotaConfig.DiskQuotaPerCollection, "1")
	setQuotaTestParam(t, &Params.QuotaConfig.DMLLimitEnabled, "true")
	setQuotaTestParam(t, &Params.QuotaConfig.DMLMaxInsertRatePerCollection, "10")
	setQuotaTestParam(t, &Params.QuotaConfig.DMLMaxInsertRatePerPartition, "5")
	setQuotaTestParam(t, &Params.QuotaConfig.TtProtectionEnabled, "false")
	meta := quotaMetaForTest(3)
	source := &localQuotaTestSource{values: make(map[int64]*metricsinfo.CollectionQuotaSnapshot), changes: make(map[int64]*metricsinfo.CollectionQuotaSnapshot)}
	for id := range meta.collID2Meta {
		source.put(id, 100)
	}
	manager := proxyutil.NewMockProxyClientManager(t)
	proxyCount := 1
	manager.EXPECT().GetProxyCount().RunAndReturn(func() int { return proxyCount })
	var sent *proxypb.SetRatesRequest
	var sendErr error
	manager.EXPECT().SetRates(mock.Anything, mock.Anything).RunAndReturn(func(_ context.Context, req *proxypb.SetRatesRequest) error {
		sent = req
		return sendErr
	})
	allocator := newMockTsoAllocator()
	allocator.GenerateTSOF = func(uint32) (uint64, error) { return 100, nil }
	local := NewQuotaCenter(manager, source, allocator, meta)
	reference := NewQuotaCenter(manager, nil, allocator, meta)
	t.Cleanup(local.cancel)
	t.Cleanup(reference.cancel)
	// round returns the collections the local calculation recalculated.
	round := func() []int64 {
		t.Helper()
		local.prepareLocalQuota()
		require.NotNil(t, local.localQuota)
		require.NoError(t, local.collectLocalQuotaData(context.Background(), typeutil.NewSet[int64](), nil, false))
		reference.clearMetrics()
		reference.dataCoordMetrics = local.dataCoordMetrics
		reference.queryNodeMetrics = local.queryNodeMetrics
		reference.dataNodeMetrics = local.dataNodeMetrics
		reference.readableCollections = local.readableCollections
		for id := range source.values {
			if coll := meta.collID2Meta[id]; coll != nil && coll.Available() {
				reference.collectionIDToDBID.Insert(id, coll.DBID)
			}
		}
		require.NoError(t, reference.calculateRates())
		require.NoError(t, local.calculateRates())
		require.Equal(t, quotaTreeSnapshot(reference.rateLimiter.GetRootLimiters()), quotaTreeSnapshot(local.rateLimiter.GetRootLimiters()))
		recalculated := slices.Sorted(maps.Keys(local.localQuota.dirty))
		require.NoError(t, local.sendRatesToProxy())
		requireQuotaRequestEqual(t, reference.toRatesRequest().RootLimiter, sent.RootLimiter)
		return recalculated
	}
	round()
	require.Empty(t, round())
	// A storage change must leave unrelated calculation nodes untouched.
	untouched := local.rateLimiter.GetCollectionLimiters(10, 101)
	source.put(100, 2*1024*1024)
	require.Equal(t, []int64{100}, round())
	require.Same(t, untouched, local.rateLimiter.GetCollectionLimiters(10, 101))
	source.put(100, 100)
	round()
	// A QueryNode's collections are recalculated only while it limits them:
	// memory pressure and release without segment changes.
	node := &metricsinfo.QueryNodeQuotaMetrics{Effect: metricsinfo.NodeEffect{CollectionIDs: []int64{100, 101}}, Hms: metricsinfo.HardwareMetrics{Memory: 100, MemoryUsage: 10}}
	local.queryNodeMetrics[1] = node
	require.Empty(t, round())
	node.Hms.MemoryUsage = 99
	require.Equal(t, []int64{100, 101}, round())
	node.Hms.MemoryUsage = 10
	require.Equal(t, []int64{100, 101}, round())
	require.Empty(t, round())
	// A neutral time tick factor is not a limit either.
	require.NoError(t, Params.Save(Params.QuotaConfig.TtProtectionEnabled.Key, "true"))
	local.dataNodeMetrics[1] = &metricsinfo.DataNodeQuotaMetrics{
		Effect: metricsinfo.NodeEffect{CollectionIDs: []int64{100, 101}},
		Fgm:    metricsinfo.FlowGraphMetric{MinFlowGraphChannel: "dml_0", MinFlowGraphTt: 100, NumFlowGraph: 1},
	}
	round() // A configuration change recalculates every collection.
	require.Empty(t, round())
	require.NoError(t, Params.Save(Params.QuotaConfig.TtProtectionEnabled.Key, "false"))
	delete(local.queryNodeMetrics, 1)
	delete(local.dataNodeMetrics, 1)
	round()
	// Limits found outside node reports still enter publication and are reset
	// after they disappear, even with no configuration/data change.
	require.NoError(t, Params.Save(Params.QuotaConfig.ForceDenyReading.Key, "true"))
	t.Cleanup(func() { _ = Params.Reset(Params.QuotaConfig.ForceDenyReading.Key) })
	round()
	local.readableCollections = map[int64]map[int64][]int64{10: {100: nil}}
	round()
	local.readableCollections = nil
	round()
	require.NoError(t, Params.Save(Params.QuotaConfig.ForceDenyReading.Key, "false"))
	// DB deny and release, including without new collection data.
	meta.ddLock.Lock()
	meta.dbName2Meta["quota"].Properties = []*commonpb.KeyValuePair{{Key: common.DatabaseForceDenyWritingKey, Value: "true"}}
	meta.quotaGeneration++
	meta.ddLock.Unlock()
	round()
	meta.ddLock.Lock()
	meta.dbName2Meta["quota"].Properties = nil
	meta.quotaGeneration++
	meta.ddLock.Unlock()
	round()
	// Changed collection properties and restoration of a default.
	meta.ddLock.Lock()
	meta.collID2Meta[100].Properties = []*commonpb.KeyValuePair{{Key: common.CollectionInsertRateMaxKey, Value: "3"}}
	meta.invalidateQuotaMetadataLocked(100)
	meta.ddLock.Unlock()
	round()
	meta.ddLock.Lock()
	meta.collID2Meta[100].Properties = nil
	meta.invalidateQuotaMetadataLocked(100)
	meta.ddLock.Unlock()
	round()
	// Proxy count changes must revisit every finite per-Proxy rate.
	proxyCount = 2
	round()
	local.prepareLocalQuota()
	require.NoError(t, local.calculateRates())
	beforeMembershipChange := sent
	proxyCount = 3
	require.ErrorIs(t, local.sendRatesToProxy(), merr.ErrServiceNotReady)
	require.Same(t, beforeMembershipChange, sent)
	round()
	// Explicit collection/partition deletion survives incremental publication.
	meta.ddLock.Lock()
	delete(meta.collID2Meta, 100)
	meta.invalidateQuotaMetadataLocked(100)
	meta.ddLock.Unlock()
	round()
	require.NotContains(t, sent.RootLimiter.Children[10].Children, int64(100))
	// Healthy segments for deleted collection still count in global quota.
	require.EqualValues(t, 300, local.dataCoordMetrics.TotalBinlogSize)
	// Roll back calculation alone, then reconstruct the local baseline.
	require.NoError(t, Params.Save(Params.QuotaConfig.LocalQuotaCalculationEnabled.Key, "false"))
	local.prepareLocalQuota()
	require.Nil(t, local.localQuota)
	require.NoError(t, local.calculateRates())
	require.NoError(t, local.sendRatesToProxy())
	require.NoError(t, Params.Save(Params.QuotaConfig.LocalQuotaCalculationEnabled.Key, "true"))
	round()
	// A disable/re-enable entirely between two cycles must also rebuild any
	// dropped metadata and reset DataCoord inputs to the replacement baseline.
	require.NoError(t, Params.Save(Params.QuotaConfig.LocalQuotaCalculationEnabled.Key, "false"))
	meta.ddLock.Lock()
	delete(meta.collID2Meta, 101)
	meta.invalidateQuotaMetadataLocked(101)
	meta.ddLock.Unlock()
	require.NoError(t, Params.Save(Params.QuotaConfig.LocalQuotaCalculationEnabled.Key, "true"))
	delete(source.values, 100)
	source.forceFull = true
	round()
	require.NotContains(t, local.localQuota.metadata, int64(101))
	require.NotContains(t, local.dataCoordMetrics.CollectionBinlogSize, int64(100))
	require.EqualValues(t, 200, local.dataCoordMetrics.TotalBinlogSize)
	// Removing a DB must also erase every descendant in the published FULL.
	meta.ddLock.Lock()
	for id := range meta.collID2Meta {
		delete(meta.collID2Meta, id)
		meta.invalidateQuotaMetadataLocked(id)
	}
	delete(meta.dbName2Meta, "quota")
	meta.quotaGeneration++
	meta.ddLock.Unlock()
	round()
	require.NotContains(t, sent.RootLimiter.Children, int64(10))

	// A calculation can fail after modifying a limiter. The next valid round
	// restores the baseline and nothing is sent during the failed round.
	meta.ddLock.Lock()
	replacement := quotaMetaForTest(1)
	meta.dbName2Meta["quota"] = replacement.dbName2Meta["quota"]
	meta.collID2Meta[100] = replacement.collID2Meta[100]
	meta.invalidateQuotaMetadataLocked(100)
	meta.ddLock.Unlock()
	source.put(100, 2*1024*1024)
	local.prepareLocalQuota()
	require.NoError(t, local.collectLocalQuotaData(context.Background(), typeutil.NewSet[int64](), nil, false))
	allocator.GenerateTSOF = func(uint32) (uint64, error) { return 0, merr.ErrServiceNotReady }
	lastComplete := sent
	require.ErrorIs(t, local.calculateRates(), merr.ErrServiceNotReady)
	limiter, _ := local.rateLimiter.GetCollectionLimiters(10, 100).GetLimiters().Get(internalpb.RateType_DMLInsert)
	require.Zero(t, limiter.Limit())
	require.Same(t, lastComplete, sent)
	allocator.GenerateTSOF = func(uint32) (uint64, error) { return 100, nil }
	source.put(100, 100)
	round()

	// A failed transport is retried with a full tree, even if no input changes.
	sendErr = merr.ErrServiceNotReady
	local.prepareLocalQuota()
	require.NoError(t, local.calculateRates())
	require.ErrorIs(t, local.sendRatesToProxy(), merr.ErrServiceNotReady)
	failed := sent
	sendErr = nil
	require.Empty(t, round())
	requireQuotaRequestEqual(t, failed.RootLimiter, sent.RootLimiter)
}

func TestQuotaLocalMetadataAcknowledgesGeneration(t *testing.T) {
	setQuotaTestParam(t, &Params.QuotaConfig.CollectionStatsCacheEnabled, "true")
	setQuotaTestParam(t, &Params.QuotaConfig.LocalQuotaCalculationEnabled, "true")
	mt := quotaMetaForTest(1)
	mt.ddLock.Lock()
	mt.invalidateQuotaMetadataLocked(100)
	mt.ddLock.Unlock()
	first := mt.readQuotaMetadata(true)
	mt.ddLock.Lock()
	mt.invalidateQuotaMetadataLocked(100)
	mt.ddLock.Unlock()
	mt.acknowledgeQuotaMetadata(first)
	require.Contains(t, mt.readQuotaMetadata(false).Collections, int64(100))
	mt.acknowledgeQuotaMetadata(mt.readQuotaMetadata(false))
	require.Empty(t, mt.readQuotaMetadata(false).Collections)
}

func requireQuotaRequestEqual(t *testing.T, want, got *proxypb.LimiterNode) {
	t.Helper()
	require.NotNil(t, got)
	type quotaState struct {
		code   commonpb.ErrorCode
		reason string
	}
	stateInfo := func(l *proxypb.Limiter) map[milvuspb.QuotaState]quotaState {
		states := make(map[milvuspb.QuotaState]quotaState, len(l.GetStates()))
		for i, state := range l.GetStates() {
			states[state] = quotaState{l.Codes[i], l.Reasons[i]}
		}
		return states
	}
	require.Equal(t, stateInfo(want.GetLimiter()), stateInfo(got.GetLimiter()))
	require.Len(t, got.GetLimiter().GetRates(), len(want.GetLimiter().GetRates()))
	rates := make(map[internalpb.RateType]float64, len(want.GetLimiter().GetRates()))
	for _, rate := range want.GetLimiter().GetRates() {
		rates[rate.GetRt()] = rate.GetR()
	}
	for _, rate := range got.GetLimiter().GetRates() {
		require.Contains(t, rates, rate.GetRt())
		require.Equal(t, rates[rate.GetRt()], rate.GetR())
	}
	require.Len(t, got.GetChildren(), len(want.GetChildren()))
	for id, child := range want.GetChildren() {
		requireQuotaRequestEqual(t, child, got.GetChildren()[id])
	}
}

func BenchmarkQuotaLocalIdle(b *testing.B) {
	setQuotaTestParam(b, &Params.QuotaConfig.CollectionStatsCacheEnabled, "true")
	setQuotaTestParam(b, &Params.QuotaConfig.LocalQuotaCalculationEnabled, "true")
	for _, count := range []int{1000, 100000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			manager := proxyutil.NewProxyClientManager(nil)
			manager.GetProxyClients().Insert(1, nil)
			allocator := newMockTsoAllocator()
			allocator.GenerateTSOF = func(uint32) (uint64, error) { return 100, nil }
			meta := quotaMetaForTest(count)
			q := NewQuotaCenter(manager, &localQuotaTestSource{}, allocator, meta)
			b.Cleanup(q.cancel)
			// A QueryNode below its memory water levels reports every collection.
			q.queryNodeMetrics[1] = &metricsinfo.QueryNodeQuotaMetrics{Effect: metricsinfo.NodeEffect{CollectionIDs: slices.Collect(maps.Keys(meta.collID2Meta))}, Hms: metricsinfo.HardwareMetrics{Memory: 100, MemoryUsage: 10}}
			q.prepareLocalQuota()
			require.NoError(b, q.calculateRates())
			q.localQuota.proxyCount = q.localQuota.calculationProxyCount
			clear(q.localQuota.dirty)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				q.prepareLocalQuota()
				if err := q.calculateRates(); err != nil {
					b.Fatal(err)
				}
				if len(q.localQuota.dirty) != 0 {
					b.Fatal("unchanged calculation reconsidered collections")
				}
			}
		})
	}
}

type quotaSyncTestClient struct {
	types.ProxyClient
	receiver *proxy.SimpleLimiter
	sent     []*proxypb.SetRatesRequest
}

func (p *quotaSyncTestClient) GetProxyMetrics(context.Context, *milvuspb.GetMetricsRequest, ...grpc.CallOption) (*milvuspb.GetMetricsResponse, error) {
	return &milvuspb.GetMetricsResponse{Status: p.receiver.QuotaSyncStatus()}, nil
}

func (p *quotaSyncTestClient) SetRates(_ context.Context, request *proxypb.SetRatesRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
	p.sent = append(p.sent, proto.Clone(request).(*proxypb.SetRatesRequest))
	return p.receiver.ApplyQuotaRequest(request), nil
}

func TestQuotaLocalVersionedPublication(t *testing.T) {
	setQuotaTestParam(t, &Params.QuotaConfig.QuotaAndLimitsEnabled, "true")
	setQuotaTestParam(t, &Params.QuotaConfig.CollectionStatsCacheEnabled, "true")
	setQuotaTestParam(t, &Params.QuotaConfig.LocalQuotaCalculationEnabled, "true")
	setQuotaTestParam(t, &Params.QuotaConfig.VersionedRatesEnabled, "true")
	setQuotaTestParam(t, &Params.QuotaConfig.IncrementalRatesEnabled, "true")
	setQuotaTestParam(t, &Params.QuotaConfig.DiskProtectionEnabled, "true")
	setQuotaTestParam(t, &Params.QuotaConfig.DiskQuotaPerCollection, "1")
	setQuotaTestParam(t, &Params.QuotaConfig.TtProtectionEnabled, "false")

	source := &localQuotaTestSource{
		values:  make(map[int64]*metricsinfo.CollectionQuotaSnapshot),
		changes: make(map[int64]*metricsinfo.CollectionQuotaSnapshot),
	}
	source.put(100, 100)
	source.put(101, 100)
	manager := proxyutil.NewProxyClientManager(nil)
	client := &quotaSyncTestClient{receiver: proxy.NewSimpleLimiter(0, 0)}
	manager.GetProxyClients().Insert(1, client)
	allocator := newMockTsoAllocator()
	allocator.GenerateTSOF = func(uint32) (uint64, error) { return 100, nil }
	q := NewQuotaCenter(manager, source, allocator, quotaMetaForTest(2))
	t.Cleanup(q.cancel)

	calculate := func() error {
		t.Helper()
		q.prepareLocalQuota()
		require.NoError(t, q.collectLocalQuotaData(context.Background(), typeutil.NewSet[int64](), nil, false))
		return q.calculateRates()
	}
	round := func() {
		t.Helper()
		require.NoError(t, calculate())
		require.NoError(t, q.sendRatesToProxy())
	}
	round()
	require.Len(t, client.sent, 1)
	require.Equal(t, proxypb.QuotaSyncMode_QUOTA_FULL, client.sent[0].GetQuotaMode())
	require.EqualValues(t, quotasync.Protocol, client.sent[0].GetQuotaProtocol())
	round()
	require.Len(t, client.sent, 1)

	source.put(100, 2*1024*1024)
	round()
	require.Len(t, client.sent, 2)
	delta := client.sent[1]
	require.Equal(t, proxypb.QuotaSyncMode_QUOTA_DELTA, delta.GetQuotaMode())
	require.Len(t, delta.GetUpserts(), 1)
	require.EqualValues(t, 100, delta.GetUpserts()[0].GetKey().GetCollectionId())
	check := func(coll int64) error {
		return client.receiver.Check(10, map[int64][]int64{coll: {coll * 10}}, internalpb.RateType_DMLInsert, 1)
	}
	require.ErrorContains(t, check(100), "disk quota exceeded")
	require.NoError(t, check(101))

	// A calculation failure after resetting the live tree must never publish
	// that candidate. A restarted Proxy can still install the last complete
	// policy, then receive the released limit after calculation recovers.
	source.put(100, 100)
	allocator.GenerateTSOF = func(uint32) (uint64, error) { return 0, merr.ErrServiceNotReady }
	require.ErrorIs(t, calculate(), merr.ErrServiceNotReady)
	client.receiver = proxy.NewSimpleLimiter(0, 0)
	require.ErrorIs(t, check(100), merr.ErrServiceNotReady)
	q.retryPublishedRates()
	require.Len(t, client.sent, 3)
	require.Equal(t, proxypb.QuotaSyncMode_QUOTA_FULL, client.sent[2].GetQuotaMode())
	require.True(t, proto.Equal(delta.GetTargetVersion(), client.sent[2].GetTargetVersion()))
	require.ErrorContains(t, check(100), "disk quota exceeded")
	allocator.GenerateTSOF = func(uint32) (uint64, error) { return 100, nil }
	round()
	require.Len(t, client.sent, 4)
	require.Equal(t, proxypb.QuotaSyncMode_QUOTA_DELTA, client.sent[3].GetQuotaMode())
	require.NoError(t, check(100))
}
