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

package proxy

import (
	"fmt"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/quotasync"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/ratelimitutil"
)

func quotaSyncFixture(t testing.TB) (*SimpleLimiter, *quotasync.Publisher, quotasync.Key) {
	t.Helper()
	paramtable.Init()
	for _, key := range []string{Params.QuotaConfig.VersionedRatesEnabled.Key, Params.QuotaConfig.QuotaAndLimitsEnabled.Key} {
		require.NoError(t, Params.Save(key, "true"))
		t.Cleanup(func() { _ = Params.Reset(key) })
	}
	limiter := NewSimpleLimiter(0, 0)
	defaults := make(map[int32]*proxypb.Limiter)
	for _, scope := range []internalpb.RateScope{internalpb.RateScope_Cluster, internalpb.RateScope_Database, internalpb.RateScope_Collection, internalpb.RateScope_Partition} {
		p := &proxypb.Limiter{}
		for rt := range getDefaultLimiterConfig(scope) {
			p.Rates = append(p.Rates, &internalpb.Rate{Rt: rt, R: float64(ratelimitutil.Inf)})
		}
		defaults[int32(scope)] = quotasync.Canonical(p)
	}
	key := quotasync.Key{Scope: internalpb.RateScope_Collection, DB: 1, Collection: 100}
	publisher := &quotasync.Publisher{}
	publisher.Publish(10, map[quotasync.Key]*proxypb.Limiter{
		{Scope: internalpb.RateScope_Cluster}: {}, {Scope: internalpb.RateScope_Database, DB: 1}: {}, key: {},
	}, defaults)
	return limiter, publisher, key
}

func applyQuotaTest(t testing.TB, m *SimpleLimiter, req *proxypb.SetRatesRequest, result string) {
	t.Helper()
	req = proto.Clone(req).(*proxypb.SetRatesRequest)
	req.QuotaSession = m.quotaSync.session
	status := m.ApplyQuotaRequest(req)
	require.NoError(t, merr.Error(status))
	actual, _, _, ok := quotasync.ParseStatus(status)
	require.True(t, ok)
	require.Equal(t, result, actual)
}

func denyQuotaPolicy() *proxypb.Limiter {
	return &proxypb.Limiter{Rates: []*internalpb.Rate{{Rt: internalpb.RateType_DMLInsert, R: 0}}, States: []milvuspb.QuotaState{milvuspb.QuotaState_DenyToWrite}, Codes: []commonpb.ErrorCode{commonpb.ErrorCode_DiskQuotaExhausted}}
}

func TestQuotaSyncLossFallbackAndRelease(t *testing.T) {
	limiter, publisher, key := quotaSyncFixture(t)
	require.ErrorIs(t, limiter.Check(1, map[int64][]int64{100: nil}, internalpb.RateType_DMLInsert, 1), merr.ErrServiceNotReady)
	first := publisher.Full()
	applyQuotaTest(t, limiter, first, quotasync.Applied)
	require.NoError(t, limiter.Check(1, map[int64][]int64{100: nil}, internalpb.RateType_DMLInsert, 1))
	// Lose the update to A, then create an independent B update.
	publisher.PublishPatch(10, map[quotasync.Key]*proxypb.Limiter{key: denyQuotaPolicy()}, nil, publisher.Defaults)
	lost := publisher.Delta
	keyB := key
	keyB.Collection = 101
	publisher.PublishPatch(10, map[quotasync.Key]*proxypb.Limiter{keyB: denyQuotaPolicy()}, nil, publisher.Defaults)
	applyQuotaTest(t, limiter, publisher.Delta, quotasync.NeedFull)
	require.Equal(t, quotasync.FromVersion(first.TargetVersion), limiter.quotaSync.version)
	applyQuotaTest(t, limiter, publisher.Full(), quotasync.Applied)
	for _, id := range []int64{100, 101} {
		require.ErrorIs(t, limiter.Check(1, map[int64][]int64{id: nil}, internalpb.RateType_DMLInsert, 1), merr.ErrServiceQuotaExceeded)
	}
	applyQuotaTest(t, limiter, lost, quotasync.Stale)
	applyQuotaTest(t, limiter, first, quotasync.Stale)
	// Empty complete policy removes the deny and restores Inf.
	publisher.PublishPatch(10, map[quotasync.Key]*proxypb.Limiter{key: {}}, nil, publisher.Defaults)
	applyQuotaTest(t, limiter, publisher.Delta, quotasync.Applied)
	require.NoError(t, limiter.Check(1, map[int64][]int64{100: nil}, internalpb.RateType_DMLInsert, 1))
	// Lost final request: no-change publication retains exactly the same delta.
	publisher.PublishPatch(10, map[quotasync.Key]*proxypb.Limiter{key: denyQuotaPolicy()}, nil, publisher.Defaults)
	last := publisher.Delta
	version := publisher.Version
	require.False(t, publisher.PublishPatch(10, map[quotasync.Key]*proxypb.Limiter{key: denyQuotaPolicy()}, nil, publisher.Defaults))
	require.Same(t, last, publisher.Delta)
	require.Equal(t, version, publisher.Version)
	applyQuotaTest(t, limiter, last, quotasync.Applied)
	applyQuotaTest(t, limiter, last, quotasync.AlreadyApplied)
	// A lost delete is repaired by FULL, including previous controlled policy.
	publisher.PublishPatch(10, nil, map[quotasync.Key]bool{key: true}, publisher.Defaults)
	publisher.PublishPatch(10, map[quotasync.Key]*proxypb.Limiter{keyB: {}}, nil, publisher.Defaults)
	applyQuotaTest(t, limiter, publisher.Full(), quotasync.Applied)
	require.NoError(t, limiter.Check(1, map[int64][]int64{100: nil}, internalpb.RateType_DMLInsert, 1))
}

func TestQuotaSyncValidationRestartAndTokens(t *testing.T) {
	limiter, publisher, key := quotaSyncFixture(t)
	publisher.Policies[key] = &proxypb.Limiter{Rates: []*internalpb.Rate{{Rt: internalpb.RateType_DMLInsert, R: 100}}}
	first := publisher.Full()
	applyQuotaTest(t, limiter, first, quotasync.Applied)
	node := limiter.quotaNode(key, false)
	bucket, ok := node.GetLimiters().Get(internalpb.RateType_DMLInsert)
	require.True(t, ok)
	require.True(t, bucket.AllowN(time.Now(), 1000))
	require.False(t, bucket.AllowN(time.Now(), 1))
	applyQuotaTest(t, limiter, first, quotasync.AlreadyApplied)
	current, _ := node.GetLimiters().Get(internalpb.RateType_DMLInsert)
	require.Same(t, bucket, current)
	require.False(t, bucket.AllowN(time.Now(), 1))
	// Malformed second upsert cannot partially apply the first.
	publisher.PublishPatch(10, map[quotasync.Key]*proxypb.Limiter{key: denyQuotaPolicy()}, nil, publisher.Defaults)
	invalid := proto.Clone(publisher.Delta).(*proxypb.SetRatesRequest)
	invalid.Upserts = append(invalid.Upserts, &proxypb.QuotaScopePolicy{Key: (&quotasync.Key{Scope: internalpb.RateScope_Collection, DB: 1, Collection: 102}).Proto(), Limiter: &proxypb.Limiter{Rates: []*internalpb.Rate{{Rt: internalpb.RateType(999), R: 0}}}})
	invalid.QuotaSession = limiter.quotaSync.session
	require.ErrorIs(t, merr.Error(limiter.ApplyQuotaRequest(invalid)), merr.ErrServiceInternal)
	require.EqualValues(t, 100, bucket.Limit())
	require.Equal(t, quotasync.FromVersion(first.TargetVersion), limiter.quotaSync.version)
	// New epoch cannot start with DELTA. Older epoch FULL cannot roll it back.
	newEpoch := proto.Clone(publisher.Delta).(*proxypb.SetRatesRequest)
	newEpoch.TargetVersion.Epoch = 11
	applyQuotaTest(t, limiter, newEpoch, quotasync.NeedFull)
	next := publisher.Full()
	next.TargetVersion = &proxypb.QuotaRuleVersion{Epoch: 11, Seq: 1}
	applyQuotaTest(t, limiter, next, quotasync.Applied)
	applyQuotaTest(t, limiter, first, quotasync.Stale)
	legacy := &proxypb.SetRatesRequest{RootLimiter: &proxypb.LimiterNode{}}
	result, _, _, _ := quotasync.ParseStatus(limiter.ApplyQuotaRequest(legacy))
	require.Equal(t, quotasync.Stale, result)
	restarted := NewSimpleLimiter(0, 0)
	require.NotEqual(t, limiter.quotaSync.session, restarted.quotaSync.session)
	applyQuotaTest(t, restarted, publisher.Delta, quotasync.NeedFull)
	wrongSession := proto.Clone(next).(*proxypb.SetRatesRequest)
	wrongSession.QuotaSession = limiter.quotaSync.session
	result, _, _, _ = quotasync.ParseStatus(restarted.ApplyQuotaRequest(wrongSession))
	require.Equal(t, quotasync.NeedFull, result)
	applyQuotaTest(t, restarted, next, quotasync.Applied)
}

func TestQuotaSyncDefaultsAndConcurrentChecks(t *testing.T) {
	limiter, publisher, key := quotaSyncFixture(t)
	defaults := maps.Clone(publisher.Defaults)
	defaults[int32(internalpb.RateScope_Cluster)] = proto.Clone(defaults[int32(internalpb.RateScope_Cluster)]).(*proxypb.Limiter)
	for _, rate := range defaults[int32(internalpb.RateScope_Cluster)].Rates {
		if rate.Rt == internalpb.RateType_DMLInsert {
			rate.R = 1
		}
	}
	defaults[int32(key.Scope)] = proto.Clone(defaults[int32(key.Scope)]).(*proxypb.Limiter)
	for _, rate := range defaults[int32(key.Scope)].Rates {
		if rate.Rt == internalpb.RateType_DMLInsert {
			rate.R = 50
		}
	}
	publisher.PublishPatch(10, map[quotasync.Key]*proxypb.Limiter{key: denyQuotaPolicy()}, nil, defaults)
	applyQuotaTest(t, limiter, publisher.Full(), quotasync.Applied)
	bucket, _ := limiter.quotaNode(key, false).GetLimiters().Get(internalpb.RateType_DMLInsert)
	require.Zero(t, bucket.Limit())
	for i := 0; i < 10; i++ {
		require.ErrorIs(t, limiter.Check(1, map[int64][]int64{100: nil}, internalpb.RateType_DMLInsert, 1), merr.ErrServiceQuotaExceeded)
	}
	publisher.PublishPatch(10, map[quotasync.Key]*proxypb.Limiter{key: {}}, nil, defaults)
	applyQuotaTest(t, limiter, publisher.Delta, quotasync.Applied)
	require.EqualValues(t, 50, bucket.Limit())
	lazy := key
	lazy.DB = 99
	lazy.Collection = 999
	limiter.Check(lazy.DB, map[int64][]int64{lazy.Collection: nil}, internalpb.RateType_DMLInsert, 1)
	lazyBucket, _ := limiter.quotaNode(lazy, false).GetLimiters().Get(internalpb.RateType_DMLInsert)
	require.EqualValues(t, 50, lazyBucket.Limit())
	// A request-created parent cannot authorize an incomplete controller delta.
	invalid := &proxypb.SetRatesRequest{
		QuotaProtocol: quotasync.Protocol, QuotaMode: proxypb.QuotaSyncMode_QUOTA_DELTA, QuotaSession: limiter.quotaSync.session,
		BaseVersion: publisher.Version.Proto(), TargetVersion: (&quotasync.Version{Epoch: 10, Seq: publisher.Version.Seq + 1}).Proto(),
		Upserts: []*proxypb.QuotaScopePolicy{{Key: lazy.Proto(), Limiter: denyQuotaPolicy()}},
	}
	require.ErrorIs(t, merr.Error(limiter.ApplyQuotaRequest(invalid)), merr.ErrServiceInternal)
	var checks sync.WaitGroup
	checks.Add(1)
	go func() {
		defer checks.Done()
		for i := 0; i < 1000; i++ {
			// A later denied scope refunds earlier buckets during concurrent apply.
			limiter.Check(1, map[int64][]int64{100: {1000}}, internalpb.RateType_DMLInsert, 1)
			limiter.QuotaSyncStatus()
		}
	}()
	for i := 0; i < 100; i++ {
		policy := denyQuotaPolicy()
		if i%2 == 1 {
			policy = &proxypb.Limiter{}
		}
		publisher.PublishPatch(10, map[quotasync.Key]*proxypb.Limiter{key: policy}, nil, defaults)
		applyQuotaTest(t, limiter, publisher.Full(), quotasync.Applied)
	}
	checks.Wait()
	current, _ := limiter.quotaNode(key, false).GetLimiters().Get(internalpb.RateType_DMLInsert)
	require.Same(t, bucket, current)
}

func BenchmarkQuotaFullApply(b *testing.B) {
	previous := metrics.CollectionLevelMetricsMode()
	metrics.SetCollectionLevelMetricsMode(metrics.CollectionLevelMetricsModeAggregate)
	b.Cleanup(func() { metrics.SetCollectionLevelMetricsMode(previous) })
	for _, count := range []int{1000, 1050104} {
		for _, finite := range []bool{false, true} {
			b.Run(fmt.Sprintf("collections=%d/finite=%t", count, finite), func(b *testing.B) {
				limiter, publisher, key := quotaSyncFixture(b)
				policy := &proxypb.Limiter{}
				if finite {
					for rt := range getDefaultLimiterConfig(internalpb.RateScope_Collection) {
						policy.Rates = append(policy.Rates, &internalpb.Rate{Rt: rt, R: 100})
					}
				}
				delete(publisher.Policies, key)
				for i := 0; i < count; i++ {
					key.Collection = 450000000000000000 + int64(i)
					publisher.Policies[key] = policy
				}
				request := publisher.Full()
				request.QuotaSession = limiter.quotaSync.session
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					request.TargetVersion.Seq = uint64(i + 1)
					status := limiter.ApplyQuotaRequest(request)
					if result, _, _, _ := quotasync.ParseStatus(status); result != quotasync.Applied {
						b.Fatalf("full snapshot not applied: %v", status)
					}
				}
			})
		}
	}
}
