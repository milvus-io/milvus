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
	"sync"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/quotasync"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
)

type QuotaSyncResult struct {
	Mode, Result string
	Err          error
}

// SyncQuota reconciles every Proxy independently, even when rule collection
// failed or no rules changed. It retains capability only for this client session;
// it never stores history, pending batches, or per-Proxy policy snapshots.
func (p *ProxyClientManager) SyncQuota(ctx context.Context, snapshot *quotasync.Publisher, incremental bool, legacy func() *proxypb.SetRatesRequest) map[int64]QuotaSyncResult {
	var fullOnce, legacyOnce sync.Once
	var fullRequest, legacyRequest *proxypb.SetRatesRequest
	full := func() *proxypb.SetRatesRequest {
		fullOnce.Do(func() {
			fullRequest = snapshot.Full()
			metrics.RootCoordQuotaSnapshotBytes.WithLabelValues("full").Observe(float64(proto.Size(fullRequest)))
		})
		return fullRequest
	}
	legacyFull := func() *proxypb.SetRatesRequest {
		legacyOnce.Do(func() { legacyRequest = legacy() })
		return legacyRequest
	}
	results := make(map[int64]QuotaSyncResult)
	var mu sync.Mutex
	group := &errgroup.Group{}
	p.proxyClient.Range(func(id int64, client types.ProxyClient) bool {
		group.Go(func() error {
			result := p.syncOneQuota(ctx, client, snapshot, incremental, full, legacyFull)
			mu.Lock()
			results[id] = result
			mu.Unlock()
			return nil
		})
		return true
	})
	_ = group.Wait()
	// A client can disappear while its RPC is in flight. Drop any capability
	// discovered after DelProxyClient removed that session.
	live := make(map[types.ProxyClient]struct{})
	p.proxyClient.Range(func(_ int64, client types.ProxyClient) bool {
		live[client] = struct{}{}
		return true
	})
	p.quotaSessions.Range(func(client, _ any) bool {
		if _, ok := live[client.(types.ProxyClient)]; !ok {
			p.quotaSessions.Delete(client)
		}
		return true
	})
	return results
}

func (p *ProxyClientManager) syncOneQuota(ctx context.Context, client types.ProxyClient, snapshot *quotasync.Publisher, incremental bool, full, legacy func() *proxypb.SetRatesRequest) QuotaSyncResult {
	known, _ := p.quotaSessions.Load(client)
	session, _ := known.(string)
	// This metric type is read-only even on an old Proxy. Never probe an old
	// SetRates endpoint with an empty root: it would reset its live limiters.
	response, err := client.GetProxyMetrics(ctx, &milvuspb.GetMetricsRequest{
		Base:    commonpbutil.NewMsgBase(),
		Request: `{"metric_type":"quota_sync"}`,
	})
	_, reported, version, supported := quotasync.ParseStatus(response.GetStatus())
	if err == nil && supported {
		session = reported
		p.quotaSessions.Store(client, session)
	} else if session == "" {
		// Explicit capability discovery; an RPC error is not evidence of legacy support.
		request, requestErr := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
		if requestErr != nil {
			return QuotaSyncResult{Err: requestErr}
		}
		normal, normalErr := client.GetProxyMetrics(ctx, request)
		if normalErr != nil {
			return QuotaSyncResult{Err: normalErr}
		}
		if normal == nil || normal.GetStatus() == nil {
			return QuotaSyncResult{Err: merr.WrapErrServiceInternalMsg("proxy capability response is empty")}
		}
		if statusErr := merr.Error(normal.GetStatus()); statusErr != nil {
			return QuotaSyncResult{Err: statusErr}
		}
		_, session, version, supported = quotasync.ParseStatus(normal.GetStatus())
		if !supported {
			status, sendErr := client.SetRates(ctx, legacy())
			if sendErr == nil {
				if status == nil {
					sendErr = merr.WrapErrServiceInternalMsg("legacy quota response is empty")
				} else {
					sendErr = merr.Error(status)
				}
			}
			return QuotaSyncResult{Mode: "legacy", Result: "full", Err: sendErr}
		}
		p.quotaSessions.Store(client, session)
	} else {
		version = quotasync.Version{}
	} // Known capability cannot downgrade on a failed sample.
	if version == snapshot.Version {
		return QuotaSyncResult{Result: quotasync.AlreadyApplied}
	}
	if version.Compare(snapshot.Version) > 0 {
		return QuotaSyncResult{Result: quotasync.Stale, Err: merr.WrapErrServiceInternalMsg("proxy quota version is newer than publisher")}
	}
	request := (*proxypb.SetRatesRequest)(nil)
	mode := "full"
	if incremental && snapshot.Delta != nil && version == quotasync.FromVersion(snapshot.Delta.BaseVersion) {
		request = snapshot.Delta
		mode = "delta"
	} else {
		request = full()
	}
	send := func(req *proxypb.SetRatesRequest) (string, string, quotasync.Version, error) {
		// Only the small envelope is per-Proxy; the policy payload stays immutable.
		envelope := &proxypb.SetRatesRequest{
			Base:          commonpbutil.NewMsgBase(),
			RootLimiter:   req.RootLimiter,
			QuotaProtocol: req.QuotaProtocol,
			QuotaMode:     req.QuotaMode,
			TargetVersion: req.TargetVersion,
			BaseVersion:   req.BaseVersion,
			Upserts:       req.Upserts,
			Deletes:       req.Deletes,
			ScopeDefaults: req.ScopeDefaults,
			QuotaSession:  session,
		}
		status, sendErr := client.SetRates(ctx, envelope)
		if sendErr != nil {
			return "", "", quotasync.Version{}, sendErr
		}
		if statusErr := merr.Error(status); statusErr != nil {
			return "", "", quotasync.Version{}, statusErr
		}
		result, reported, applied, ok := quotasync.ParseStatus(status)
		if !ok {
			return "", "", quotasync.Version{}, merr.WrapErrServiceInternalMsg("versioned quota response lacks synchronization status")
		}
		return result, reported, applied, nil
	}
	result, reported, applied, sendErr := send(request)
	if sendErr == nil && result == quotasync.NeedFull {
		// A restart between sample and apply returns the new process identity.
		session = reported
		p.quotaSessions.Store(client, session)
		mode = "full"
		result, reported, applied, sendErr = send(full())
	}
	if sendErr == nil && (reported != session || applied != snapshot.Version || (result != quotasync.Applied && result != quotasync.AlreadyApplied)) {
		sendErr = merr.WrapErrServiceNotReadyMsg("quota synchronization did not install target version: %s", result)
	}
	return QuotaSyncResult{Mode: mode, Result: result, Err: sendErr}
}
