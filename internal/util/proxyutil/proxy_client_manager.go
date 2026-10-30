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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	grpcproxyclient "github.com/milvus-io/milvus/internal/distributed/proxy/client"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type ExpireCacheConfig struct {
	msgType commonpb.MsgType
}

func (c ExpireCacheConfig) Apply(req *proxypb.InvalidateCollMetaCacheRequest) {
	if req.GetBase() == nil {
		req.Base = commonpbutil.NewMsgBase()
	}
	req.Base.MsgType = c.msgType
}

func DefaultExpireCacheConfig() ExpireCacheConfig {
	return ExpireCacheConfig{}
}

type ExpireCacheOpt func(c *ExpireCacheConfig)

func SetMsgType(msgType commonpb.MsgType) ExpireCacheOpt {
	return func(c *ExpireCacheConfig) {
		c.msgType = msgType
	}
}

type ProxyCreator func(ctx context.Context, addr string, nodeID int64) (types.ProxyClient, error)

func DefaultProxyCreator(ctx context.Context, addr string, nodeID int64) (types.ProxyClient, error) {
	cli, err := grpcproxyclient.NewClient(ctx, addr, nodeID)
	if err != nil {
		return nil, err
	}
	return cli, nil
}

type ProxyClientManagerHelper struct {
	afterConnect func()
}

var defaultClientManagerHelper = ProxyClientManagerHelper{
	afterConnect: func() {},
}

type ProxyClientManagerInterface interface {
	AddProxyClient(session *sessionutil.Session)
	SetProxyClients(session []*sessionutil.Session)
	GetProxyClients() *typeutil.ConcurrentMap[int64, types.ProxyClient]
	DelProxyClient(s *sessionutil.Session)
	GetProxyCount() int

	InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest, opts ...ExpireCacheOpt) error
	InvalidateShardLeaderCache(ctx context.Context, request *proxypb.InvalidateShardLeaderCacheRequest) error
	InvalidateCredentialCache(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) error
	UpdateCredentialCache(ctx context.Context, request *proxypb.UpdateCredCacheRequest) error
	RefreshPolicyInfoCache(ctx context.Context, req *proxypb.RefreshPolicyInfoCacheRequest) error
	GetProxyMetrics(ctx context.Context) ([]*milvuspb.GetMetricsResponse, error)
	SetRates(ctx context.Context, request *proxypb.SetRatesRequest) error
	ClearReadTaskQueue(ctx context.Context, request *internalpb.ClearReadTaskQueueRequest) ([]*internalpb.ClearReadTaskQueueComponentResult, error)
	ListRunningRequests(ctx context.Context, request *milvuspb.ListRunningRequestsRequest) ([]*milvuspb.RunningRequestInfo, []*milvuspb.RunningRequestNodeResult, error)
	CancelRequests(ctx context.Context, request *milvuspb.CancelRequestsRequest) (CancelResult, error)
	GetComponentStates(ctx context.Context) (map[int64]*milvuspb.ComponentStates, error)
}

// RunningRequestTimeout bounds a running-request fan-out. Without it one
// wedged proxy would hold the caller's request open for as long as its own
// deadline allows; the other proxies' answers are worth more than waiting.
const RunningRequestTimeout = 10 * time.Second

type ProxyClientManager struct {
	creator     ProxyCreator
	proxyClient *typeutil.ConcurrentMap[int64, types.ProxyClient]
	helper      ProxyClientManagerHelper
}

func NewProxyClientManager(creator ProxyCreator) *ProxyClientManager {
	return &ProxyClientManager{
		creator:     creator,
		proxyClient: typeutil.NewConcurrentMap[int64, types.ProxyClient](),
		helper:      defaultClientManagerHelper,
	}
}

// SetProxyClients sets proxy clients from a full snapshot of sessions.
// It removes stale clients not in the new snapshot and adds new ones.
// This is called during initial setup or when re-watching after etcd error.
func (p *ProxyClientManager) SetProxyClients(sessions []*sessionutil.Session) {
	aliveSessions := lo.KeyBy(sessions, func(session *sessionutil.Session) int64 {
		return session.ServerID
	})

	// Remove stale clients not in the alive sessions
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		if _, ok := aliveSessions[key]; !ok {
			if cli, loaded := p.proxyClient.GetAndRemove(key); loaded {
				cli.Close()
				mlog.Info(context.TODO(), "remove stale proxy client", mlog.Int64("serverID", key))
			}
		}
		return true
	})

	// Add new clients
	for _, session := range sessions {
		p.AddProxyClient(session)
	}
}

func (p *ProxyClientManager) GetProxyClients() *typeutil.ConcurrentMap[int64, types.ProxyClient] {
	return p.proxyClient
}

func (p *ProxyClientManager) AddProxyClient(session *sessionutil.Session) {
	_, ok := p.proxyClient.Get(session.ServerID)
	if ok {
		return
	}

	p.connect(session)
	p.updateProxyNumMetric()
}

// GetProxyCount returns number of proxy clients.
func (p *ProxyClientManager) GetProxyCount() int {
	return p.proxyClient.Len()
}

// mutex.Lock is required before calling this method.
func (p *ProxyClientManager) updateProxyNumMetric() {
	metrics.RootCoordProxyCounter.WithLabelValues().Set(float64(p.proxyClient.Len()))
}

func (p *ProxyClientManager) connect(session *sessionutil.Session) {
	pc, err := p.creator(context.Background(), session.Address, session.ServerID)
	if err != nil {
		mlog.Warn(context.TODO(), "failed to create proxy client", mlog.String("address", session.Address), mlog.Int64("serverID", session.ServerID), mlog.Err(err))
		return
	}

	_, ok := p.proxyClient.GetOrInsert(session.GetServerID(), pc)
	if ok {
		pc.Close()
		return
	}
	mlog.Info(context.TODO(), "succeed to create proxy client", mlog.String("address", session.Address), mlog.Int64("serverID", session.ServerID))
	p.helper.afterConnect()
}

func (p *ProxyClientManager) DelProxyClient(s *sessionutil.Session) {
	cli, ok := p.proxyClient.GetAndRemove(s.GetServerID())
	if ok {
		cli.Close()
	}

	p.updateProxyNumMetric()
	mlog.Info(context.TODO(), "remove proxy client", mlog.String("proxy address", s.Address), mlog.Int64("proxy id", s.ServerID))
}

func (p *ProxyClientManager) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest, opts ...ExpireCacheOpt) error {
	c := DefaultExpireCacheConfig()
	for _, opt := range opts {
		opt(&c)
	}
	c.Apply(request)
	invalidate := func(proxyID int64, client types.ProxyClient) error {
		sta, err := client.InvalidateCollectionMetaCache(ctx, request)
		if err != nil {
			if errors.Is(err, merr.ErrNodeNotFound) {
				mlog.Warn(ctx, "InvalidateCollectionMetaCache failed due to proxy service not found", mlog.Err(err))
				return nil
			}

			if errors.Is(err, merr.ErrServiceUnimplemented) {
				return nil
			}

			return merr.Wrapf(err, "InvalidateCollectionMetaCache failed, proxyID = %d", proxyID)
		}
		if sta.ErrorCode != commonpb.ErrorCode_Success {
			return merr.Wrapf(merr.Error(sta), "InvalidateCollectionMetaCache failed, proxyID = %d", proxyID)
		}
		return nil
	}

	if p.proxyClient.Len() == 0 {
		mlog.Warn(ctx, "proxy client is empty, InvalidateCollectionMetaCache will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		k, v := key, value
		group.Go(func() error {
			return invalidate(k, v)
		})
		return true
	})
	return group.Wait()
}

// InvalidateCredentialCache TODO: too many codes similar to InvalidateCollectionMetaCache.
func (p *ProxyClientManager) InvalidateCredentialCache(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) error {
	if p.proxyClient.Len() == 0 {
		mlog.Warn(ctx, "proxy client is empty, InvalidateCredentialCache will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		k, v := key, value
		group.Go(func() error {
			sta, err := v.InvalidateCredentialCache(ctx, request)
			if err != nil {
				return merr.Wrapf(err, "InvalidateCredentialCache failed, proxyID = %d", k)
			}
			if sta.ErrorCode != commonpb.ErrorCode_Success {
				return merr.Wrapf(merr.Error(sta), "InvalidateCredentialCache failed, proxyID = %d", k)
			}
			return nil
		})
		return true
	})

	return group.Wait()
}

// UpdateCredentialCache TODO: too many codes similar to InvalidateCollectionMetaCache.
func (p *ProxyClientManager) UpdateCredentialCache(ctx context.Context, request *proxypb.UpdateCredCacheRequest) error {
	if p.proxyClient.Len() == 0 {
		mlog.Warn(ctx, "proxy client is empty, UpdateCredentialCache will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		k, v := key, value
		group.Go(func() error {
			sta, err := v.UpdateCredentialCache(ctx, request)
			if err != nil {
				return merr.Wrapf(err, "UpdateCredentialCache failed, proxyID = %d", k)
			}
			if sta.ErrorCode != commonpb.ErrorCode_Success {
				return merr.Wrapf(merr.Error(sta), "UpdateCredentialCache failed, proxyID = %d", k)
			}
			return nil
		})
		return true
	})
	return group.Wait()
}

// RefreshPolicyInfoCache TODO: too many codes similar to InvalidateCollectionMetaCache.
func (p *ProxyClientManager) RefreshPolicyInfoCache(ctx context.Context, req *proxypb.RefreshPolicyInfoCacheRequest) error {
	if p.proxyClient.Len() == 0 {
		mlog.Warn(ctx, "proxy client is empty, RefreshPrivilegeInfoCache will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		k, v := key, value
		group.Go(func() error {
			status, err := v.RefreshPolicyInfoCache(ctx, req)
			if err != nil {
				return merr.Wrapf(err, "RefreshPolicyInfoCache failed, proxyID = %d", k)
			}
			if status.GetErrorCode() != commonpb.ErrorCode_Success {
				return merr.Error(status)
			}
			return nil
		})
		return true
	})
	return group.Wait()
}

// GetProxyMetrics sends requests to proxies to get metrics.
func (p *ProxyClientManager) GetProxyMetrics(ctx context.Context) ([]*milvuspb.GetMetricsResponse, error) {
	if p.proxyClient.Len() == 0 {
		mlog.Warn(ctx, "proxy client is empty, GetMetrics will not send to any client")
		return nil, nil
	}

	req, err := metricsinfo.ConstructRequestByMetricType(metricsinfo.SystemInfoMetrics)
	if err != nil {
		return nil, err
	}

	group := &errgroup.Group{}
	var metricRspsMu sync.Mutex
	metricRsps := make([]*milvuspb.GetMetricsResponse, 0)
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		k, v := key, value
		group.Go(func() error {
			rsp, err := v.GetProxyMetrics(ctx, req)
			if err != nil {
				return merr.Wrapf(err, "GetMetrics failed, proxyID = %d", k)
			}
			if rsp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				return merr.Wrapf(merr.Error(rsp.GetStatus()), "GetMetrics failed, proxyID = %d", k)
			}
			metricRspsMu.Lock()
			metricRsps = append(metricRsps, rsp)
			metricRspsMu.Unlock()
			return nil
		})
		return true
	})
	err = group.Wait()
	if err != nil {
		return nil, err
	}
	return metricRsps, nil
}

// SetRates notifies Proxy to limit rates of requests.
func (p *ProxyClientManager) SetRates(ctx context.Context, request *proxypb.SetRatesRequest) error {
	if p.proxyClient.Len() == 0 {
		mlog.Warn(ctx, "proxy client is empty, SetRates will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		k, v := key, value
		group.Go(func() error {
			sta, err := v.SetRates(ctx, request)
			if err != nil {
				return merr.Wrapf(err, "SetRates failed, proxyID = %d", k)
			}
			if sta.GetErrorCode() != commonpb.ErrorCode_Success {
				return merr.Wrapf(merr.Error(sta), "SetRates failed, proxyID = %d", k)
			}
			return nil
		})
		return true
	})
	return group.Wait()
}

func (p *ProxyClientManager) ClearReadTaskQueue(ctx context.Context, request *internalpb.ClearReadTaskQueueRequest) ([]*internalpb.ClearReadTaskQueueComponentResult, error) {
	if p.proxyClient.Len() == 0 {
		mlog.Warn(ctx, "proxy client is empty, ClearReadTaskQueue will not send to any client")
		return nil, nil
	}

	group := &errgroup.Group{}
	var resultsMu sync.Mutex
	results := make([]*internalpb.ClearReadTaskQueueComponentResult, 0, p.proxyClient.Len())
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		nodeID, client := key, value
		group.Go(func() error {
			resp, err := client.ClearReadTaskQueue(ctx, request)
			if errors.Is(err, merr.ErrServiceUnimplemented) {
				return nil
			}
			if err != nil {
				result := &internalpb.ClearReadTaskQueueComponentResult{
					Status: merr.Status(err),
					Role:   typeutil.ProxyRole,
					NodeID: nodeID,
				}
				resultsMu.Lock()
				results = append(results, result)
				resultsMu.Unlock()
				return errors.Wrapf(err, "ClearReadTaskQueue failed, proxyID = %d", nodeID)
			}

			status := resp.GetStatus()
			if len(resp.GetResults()) > 0 {
				resultsMu.Lock()
				results = append(results, resp.GetResults()...)
				resultsMu.Unlock()
			} else {
				resultsMu.Lock()
				results = append(results, &internalpb.ClearReadTaskQueueComponentResult{
					Status:        status,
					Role:          typeutil.ProxyRole,
					NodeID:        nodeID,
					QueuedCleared: resp.GetProxyQueuedCleared(),
				})
				resultsMu.Unlock()
			}
			if !merr.Ok(status) {
				return errors.Wrapf(merr.Error(status), "ClearReadTaskQueue failed, proxyID = %d", nodeID)
			}
			return nil
		})
		return true
	})
	return results, group.Wait()
}

// ListRunningRequests collects the requests registered on every proxy and
// returns them together with one result row per proxy.
//
// Partial results are the point. A proxy that is unreachable, unhealthy or
// running an older build shows up in nodeResults and the requests the other
// proxies reported are still returned, with the call itself succeeding: the
// reason to be listing running requests is usually that something is wrong,
// which is exactly when one proxy is most likely to be unreachable, and an
// operator who cannot see the other proxies' requests cannot act at all.
// nodeResults is what keeps such an answer honest about being incomplete.
//
// The call fails only when no proxy answered, including an empty client map,
// because "no proxy answered" and "no request is running" must not look the
// same to an operator.
func (p *ProxyClientManager) ListRunningRequests(ctx context.Context, request *milvuspb.ListRunningRequestsRequest) ([]*milvuspb.RunningRequestInfo, []*milvuspb.RunningRequestNodeResult, error) {
	if p.proxyClient.Len() == 0 {
		return nil, nil, merr.WrapErrServiceUnavailable("no proxy is registered, cannot list running requests")
	}

	ctx, cancel := context.WithTimeout(ctx, RunningRequestTimeout)
	defer cancel()

	var mu sync.Mutex
	requests := make([]*milvuspb.RunningRequestInfo, 0, p.proxyClient.Len())
	nodeResults := make([]*milvuspb.RunningRequestNodeResult, 0, p.proxyClient.Len())
	var answered int
	var wg sync.WaitGroup
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		nodeID, client := key, value
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.ListLocalRunningRequests(ctx, request)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				nodeResults = append(nodeResults, newRunningRequestNodeResult(nodeID, err))
				if errors.Is(err, merr.ErrServiceUnimplemented) {
					answered++
				}
				return
			}
			nodeResults = append(nodeResults, &milvuspb.RunningRequestNodeResult{
				NodeId: nodeID,
				Status: resp.GetStatus(),
			})
			if !merr.Ok(resp.GetStatus()) {
				return
			}
			answered++
			requests = append(requests, resp.GetRequests()...)
		}()
		return true
	})
	wg.Wait()
	if answered == 0 {
		return requests, nodeResults, noProxyAnswered("cannot list running requests", nodeResults)
	}
	return requests, nodeResults, nil
}

// CancelResult is the cluster's answer to a cancel, merged over every proxy.
type CancelResult struct {
	// Canceled describes each canceled request as it was when it was stopped.
	Canceled []*milvuspb.RunningRequestInfo
	// NotFound lists the ids no proxy holds. It is filled only when every
	// proxy answered.
	NotFound []int64
	// Undetermined lists the ids nobody canceled while some proxy did not
	// answer: any of them may be running on that proxy.
	Undetermined []int64
	// NodeResults has one row per proxy.
	NodeResults []*milvuspb.RunningRequestNodeResult
}

// CancelRequests asks every proxy to cancel the given request ids.
//
// The call is broadcast because a request id does not say which proxy serves
// it. An id nobody canceled is reported as not found only when every proxy
// answered; if some proxy did not, that proxy may hold it, so it is reported
// as undetermined instead. A proxy too old to serve the call counts as
// having answered, since it registers no requests.
//
// Like the list, this succeeds as long as one proxy answered, so that a proxy
// that is down cannot stop the others from canceling what they hold.
func (p *ProxyClientManager) CancelRequests(ctx context.Context, request *milvuspb.CancelRequestsRequest) (CancelResult, error) {
	if p.proxyClient.Len() == 0 {
		return CancelResult{}, merr.WrapErrServiceUnavailable("no proxy is registered, cannot cancel requests")
	}

	ctx, cancel := context.WithTimeout(ctx, RunningRequestTimeout)
	defer cancel()

	var mu sync.Mutex
	canceled := make([]*milvuspb.RunningRequestInfo, 0, len(request.GetRequestIds()))
	// Start from every requested id and strike out the ones a proxy canceled.
	// Whatever remains was held by nobody that answered.
	pending := typeutil.NewSet(request.GetRequestIds()...)
	nodeResults := make([]*milvuspb.RunningRequestNodeResult, 0, p.proxyClient.Len())
	var answered, silent int
	var wg sync.WaitGroup
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		nodeID, client := key, value
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.CancelLocalRequests(ctx, request)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				nodeResults = append(nodeResults, newRunningRequestNodeResult(nodeID, err))
				if errors.Is(err, merr.ErrServiceUnimplemented) {
					answered++
				} else {
					silent++
				}
				return
			}
			nodeResults = append(nodeResults, &milvuspb.RunningRequestNodeResult{
				NodeId: nodeID,
				Status: resp.GetStatus(),
			})
			if !merr.Ok(resp.GetStatus()) {
				silent++
				return
			}
			answered++
			for _, info := range resp.GetCanceled() {
				canceled = append(canceled, info)
				pending.Remove(info.GetRequestId())
			}
		}()
		return true
	})
	wg.Wait()

	result := CancelResult{Canceled: canceled, NodeResults: nodeResults}
	if silent > 0 {
		result.Undetermined = pending.Collect()
	} else {
		result.NotFound = pending.Collect()
	}
	if answered == 0 {
		return result, noProxyAnswered("cannot cancel requests", nodeResults)
	}
	return result, nil
}

// noProxyAnswered builds the error for a fan-out that reached nobody, naming
// one of the proxies that failed so the cause is not lost.
func noProxyAnswered(what string, nodeResults []*milvuspb.RunningRequestNodeResult) error {
	for _, r := range nodeResults {
		if !merr.Ok(r.GetStatus()) {
			return errors.Wrapf(merr.Error(r.GetStatus()), "no proxy answered, %s (proxyID = %d)", what, r.GetNodeId())
		}
	}
	return merr.WrapErrServiceUnavailable("no proxy answered, " + what)
}

// newRunningRequestNodeResult records why one proxy could not answer. A node
// running an older build is flagged so that the caller can tell "this proxy
// cannot do it yet" from "this proxy failed".
func newRunningRequestNodeResult(nodeID int64, err error) *milvuspb.RunningRequestNodeResult {
	return &milvuspb.RunningRequestNodeResult{
		NodeId:        nodeID,
		Status:        merr.Status(err),
		Unimplemented: errors.Is(err, merr.ErrServiceUnimplemented),
	}
}

func (p *ProxyClientManager) GetComponentStates(ctx context.Context) (map[int64]*milvuspb.ComponentStates, error) {
	group, ctx := errgroup.WithContext(ctx)
	states := make(map[int64]*milvuspb.ComponentStates)

	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		k, v := key, value
		group.Go(func() error {
			sta, err := v.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
			if err != nil {
				return err
			}
			states[k] = sta
			return nil
		})
		return true
	})
	err := group.Wait()
	if err != nil {
		return nil, err
	}

	return states, nil
}

func (p *ProxyClientManager) InvalidateShardLeaderCache(ctx context.Context, request *proxypb.InvalidateShardLeaderCacheRequest) error {
	if p.proxyClient.Len() == 0 {
		mlog.Warn(ctx, "proxy client is empty, InvalidateShardLeaderCache will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		k, v := key, value
		group.Go(func() error {
			sta, err := v.InvalidateShardLeaderCache(ctx, request)
			if err != nil {
				if errors.Is(err, merr.ErrNodeNotFound) {
					mlog.Warn(ctx, "InvalidateShardLeaderCache failed due to proxy service not found", mlog.Err(err))
					return nil
				}
				return merr.Wrapf(err, "InvalidateShardLeaderCache failed, proxyID = %d", k)
			}
			if sta.ErrorCode != commonpb.ErrorCode_Success {
				return merr.Wrapf(merr.Error(sta), "InvalidateShardLeaderCache failed, proxyID = %d", k)
			}
			return nil
		})
		return true
	})
	return group.Wait()
}
