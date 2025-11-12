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

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	grpcproxyclient "github.com/milvus-io/milvus/internal/distributed/proxy/client"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	GetComponentStates(ctx context.Context) (map[int64]*milvuspb.ComponentStates, error)
}

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
				log.Info("remove stale proxy client", zap.Int64("serverID", key))
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
		log.Warn("failed to create proxy client", zap.String("address", session.Address), zap.Int64("serverID", session.ServerID), zap.Error(err))
		return
	}

	_, ok := p.proxyClient.GetOrInsert(session.GetServerID(), pc)
	if ok {
		pc.Close()
		return
	}
	log.Info("succeed to create proxy client", zap.String("address", session.Address), zap.Int64("serverID", session.ServerID))
	p.helper.afterConnect()
}

func (p *ProxyClientManager) DelProxyClient(s *sessionutil.Session) {
	cli, ok := p.proxyClient.GetAndRemove(s.GetServerID())
	if ok {
		cli.Close()
	}

	p.updateProxyNumMetric()
	log.Info("remove proxy client", zap.String("proxy address", s.Address), zap.Int64("proxy id", s.ServerID))
}

func (p *ProxyClientManager) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest, opts ...ExpireCacheOpt) error {
	c := DefaultExpireCacheConfig()
	for _, opt := range opts {
		opt(&c)
	}
	c.Apply(request)

	if p.proxyClient.Len() == 0 {
		log.Warn("proxy client is empty, InvalidateCollectionMetaCache will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		k, v := key, value
		group.Go(func() error {
			sta, err := v.InvalidateCollectionMetaCache(ctx, request)
			if err != nil {
				if errors.Is(err, merr.ErrNodeNotFound) {
					log.Warn("InvalidateCollectionMetaCache failed due to proxy service not found", zap.Error(err))
					return nil
				}

				if errors.Is(err, merr.ErrServiceUnimplemented) {
					return nil
				}
				return merr.WrapErrServiceInternalErr(err, "InvalidateCollectionMetaCache failed, proxyID = %d", k)
			}
			if sta.ErrorCode != commonpb.ErrorCode_Success {
				return merr.WrapErrServiceInternalMsg("InvalidateCollectionMetaCache failed, proxyID = %d, reason = %s", k, sta.GetReason())
			}
			return nil
		})
		return true
	})
	return group.Wait()
}

// InvalidateCredentialCache TODO: too many codes similar to InvalidateCollectionMetaCache.
func (p *ProxyClientManager) InvalidateCredentialCache(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) error {
	if p.proxyClient.Len() == 0 {
		log.Warn("proxy client is empty, InvalidateCredentialCache will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		k, v := key, value
		group.Go(func() error {
			sta, err := v.InvalidateCredentialCache(ctx, request)
			if err != nil {
				return merr.WrapErrServiceInternalErr(err, "InvalidateCredentialCache failed, proxyID = %d", k)
			}
			if sta.ErrorCode != commonpb.ErrorCode_Success {
				return merr.WrapErrServiceInternalMsg("InvalidateCollectionMetaCache failed, proxyID = %d, reason = %s", k, sta.GetReason())
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
		log.Warn("proxy client is empty, UpdateCredentialCache will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		k, v := key, value
		group.Go(func() error {
			sta, err := v.UpdateCredentialCache(ctx, request)
			if err != nil {
				return merr.WrapErrServiceInternalErr(err, "UpdateCredentialCache failed, proxyID = %d", k)
			}
			if sta.ErrorCode != commonpb.ErrorCode_Success {
				return merr.WrapErrServiceInternalErr(err, "UpdateCredentialCache failed, proxyID = %d, reason = %s", k, sta.GetReason())
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
		log.Warn("proxy client is empty, RefreshPrivilegeInfoCache will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		k, v := key, value
		group.Go(func() error {
			status, err := v.RefreshPolicyInfoCache(ctx, req)
			if err != nil {
				return merr.WrapErrServiceInternalErr(err, "RefreshPolicyInfoCache failed, proxyID = %d", k)
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
		log.Warn("proxy client is empty, GetMetrics will not send to any client")
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
				return merr.WrapErrServiceInternalErr(err, "GetMetrics failed, proxyID = %d", k)
			}
			if rsp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
				return merr.WrapErrServiceInternalMsg("GetMetrics failed, proxyID = %d, reason = %s", k, rsp.GetStatus().GetReason())
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
		log.Warn("proxy client is empty, SetRates will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		k, v := key, value
		group.Go(func() error {
			sta, err := v.SetRates(ctx, request)
			if err != nil {
				return merr.WrapErrServiceInternalErr(err, "SetRates failed, proxyID = %d", k)
			}
			if sta.GetErrorCode() != commonpb.ErrorCode_Success {
				return merr.WrapErrServiceInternalMsg("SetRates failed, proxyID = %d, reason = %s", k, sta.GetReason())
			}
			return nil
		})
		return true
	})
	return group.Wait()
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
		log.Warn("proxy client is empty, InvalidateShardLeaderCache will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	p.proxyClient.Range(func(key int64, value types.ProxyClient) bool {
		k, v := key, value
		group.Go(func() error {
			sta, err := v.InvalidateShardLeaderCache(ctx, request)
			if err != nil {
				if errors.Is(err, merr.ErrNodeNotFound) {
					log.Warn("InvalidateShardLeaderCache failed due to proxy service not found", zap.Error(err))
					return nil
				}
				return merr.WrapErrServiceInternalErr(err, "InvalidateShardLeaderCache failed, proxyID = %d, reason = %s", k, sta.GetReason())
			}
			if sta.ErrorCode != commonpb.ErrorCode_Success {
				return merr.WrapErrServiceInternalMsg("InvalidateShardLeaderCache failed, proxyID = %d, reason = %s", k, sta.GetReason())
			}
			return nil
		})
		return true
	})
	return group.Wait()
}
