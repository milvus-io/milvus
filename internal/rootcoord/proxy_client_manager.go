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
	"errors"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

type proxyCreator func(sess *sessionutil.Session) (types.Proxy, error)

type proxyClientManager struct {
	creator     proxyCreator
	lock        sync.RWMutex
	proxyClient map[int64]types.Proxy
	helper      proxyClientManagerHelper
}

type proxyClientManagerHelper struct {
	afterConnect func()
}

var defaultClientManagerHelper = proxyClientManagerHelper{
	afterConnect: func() {},
}

func newProxyClientManager(creator proxyCreator) *proxyClientManager {
	return &proxyClientManager{
		creator:     creator,
		proxyClient: make(map[int64]types.Proxy),
		helper:      defaultClientManagerHelper,
	}
}

func (p *proxyClientManager) GetProxyClients(sessions []*sessionutil.Session) {
	for _, session := range sessions {
		p.AddProxyClient(session)
	}
}

func (p *proxyClientManager) AddProxyClient(session *sessionutil.Session) {
	p.lock.RLock()
	_, ok := p.proxyClient[session.ServerID]
	p.lock.RUnlock()
	if ok {
		return
	}

	go p.connect(session)
}

func (p *proxyClientManager) connect(session *sessionutil.Session) {
	pc, err := p.creator(session)
	if err != nil {
		log.Warn("failed to create proxy client", zap.String("address", session.Address), zap.Int64("serverID", session.ServerID), zap.Error(err))
		return
	}

	p.lock.Lock()
	defer p.lock.Unlock()

	_, ok := p.proxyClient[session.ServerID]
	if ok {
		pc.Stop()
		return
	}
	p.proxyClient[session.ServerID] = pc
	log.Debug("succeed to create proxy client", zap.String("address", session.Address), zap.Int64("serverID", session.ServerID))
	p.helper.afterConnect()
}

func (p *proxyClientManager) DelProxyClient(s *sessionutil.Session) {
	p.lock.Lock()
	defer p.lock.Unlock()

	cli, ok := p.proxyClient[s.ServerID]
	if ok {
		cli.Stop()
	}

	delete(p.proxyClient, s.ServerID)
	log.Debug("remove proxy client", zap.String("proxy address", s.Address), zap.Int64("proxy id", s.ServerID))
}

func (p *proxyClientManager) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if len(p.proxyClient) == 0 {
		log.Warn("proxy client is empty, InvalidateCollectionMetaCache will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	for k, v := range p.proxyClient {
		k, v := k, v
		group.Go(func() error {
			sta, err := v.InvalidateCollectionMetaCache(ctx, request)
			if err != nil {
				return fmt.Errorf("InvalidateCollectionMetaCache failed, proxyID = %d, err = %s", k, err)
			}
			if sta.ErrorCode != commonpb.ErrorCode_Success {
				return fmt.Errorf("InvalidateCollectionMetaCache failed, proxyID = %d, err = %s", k, sta.Reason)
			}
			return nil
		})
	}
	return group.Wait()
}

// InvalidateCredentialCache TODO: too many codes similar to InvalidateCollectionMetaCache.
func (p *proxyClientManager) InvalidateCredentialCache(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if len(p.proxyClient) == 0 {
		log.Warn("proxy client is empty, InvalidateCredentialCache will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	for k, v := range p.proxyClient {
		k, v := k, v
		group.Go(func() error {
			sta, err := v.InvalidateCredentialCache(ctx, request)
			if err != nil {
				return fmt.Errorf("InvalidateCredentialCache failed, proxyID = %d, err = %s", k, err)
			}
			if sta.ErrorCode != commonpb.ErrorCode_Success {
				return fmt.Errorf("InvalidateCredentialCache failed, proxyID = %d, err = %s", k, sta.Reason)
			}
			return nil
		})
	}
	return group.Wait()
}

// UpdateCredentialCache TODO: too many codes similar to InvalidateCollectionMetaCache.
func (p *proxyClientManager) UpdateCredentialCache(ctx context.Context, request *proxypb.UpdateCredCacheRequest) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if len(p.proxyClient) == 0 {
		log.Warn("proxy client is empty, UpdateCredentialCache will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	for k, v := range p.proxyClient {
		k, v := k, v
		group.Go(func() error {
			sta, err := v.UpdateCredentialCache(ctx, request)
			if err != nil {
				return fmt.Errorf("UpdateCredentialCache failed, proxyID = %d, err = %s", k, err)
			}
			if sta.ErrorCode != commonpb.ErrorCode_Success {
				return fmt.Errorf("UpdateCredentialCache failed, proxyID = %d, err = %s", k, sta.Reason)
			}
			return nil
		})
	}
	return group.Wait()
}

// RefreshPolicyInfoCache TODO: too many codes similar to InvalidateCollectionMetaCache.
func (p *proxyClientManager) RefreshPolicyInfoCache(ctx context.Context, req *proxypb.RefreshPolicyInfoCacheRequest) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if len(p.proxyClient) == 0 {
		log.Warn("proxy client is empty, RefreshPrivilegeInfoCache will not send to any client")
		return nil
	}

	group := &errgroup.Group{}
	for k, v := range p.proxyClient {
		k, v := k, v
		group.Go(func() error {
			status, err := v.RefreshPolicyInfoCache(ctx, req)
			if err != nil {
				return fmt.Errorf("RefreshPolicyInfoCache failed, proxyID = %d, err = %s", k, err)
			}
			if status.GetErrorCode() != commonpb.ErrorCode_Success {
				return errors.New(status.GetReason())
			}
			return nil
		})
	}
	return group.Wait()
}
