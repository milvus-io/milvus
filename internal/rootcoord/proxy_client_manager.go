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
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/proto/internalpb"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"go.uber.org/zap"
)

type proxyClientManager struct {
	core        *Core
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

func newProxyClientManager(c *Core) *proxyClientManager {
	return &proxyClientManager{
		core:        c,
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
	pc, err := p.core.NewProxyClient(session)
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

func (p *proxyClientManager) InvalidateCollectionMetaCache(ctx context.Context, request *proxypb.InvalidateCollMetaCacheRequest) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if len(p.proxyClient) == 0 {
		log.Debug("proxy client is empty,InvalidateCollectionMetaCache will not send to any client")
		return
	}

	for k, f := range p.proxyClient {
		err := func() error {
			defer func() {
				if err := recover(); err != nil {
					log.Debug("call InvalidateCollectionMetaCache panic", zap.Int64("proxy id", k), zap.Any("msg", err))
				}

			}()
			sta, err := f.InvalidateCollectionMetaCache(ctx, request)
			if err != nil {
				return fmt.Errorf("grpc fail,error=%w", err)
			}
			if sta.ErrorCode != commonpb.ErrorCode_Success {
				return fmt.Errorf("message = %s", sta.Reason)
			}
			return nil
		}()
		if err != nil {
			log.Error("Failed to call invalidate collection meta", zap.Int64("proxy id", k), zap.Error(err))
		} else {
			log.Debug("send invalidate collection meta cache to proxy node", zap.Int64("node id", k))
		}

	}
}

func (p *proxyClientManager) ReleaseDQLMessageStream(ctx context.Context, in *proxypb.ReleaseDQLMessageStreamRequest) (*commonpb.Status, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if len(p.proxyClient) == 0 {
		log.Debug("proxy client is empty,ReleaseDQLMessageStream will not send to any client")
		return &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		}, nil
	}
	for k, f := range p.proxyClient {
		sta, err := func() (retSta *commonpb.Status, retErr error) {
			defer func() {
				if err := recover(); err != nil {
					log.Debug("call proxy node ReleaseDQLMessageStream panic", zap.Int64("proxy node id", k), zap.Any("error", err))
					retSta.ErrorCode = commonpb.ErrorCode_UnexpectedError
					retSta.Reason = fmt.Sprintf("call proxy node ReleaseDQLMessageStream panic, proxy node id =%d, error = %v", k, err)
					retErr = nil
				}
			}()
			retSta, retErr = f.ReleaseDQLMessageStream(ctx, in)
			return
		}()
		if err != nil {
			return sta, err
		}
		if sta.ErrorCode != commonpb.ErrorCode_Success {
			return sta, err
		}

	}
	return &commonpb.Status{
		ErrorCode: commonpb.ErrorCode_Success,
	}, nil
}

func (p *proxyClientManager) InvalidateCredentialCache(ctx context.Context, request *proxypb.InvalidateCredCacheRequest) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	defer func() {
		if err := recover(); err != nil {
			log.Debug("call InvalidateCredentialCache panic", zap.Any("msg", err))
		}
	}()

	if len(p.proxyClient) == 0 {
		log.Warn("proxy client is empty, InvalidateCredentialCache will not send to any client")
		return nil
	}

	for _, f := range p.proxyClient {
		sta, err := f.InvalidateCredentialCache(ctx, request)
		if err != nil {
			return fmt.Errorf("grpc fail, error=%w", err)
		}
		if sta.ErrorCode != commonpb.ErrorCode_Success {
			return fmt.Errorf("message = %s", sta.Reason)
		}
	}
	return nil
}

func (p *proxyClientManager) UpdateCredentialCache(ctx context.Context, request *proxypb.UpdateCredCacheRequest) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	defer func() {
		if err := recover(); err != nil {
			log.Debug("call UpdateCredentialCache panic", zap.Any("msg", err))
		}
	}()

	if len(p.proxyClient) == 0 {
		log.Warn("proxy client is empty, UpdateCredentialCache will not send to any client")
		return nil
	}

	for _, f := range p.proxyClient {
		sta, err := f.UpdateCredentialCache(ctx, request)
		if err != nil {
			return fmt.Errorf("grpc fail, error=%w", err)
		}
		if sta.ErrorCode != commonpb.ErrorCode_Success {
			return fmt.Errorf("message = %s", sta.Reason)
		}
	}
	return nil
}

func (p *proxyClientManager) ClearCredUsersCache(ctx context.Context, request *internalpb.ClearCredUsersCacheRequest) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	defer func() {
		if err := recover(); err != nil {
			log.Debug("call ClearCredUsersCache panic", zap.Any("msg", err))
		}
	}()

	if len(p.proxyClient) == 0 {
		log.Warn("proxy client is empty, ClearCredUsersCache will not send to any client")
		return nil
	}

	for _, f := range p.proxyClient {
		sta, err := f.ClearCredUsersCache(ctx, request)
		if err != nil {
			return fmt.Errorf("grpc fail, error=%w", err)
		}
		if sta.ErrorCode != commonpb.ErrorCode_Success {
			return fmt.Errorf("message = %s", sta.Reason)
		}
	}
	return nil
}
