// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package rootcoord

import (
	"context"
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/proxypb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"go.uber.org/zap"
)

type proxyClientManager struct {
	core        *Core
	lock        sync.Mutex
	proxyClient map[int64]types.Proxy
}

func newProxyClientManager(c *Core) *proxyClientManager {
	return &proxyClientManager{
		core:        c,
		lock:        sync.Mutex{},
		proxyClient: make(map[int64]types.Proxy),
	}
}

func (p *proxyClientManager) GetProxyClients(sess []*sessionutil.Session) {
	p.lock.Lock()
	defer p.lock.Unlock()
	var pl map[int64]*sessionutil.Session
	for _, s := range sess {
		if _, ok := p.proxyClient[s.ServerID]; ok {
			continue
		}
		if len(pl) > 0 {
			if _, ok := pl[s.ServerID]; !ok {
				continue
			}
		}

		pc, err := p.core.NewProxyClient(s)
		if err != nil {
			log.Debug("create proxy client failed", zap.String("proxy address", s.Address), zap.Int64("proxy id", s.ServerID), zap.Error(err))
			pl, _ = listProxyInEtcd(p.core.ctx, p.core.etcdCli)
			continue
		}
		p.proxyClient[s.ServerID] = pc
		log.Debug("create proxy client", zap.String("proxy address", s.Address), zap.Int64("proxy id", s.ServerID))
	}
}

func (p *proxyClientManager) AddProxyClient(s *sessionutil.Session) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if _, ok := p.proxyClient[s.ServerID]; ok {
		return
	}
	pc, err := p.core.NewProxyClient(s)
	if err != nil {
		log.Debug("create proxy client", zap.String("proxy address", s.Address), zap.Int64("proxy id", s.ServerID), zap.Error(err))
		return
	}
	p.proxyClient[s.ServerID] = pc
	log.Debug("create proxy client", zap.String("proxy address", s.Address), zap.Int64("proxy id", s.ServerID))
}

func (p *proxyClientManager) DelProxyClient(s *sessionutil.Session) {
	p.lock.Lock()
	defer p.lock.Unlock()
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
