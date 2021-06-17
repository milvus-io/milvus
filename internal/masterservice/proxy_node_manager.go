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

package masterservice

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sync"

	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

type proxyNodeManager struct {
	ctx         context.Context
	cancel      context.CancelFunc
	lock        sync.Mutex
	etcdCli     *clientv3.Client
	getSessions []func([]*sessionutil.Session)
	addSessions []func(*sessionutil.Session)
	delSessions []func(*sessionutil.Session)
}

func newProxyNodeManager(ctx context.Context, etcdEndpoints []string, fns ...func([]*sessionutil.Session)) (*proxyNodeManager, error) {
	cli, err := clientv3.New(clientv3.Config{Endpoints: etcdEndpoints})
	if err != nil {
		return nil, err
	}
	ctx2, cancel2 := context.WithCancel(ctx)
	p := &proxyNodeManager{
		ctx:     ctx2,
		cancel:  cancel2,
		lock:    sync.Mutex{},
		etcdCli: cli,
	}
	p.getSessions = append(p.getSessions, fns...)
	return p, nil
}

func (p *proxyNodeManager) AddSession(fns ...func(*sessionutil.Session)) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.addSessions = append(p.addSessions, fns...)
}

func (p *proxyNodeManager) DelSession(fns ...func(*sessionutil.Session)) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.delSessions = append(p.delSessions, fns...)
}

func (p *proxyNodeManager) WatchProxyNode() error {
	ctx2, cancel := context.WithTimeout(p.ctx, RequestTimeout)
	defer cancel()
	resp, err := p.etcdCli.Get(
		ctx2,
		path.Join(Params.MetaRootPath, sessionutil.DefaultServiceRoot, typeutil.ProxyNodeRole),
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	if err != nil {
		return fmt.Errorf("proxyNodeManager,watch proxy node failed, error = %w", err)
	}
	sessions := []*sessionutil.Session{}
	for _, v := range resp.Kvs {
		sess := new(sessionutil.Session)
		err := json.Unmarshal(v.Value, sess)
		if err != nil {
			log.Debug("unmarshal SvrSession failed", zap.Error(err))
			continue
		}
		sessions = append(sessions, sess)
	}
	for _, f := range p.getSessions {
		f(sessions)
	}
	for _, s := range sessions {
		metrics.MasterProxyNodeLister.WithLabelValues(metricProxyNode(s.ServerID)).Set(1)
	}
	for _, s := range sessions {
		log.Debug("Get proxy node", zap.Int64("node id", s.ServerID), zap.String("node addr", s.Address), zap.String("node name", s.ServerName))
	}

	rch := p.etcdCli.Watch(
		p.ctx,
		path.Join(Params.MetaRootPath, sessionutil.DefaultServiceRoot, typeutil.ProxyNodeRole),
		clientv3.WithPrefix(),
		clientv3.WithCreatedNotify(),
		clientv3.WithPrevKV(),
		clientv3.WithRev(resp.Header.Revision+1),
	)

	go func() {
		for {
			select {
			case <-p.ctx.Done():
				log.Debug("context done", zap.Error(p.ctx.Err()))
				return
			case wresp, ok := <-rch:
				if !ok {
					log.Debug("watch proxy node failed")
					return
				}
				for _, ev := range wresp.Events {
					switch ev.Type {
					case mvccpb.PUT:
						sess := new(sessionutil.Session)
						err := json.Unmarshal(ev.Kv.Value, sess)
						if err != nil {
							log.Debug("watch proxy node, unmarshal failed", zap.Error(err))
							continue
						}
						p.lock.Lock()
						for _, f := range p.addSessions {
							f(sess)
						}
						p.lock.Unlock()
						metrics.MasterProxyNodeLister.WithLabelValues(metricProxyNode(sess.ServerID)).Set(1)
					case mvccpb.DELETE:
						sess := new(sessionutil.Session)
						err := json.Unmarshal(ev.PrevKv.Value, sess)
						if err != nil {
							log.Debug("watch proxy node, unmarshal failed", zap.Error(err))
							continue
						}
						p.lock.Lock()
						for _, f := range p.delSessions {
							f(sess)
						}
						p.lock.Unlock()
						metrics.MasterProxyNodeLister.WithLabelValues(metricProxyNode(sess.ServerID)).Set(0)
					}
				}
			}
		}
	}()

	return nil
}

func (p *proxyNodeManager) Stop() {
	p.cancel()
}
