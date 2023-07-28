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
	"encoding/json"
	"fmt"
	"path"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// proxyManager manages proxy connected to the rootcoord
type proxyManager struct {
	ctx              context.Context
	cancel           context.CancelFunc
	lock             sync.Mutex
	etcdCli          *clientv3.Client
	initSessionsFunc []func([]*sessionutil.Session)
	addSessionsFunc  []func(*sessionutil.Session)
	delSessionsFunc  []func(*sessionutil.Session)
}

// newProxyManager helper function to create a proxyManager
// etcdEndpoints is the address list of etcd
// fns are the custom getSessions function list
func newProxyManager(ctx context.Context, client *clientv3.Client, fns ...func([]*sessionutil.Session)) *proxyManager {
	ctx2, cancel2 := context.WithCancel(ctx)
	p := &proxyManager{
		ctx:     ctx2,
		cancel:  cancel2,
		lock:    sync.Mutex{},
		etcdCli: client,
	}
	p.initSessionsFunc = append(p.initSessionsFunc, fns...)
	return p
}

// AddSessionFunc adds functions to addSessions function list
func (p *proxyManager) AddSessionFunc(fns ...func(*sessionutil.Session)) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.addSessionsFunc = append(p.addSessionsFunc, fns...)
}

// DelSessionFunc add functions to delSessions function list
func (p *proxyManager) DelSessionFunc(fns ...func(*sessionutil.Session)) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.delSessionsFunc = append(p.delSessionsFunc, fns...)
}

// WatchProxy starts a goroutine to watch proxy session changes on etcd
func (p *proxyManager) WatchProxy() error {
	ctx, cancel := context.WithTimeout(p.ctx, etcdkv.RequestTimeout)
	defer cancel()

	sessions, rev, err := p.getSessionsOnEtcd(ctx)
	if err != nil {
		return err
	}
	log.Info("succeed to init sessions on etcd", zap.Any("sessions", sessions), zap.Int64("revision", rev))
	// all init function should be clear meta firstly.
	for _, f := range p.initSessionsFunc {
		f(sessions)
	}

	eventCh := p.etcdCli.Watch(
		p.ctx,
		path.Join(Params.EtcdCfg.MetaRootPath.GetValue(), sessionutil.DefaultServiceRoot, typeutil.ProxyRole),
		clientv3.WithPrefix(),
		clientv3.WithCreatedNotify(),
		clientv3.WithPrevKV(),
		clientv3.WithRev(rev+1),
	)
	go p.startWatchEtcd(p.ctx, eventCh)
	return nil
}

func (p *proxyManager) startWatchEtcd(ctx context.Context, eventCh clientv3.WatchChan) {
	log.Info("start to watch etcd")
	for {
		select {
		case <-ctx.Done():
			log.Warn("stop watching etcd loop")
			return
			// TODO @xiaocai2333: watch proxy by session WatchService.
		case event, ok := <-eventCh:
			if !ok {
				log.Warn("stop watching etcd loop due to closed etcd event channel")
				panic("stop watching etcd loop due to closed etcd event channel")
			}
			if err := event.Err(); err != nil {
				if err == v3rpc.ErrCompacted {
					err2 := p.WatchProxy()
					if err2 != nil {
						log.Error("re watch proxy fails when etcd has a compaction error",
							zap.String("etcd error", err.Error()), zap.Error(err2))
						panic("failed to handle etcd request, exit..")
					}
					return
				}
				log.Error("Watch proxy service failed", zap.Error(err))
				panic(err)
			}
			for _, e := range event.Events {
				var err error
				switch e.Type {
				case mvccpb.PUT:
					err = p.handlePutEvent(e)
				case mvccpb.DELETE:
					err = p.handleDeleteEvent(e)
				}
				if err != nil {
					log.Warn("failed to handle proxy event", zap.Any("event", e), zap.Error(err))
				}
			}
		}
	}
}

func (p *proxyManager) handlePutEvent(e *clientv3.Event) error {
	session, err := p.parseSession(e.Kv.Value)
	if err != nil {
		return err
	}
	log.Debug("received proxy put event with session", zap.Any("session", session))
	for _, f := range p.addSessionsFunc {
		f(session)
	}
	return nil
}

func (p *proxyManager) handleDeleteEvent(e *clientv3.Event) error {
	session, err := p.parseSession(e.PrevKv.Value)
	if err != nil {
		return err
	}
	log.Debug("received proxy delete event with session", zap.Any("session", session))
	for _, f := range p.delSessionsFunc {
		f(session)
	}
	return nil
}

func (p *proxyManager) parseSession(value []byte) (*sessionutil.Session, error) {
	session := new(sessionutil.Session)
	err := json.Unmarshal(value, session)
	if err != nil {
		return nil, err
	}
	return session, nil
}

func (p *proxyManager) getSessionsOnEtcd(ctx context.Context) ([]*sessionutil.Session, int64, error) {
	resp, err := p.etcdCli.Get(
		ctx,
		path.Join(Params.EtcdCfg.MetaRootPath.GetValue(), sessionutil.DefaultServiceRoot, typeutil.ProxyRole),
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("proxy manager failed to watch proxy with error %w", err)
	}

	var sessions []*sessionutil.Session
	for _, v := range resp.Kvs {
		session, err := p.parseSession(v.Value)
		if err != nil {
			log.Warn("failed to unmarshal session", zap.Error(err))
			return nil, 0, err
		}
		sessions = append(sessions, session)
	}

	return sessions, resp.Header.Revision, nil
}

// Stop stops the proxyManager
func (p *proxyManager) Stop() {
	p.cancel()
}
