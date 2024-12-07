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
	"fmt"
	"path"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type ProxyWatcherInterface interface {
	AddSessionFunc(fns ...func(*sessionutil.Session))
	DelSessionFunc(fns ...func(*sessionutil.Session))

	WatchProxy(ctx context.Context) error
	Stop()
}

// ProxyWatcher manages proxy clients
type ProxyWatcher struct {
	wg               errgroup.Group
	lock             sync.Mutex
	etcdCli          *clientv3.Client
	initSessionsFunc []func([]*sessionutil.Session)
	addSessionsFunc  []func(*sessionutil.Session)
	delSessionsFunc  []func(*sessionutil.Session)

	closeOnce sync.Once
	closeCh   lifetime.SafeChan
}

// NewProxyWatcher helper function to create a proxyWatcher
// fns are the custom getSessions function list
func NewProxyWatcher(client *clientv3.Client, fns ...func([]*sessionutil.Session)) *ProxyWatcher {
	p := &ProxyWatcher{
		lock:    sync.Mutex{},
		etcdCli: client,
		closeCh: lifetime.NewSafeChan(),
	}
	p.initSessionsFunc = append(p.initSessionsFunc, fns...)
	return p
}

// AddSessionFunc adds functions to addSessions function list
func (p *ProxyWatcher) AddSessionFunc(fns ...func(*sessionutil.Session)) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.addSessionsFunc = append(p.addSessionsFunc, fns...)
}

// DelSessionFunc add functions to delSessions function list
func (p *ProxyWatcher) DelSessionFunc(fns ...func(*sessionutil.Session)) {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.delSessionsFunc = append(p.delSessionsFunc, fns...)
}

// WatchProxy starts a goroutine to watch proxy session changes on etcd
func (p *ProxyWatcher) WatchProxy(ctx context.Context) error {
	childCtx, cancel := context.WithTimeout(ctx, paramtable.Get().ServiceParam.EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond))
	defer cancel()

	sessions, rev, err := p.getSessionsOnEtcd(childCtx)
	if err != nil {
		return err
	}
	log.Info("succeed to init sessions on etcd", zap.Any("sessions", sessions), zap.Int64("revision", rev))
	// all init function should be clear meta firstly.
	for _, f := range p.initSessionsFunc {
		f(sessions)
	}

	eventCh := p.etcdCli.Watch(
		ctx,
		path.Join(paramtable.Get().EtcdCfg.MetaRootPath.GetValue(), sessionutil.DefaultServiceRoot, typeutil.ProxyRole),
		clientv3.WithPrefix(),
		clientv3.WithCreatedNotify(),
		clientv3.WithPrevKV(),
		clientv3.WithRev(rev+1),
	)

	p.wg.Go(func() error {
		p.startWatchEtcd(ctx, eventCh)
		return nil
	})
	return nil
}

func (p *ProxyWatcher) startWatchEtcd(ctx context.Context, eventCh clientv3.WatchChan) {
	log.Info("start to watch etcd")
	for {
		select {
		case <-ctx.Done():
			log.Warn("stop watching etcd loop")
			return

		case <-p.closeCh.CloseCh():
			log.Warn("stop watching etcd loop")
			return

		case event, ok := <-eventCh:
			if !ok {
				log.Warn("stop watching etcd loop due to closed etcd event channel")
				panic("stop watching etcd loop due to closed etcd event channel")
			}
			if err := event.Err(); err != nil {
				if err == v3rpc.ErrCompacted {
					err2 := p.WatchProxy(ctx)
					if err2 != nil {
						log.Error("re watch proxy fails when etcd has a compaction error",
							zap.Error(err), zap.Error(err2))
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

func (p *ProxyWatcher) handlePutEvent(e *clientv3.Event) error {
	session, err := p.parseSession(e.Kv.Value)
	if err != nil {
		return err
	}
	log.Ctx(context.TODO()).Debug("received proxy put event with session", zap.Any("session", session))
	for _, f := range p.addSessionsFunc {
		f(session)
	}
	return nil
}

func (p *ProxyWatcher) handleDeleteEvent(e *clientv3.Event) error {
	session, err := p.parseSession(e.PrevKv.Value)
	if err != nil {
		return err
	}
	log.Ctx(context.TODO()).Debug("received proxy delete event with session", zap.Any("session", session))
	for _, f := range p.delSessionsFunc {
		f(session)
	}
	return nil
}

func (p *ProxyWatcher) parseSession(value []byte) (*sessionutil.Session, error) {
	session := new(sessionutil.Session)
	err := json.Unmarshal(value, session)
	if err != nil {
		return nil, err
	}
	return session, nil
}

func (p *ProxyWatcher) getSessionsOnEtcd(ctx context.Context) ([]*sessionutil.Session, int64, error) {
	resp, err := p.etcdCli.Get(
		ctx,
		path.Join(paramtable.Get().EtcdCfg.MetaRootPath.GetValue(), sessionutil.DefaultServiceRoot, typeutil.ProxyRole),
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

// Stop stops the ProxyManager
func (p *ProxyWatcher) Stop() {
	p.closeOnce.Do(func() {
		p.closeCh.Close()
		p.wg.Wait()
	})
}
