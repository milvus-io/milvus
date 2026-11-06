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
	"path"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type ProxyWatcherInterface interface {
	AddSessionFunc(fns ...func(*sessionutil.Session))
	DelSessionFunc(fns ...func(*sessionutil.Session))

	WatchProxy(ctx context.Context) error
	Stop()
}

// ProxyWatcher manages proxy clients
type ProxyWatcher struct {
	wg               sync.WaitGroup
	lock             sync.Mutex
	etcdCli          *clientv3.Client
	initSessionsFunc []func([]*sessionutil.Session)
	addSessionsFunc  []func(*sessionutil.Session)
	delSessionsFunc  []func(*sessionutil.Session)

	closeOnce sync.Once
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewProxyWatcher helper function to create a proxyWatcher
// fns are the custom getSessions function list
func NewProxyWatcher(client *clientv3.Client, fns ...func([]*sessionutil.Session)) *ProxyWatcher {
	ctx, cancel := context.WithCancel(context.Background()) //nolint:gosec // G118: cancel is stored in the watcher and called by Stop.
	p := &ProxyWatcher{
		etcdCli: client,
		ctx:     ctx,
		cancel:  cancel,
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

// WatchProxy initializes the proxy snapshot and starts a resumable watch loop.
// It does not wait for the first watch stream to be established.
func (p *ProxyWatcher) WatchProxy(ctx context.Context) error {
	p.lock.Lock()
	if err := p.ctx.Err(); err != nil {
		p.lock.Unlock()
		return err
	}
	p.wg.Add(1)
	p.lock.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	stopCancel := context.AfterFunc(p.ctx, cancel)
	cleanup := func() {
		stopCancel()
		cancel()
		p.wg.Done()
	}
	// Bound synchronous initialization, but let background recovery wait until
	// etcd is available again or the watcher is stopped.
	initCtx, initCancel := context.WithTimeout(ctx, paramtable.Get().EtcdCfg.RequestTimeout.GetAsDuration(time.Millisecond))
	revision, err := p.initSessions(initCtx)
	initCancel()
	if err != nil {
		cleanup()
		return err
	}
	go func() {
		defer cleanup()
		if err := p.startWatchEtcd(ctx, revision); err != nil && ctx.Err() == nil {
			log.Ctx(ctx).Error("watch proxy service failed", zap.Error(err))
			panic(err)
		}
	}()
	return nil
}

func (p *ProxyWatcher) initSessions(ctx context.Context) (int64, error) {
	sessions, revision, err := p.getSessionsOnEtcd(ctx)
	if err != nil {
		return 0, err
	}
	// Reconcile the full snapshot, including sessions removed while disconnected.
	for _, f := range p.initSessionsFunc {
		f(sessions)
	}
	log.Ctx(ctx).Info("succeed to init sessions on etcd", zap.Int64("revision", revision))
	return revision, nil
}

// startWatchEtcd owns all watch attempts in the same goroutine, like the
// streaming resolver's discovery loop. Setup and runtime errors take the same
// recovery path: wait, reconcile a fresh snapshot, then watch from revision+1.
func (p *ProxyWatcher) startWatchEtcd(ctx context.Context, revision int64) error {
	var err error
	for {
		if err == nil {
			err = p.watch(ctx, revision)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !etcd.IsRetriableWatchErr(err) {
			return err
		}
		log.Ctx(ctx).Warn("proxy watch interrupted, retrying discovery", zap.Error(err))
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		revision, err = p.initSessions(ctx)
	}
}

// watch consumes one stream until it fails or is canceled. Canceling each
// attempt also releases the underlying etcd watch before the next attempt.
func (p *ProxyWatcher) watch(ctx context.Context, revision int64) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	eventCh := p.etcdCli.Watch(
		ctx,
		path.Join(paramtable.Get().EtcdCfg.MetaRootPath.GetValue(), sessionutil.DefaultServiceRoot, typeutil.ProxyRole),
		clientv3.WithPrefix(),
		clientv3.WithPrevKV(),
		clientv3.WithRev(revision+1),
	)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-eventCh:
			if !ok {
				return merr.WrapErrServiceUnavailable("proxy watch channel closed unexpectedly")
			}
			if err := event.Err(); err != nil {
				return err
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
					log.Ctx(ctx).Warn("failed to handle proxy event", zap.Any("event", e), zap.Error(err))
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
		return nil, 0, merr.Wrapf(err, "proxy manager failed to watch proxy")
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
		p.lock.Lock()
		p.cancel()
		p.lock.Unlock()
		p.wg.Wait()
	})
}
