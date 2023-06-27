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

package sessionutil

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

const (
	// DefaultServiceRoot default root path used in kv by Session
	DefaultServiceRoot = "session/"
	// DefaultIDKey default id key for Session
	DefaultIDKey = "id"
)

// SessionEventType session event type
type SessionEventType int

func (t SessionEventType) String() string {
	switch t {
	case SessionAddEvent:
		return "SessionAddEvent"
	case SessionDelEvent:
		return "SessionDelEvent"
	case SessionUpdateEvent:
		return "SessionUpdateEvent"
	default:
		return ""
	}
}

// Rewatch defines the behavior outer session watch handles ErrCompacted
// it should process the current full list of session
// and returns err if meta error or anything else goes wrong
type Rewatch func(sessions map[string]*Session) error

const (
	// SessionNoneEvent place holder for zero value
	SessionNoneEvent SessionEventType = iota
	// SessionAddEvent event type for a new Session Added
	SessionAddEvent
	// SessionDelEvent event type for a Session deleted
	SessionDelEvent
	// SessionUpdateEvent event type for a Session stopping
	SessionUpdateEvent
)

// Session is a struct to store service's session, including ServerID, ServerName,
// Address.
// Exclusive indicates that this server can only start one.
type Session struct {
	ctx context.Context
	// When outside context done, Session cancels its goroutines first, then uses
	// keepAliveCancel to cancel the etcd KeepAlive
	keepAliveCancel context.CancelFunc
	keepAliveCtx    context.Context

	ServerID    int64  `json:"ServerID,omitempty"`
	ServerName  string `json:"ServerName,omitempty"`
	Address     string `json:"Address,omitempty"`
	Exclusive   bool   `json:"Exclusive,omitempty"`
	Stopping    bool   `json:"Stopping,omitempty"`
	TriggerKill bool
	Version     semver.Version `json:"Version,omitempty"`

	liveChOnce        sync.Once
	liveCh            chan bool
	etcdCli           *clientv3.Client
	leaseID           *clientv3.LeaseID
	watchSessionKeyCh clientv3.WatchChan
	wg                sync.WaitGroup

	metaRoot string

	registered           atomic.Value
	disconnected         atomic.Value
	retryKeepAlive       atomic.Value
	enableRetryKeepAlive bool

	isStandby           atomic.Value
	enableActiveStandBy bool
	activeKey           string

	sessionTTL        int64
	sessionRetryTimes int64
	reuseNodeID       bool
}

type SessionOption func(session *Session)

func WithTTL(ttl int64) SessionOption {
	return func(session *Session) { session.sessionTTL = ttl }
}

func WithRetryTimes(n int64) SessionOption {
	return func(session *Session) { session.sessionRetryTimes = n }
}

func WithResueNodeID(b bool) SessionOption {
	return func(session *Session) { session.reuseNodeID = b }
}

func (s *Session) apply(opts ...SessionOption) {
	for _, opt := range opts {
		opt(s)
	}
}

// UnmarshalJSON unmarshal bytes to Session.
func (s *Session) UnmarshalJSON(data []byte) error {
	var raw struct {
		ServerID    int64  `json:"ServerID,omitempty"`
		ServerName  string `json:"ServerName,omitempty"`
		Address     string `json:"Address,omitempty"`
		Exclusive   bool   `json:"Exclusive,omitempty"`
		Stopping    bool   `json:"Stopping,omitempty"`
		TriggerKill bool
		Version     string `json:"Version"`
	}
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}

	if raw.Version != "" {
		s.Version, err = semver.Parse(raw.Version)
		if err != nil {
			return err
		}
	}

	s.ServerID = raw.ServerID
	s.ServerName = raw.ServerName
	s.Address = raw.Address
	s.Exclusive = raw.Exclusive
	s.Stopping = raw.Stopping
	s.TriggerKill = raw.TriggerKill
	return nil
}

// MarshalJSON marshals session to bytes.
func (s *Session) MarshalJSON() ([]byte, error) {

	verStr := s.Version.String()
	return json.Marshal(&struct {
		ServerID    int64  `json:"ServerID,omitempty"`
		ServerName  string `json:"ServerName,omitempty"`
		Address     string `json:"Address,omitempty"`
		Exclusive   bool   `json:"Exclusive,omitempty"`
		Stopping    bool   `json:"Stopping,omitempty"`
		TriggerKill bool
		Version     string `json:"Version"`
	}{
		ServerID:    s.ServerID,
		ServerName:  s.ServerName,
		Address:     s.Address,
		Exclusive:   s.Exclusive,
		Stopping:    s.Stopping,
		TriggerKill: s.TriggerKill,
		Version:     verStr,
	})

}

// NewSession is a helper to build Session object.
// ServerID, ServerName, Address, Exclusive will be assigned after Init().
// metaRoot is a path in etcd to save session information.
// etcdEndpoints is to init etcdCli when NewSession
func NewSession(ctx context.Context, metaRoot string, client *clientv3.Client, opts ...SessionOption) *Session {
	session := &Session{
		ctx:      ctx,
		metaRoot: metaRoot,
		Version:  common.Version,

		// options
		sessionTTL:           paramtable.Get().CommonCfg.SessionTTL.GetAsInt64(),
		sessionRetryTimes:    paramtable.Get().CommonCfg.SessionRetryTimes.GetAsInt64(),
		reuseNodeID:          true,
		enableRetryKeepAlive: true,
	}

	// integration test create cluster with different nodeId in one process
	if paramtable.Get().IntegrationTestCfg.IntegrationMode.GetAsBool() {
		session.reuseNodeID = false
	}

	session.apply(opts...)

	session.UpdateRegistered(false)

	connectEtcdFn := func() error {
		log.Debug("Session try to connect to etcd")
		ctx2, cancel2 := context.WithTimeout(session.ctx, 5*time.Second)
		defer cancel2()
		if _, err := client.Get(ctx2, "health"); err != nil {
			return err
		}
		session.etcdCli = client
		return nil
	}
	err := retry.Do(ctx, connectEtcdFn, retry.Attempts(100))
	if err != nil {
		log.Warn("failed to initialize session",
			zap.Error(err))
		return nil
	}
	log.Debug("Session connect to etcd success")
	return session
}

// Init will initialize base struct of the Session, including ServerName, ServerID,
// Address, Exclusive. ServerID is obtained in getServerID.
func (s *Session) Init(serverName, address string, exclusive bool, triggerKill bool) {
	s.ServerName = serverName
	s.Address = address
	s.Exclusive = exclusive
	s.TriggerKill = triggerKill
	s.checkIDExist()
	serverID, err := s.getServerID()
	if err != nil {
		panic(err)
	}
	s.ServerID = serverID
	log.Info("start server", zap.String("name", serverName), zap.String("address", address), zap.Int64("id", s.ServerID))
}

// String makes Session struct able to be logged by zap
func (s *Session) String() string {
	return fmt.Sprintf("Session:<ServerID: %d, ServerName: %s, Version: %s>", s.ServerID, s.ServerName, s.Version.String())
}

// Register will process keepAliveResponse to keep alive with etcd.
func (s *Session) Register() {
	ch, err := s.registerService()
	if err != nil {
		log.Error("Register failed", zap.Error(err))
		panic(err)
	}
	s.liveCh = make(chan bool)
	s.processKeepAliveResponse(ch)
	s.UpdateRegistered(true)
}

var serverIDMu sync.Mutex

func (s *Session) getServerID() (int64, error) {
	serverIDMu.Lock()
	defer serverIDMu.Unlock()

	log.Debug("getServerID", zap.Bool("reuse", s.reuseNodeID))
	if s.reuseNodeID {
		// Notice, For standalone, all process share the same nodeID.
		if nodeID := paramtable.GetNodeID(); nodeID != 0 {
			return nodeID, nil
		}
	}
	nodeID, err := s.getServerIDWithKey(DefaultIDKey)
	if err != nil {
		return nodeID, err
	}
	if s.reuseNodeID {
		paramtable.SetNodeID(nodeID)
	}
	return nodeID, nil
}

func (s *Session) checkIDExist() {
	s.etcdCli.Txn(s.ctx).If(
		clientv3.Compare(
			clientv3.Version(path.Join(s.metaRoot, DefaultServiceRoot, DefaultIDKey)),
			"=",
			0)).
		Then(clientv3.OpPut(path.Join(s.metaRoot, DefaultServiceRoot, DefaultIDKey), "1")).Commit()
}

func (s *Session) getServerIDWithKey(key string) (int64, error) {
	for {
		getResp, err := s.etcdCli.Get(s.ctx, path.Join(s.metaRoot, DefaultServiceRoot, key))
		if err != nil {
			log.Warn("Session get etcd key error", zap.String("key", key), zap.Error(err))
			return -1, err
		}
		if getResp.Count <= 0 {
			log.Warn("Session there is no value", zap.String("key", key))
			continue
		}
		value := string(getResp.Kvs[0].Value)
		valueInt, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			log.Warn("Session ParseInt error", zap.String("value", value), zap.Error(err))
			continue
		}
		txnResp, err := s.etcdCli.Txn(s.ctx).If(
			clientv3.Compare(
				clientv3.Value(path.Join(s.metaRoot, DefaultServiceRoot, key)),
				"=",
				value)).
			Then(clientv3.OpPut(path.Join(s.metaRoot, DefaultServiceRoot, key), strconv.FormatInt(valueInt+1, 10))).Commit()
		if err != nil {
			log.Warn("Session Txn failed", zap.String("key", key), zap.Error(err))
			return -1, err
		}

		if !txnResp.Succeeded {
			log.Warn("Session Txn unsuccessful", zap.String("key", key))
			continue
		}
		log.Debug("Session get serverID success", zap.String("key", key), zap.Int64("ServerId", valueInt))
		return valueInt, nil
	}
}

func (s *Session) getCompleteKey() string {
	key := s.ServerName
	if !s.Exclusive || (s.enableActiveStandBy && s.isStandby.Load().(bool)) {
		key = fmt.Sprintf("%s-%d", key, s.ServerID)
	}
	return path.Join(s.metaRoot, DefaultServiceRoot, key)
}

func (s *Session) getSessionKey() string {
	key := s.ServerName
	if !s.Exclusive {
		key = fmt.Sprintf("%s-%d", key, s.ServerID)
	}
	return path.Join(s.metaRoot, DefaultServiceRoot, key)
}

func (s *Session) initWatchSessionCh() {
	var (
		err     error
		getResp *clientv3.GetResponse
	)

	err = retry.Do(context.Background(), func() error {
		getResp, err = s.etcdCli.Get(context.Background(), s.getSessionKey())
		log.Warn("fail to get the session key from the etcd", zap.Error(err))
		return err
	}, retry.Attempts(100))
	if err != nil {
		panic(err)
	}
	s.watchSessionKeyCh = s.etcdCli.Watch(context.Background(), s.getSessionKey(), clientv3.WithRev(getResp.Header.Revision))
}

// registerService registers the service to etcd so that other services
// can find that the service is online and issue subsequent operations
// RegisterService will save a key-value in etcd
// key: metaRootPath + "/services" + "/ServerName-ServerID"
// value: json format
//
//	{
//	    ServerID   int64  `json:"ServerID,omitempty"`
//	    ServerName string `json:"ServerName,omitempty"`
//	    Address    string `json:"Address,omitempty"`
//	    Exclusive  bool   `json:"Exclusive,omitempty"`
//	}
//
// Exclusive means whether this service can exist two at the same time, if so,
// it is false. Otherwise, set it to true.
func (s *Session) registerService() (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	if s.enableActiveStandBy {
		s.updateStandby(true)
	}
	completeKey := s.getCompleteKey()
	var ch <-chan *clientv3.LeaseKeepAliveResponse
	log.Debug("service begin to register to etcd", zap.String("serverName", s.ServerName), zap.Int64("ServerID", s.ServerID))

	registerFn := func() error {
		resp, err := s.etcdCli.Grant(s.ctx, s.sessionTTL)
		if err != nil {
			log.Error("register service", zap.Error(err))
			return err
		}
		s.leaseID = &resp.ID

		sessionJSON, err := json.Marshal(s)
		if err != nil {
			return err
		}

		txnResp, err := s.etcdCli.Txn(s.ctx).If(
			clientv3.Compare(
				clientv3.Version(completeKey),
				"=",
				0)).
			Then(clientv3.OpPut(completeKey, string(sessionJSON), clientv3.WithLease(resp.ID))).Commit()

		if err != nil {
			log.Warn("compare and swap error, maybe the key has already been registered", zap.Error(err))
			return err
		}

		if !txnResp.Succeeded {
			return fmt.Errorf("function CompareAndSwap error for compare is false for key: %s", s.ServerName)
		}
		log.Debug("put session key into etcd", zap.String("key", completeKey), zap.String("value", string(sessionJSON)))

		keepAliveCtx, keepAliveCancel := context.WithCancel(context.Background())
		s.keepAliveCtx = keepAliveCtx
		s.keepAliveCancel = keepAliveCancel
		ch, err = s.etcdCli.KeepAlive(keepAliveCtx, resp.ID)
		if err != nil {
			log.Warn("go error during keeping alive with etcd", zap.Error(err))
			return err
		}
		log.Info("Service registered successfully", zap.String("ServerName", s.ServerName), zap.Int64("serverID", s.ServerID))
		return nil
	}
	err := retry.Do(s.ctx, registerFn, retry.Attempts(uint(s.sessionRetryTimes)))
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// processKeepAliveResponse processes the response of etcd keepAlive interface
// If keepAlive fails for unexpected error, it will send a signal to the channel.
func (s *Session) processKeepAliveResponse(ch <-chan *clientv3.LeaseKeepAliveResponse) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case <-s.ctx.Done():
				log.Warn("keep alive", zap.Error(errors.New("context done")))
				if s.keepAliveCancel != nil {
					s.keepAliveCancel()
				}
				return
			case resp, ok := <-ch:
				if !ok {
					log.Warn("session keepalive channel closed")
					if !s.enableRetryKeepAlive {
						s.safeCloseLiveCh()
						return
					}
					log.Info("start try to KeepAliveOnce", zap.String("serverName", s.ServerName))
					s.retryKeepAlive.Store(true)
					// have to KeepAliveOnce before KeepAlive because KeepAlive won't throw error even when lease OT
					var keepAliveOnceResp *clientv3.LeaseKeepAliveResponse
					s.keepAliveCtx.Done()
					s.keepAliveCtx = context.Background()
					err := retry.Do(s.keepAliveCtx, func() error {
						ctx, cancel := context.WithTimeout(s.keepAliveCtx, time.Second*5)
						defer cancel()
						resp, err := s.etcdCli.KeepAliveOnce(ctx, *s.leaseID)
						keepAliveOnceResp = resp
						return err
					}, retry.Attempts(3))

					if err != nil {
						log.Warn("fail to retry keepAliveOnce", zap.String("serverName", s.ServerName), zap.Int64("leaseID", int64(*s.leaseID)), zap.Error(err))
						s.safeCloseLiveCh()
						return
					}
					log.Info("succeed to KeepAliveOnce", zap.String("serverName", s.ServerName), zap.Int64("leaseID", int64(*s.leaseID)), zap.Any("resp", keepAliveOnceResp))

					var chNew <-chan *clientv3.LeaseKeepAliveResponse
					err = retry.Do(s.keepAliveCtx, func() error {
						ctx, cancel := context.WithTimeout(s.keepAliveCtx, time.Second*5)
						defer cancel()
						ch, err := s.etcdCli.KeepAlive(ctx, *s.leaseID)
						chNew = ch
						return err
					}, retry.Attempts(3))

					if err != nil {
						log.Warn("fail to retry keepAlive", zap.Error(err))
						s.safeCloseLiveCh()
						return
					}
					s.processKeepAliveResponse(chNew)
					s.retryKeepAlive.Store(false)
					return
				}
				if resp == nil {
					log.Warn("session keepalive response failed")
					s.safeCloseLiveCh()
				}
				//failCh <- true
			}
		}
	}()
}

// GetSessions will get all sessions registered in etcd.
// Revision is returned for WatchServices to prevent key events from being missed.
func (s *Session) GetSessions(prefix string) (map[string]*Session, int64, error) {
	res := make(map[string]*Session)
	key := path.Join(s.metaRoot, DefaultServiceRoot, prefix)
	resp, err := s.etcdCli.Get(s.ctx, key, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, 0, err
	}
	for _, kv := range resp.Kvs {
		session := &Session{}
		err = json.Unmarshal(kv.Value, session)
		if err != nil {
			return nil, 0, err
		}
		_, mapKey := path.Split(string(kv.Key))
		log.Debug("SessionUtil GetSessions ", zap.Any("prefix", prefix),
			zap.String("key", mapKey),
			zap.Any("address", session.Address))
		res[mapKey] = session
	}
	return res, resp.Header.Revision, nil
}

// GetSessionsWithVersionRange will get all sessions with provided prefix and version range in etcd.
// Revision is returned for WatchServices to prevent missing events.
func (s *Session) GetSessionsWithVersionRange(prefix string, r semver.Range) (map[string]*Session, int64, error) {
	res := make(map[string]*Session)
	key := path.Join(s.metaRoot, DefaultServiceRoot, prefix)
	resp, err := s.etcdCli.Get(s.ctx, key, clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return nil, 0, err
	}
	for _, kv := range resp.Kvs {
		session := &Session{}
		err = json.Unmarshal(kv.Value, session)
		if err != nil {
			return nil, 0, err
		}
		if !r(session.Version) {
			log.Debug("Session version out of range", zap.String("version", session.Version.String()), zap.Int64("serverID", session.ServerID))
			continue
		}
		_, mapKey := path.Split(string(kv.Key))
		log.Debug("SessionUtil GetSessions ", zap.String("prefix", prefix),
			zap.String("key", mapKey),
			zap.String("address", session.Address))
		res[mapKey] = session
	}
	return res, resp.Header.Revision, nil
}

func (s *Session) GoingStop() error {
	if s == nil || s.etcdCli == nil || s.leaseID == nil {
		return errors.New("the session hasn't been init")
	}

	if s.Disconnected() {
		return errors.New("this session has disconnected")
	}

	completeKey := s.getCompleteKey()
	resp, err := s.etcdCli.Get(s.ctx, completeKey, clientv3.WithCountOnly())
	if err != nil {
		log.Error("fail to get the session", zap.String("key", completeKey), zap.Error(err))
		return err
	}
	if resp.Count == 0 {
		return nil
	}
	s.Stopping = true
	sessionJSON, err := json.Marshal(s)
	if err != nil {
		log.Error("fail to marshal the session", zap.String("key", completeKey))
		return err
	}
	_, err = s.etcdCli.Put(s.ctx, completeKey, string(sessionJSON), clientv3.WithLease(*s.leaseID))
	if err != nil {
		log.Error("fail to update the session to stopping state", zap.String("key", completeKey))
		return err
	}
	return nil
}

// SessionEvent indicates the changes of other servers.
// if a server is up, EventType is SessAddEvent.
// if a server is down, EventType is SessDelEvent.
// Session Saves the changed server's information.
type SessionEvent struct {
	EventType SessionEventType
	Session   *Session
}

type sessionWatcher struct {
	s        *Session
	rch      clientv3.WatchChan
	eventCh  chan *SessionEvent
	prefix   string
	rewatch  Rewatch
	validate func(*Session) bool
}

func (w *sessionWatcher) start() {
	go func() {
		for {
			select {
			case <-w.s.ctx.Done():
				return
			case wresp, ok := <-w.rch:
				if !ok {
					log.Warn("session watch channel closed")
					return
				}
				w.handleWatchResponse(wresp)
			}
		}
	}()
}

// WatchServices watches the service's up and down in etcd, and sends event to
// eventChannel.
// prefix is a parameter to know which service to watch and can be obtained in
// typeutil.type.go.
// revision is a etcd reversion to prevent missing key events and can be obtained
// in GetSessions.
// If a server up, an event will be add to channel with eventType SessionAddType.
// If a server down, an event will be add to channel with eventType SessionDelType.
func (s *Session) WatchServices(prefix string, revision int64, rewatch Rewatch) (eventChannel <-chan *SessionEvent) {
	w := &sessionWatcher{
		s:        s,
		eventCh:  make(chan *SessionEvent, 100),
		rch:      s.etcdCli.Watch(s.ctx, path.Join(s.metaRoot, DefaultServiceRoot, prefix), clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(revision)),
		prefix:   prefix,
		rewatch:  rewatch,
		validate: func(s *Session) bool { return true },
	}
	w.start()
	return w.eventCh
}

// WatchServicesWithVersionRange watches the service's up and down in etcd, and sends event to event Channel.
// Acts like WatchServices but with extra version range check.
// prefix is a parameter to know which service to watch and can be obtained in type util.type.go.
// revision is a etcd reversion to prevent missing key events and can be obtained in GetSessions.
// If a server up, an event will be add to channel with eventType SessionAddType.
// If a server down, an event will be add to channel with eventType SessionDelType.
func (s *Session) WatchServicesWithVersionRange(prefix string, r semver.Range, revision int64, rewatch Rewatch) (eventChannel <-chan *SessionEvent) {
	w := &sessionWatcher{
		s:        s,
		eventCh:  make(chan *SessionEvent, 100),
		rch:      s.etcdCli.Watch(s.ctx, path.Join(s.metaRoot, DefaultServiceRoot, prefix), clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(revision)),
		prefix:   prefix,
		rewatch:  rewatch,
		validate: func(s *Session) bool { return r(s.Version) },
	}
	w.start()
	return w.eventCh
}

func (w *sessionWatcher) handleWatchResponse(wresp clientv3.WatchResponse) {
	if wresp.Err() != nil {
		err := w.handleWatchErr(wresp.Err())
		if err != nil {
			log.Error("failed to handle watch session response", zap.Error(err))
			panic(err)
		}
		return
	}
	for _, ev := range wresp.Events {
		session := &Session{}
		var eventType SessionEventType
		switch ev.Type {
		case mvccpb.PUT:
			log.Debug("watch services",
				zap.Any("add kv", ev.Kv))
			err := json.Unmarshal(ev.Kv.Value, session)
			if err != nil {
				log.Error("watch services", zap.Error(err))
				continue
			}
			if !w.validate(session) {
				continue
			}
			if session.Stopping {
				eventType = SessionUpdateEvent
			} else {
				eventType = SessionAddEvent
			}
		case mvccpb.DELETE:
			log.Debug("watch services",
				zap.Any("delete kv", ev.PrevKv))
			err := json.Unmarshal(ev.PrevKv.Value, session)
			if err != nil {
				log.Error("watch services", zap.Error(err))
				continue
			}
			if !w.validate(session) {
				continue
			}
			eventType = SessionDelEvent
		}
		log.Debug("WatchService", zap.Any("event type", eventType))
		w.eventCh <- &SessionEvent{
			EventType: eventType,
			Session:   session,
		}
	}
}

func (w *sessionWatcher) handleWatchErr(err error) error {
	// if not ErrCompacted, just close the channel
	if err != v3rpc.ErrCompacted {
		//close event channel
		log.Warn("Watch service found error", zap.Error(err))
		close(w.eventCh)
		return err
	}

	sessions, revision, err := w.s.GetSessions(w.prefix)
	if err != nil {
		log.Warn("GetSession before rewatch failed", zap.String("prefix", w.prefix), zap.Error(err))
		close(w.eventCh)
		return err
	}
	// rewatch is nil, no logic to handle
	if w.rewatch == nil {
		log.Warn("Watch service with ErrCompacted but no rewatch logic provided")
	} else {
		err = w.rewatch(sessions)
	}
	if err != nil {
		log.Warn("WatchServices rewatch failed", zap.String("prefix", w.prefix), zap.Error(err))
		close(w.eventCh)
		return err
	}

	w.rch = w.s.etcdCli.Watch(w.s.ctx, path.Join(w.s.metaRoot, DefaultServiceRoot, w.prefix), clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(revision))
	return nil
}

// LivenessCheck performs liveness check with provided context and channel
// ctx controls the liveness check loop
// ch is the liveness signal channel, ch is closed only when the session is expired
// callback is the function to call when ch is closed, note that callback will not be invoked when loop exits due to context
func (s *Session) LivenessCheck(ctx context.Context, callback func()) {
	s.initWatchSessionCh()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			select {
			case _, ok := <-s.liveCh:
				// ok, still alive
				if ok {
					continue
				}
				// not ok, connection lost
				log.Warn("connection lost detected, shuting down")
				s.SetDisconnected(true)
				if callback != nil {
					go callback()
				}
				return
			case <-ctx.Done():
				log.Debug("liveness exits due to context done")
				s.enableRetryKeepAlive = false
				// cancel the etcd keepAlive context
				if s.keepAliveCancel != nil {
					s.keepAliveCancel()
				}
				return
			case resp, ok := <-s.watchSessionKeyCh:
				if !ok {
					log.Warn("watch session key channel closed")
					if s.keepAliveCancel != nil {
						s.keepAliveCancel()
					}
					return
				}
				if resp.Err() != nil {
					// if not ErrCompacted, just close the channel
					if resp.Err() != v3rpc.ErrCompacted {
						//close event channel
						log.Warn("Watch service found error", zap.Error(resp.Err()))
						if s.keepAliveCancel != nil {
							s.keepAliveCancel()
						}
						return
					}
					log.Warn("Watch service found compacted error", zap.Error(resp.Err()))
					getResp, err := s.etcdCli.Get(s.ctx, s.getSessionKey())
					if err != nil || len(getResp.Kvs) == 0 {
						if s.keepAliveCancel != nil {
							s.keepAliveCancel()
						}
						return
					}
					s.watchSessionKeyCh = s.etcdCli.Watch(s.ctx, s.getSessionKey(), clientv3.WithRev(getResp.Header.Revision))
					continue
				}
				for _, event := range resp.Events {
					switch event.Type {
					case mvccpb.PUT:
						log.Info("register session success", zap.String("role", s.ServerName), zap.String("key", string(event.Kv.Key)))
					case mvccpb.DELETE:
						if s.isRetryingKeepAlive() {
							log.Info("session key is deleted during re register, ignore this DELETE event", zap.String("role", s.ServerName), zap.String("key", string(event.Kv.Key)))
							continue
						}
						log.Info("session key is deleted, exit...", zap.String("role", s.ServerName), zap.String("key", string(event.Kv.Key)))
						if s.keepAliveCancel != nil {
							s.keepAliveCancel()
						}
					}
				}
			}
		}
	}()
}

func (s *Session) Stop() {
	s.Revoke(time.Second)
	if s.keepAliveCancel != nil {
		s.keepAliveCancel()
	}
	s.wg.Wait()
}

// Revoke revokes the internal leaseID for the session key
func (s *Session) Revoke(timeout time.Duration) {
	if s == nil {
		return
	}
	if s.etcdCli == nil || s.leaseID == nil {
		return
	}
	if s.Disconnected() {
		return
	}
	// can NOT use s.ctx, it may be Done here
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// ignores resp & error, just do best effort to revoke
	_, _ = s.etcdCli.Revoke(ctx, *s.leaseID)
}

// UpdateRegistered update the state of registered.
func (s *Session) UpdateRegistered(b bool) {
	s.registered.Store(b)
}

// Registered check if session was registered into etcd.
func (s *Session) Registered() bool {
	b, ok := s.registered.Load().(bool)
	if !ok {
		return false
	}
	return b
}

func (s *Session) SetDisconnected(b bool) {
	s.disconnected.Store(b)
}

func (s *Session) Disconnected() bool {
	b, ok := s.disconnected.Load().(bool)
	if !ok {
		return false
	}
	return b
}

func (s *Session) SetEnableActiveStandBy(enable bool) {
	s.enableActiveStandBy = enable
}

func (s *Session) updateStandby(b bool) {
	s.isStandby.Store(b)
}

func (s *Session) SetEnableRetryKeepAlive(enable bool) {
	s.enableRetryKeepAlive = enable
}

func (s *Session) isRetryingKeepAlive() bool {
	v, ok := s.retryKeepAlive.Load().(bool)
	if !ok {
		return false
	}
	return v
}

func (s *Session) safeCloseLiveCh() {
	s.liveChOnce.Do(func() {
		close(s.liveCh)
	})
}

// ProcessActiveStandBy is used by coordinators to do active-standby mechanism.
// coordinator enabled active-standby will first call Register and then call ProcessActiveStandBy.
// steps:
// 1, Enter STANDBY mode
// 2, Try to register to active key.
// 3, If 2. return true, this service becomes ACTIVE. Exit STANDBY mode.
// 4, If 2. return false, which means an ACTIVE service already exist.
//
//	Start watching the active key. Whenever active key disappears, STANDBY node will go backup to 2.
//
// activateFunc is the function to re-active the service.
func (s *Session) ProcessActiveStandBy(activateFunc func() error) error {
	s.activeKey = path.Join(s.metaRoot, DefaultServiceRoot, s.ServerName)

	// try to register to the active_key.
	// return
	//   1. doRegistered: if registered the active_key by this session or by other session
	//   2. revision: revision of the active_key
	//   3. err: etcd error, should retry
	registerActiveFn := func() (bool, int64, error) {
		log.Info(fmt.Sprintf("try to register as ACTIVE %v service...", s.ServerName))
		sessionJSON, err := json.Marshal(s)
		if err != nil {
			log.Error("json marshal error", zap.Error(err))
			return false, -1, err
		}
		txnResp, err := s.etcdCli.Txn(s.ctx).If(
			clientv3.Compare(
				clientv3.Version(s.activeKey),
				"=",
				0)).
			Then(clientv3.OpPut(s.activeKey, string(sessionJSON), clientv3.WithLease(*s.leaseID))).Commit()
		if err != nil {
			log.Error("register active key to etcd failed", zap.Error(err))
			return false, -1, err
		}
		doRegistered := txnResp.Succeeded
		if doRegistered {
			log.Info(fmt.Sprintf("register ACTIVE %s", s.ServerName))
		} else {
			log.Info(fmt.Sprintf("ACTIVE %s has already been registered", s.ServerName))
		}
		revision := txnResp.Header.GetRevision()
		return doRegistered, revision, nil
	}
	s.updateStandby(true)
	log.Info(fmt.Sprintf("serverName: %v enter STANDBY mode", s.ServerName))
	go func() {
		for s.isStandby.Load().(bool) {
			log.Debug(fmt.Sprintf("serverName: %v is in STANDBY ...", s.ServerName))
			time.Sleep(10 * time.Second)
		}
	}()

	for {
		registered, revision, err := registerActiveFn()
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if registered {
			break
		}
		log.Info(fmt.Sprintf("%s start to watch ACTIVE key %s", s.ServerName, s.activeKey))
		ctx, cancel := context.WithCancel(s.ctx)
		watchChan := s.etcdCli.Watch(ctx, s.activeKey, clientv3.WithPrevKV(), clientv3.WithRev(revision))
		select {
		case <-ctx.Done():
			cancel()
		case wresp, ok := <-watchChan:
			if !ok {
				cancel()
			}
			if wresp.Err() != nil {
				cancel()
			}
			for _, event := range wresp.Events {
				switch event.Type {
				case mvccpb.PUT:
					log.Debug("watch the ACTIVE key", zap.Any("ADD", event.Kv))
				case mvccpb.DELETE:
					log.Debug("watch the ACTIVE key", zap.Any("DELETE", event.Kv))
					cancel()
				}
			}
		}
		cancel()
		log.Info(fmt.Sprintf("stop watching ACTIVE key %v", s.activeKey))
	}

	s.updateStandby(false)
	log.Info(fmt.Sprintf("serverName: %v quit STANDBY mode, this node will become ACTIVE", s.ServerName))
	if activateFunc != nil {
		return activateFunc()
	}
	return nil
}
