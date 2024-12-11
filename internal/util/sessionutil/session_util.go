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
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
)

const (
	// DefaultServiceRoot default root path used in kv by Session
	DefaultServiceRoot = "session/"
	// DefaultIDKey default id key for Session
	DefaultIDKey         = "id"
	SupportedLabelPrefix = "MILVUS_SERVER_LABEL_"
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

type IndexEngineVersion struct {
	MinimalIndexVersion int32 `json:"MinimalIndexVersion,omitempty"`
	CurrentIndexVersion int32 `json:"CurrentIndexVersion,omitempty"`
}

// SessionRaw the persistent part of Session.
type SessionRaw struct {
	ServerID           int64  `json:"ServerID,omitempty"`
	ServerName         string `json:"ServerName,omitempty"`
	Address            string `json:"Address,omitempty"`
	Exclusive          bool   `json:"Exclusive,omitempty"`
	Stopping           bool   `json:"Stopping,omitempty"`
	TriggerKill        bool
	Version            string             `json:"Version"`
	IndexEngineVersion IndexEngineVersion `json:"IndexEngineVersion,omitempty"`
	LeaseID            *clientv3.LeaseID  `json:"LeaseID,omitempty"`

	HostName     string            `json:"HostName,omitempty"`
	EnableDisk   bool              `json:"EnableDisk,omitempty"`
	ServerLabels map[string]string `json:"ServerLabels,omitempty"`
}

func (s *SessionRaw) GetAddress() string {
	return s.Address
}

func (s *SessionRaw) GetServerID() int64 {
	return s.ServerID
}

func (s *SessionRaw) GetServerLabel() map[string]string {
	return s.ServerLabels
}

func (s *SessionRaw) IsTriggerKill() bool {
	return s.TriggerKill
}

// Session is a struct to store service's session, including ServerID, ServerName,
// Address.
// Exclusive indicates that this server can only start one.
type Session struct {
	ctx context.Context
	// When outside context done, Session cancels its goroutines first, then uses
	// keepAliveCancel to cancel the etcd KeepAlive
	keepAliveLock   sync.Mutex
	keepAliveCancel context.CancelFunc
	keepAliveCtx    context.Context

	SessionRaw

	Version semver.Version `json:"Version,omitempty"`

	liveChOnce sync.Once
	liveCh     chan struct{}

	etcdCli           *clientv3.Client
	watchSessionKeyCh clientv3.WatchChan
	watchCancel       atomic.Pointer[context.CancelFunc]
	wg                sync.WaitGroup

	metaRoot string

	registered   atomic.Value
	disconnected atomic.Value

	isStandby           atomic.Value
	enableActiveStandBy bool
	activeKey           string

	sessionTTL        int64
	sessionRetryTimes int64
	reuseNodeID       bool

	isStopped atomic.Bool // set to true if stop method is invoked
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

// WithIndexEngineVersion should be only used by querynode.
func WithIndexEngineVersion(minimal, current int32) SessionOption {
	return func(session *Session) {
		session.IndexEngineVersion.MinimalIndexVersion = minimal
		session.IndexEngineVersion.CurrentIndexVersion = current
	}
}

func WithEnableDisk(enableDisk bool) SessionOption {
	return func(s *Session) {
		s.EnableDisk = enableDisk
	}
}

func (s *Session) apply(opts ...SessionOption) {
	for _, opt := range opts {
		opt(s)
	}
}

// UnmarshalJSON unmarshal bytes to Session.
func (s *Session) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &s.SessionRaw)
	if err != nil {
		return err
	}

	if s.SessionRaw.Version != "" {
		s.Version, err = semver.Parse(s.SessionRaw.Version)
		if err != nil {
			return err
		}
	}

	return nil
}

// MarshalJSON marshals session to bytes.
func (s *Session) MarshalJSON() ([]byte, error) {
	s.SessionRaw.Version = s.Version.String()
	return json.Marshal(s.SessionRaw)
}

// Create a new Session object. Will use global etcd client
func NewSession(ctx context.Context, opts ...SessionOption) *Session {
	client, path := kvfactory.GetEtcdAndPath()
	return NewSessionWithEtcd(ctx, path, client, opts...)
}

// NewSessionWithEtcd is a helper to build a Session object.
// ServerID, ServerName, Address, Exclusive will be assigned after Init().
// metaRoot is a path in etcd to save session information.
// etcdEndpoints is to init etcdCli when NewSession
func NewSessionWithEtcd(ctx context.Context, metaRoot string, client *clientv3.Client, opts ...SessionOption) *Session {
	hostName, hostNameErr := os.Hostname()
	if hostNameErr != nil {
		log.Ctx(ctx).Error("get host name fail", zap.Error(hostNameErr))
	}

	session := &Session{
		ctx:      ctx,
		metaRoot: metaRoot,
		Version:  common.Version,

		SessionRaw: SessionRaw{
			HostName: hostName,
		},

		// options
		sessionTTL:        paramtable.Get().CommonCfg.SessionTTL.GetAsInt64(),
		sessionRetryTimes: paramtable.Get().CommonCfg.SessionRetryTimes.GetAsInt64(),
		reuseNodeID:       true,
		isStopped:         *atomic.NewBool(false),
	}

	// integration test create cluster with different nodeId in one process
	if paramtable.Get().IntegrationTestCfg.IntegrationMode.GetAsBool() {
		session.reuseNodeID = false
	}

	session.apply(opts...)

	session.UpdateRegistered(false)

	connectEtcdFn := func() error {
		log.Ctx(ctx).Debug("Session try to connect to etcd")
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
		log.Ctx(ctx).Warn("failed to initialize session",
			zap.Error(err))
		return nil
	}
	log.Ctx(ctx).Debug("Session connect to etcd success")
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
	s.ServerLabels = GetServerLabelsFromEnv(serverName)
	log.Info("start server", zap.String("name", serverName), zap.String("address", address), zap.Int64("id", s.ServerID), zap.Any("server_labels", s.ServerLabels))
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
	s.liveCh = make(chan struct{})
	s.processKeepAliveResponse(ch)
	s.UpdateRegistered(true)
}

var serverIDMu sync.Mutex

func (s *Session) getServerID() (int64, error) {
	serverIDMu.Lock()
	defer serverIDMu.Unlock()

	log.Ctx(s.ctx).Debug("getServerID", zap.Bool("reuse", s.reuseNodeID))
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

func GetServerLabelsFromEnv(role string) map[string]string {
	ret := make(map[string]string)
	switch role {
	case "querynode":
		for _, value := range os.Environ() {
			rs := []rune(value)
			in := strings.Index(value, "=")
			key := string(rs[0:in])
			value := string(rs[in+1:])

			if strings.HasPrefix(key, SupportedLabelPrefix) {
				label := strings.TrimPrefix(key, SupportedLabelPrefix)
				ret[label] = value
			}
		}
	}
	return ret
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
	log := log.Ctx(s.ctx)
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

func (s *Session) initWatchSessionCh(ctx context.Context) error {
	var (
		err     error
		getResp *clientv3.GetResponse
	)

	ctx, cancel := context.WithCancel(ctx)
	s.watchCancel.Store(&cancel)

	err = retry.Do(ctx, func() error {
		getResp, err = s.etcdCli.Get(ctx, s.getSessionKey())
		return err
	}, retry.Attempts(uint(s.sessionRetryTimes)))
	if err != nil {
		log.Warn("fail to get the session key from the etcd", zap.Error(err))
		return err
	}
	s.watchSessionKeyCh = s.etcdCli.Watch(ctx, s.getSessionKey(), clientv3.WithRev(getResp.Header.Revision))
	return nil
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
	log := log.Ctx(s.ctx)
	log.Debug("service begin to register to etcd", zap.String("serverName", s.ServerName), zap.Int64("ServerID", s.ServerID))

	registerFn := func() error {
		resp, err := s.etcdCli.Grant(s.ctx, s.sessionTTL)
		if err != nil {
			log.Error("register service", zap.Error(err))
			return err
		}
		s.LeaseID = &resp.ID

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
			log.Warn("register on etcd error, check the availability of etcd ", zap.Error(err))
			return err
		}

		if txnResp != nil && !txnResp.Succeeded {
			s.handleRestart(completeKey)
			return fmt.Errorf("function CompareAndSwap error for compare is false for key: %s", s.ServerName)
		}
		log.Info("put session key into etcd", zap.String("key", completeKey), zap.String("value", string(sessionJSON)))

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

// Handle restart is fast path to handle node restart.
// This should be only a fast path for coordinator
// If we find previous session have same address as current , simply purge the old one so the recovery can be much faster
func (s *Session) handleRestart(key string) {
	resp, err := s.etcdCli.Get(s.ctx, key)
	log := log.With(zap.String("key", key))
	if err != nil {
		log.Warn("failed to read old session from etcd, ignore", zap.Error(err))
		return
	}
	for _, kv := range resp.Kvs {
		session := &Session{}
		err = json.Unmarshal(kv.Value, session)
		if err != nil {
			log.Warn("failed to unmarshal old session from etcd, ignore", zap.Error(err))
			return
		}

		if session.Address == s.Address && session.ServerID < s.ServerID {
			log.Warn("find old session is same as current node, assume it as restart, purge old session", zap.String("key", key),
				zap.String("address", session.Address))
			_, err := s.etcdCli.Delete(s.ctx, key)
			if err != nil {
				log.Warn("failed to unmarshal old session from etcd, ignore", zap.Error(err))
				return
			}
		}
	}
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
				s.cancelKeepAlive()
				return
			case resp, ok := <-ch:
				if !ok {
					log.Warn("session keepalive channel closed")

					// if keep alive is canceled, keepAliveCtx.Err() will return a non-nil error
					if s.keepAliveCtx.Err() != nil {
						s.safeCloseLiveCh()
						return
					}

					log.Info("keepAlive channel close caused by etcd, try to KeepAliveOnce", zap.String("serverName", s.ServerName))
					s.keepAliveLock.Lock()
					defer s.keepAliveLock.Unlock()
					// have to KeepAliveOnce before KeepAlive because KeepAlive won't throw error even when lease OT
					var keepAliveOnceResp *clientv3.LeaseKeepAliveResponse
					s.keepAliveCancel()
					s.keepAliveCtx, s.keepAliveCancel = context.WithCancel(context.Background())
					err := retry.Do(s.ctx, func() error {
						ctx, cancel := context.WithTimeout(s.keepAliveCtx, time.Second*10)
						defer cancel()
						resp, err := s.etcdCli.KeepAliveOnce(ctx, *s.LeaseID)
						keepAliveOnceResp = resp
						return err
					}, retry.Attempts(3))
					if err != nil {
						log.Warn("fail to retry keepAliveOnce", zap.String("serverName", s.ServerName), zap.Int64("LeaseID", int64(*s.LeaseID)), zap.Error(err))
						s.safeCloseLiveCh()
						return
					}
					log.Info("succeed to KeepAliveOnce", zap.String("serverName", s.ServerName), zap.Int64("LeaseID", int64(*s.LeaseID)), zap.Any("resp", keepAliveOnceResp))

					var chNew <-chan *clientv3.LeaseKeepAliveResponse
					keepAliveFunc := func() error {
						var err1 error
						chNew, err1 = s.etcdCli.KeepAlive(s.keepAliveCtx, *s.LeaseID)
						return err1
					}
					err = fnWithTimeout(keepAliveFunc, time.Second*10)
					if err != nil {
						log.Warn("fail to retry keepAlive", zap.Error(err))
						s.safeCloseLiveCh()
						return
					}
					go s.processKeepAliveResponse(chNew)
					return
				}
				if resp == nil {
					log.Warn("session keepalive response failed")
					s.safeCloseLiveCh()
				}
			}
		}
	}()
}

func fnWithTimeout(fn func() error, d time.Duration) error {
	if d != 0 {
		resultChan := make(chan bool)
		var err1 error
		go func() {
			err1 = fn()
			resultChan <- true
		}()

		select {
		case <-resultChan:
			log.Ctx(context.TODO()).Debug("retry func success")
		case <-time.After(d):
			return fmt.Errorf("func timed out")
		}
		return err1
	}
	return fn()
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
		log.Ctx(s.ctx).Debug("SessionUtil GetSessions",
			zap.String("prefix", prefix),
			zap.String("key", mapKey),
			zap.String("address", session.Address))
		res[mapKey] = session
	}
	return res, resp.Header.Revision, nil
}

// GetSessionsWithVersionRange will get all sessions with provided prefix and version range in etcd.
// Revision is returned for WatchServices to prevent missing events.
func (s *Session) GetSessionsWithVersionRange(prefix string, r semver.Range) (map[string]*Session, int64, error) {
	log := log.Ctx(s.ctx)
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
	if s == nil || s.etcdCli == nil || s.LeaseID == nil {
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
	_, err = s.etcdCli.Put(s.ctx, completeKey, string(sessionJSON), clientv3.WithLease(*s.LeaseID))
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
	log := log.Ctx(context.TODO())
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
		// close event channel
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
// callback must be called before liveness check exit, to close the session's owner component
func (s *Session) LivenessCheck(ctx context.Context, callback func()) {
	err := s.initWatchSessionCh(ctx)
	if err != nil {
		log.Error("failed to get session for liveness check", zap.Error(err))
		s.cancelKeepAlive()
		if callback != nil {
			go callback()
		}
		return
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if callback != nil {
			// before exit liveness check, callback to exit the session owner
			defer func() {
				// the callback method will not be invoked if session is stopped.
				if ctx.Err() == nil && !s.isStopped.Load() {
					go callback()
				}
			}()
		}
		defer s.SetDisconnected(true)
		for {
			select {
			case _, ok := <-s.liveCh:
				// ok, still alive
				if ok {
					continue
				}
				// not ok, connection lost
				log.Warn("connection lost detected, shuting down")
				return
			case <-ctx.Done():
				log.Warn("liveness exits due to context done")
				// cancel the etcd keepAlive context
				s.cancelKeepAlive()
				return
			case resp, ok := <-s.watchSessionKeyCh:
				if !ok {
					log.Warn("watch session key channel closed")
					s.cancelKeepAlive()
					return
				}
				if resp.Err() != nil {
					// if not ErrCompacted, just close the channel
					if resp.Err() != v3rpc.ErrCompacted {
						// close event channel
						log.Warn("Watch service found error", zap.Error(resp.Err()))
						s.cancelKeepAlive()
						return
					}
					log.Warn("Watch service found compacted error", zap.Error(resp.Err()))
					err := s.initWatchSessionCh(ctx)
					if err != nil {
						log.Warn("failed to get session during reconnecting", zap.Error(err))
						s.cancelKeepAlive()
					}
					continue
				}
				for _, event := range resp.Events {
					switch event.Type {
					case mvccpb.PUT:
						log.Info("register session success", zap.String("role", s.ServerName), zap.String("key", string(event.Kv.Key)))
					case mvccpb.DELETE:
						log.Info("session key is deleted, exit...", zap.String("role", s.ServerName), zap.String("key", string(event.Kv.Key)))
						s.cancelKeepAlive()
					}
				}
			}
		}
	}()
}

func (s *Session) cancelKeepAlive() {
	s.keepAliveLock.Lock()
	defer s.keepAliveLock.Unlock()
	if s.keepAliveCancel != nil {
		s.keepAliveCancel()
	}
}

func (s *Session) Stop() {
	s.isStopped.Store(true)
	s.Revoke(time.Second)
	s.cancelKeepAlive()
	s.wg.Wait()
}

// Revoke revokes the internal LeaseID for the session key
func (s *Session) Revoke(timeout time.Duration) {
	if s == nil {
		return
	}
	log.Info("start to revoke session", zap.String("sessionKey", s.activeKey))
	if s.etcdCli == nil || s.LeaseID == nil {
		log.Warn("skip remove session",
			zap.String("sessionKey", s.activeKey),
			zap.Bool("etcdCliIsNil", s.etcdCli == nil),
			zap.Bool("LeaseIDIsNil", s.LeaseID == nil),
		)
		return
	}
	if s.Disconnected() {
		log.Warn("skip remove session, connection is disconnected", zap.String("sessionKey", s.activeKey))
		return
	}
	// can NOT use s.ctx, it may be Done here
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	// ignores resp & error, just do best effort to revoke
	_, err := s.etcdCli.Revoke(ctx, *s.LeaseID)
	if err != nil {
		log.Warn("failed to revoke session", zap.String("sessionKey", s.activeKey), zap.Error(err))
	}
	log.Info("revoke session successfully", zap.String("sessionKey", s.activeKey))
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

func (s *Session) safeCloseLiveCh() {
	s.liveChOnce.Do(func() {
		close(s.liveCh)
		if s.watchCancel.Load() != nil {
			(*s.watchCancel.Load())()
		}
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
	log := log.Ctx(s.ctx)
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
			Then(clientv3.OpPut(s.activeKey, string(sessionJSON), clientv3.WithLease(*s.LeaseID))).Commit()
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
	log.Info(fmt.Sprintf("serverName: %v quit STANDBY mode, this node will become ACTIVE, ID: %d", s.ServerName, s.ServerID))
	if activateFunc != nil {
		return activateFunc()
	}
	return nil
}

func (s *Session) ForceActiveStandby(activateFunc func() error) error {
	s.activeKey = path.Join(s.metaRoot, DefaultServiceRoot, s.ServerName)

	// force register to the active_key.
	forceRegisterActiveFn := func() error {
		log.Info(fmt.Sprintf("try to register as ACTIVE %v service...", s.ServerName))
		sessionJSON, err := json.Marshal(s)
		if err != nil {
			log.Error("json marshal error", zap.Error(err))
			return err
		}

		// try to release old session first
		sessions, _, err := s.GetSessions(s.ServerName)
		if err != nil {
			return err
		}

		if len(sessions) != 0 {
			activeSess := sessions[s.ServerName]
			if activeSess == nil || activeSess.LeaseID == nil {
				// force delete all old sessions
				s.etcdCli.Delete(s.ctx, s.activeKey)
				for _, sess := range sessions {
					if sess.ServerID != s.ServerID {
						sess.getCompleteKey()
						key := path.Join(s.metaRoot, DefaultServiceRoot, fmt.Sprintf("%s-%d", sess.ServerName, sess.ServerID))
						s.etcdCli.Delete(s.ctx, key)
					}
				}
			} else {
				// force release old active session
				_, _ = s.etcdCli.Revoke(s.ctx, *activeSess.LeaseID)
			}
		}

		// then try to register as active
		resp, err := s.etcdCli.Txn(s.ctx).If(
			clientv3.Compare(
				clientv3.Version(s.activeKey),
				"=",
				0)).
			Then(clientv3.OpPut(s.activeKey, string(sessionJSON), clientv3.WithLease(*s.LeaseID))).Commit()

		if err != nil || !resp.Succeeded {
			msg := fmt.Sprintf("failed to force register ACTIVE %s", s.ServerName)
			log.Error(msg, zap.Error(err), zap.Any("resp", resp))
			return errors.New(msg)
		}

		log.Info(fmt.Sprintf("force register ACTIVE %s", s.ServerName))
		return nil
	}

	err := retry.Do(s.ctx, forceRegisterActiveFn, retry.Attempts(uint(s.sessionRetryTimes)))
	if err != nil {
		log.Warn(fmt.Sprintf("failed to force register ACTIVE %s", s.ServerName))
		return err
	}
	s.updateStandby(false)
	log.Info(fmt.Sprintf("serverName: %v quit STANDBY mode, this node will become ACTIVE, ID: %d", s.ServerName, s.ServerID))
	if activateFunc != nil {
		return activateFunc()
	}
	return nil
}

func filterEmptyStrings(s []string) []string {
	var filtered []string
	for _, str := range s {
		if str != "" {
			filtered = append(filtered, str)
		}
	}
	return filtered
}

func GetSessions(pid int) []string {
	fileFullName := GetServerInfoFilePath(pid)
	if _, err := os.Stat(fileFullName); errors.Is(err, os.ErrNotExist) {
		log.Warn("not found server info file path", zap.String("filePath", fileFullName), zap.Error(err))
		return []string{}
	}

	v, err := storage.ReadFile(fileFullName)
	if err != nil {
		log.Warn("read server info file path failed", zap.String("filePath", fileFullName), zap.Error(err))
		return []string{}
	}

	return filterEmptyStrings(strings.Split(string(v), "\n"))
}

func RemoveServerInfoFile(pid int) {
	fullPath := GetServerInfoFilePath(pid)
	_ = os.Remove(fullPath)
}

// GetServerInfoFilePath get server info file path, eg: /tmp/milvus/server_id_123456789
// Notes: this method will not support Windows OS
// return file path
func GetServerInfoFilePath(pid int) string {
	tmpDir := "/tmp/milvus"
	_ = os.Mkdir(tmpDir, os.ModePerm)
	fileName := fmt.Sprintf("server_id_%d", pid)
	filePath := filepath.Join(tmpDir, fileName)
	return filePath
}

func saveServerInfoInternal(role string, serverID int64, pid int) {
	fileFullPath := GetServerInfoFilePath(pid)
	fd, err := os.OpenFile(fileFullPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o664)
	if err != nil {
		log.Warn("open server info file fail", zap.String("filePath", fileFullPath), zap.Error(err))
		return
	}
	defer fd.Close()

	data := fmt.Sprintf("%s-%d\n", role, serverID)
	_, err = fd.WriteString(data)
	if err != nil {
		log.Warn("write server info file fail", zap.String("filePath", fileFullPath), zap.Error(err))
	}

	log.Info("save server info into file", zap.String("content", data), zap.String("filePath", fileFullPath))
}

func SaveServerInfo(role string, serverID int64) {
	saveServerInfoInternal(role, serverID, os.Getpid())
}

// GetSessionPrefixByRole get session prefix by role
func GetSessionPrefixByRole(role string) string {
	return path.Join(paramtable.Get().EtcdCfg.MetaRootPath.GetValue(), DefaultServiceRoot, role)
}
