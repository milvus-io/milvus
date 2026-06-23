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
	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	// DefaultServiceRoot default root path used in kv by Session
	DefaultServiceRoot = "session/"
	// DefaultIDKey default id key for Session
	DefaultIDKey           = "id"
	MilvusNodeIDForTesting = "MILVUS_NODE_ID_FOR_TESTING"
	// ExitCodeEtcd is the exit code used when the process must terminate due to
	// an unrecoverable etcd failure (session lease expired, watch channel closed, etc.).
	// Using a distinctive code (80) so K8s pod status can identify etcd-related crashes.
	ExitCodeEtcd = 80

	serverVersionKey = "version"
)

var errSessionVersionCheckFailure = errors.New("session version check failure")

// errSessionExpiredAtClientSide is a keepalive deadline cause compared by
// identity via errors.Is(context.Cause(ctx), ...). It must stay a bare
// package-level sentinel rather than a merr error: merr's errors.Is walks the
// wrappedMilvusError/code chain and would not preserve this identity check.
var errSessionExpiredAtClientSide = errors.New("session expired at client side")

// isNotSessionVersionCheckFailure checks if the error is not a session version check failure.
func isNotSessionVersionCheckFailure(err error) bool {
	return !errors.Is(err, errSessionVersionCheckFailure)
}

// EnableEmbededQueryNodeLabel set server labels for embedded query node.
func EnableEmbededQueryNodeLabel() {
	os.Setenv(NewServerLabel(typeutil.QueryNodeRole, LabelStreamingNodeEmbeddedQueryNode), "1")
}

// EnableStandaloneLabel set server labels for standalone.
func EnableStandaloneLabel() {
	os.Setenv(NewServerLabel("", LabelStandalone), "1")
}

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
	MaximumIndexVersion int32 `json:"MaximumIndexVersion,omitempty"`
}

// SessionRaw the persistent part of Session.
type SessionRaw struct {
	ServerID                 int64              `json:"ServerID,omitempty"`
	ServerName               string             `json:"ServerName,omitempty"`
	Address                  string             `json:"Address,omitempty"`
	Exclusive                bool               `json:"Exclusive,omitempty"`
	Stopping                 bool               `json:"Stopping,omitempty"`
	Version                  string             `json:"Version"`
	IndexEngineVersion       IndexEngineVersion `json:"IndexEngineVersion,omitempty"`
	ScalarIndexEngineVersion IndexEngineVersion `json:"ScalarIndexEngineVersion,omitempty"`
	IndexNonEncoding         bool               `json:"IndexNonEncoding,omitempty"`
	LeaseID                  *clientv3.LeaseID  `json:"LeaseID,omitempty"`

	HostName     string            `json:"HostName,omitempty"`
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

// GetResourceGroupName returns the resource group name of the session.
func (s *SessionRaw) GetResourceGroupName() string {
	if s.ServerLabels == nil {
		return ""
	}
	return s.ServerLabels[LabelResourceGroup]
}

// Session is a struct to store service's session, including ServerID, ServerName,
// Address.
// Exclusive indicates that this server can only start one.
// TODO: it's a bad implementation to mix up the service registration and service diescovery into one struct.
// because the registration is used by server side, but the discovery is used by client side.
// we should split the service registration and service diescovery.
type Session struct {
	mlog.Binder

	ctx    context.Context
	cancel context.CancelFunc

	SessionRaw

	Version semver.Version `json:"Version,omitempty"`

	etcdCli           *clientv3.Client
	watchSessionKeyCh clientv3.WatchChan
	watchCancel       atomic.Pointer[context.CancelFunc]
	wg                sync.WaitGroup

	metaRoot       string
	isMixCoordMode atomic.Bool

	registered         atomic.Value
	registeredRevision atomic.Int64
	disconnected       atomic.Value

	isStandby           atomic.Value
	enableActiveStandBy bool
	activeKey           string
	versionKey          string

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

// WithIndexEngineVersion should be only used by querynode.
func WithIndexEngineVersion(minimal, current, maximum int32) SessionOption {
	return func(session *Session) {
		session.IndexEngineVersion.MinimalIndexVersion = minimal
		session.IndexEngineVersion.CurrentIndexVersion = current
		session.IndexEngineVersion.MaximumIndexVersion = maximum
	}
}

// WithScalarIndexEngineVersion should be only used by querynode.
func WithScalarIndexEngineVersion(minimal, current, maximum int32) SessionOption {
	return func(session *Session) {
		session.ScalarIndexEngineVersion.MinimalIndexVersion = minimal
		session.ScalarIndexEngineVersion.CurrentIndexVersion = current
		session.ScalarIndexEngineVersion.MaximumIndexVersion = maximum
	}
}

func WithIndexNonEncoding() SessionOption {
	return func(session *Session) {
		session.IndexNonEncoding = true
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
		mlog.Error(ctx, "get host name fail", mlog.Err(hostNameErr))
	}

	ctx, cancel := context.WithCancel(ctx)
	session := &Session{
		ctx:    ctx,
		cancel: cancel,

		metaRoot: metaRoot,
		Version:  common.Version,

		SessionRaw: SessionRaw{
			HostName: hostName,
		},

		// options
		sessionTTL:        paramtable.Get().CommonCfg.SessionTTL.GetAsInt64(),
		sessionRetryTimes: paramtable.Get().CommonCfg.SessionRetryTimes.GetAsInt64(),
		reuseNodeID:       true,
	}
	session.registeredRevision.Store(-1)

	// integration test create cluster with different nodeId in one process
	if paramtable.Get().IntegrationTestCfg.IntegrationMode.GetAsBool() {
		session.reuseNodeID = false
	}

	session.apply(opts...)

	session.UpdateRegistered(false)
	session.etcdCli = client
	return session
}

// Init will initialize base struct of the Session, including ServerName, ServerID,
// Address, Exclusive. ServerID is obtained in getServerID.
func (s *Session) Init(serverName, address string, exclusive bool) {
	s.ServerName = serverName
	s.Address = address
	s.Exclusive = exclusive
	s.checkIDExist()
	serverID, err := s.getServerID()
	if err != nil {
		panic(err)
	}
	s.ServerID = serverID
	s.ServerLabels = getServerLabelsFromEnv(serverName)
	s.versionKey = path.Join(s.metaRoot, DefaultServiceRoot, serverVersionKey)

	s.SetLogger(mlog.With(
		mlog.FieldComponent("service-registration"),
		mlog.String("role", serverName),
		mlog.Int64("serverID", s.ServerID),
		mlog.String("address", address),
	))
}

// String makes Session struct able to be logged by zap
func (s *Session) String() string {
	return fmt.Sprintf("Session:<ServerID: %d, ServerName: %s, Version: %s>", s.ServerID, s.ServerName, s.Version.String())
}

// Register will process keepAliveResponse to keep alive with etcd.
func (s *Session) Register() {
	err := s.registerService()
	if err != nil {
		s.Logger().Error(s.ctx, "register failed", mlog.Err(err))
		panic(err)
	}
	s.UpdateRegistered(true)
	s.startKeepAliveLoop()
}

func (s *Session) GetRegisteredRevision() int64 {
	revision := s.registeredRevision.Load()
	if revision < 0 {
		panic("registeredRevision is used before register")
	}
	return revision
}

// isCoordinator checks if the session needs to check the version.
func (s *Session) isCoordinator() bool {
	return s.ServerName == typeutil.MixCoordRole ||
		s.ServerName == typeutil.QueryCoordRole ||
		s.ServerName == typeutil.DataCoordRole ||
		s.ServerName == typeutil.RootCoordRole ||
		s.ServerName == typeutil.IndexCoordRole
}

// checkVersion checks the version of the session and returns the error if the version is not found or expired.
func (s *Session) checkVersionForCoordinator() (*mvccpb.KeyValue, error) {
	resp, err := s.etcdCli.Get(s.ctx, s.versionKey)
	if err != nil {
		return nil, err
	}
	if resp.Count <= 0 {
		// no version key found.
		return nil, nil
	}
	version, err := semver.Parse(string(resp.Kvs[0].Value))
	if err != nil {
		return nil, err
	}
	if common.Version.Major < version.Major || (common.Version.Major == version.Major && common.Version.Minor < version.Minor) {
		return nil, errors.Wrapf(errSessionVersionCheckFailure, "current version(%s), session version(%s)", common.Version.String(), version.String())
	}
	return resp.Kvs[0], nil
}

var serverIDMu sync.Mutex

func (s *Session) getServerID() (int64, error) {
	serverIDMu.Lock()
	defer serverIDMu.Unlock()

	mlog.Debug(s.ctx, "getServerID", mlog.Bool("reuse", s.reuseNodeID))
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
	if os.Getenv(MilvusNodeIDForTesting) != "" {
		mlog.Info(context.TODO(), "use node id for testing", mlog.String("nodeID", os.Getenv(MilvusNodeIDForTesting)))
		return strconv.ParseInt(os.Getenv(MilvusNodeIDForTesting), 10, 64)
	}
	for {
		getResp, err := s.etcdCli.Get(s.ctx, path.Join(s.metaRoot, DefaultServiceRoot, key))
		if err != nil {
			mlog.Warn(context.TODO(), "Session get etcd key error", mlog.String("key", key), mlog.Err(err))
			return -1, err
		}
		if getResp.Count <= 0 {
			mlog.Warn(context.TODO(), "Session there is no value", mlog.String("key", key))
			continue
		}
		value := string(getResp.Kvs[0].Value)
		valueInt, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			mlog.Warn(context.TODO(), "Session ParseInt error", mlog.String("value", value), mlog.Err(err))
			continue
		}
		txnResp, err := s.etcdCli.Txn(s.ctx).If(
			clientv3.Compare(
				clientv3.Value(path.Join(s.metaRoot, DefaultServiceRoot, key)),
				"=",
				value)).
			Then(clientv3.OpPut(path.Join(s.metaRoot, DefaultServiceRoot, key), strconv.FormatInt(valueInt+1, 10))).Commit()
		if err != nil {
			mlog.Warn(context.TODO(), "Session Txn failed", mlog.String("key", key), mlog.Err(err))
			return -1, err
		}

		if !txnResp.Succeeded {
			mlog.Warn(context.TODO(), "Session Txn unsuccessful", mlog.String("key", key))
			continue
		}
		mlog.Debug(context.TODO(), "Session get serverID success", mlog.String("key", key), mlog.Int64("ServerId", valueInt))
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
func (s *Session) registerService() error {
	if s.enableActiveStandBy {
		s.updateStandby(true)
	}
	completeKey := s.getCompleteKey()
	s.Logger().Info(s.ctx, "service begin to register to etcd")

	registerFn := func() error {
		resp, err := s.etcdCli.Grant(s.ctx, s.sessionTTL)
		if err != nil {
			s.Logger().Error(s.ctx, "register service: failed to grant lease from etcd", mlog.Err(err))
			return err
		}
		s.LeaseID = &resp.ID

		sessionJSON, err := json.Marshal(s)
		if err != nil {
			s.Logger().Error(s.ctx, "register service: failed to marshal session", mlog.Err(err))
			return err
		}

		compareOps := []clientv3.Cmp{
			clientv3.Compare(clientv3.Version(completeKey), "=", 0),
		}
		ops := []clientv3.Op{
			clientv3.OpPut(completeKey, string(sessionJSON), clientv3.WithLease(resp.ID)),
		}

		// if enable active-standby, we don't need to check the version now,
		// only check the version when the standby is activated.
		if s.isCoordinator() && !s.enableActiveStandBy {
			if ops, compareOps, err = s.getOpsForCoordinator(ops, compareOps, sessionJSON); err != nil {
				return err
			}
		}

		txnResp, err := s.etcdCli.Txn(s.ctx).If(compareOps...).Then(ops...).Commit()
		if err != nil {
			s.Logger().Warn(s.ctx, "register on etcd error, check the availability of etcd", mlog.Err(err))
			return err
		}
		if txnResp != nil && !txnResp.Succeeded {
			return merr.WrapErrServiceUnavailableMsg("CompareAndSwap failed for session key %s: compare is false", s.ServerName)
		}
		if !s.enableActiveStandBy {
			s.registeredRevision.Store(txnResp.Header.GetRevision())
		}
		s.Logger().Info(s.ctx, "put session key into etcd, service registered successfully", mlog.String("key", completeKey), mlog.String("value", string(sessionJSON)))
		return nil
	}
	return retry.Do(s.ctx, registerFn, retry.Attempts(uint(s.sessionRetryTimes)), retry.RetryErr(isNotSessionVersionCheckFailure))
}

// getOpsForCoordinator gets the ops and compare ops for coordinator.
func (s *Session) getOpsForCoordinator(ops []clientv3.Op, compareOps []clientv3.Cmp, sessionJSON []byte) ([]clientv3.Op, []clientv3.Cmp, error) {
	previousVersion, err := s.checkVersionForCoordinator()
	if err != nil {
		return nil, nil, err
	}
	expectedVersion := int64(0)
	if previousVersion != nil {
		expectedVersion = previousVersion.Version
	}
	legacyCoord := []string{
		typeutil.QueryCoordRole,
		typeutil.DataCoordRole,
		typeutil.RootCoordRole,
	}
	for _, role := range legacyCoord {
		key := path.Join(s.metaRoot, DefaultServiceRoot, role)
		var newSession SessionRaw
		if err := json.Unmarshal(sessionJSON, &newSession); err != nil {
			return nil, nil, err
		}
		newSession.ServerName = role
		newSessionJSON, err := json.Marshal(newSession)
		if err != nil {
			return nil, nil, err
		}
		ops = append(ops, clientv3.OpPut(key, string(newSessionJSON), clientv3.WithLease(*s.LeaseID)))
		compareOps = append(compareOps, clientv3.Compare(clientv3.Version(key), "=", 0))
	}
	// promise the legacy coordinator version not available.
	compareOps = append(compareOps, clientv3.Compare(clientv3.Version(s.versionKey), "=", expectedVersion))
	// setup the version key if is a coordinator.
	ops = append(ops, clientv3.OpPut(s.versionKey, common.Version.String()))
	return ops, compareOps, nil
}

// processKeepAliveResponse processes the response of etcd keepAlive interface
// If keepAlive fails for unexpected error, it will send a signal to the channel.
func (s *Session) processKeepAliveResponse() {
	defer func() {
		s.Logger().Info(s.ctx, "keep alive loop exited successfully, try to revoke lease right away...")
		// here the s.ctx may be already done, so we use context.Background() with a timeout to revoke the lease.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := s.etcdCli.Revoke(ctx, *s.LeaseID); err != nil {
			s.Logger().Error(s.ctx, "failed to revoke lease", mlog.Err(err), mlog.Int64("leaseID", int64(*s.LeaseID)))
		}
		s.Logger().Info(s.ctx, "lease revoked successfully", mlog.Int64("leaseID", int64(*s.LeaseID)))
		s.wg.Done()
	}()

	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 10 * time.Millisecond
	backoff.MaxInterval = 100 * time.Second
	backoff.MaxElapsedTime = 0
	backoff.Reset()

	var ch <-chan *clientv3.LeaseKeepAliveResponse
	var lastErr error
	nextKeepaliveInstant := time.Now().Add(time.Duration(s.sessionTTL) * time.Second)

	for {
		if s.ctx.Err() != nil {
			return
		}
		if lastErr != nil {
			nextBackoffInterval := backoff.NextBackOff()
			s.Logger().Warn(s.ctx, "failed to start keep alive, wait for retry...", mlog.Err(lastErr), mlog.Duration("nextBackoffInterval", nextBackoffInterval))
			select {
			case <-time.After(nextBackoffInterval):
			case <-s.ctx.Done():
				return
			}
		}

		if ch == nil {
			if err := s.checkKeepaliveTTL(nextKeepaliveInstant); err != nil {
				lastErr = err
				continue
			}
			newCH, err := s.etcdCli.KeepAlive(s.ctx, *s.LeaseID)
			if err != nil {
				s.Logger().Error(s.ctx, "failed to keep alive with etcd", mlog.Err(err))
				lastErr = merr.Wrap(err, "failed to keep alive")
				continue
			}
			s.Logger().Info(s.ctx, "keep alive...", mlog.Int64("leaseID", int64(*s.LeaseID)))
			ch = newCH
		}

		// Block until the keep alive failure.
		for range ch {
		}

		// receive a keep alive response, continue the opeartion.
		// the keep alive channel may be closed because of network error, we should retry the keep alive.
		ch = nil
		nextKeepaliveInstant = time.Now().Add(time.Duration(s.sessionTTL) * time.Second)
		lastErr = nil
		backoff.Reset()
	}
}

// checkKeepaliveTTL checks the TTL of the lease and returns the error if the lease is not found or expired.
func (s *Session) checkKeepaliveTTL(nextKeepaliveInstant time.Time) error {
	ctx, cancel := context.WithDeadlineCause(s.ctx, nextKeepaliveInstant, errSessionExpiredAtClientSide)
	defer cancel()

	ttlResp, err := s.etcdCli.TimeToLive(ctx, *s.LeaseID)
	if err != nil {
		if errors.Is(err, v3rpc.ErrLeaseNotFound) {
			s.Logger().Error(s.ctx, "confirm the lease is not found, the session is expired without activing closing", mlog.Err(err))
			mlog.Cleanup()
			os.Exit(ExitCodeEtcd)
		}
		if ctx.Err() != nil && errors.Is(context.Cause(ctx), errSessionExpiredAtClientSide) {
			s.Logger().Error(s.ctx, "session expired at client side, the session is expired without activing closing", mlog.Err(err))
			mlog.Cleanup()
			os.Exit(ExitCodeEtcd)
		}
		return merr.Wrap(err, "failed to check TTL")
	}
	if ttlResp.TTL <= 0 {
		s.Logger().Error(s.ctx, "confirm the lease is expired, the session is expired without activing closing", mlog.Err(err))
		mlog.Cleanup()
		os.Exit(ExitCodeEtcd)
	}
	s.Logger().Info(s.ctx, "check TTL success, try to keep alive...", mlog.Int64("ttl", ttlResp.TTL))
	return nil
}

func (s *Session) startKeepAliveLoop() {
	s.wg.Add(1)
	go s.processKeepAliveResponse()
}

// GetSessions will get all sessions registered in etcd.
// Revision is returned for WatchServices to prevent key events from being missed.
func (s *Session) GetSessions(ctx context.Context, prefix string) (map[string]*Session, int64, error) {
	res := make(map[string]*Session)
	key := path.Join(s.metaRoot, DefaultServiceRoot, prefix)
	resp, err := s.etcdCli.Get(ctx, key, clientv3.WithPrefix(),
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
		mlog.Debug(s.ctx, "SessionUtil GetSessions",
			mlog.String("prefix", prefix),
			mlog.String("key", mapKey),
			mlog.String("address", session.Address))
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
			mlog.Debug(context.TODO(), "Session version out of range", mlog.String("version", session.Version.String()), mlog.Int64("serverID", session.ServerID))
			continue
		}
		_, mapKey := path.Split(string(kv.Key))
		mlog.Debug(context.TODO(), "SessionUtil GetSessions ", mlog.String("prefix", prefix),
			mlog.String("key", mapKey),
			mlog.String("address", session.Address))
		res[mapKey] = session
	}
	return res, resp.Header.Revision, nil
}

func (s *Session) GoingStop() error {
	if s == nil || s.etcdCli == nil || s.LeaseID == nil {
		return merr.WrapErrServiceInternalMsg("the session hasn't been init")
	}

	if s.Disconnected() {
		return merr.WrapErrServiceUnavailable("this session has disconnected")
	}

	completeKey := s.getCompleteKey()
	resp, err := s.etcdCli.Get(s.ctx, completeKey, clientv3.WithCountOnly())
	if err != nil {
		s.Logger().Error(s.ctx, "fail to get the session", mlog.String("key", completeKey), mlog.Err(err))
		return err
	}
	if resp.Count == 0 {
		return nil
	}
	s.Stopping = true
	sessionJSON, err := json.Marshal(s)
	if err != nil {
		s.Logger().Error(s.ctx, "fail to marshal the session", mlog.String("key", completeKey))
		return err
	}
	_, err = s.etcdCli.Put(s.ctx, completeKey, string(sessionJSON), clientv3.WithLease(*s.LeaseID))
	if err != nil {
		s.Logger().Error(s.ctx, "fail to update the session to stopping state", mlog.String("key", completeKey))
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
	s         *Session
	cancel    context.CancelFunc
	rch       clientv3.WatchChan
	eventCh   chan *SessionEvent
	prefix    string
	rewatch   Rewatch
	validate  func(*Session) bool
	wg        sync.WaitGroup
	closeOnce sync.Once
}

func (w *sessionWatcher) closeEventCh() {
	w.closeOnce.Do(func() {
		close(w.eventCh)
	})
}

func (w *sessionWatcher) start(ctx context.Context) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case wresp, ok := <-w.rch:
				if !ok {
					w.closeEventCh()
					mlog.Warn(ctx, "session watch channel closed")
					return
				}
				w.handleWatchResponse(wresp)
			}
		}
	}()
}

func (w *sessionWatcher) Stop() {
	w.cancel()
	w.wg.Wait()
}

// EmptySessionWatcher returns a place holder for IndexNodeBinding mode datacoord
func EmptySessionWatcher() SessionWatcher {
	return emptySessionWatcher{}
}

// emptySessionWatcher is a place holder for IndexNodeBinding mode datacoord
type emptySessionWatcher struct{}

func (emptySessionWatcher) EventChannel() <-chan *SessionEvent {
	return nil
}

func (emptySessionWatcher) Stop() {}

// WatchServices watches the service's up and down in etcd, and sends event to
// eventChannel.
// prefix is a parameter to know which service to watch and can be obtained in
// typeutil.type.go.
// revision is a etcd reversion to prevent missing key events and can be obtained
// in GetSessions.
// If a server up, an event will be add to channel with eventType SessionAddType.
// If a server down, an event will be add to channel with eventType SessionDelType.
func (s *Session) WatchServices(prefix string, revision int64, rewatch Rewatch) (watcher SessionWatcher) {
	ctx, cancel := context.WithCancel(s.ctx)
	w := &sessionWatcher{
		s:        s,
		cancel:   cancel,
		eventCh:  make(chan *SessionEvent, 100),
		rch:      s.etcdCli.Watch(s.ctx, path.Join(s.metaRoot, DefaultServiceRoot, prefix), clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(revision)),
		prefix:   prefix,
		rewatch:  rewatch,
		validate: func(s *Session) bool { return true },
	}
	w.start(ctx)
	return w
}

// WatchServicesWithVersionRange watches the service's up and down in etcd, and sends event to event Channel.
// Acts like WatchServices but with extra version range check.
// prefix is a parameter to know which service to watch and can be obtained in type util.type.go.
// revision is a etcd reversion to prevent missing key events and can be obtained in GetSessions.
// If a server up, an event will be add to channel with eventType SessionAddType.
// If a server down, an event will be add to channel with eventType SessionDelType.
func (s *Session) WatchServicesWithVersionRange(prefix string, r semver.Range, revision int64, rewatch Rewatch) (watcher SessionWatcher) {
	ctx, cancel := context.WithCancel(s.ctx)
	w := &sessionWatcher{
		s:        s,
		cancel:   cancel,
		eventCh:  make(chan *SessionEvent, 100),
		rch:      s.etcdCli.Watch(s.ctx, path.Join(s.metaRoot, DefaultServiceRoot, prefix), clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(revision)),
		prefix:   prefix,
		rewatch:  rewatch,
		validate: func(s *Session) bool { return r(s.Version) },
	}
	w.start(ctx)
	return w
}

func (w *sessionWatcher) handleWatchResponse(wresp clientv3.WatchResponse) {
	if wresp.Err() != nil {
		err := w.handleWatchErr(wresp.Err())
		if err != nil {
			mlog.Error(context.TODO(), "failed to handle watch session response", mlog.Err(err))
			panic(err)
		}
		return
	}
	for _, ev := range wresp.Events {
		session := &Session{}
		var eventType SessionEventType
		switch ev.Type {
		case mvccpb.PUT:
			mlog.Debug(context.TODO(), "watch services",
				mlog.Any("add kv", ev.Kv))
			err := json.Unmarshal(ev.Kv.Value, session)
			if err != nil {
				mlog.Error(context.TODO(), "watch services", mlog.Err(err))
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
			mlog.Debug(context.TODO(), "watch services",
				mlog.Any("delete kv", ev.PrevKv))
			err := json.Unmarshal(ev.PrevKv.Value, session)
			if err != nil {
				mlog.Error(context.TODO(), "watch services", mlog.Err(err))
				continue
			}
			if !w.validate(session) {
				continue
			}
			eventType = SessionDelEvent
		}
		mlog.Debug(context.TODO(), "WatchService", mlog.Any("event type", eventType))
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
		mlog.Warn(context.TODO(), "Watch service found error", mlog.Err(err))
		w.closeEventCh()
		return err
	}

	sessions, revision, err := w.s.GetSessions(w.s.ctx, w.prefix)
	if err != nil {
		mlog.Warn(context.TODO(), "GetSession before rewatch failed", mlog.String("prefix", w.prefix), mlog.Err(err))
		w.closeEventCh()
		return err
	}
	// rewatch is nil, no logic to handle
	if w.rewatch == nil {
		mlog.Warn(context.TODO(), "Watch service with ErrCompacted but no rewatch logic provided")
	} else {
		err = w.rewatch(sessions)
	}
	if err != nil {
		mlog.Warn(context.TODO(), "WatchServices rewatch failed", mlog.String("prefix", w.prefix), mlog.Err(err))
		w.closeEventCh()
		return err
	}

	w.rch = w.s.etcdCli.Watch(w.s.ctx, path.Join(w.s.metaRoot, DefaultServiceRoot, w.prefix), clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(revision))
	return nil
}

func (w *sessionWatcher) EventChannel() <-chan *SessionEvent {
	return w.eventCh
}

// SetMixCoordMode marks this session as shared across multiple coordinators in MixCoord mode.
// When in MixCoord mode, Stop() is a no-op — MixCoord is responsible for calling Stop() after
// clearing the flag.
func (s *Session) SetMixCoordMode(enable bool) {
	s.isMixCoordMode.Store(enable)
}

func (s *Session) Stop() {
	if s.isMixCoordMode.Load() {
		mlog.Info(context.TODO(), "session stop skipped, session is in MixCoord mode", mlog.String("serverName", s.ServerName))
		return
	}
	mlog.Info(context.TODO(), "session stopping", mlog.String("serverName", s.ServerName))
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
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

	oldRoles := []string{
		typeutil.RootCoordRole,
		typeutil.DataCoordRole,
		typeutil.QueryCoordRole,
	}

	registerActiveFn := func() (bool, int64, error) {
		for _, role := range oldRoles {
			sessions, _, err := s.GetSessions(s.ctx, role)
			if err != nil {
				mlog.Debug(context.TODO(), "failed to get old sessions", mlog.String("role", role), mlog.Err(err))
				continue
			}
			if len(sessions) > 0 {
				mlog.Info(context.TODO(), "old session exists", mlog.String("role", role))
				return false, -1, merr.ErrOldSessionExists
			}
		}

		mlog.Info(s.ctx, fmt.Sprintf("try to register as ACTIVE %v service...", s.ServerName))
		sessionJSON, err := json.Marshal(s)
		if err != nil {
			mlog.Error(context.TODO(), "json marshal error", mlog.Err(err))
			return false, -1, err
		}

		compareOps := []clientv3.Cmp{
			clientv3.Compare(clientv3.Version(s.activeKey), "=", 0),
		}
		ops := []clientv3.Op{
			clientv3.OpPut(s.activeKey, string(sessionJSON), clientv3.WithLease(*s.LeaseID)),
		}

		if s.isCoordinator() {
			if ops, compareOps, err = s.getOpsForCoordinator(ops, compareOps, sessionJSON); err != nil {
				return false, -1, err
			}
		}

		txnResp, err := s.etcdCli.Txn(s.ctx).If(compareOps...).Then(ops...).Commit()
		if err != nil {
			mlog.Error(context.TODO(), "register active key to etcd failed", mlog.Err(err))
			return false, -1, err
		}
		doRegistered := txnResp.Succeeded
		revision := txnResp.Header.GetRevision()
		if doRegistered {
			s.registeredRevision.Store(revision)
			mlog.Info(s.ctx, fmt.Sprintf("register ACTIVE %s", s.ServerName), mlog.Int64("revision", revision))
		} else {
			mlog.Info(s.ctx, fmt.Sprintf("ACTIVE %s has already been registered", s.ServerName))
		}
		return doRegistered, revision, nil
	}
	s.updateStandby(true)
	mlog.Info(s.ctx, fmt.Sprintf("serverName: %v enter STANDBY mode", s.ServerName))
	go func() {
		for s.isStandby.Load().(bool) {
			mlog.Debug(s.ctx, fmt.Sprintf("serverName: %v is in STANDBY ...", s.ServerName))
			time.Sleep(10 * time.Second)
		}
	}()

	for {
		registered, revision, err := registerActiveFn()
		if err != nil {
			if err == merr.ErrOldSessionExists {
				// If old session exists, wait and retry
				time.Sleep(100 * time.Millisecond)
				continue
			}
			// Some error such as ErrLeaseNotFound, is not retryable.
			// Just return error to stop the standby process and wait for retry.
			return err
		}
		if registered {
			break
		}
		mlog.Info(s.ctx, fmt.Sprintf("%s start to watch ACTIVE key %s", s.ServerName, s.activeKey))
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
					mlog.Debug(context.TODO(), "watch the ACTIVE key", mlog.Any("ADD", event.Kv))
				case mvccpb.DELETE:
					mlog.Debug(context.TODO(), "watch the ACTIVE key", mlog.Any("DELETE", event.Kv))
					cancel()
				}
			}
		}
		cancel()
		mlog.Info(s.ctx, fmt.Sprintf("stop watching ACTIVE key %v", s.activeKey))
	}

	s.updateStandby(false)
	mlog.Info(s.ctx, fmt.Sprintf("serverName: %v quit STANDBY mode, this node will become ACTIVE, ID: %d", s.ServerName, s.ServerID))
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
		mlog.Warn(context.TODO(), "not found server info file path", mlog.String("filePath", fileFullName), mlog.Err(err))
		return []string{}
	}

	v, err := storage.ReadFile(fileFullName)
	if err != nil {
		mlog.Warn(context.TODO(), "read server info file path failed", mlog.String("filePath", fileFullName), mlog.Err(err))
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
		mlog.Warn(context.TODO(), "open server info file fail", mlog.String("filePath", fileFullPath), mlog.Err(err))
		return
	}
	defer fd.Close()

	data := fmt.Sprintf("%s-%d\n", role, serverID)
	_, err = fd.WriteString(data)
	if err != nil {
		mlog.Warn(context.TODO(), "write server info file fail", mlog.String("filePath", fileFullPath), mlog.Err(err))
	}

	mlog.Info(context.TODO(), "save server info into file", mlog.String("content", data), mlog.String("filePath", fileFullPath))
}

func SaveServerInfo(role string, serverID int64) {
	saveServerInfoInternal(role, serverID, os.Getpid())
}

// GetSessionPrefixByRole get session prefix by role
func GetSessionPrefixByRole(role string) string {
	return path.Join(paramtable.Get().EtcdCfg.MetaRootPath.GetValue(), DefaultServiceRoot, role)
}
