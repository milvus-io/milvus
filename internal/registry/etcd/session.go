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

package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"sync"

	"github.com/blang/semver/v4"
	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/registry"
	"github.com/milvus-io/milvus/internal/registry/common"
	"github.com/milvus-io/milvus/internal/registry/options"
	milvuscommon "github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/retry"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var _ registry.Session = (*etcdSession)(nil)

type etcdSession struct {
	common.ServiceEntryBase

	idOnce sync.Once

	// session info
	exclusive       bool
	stopping        *atomic.Bool
	version         semver.Version
	client          *clientv3.Client
	leaseID         clientv3.LeaseID
	isStandby       *atomic.Bool
	keepaliveCancel context.CancelFunc
	triggerKill     bool
	liveCh          <-chan struct{}
	closeCh         <-chan struct{}

	// options
	metaRoot            string
	useCustomConfig     bool
	sessionTTL          int64
	sessionRetryTimes   int64
	reuseNodeID         bool
	enableActiveStandBy bool
}

func newEtcdSession(cli *clientv3.Client, metaRoot string, addr string, component string,
	opt options.SessionOpt) *etcdSession {
	return &etcdSession{
		ServiceEntryBase: common.NewServiceEntryBase(addr, component),
		exclusive:        opt.Exclusive,
		client:           cli,
		metaRoot:         metaRoot,
		isStandby:        atomic.NewBool(opt.StandBy),
		version:          milvuscommon.Version,
		stopping:         atomic.NewBool(false),
	}
}

// UnmarshalJSON unmarshal bytes to Session.
func (s *etcdSession) UnmarshalJSON(data []byte) error {
	var raw struct {
		ServerID   int64  `json:"ServerID,omitempty"`
		ServerName string `json:"ServerName,omitempty"`
		Address    string `json:"Address,omitempty"`
		Exclusive  bool   `json:"Exclusive,omitempty"`
		Stopping   bool   `json:"Stopping,omitempty"`
		Version    string `json:"Version"`
	}
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}

	if raw.Version != "" {
		s.version, err = semver.Parse(raw.Version)
		if err != nil {
			return err
		}
	}

	s.SetID(raw.ServerID)
	s.SetAddr(raw.Address)
	s.SetComponentType(raw.ServerName)
	s.exclusive = raw.Exclusive
	s.stopping = atomic.NewBool(raw.Stopping)
	return nil
}

// MarshalJSON marshals session to bytes.
func (s *etcdSession) MarshalJSON() ([]byte, error) {

	verStr := s.version.String()
	return json.Marshal(&struct {
		ServerID    int64  `json:"ServerID,omitempty"`
		ServerName  string `json:"ServerName,omitempty"`
		Address     string `json:"Address,omitempty"`
		Exclusive   bool   `json:"Exclusive,omitempty"`
		Stopping    bool   `json:"Stopping,omitempty"`
		TriggerKill bool
		Version     string `json:"Version"`
	}{
		ServerID:    s.ID(),
		ServerName:  s.ComponentType(),
		Address:     s.Addr(),
		Exclusive:   s.exclusive,
		Stopping:    s.stopping.Load(),
		TriggerKill: s.triggerKill,
		Version:     verStr,
	})

}

// Init will initialize serverID with CAS operation with etcd `ID` kv.
func (s *etcdSession) Init(ctx context.Context) error {
	// use custom config, for migration tool only
	if s.useCustomConfig {
		return nil
	}
	s.checkIDExist(ctx)
	serverID, err := s.getServerID(ctx)
	if err != nil {
		return err
	}
	s.SetID(serverID)
	return nil
}

func (s *etcdSession) checkIDExist(ctx context.Context) {
	s.client.Txn(ctx).If(
		clientv3.Compare(
			clientv3.Version(path.Join(s.metaRoot, milvuscommon.DefaultServiceRoot, milvuscommon.DefaultIDKey)),
			"=",
			0)).
		Then(clientv3.OpPut(path.Join(s.metaRoot, milvuscommon.DefaultServiceRoot, milvuscommon.DefaultIDKey), "1")).Commit()
}

// Register writes session entry in etcd paths with ttl lease.
// internally it will start `keepAlive` to keep alive with etcd.
func (s *etcdSession) Register(ctx context.Context) error {
	ch, err := s.registerService(ctx)
	if err != nil {
		log.Error("session register failed", zap.Error(err))
		return err
	}
	s.liveCh = s.keepAlive(ch)
	return nil
}

func (s *etcdSession) Revoke(ctx context.Context) error {
	if s == nil {
		return nil
	}

	if s.client == nil || s.leaseID == 0 {
		// TODO audit error type here
		return merr.WrapErrParameterInvalid("valid session", "session not valid")
	}

	_, err := s.client.Revoke(ctx, s.leaseID)
	if err != nil {
		if errors.IsAny(err, context.Canceled, context.DeadlineExceeded) {
			return errors.Wrapf(err, "context canceled when revoking component %s serverID:%d", s.ComponentType(), s.ID())
		}
		return merr.WrapErrIoFailed(fmt.Sprintf("%s-%d", s.ComponentType(), s.ID()), err.Error())
	}

	return nil
}

// Stop marks session as `Stopping` state for graceful stop.
func (s *etcdSession) Stop(ctx context.Context) error {
	if s == nil || s.client == nil || s.leaseID == 0 {
		return merr.WrapErrParameterInvalid("valid session", "session not valid")
	}

	completeKey := s.getCompleteKey()
	log := log.Ctx(ctx).With(
		zap.String("key", completeKey),
	)
	resp, err := s.client.Get(ctx, completeKey, clientv3.WithCountOnly())
	if err != nil {
		log.Error("fail to get the session", zap.Error(err))
		return err
	}
	if resp.Count == 0 {
		return nil
	}
	s.stopping.Store(true)
	sessionJSON, err := json.Marshal(s)
	if err != nil {
		log.Error("fail to marshal the session")
		return err
	}
	_, err = s.client.Put(ctx, completeKey, string(sessionJSON), clientv3.WithLease(s.leaseID))
	if err != nil {
		log.Error("fail to update the session to stopping state")
		return err
	}
	return nil
}

func (s *etcdSession) getServerID(ctx context.Context) (int64, error) {
	var nodeID int64
	var err error
	s.idOnce.Do(func() {
		log.Debug("getServerID", zap.Bool("reuse", s.reuseNodeID))
		if s.reuseNodeID {
			// Notice, For standalone, all process share the same nodeID.
			if nodeID = paramtable.GetNodeID(); nodeID != 0 {
				return
			}
		}
		nodeID, err = s.getServerIDWithKey(ctx, milvuscommon.DefaultIDKey)
		if err != nil {
			return
		}
		if s.reuseNodeID {
			paramtable.SetNodeID(nodeID)
		}
	})
	return nodeID, err
}

func (s *etcdSession) getServerIDWithKey(ctx context.Context, key string) (int64, error) {
	log := log.Ctx(ctx).With(zap.String("key", key))
	completeKey := path.Join(s.metaRoot, milvuscommon.DefaultServiceRoot, key)
	for {
		getResp, err := s.client.Get(ctx, completeKey)
		if err != nil {
			log.Warn("Session get etcd key error", zap.Error(err))
			return -1, err
		}
		if getResp.Count <= 0 {
			log.Warn("Session there is no value")
			continue
		}
		value := string(getResp.Kvs[0].Value)
		valueInt, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			log.Warn("Session ParseInt error", zap.Error(err))
			continue
		}
		nextValue := strconv.FormatInt(valueInt+1, 10)
		txnResp, err := s.client.Txn(ctx).If(
			clientv3.Compare(
				clientv3.Value(completeKey),
				"=",
				value)).
			Then(clientv3.OpPut(completeKey, nextValue)).Commit()
		if err != nil {
			log.Warn("Session Txn failed", zap.Error(err))
			return -1, err
		}

		if !txnResp.Succeeded {
			log.Warn("Session Txn unsuccessful")
			continue
		}
		log.Info("Session get serverID success", zap.Int64("ServerId", valueInt), zap.String("completeKey", completeKey))
		return valueInt, nil
	}
}

func (s *etcdSession) getCompleteKey() string {
	key := s.ComponentType()
	if !s.exclusive || (s.enableActiveStandBy && s.isStandby.Load()) {
		key = fmt.Sprintf("%s-%d", key, s.ID())
	}
	return path.Join(s.metaRoot, milvuscommon.DefaultServiceRoot, key)
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
func (s *etcdSession) registerService(ctx context.Context) (<-chan *clientv3.LeaseKeepAliveResponse, error) {
	log := log.Ctx(ctx).With(
		zap.String("ComponentType", s.ComponentType()),
		zap.Int64("ServerID", s.ID()),
	)

	if s.enableActiveStandBy {
		s.isStandby.Store(true)
	}
	completeKey := s.getCompleteKey()
	var ch <-chan *clientv3.LeaseKeepAliveResponse
	log.Debug("service begin to register to etcd", zap.String("key", completeKey))

	ttl := s.sessionTTL
	retryTimes := s.sessionRetryTimes
	if !s.useCustomConfig {
		ttl = paramtable.Get().CommonCfg.SessionTTL.GetAsInt64()
		retryTimes = paramtable.Get().CommonCfg.SessionRetryTimes.GetAsInt64()
	}

	registerFn := func() error {
		resp, err := s.client.Grant(ctx, ttl)
		if err != nil {
			log.Error("register service", zap.Error(err))
			return err
		}
		s.leaseID = resp.ID

		sessionJSON, err := json.Marshal(s)
		if err != nil {
			return err
		}

		txnResp, err := s.client.Txn(ctx).If(
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
			return fmt.Errorf("function CompareAndSwap error for compare is false for key: %s", s.ComponentType())
		}
		log.Debug("put session key into etcd", zap.String("key", completeKey), zap.String("value", string(sessionJSON)))

		keepAliveCtx, keepAliveCancel := context.WithCancel(context.Background())
		s.keepaliveCancel = keepAliveCancel

		ch, err = s.client.KeepAlive(keepAliveCtx, resp.ID)
		if err != nil {
			log.Warn("go error during keeping alive with etcd", zap.Error(err))
			return err
		}
		log.Info("Service registered successfully")
		return nil
	}
	err := retry.Do(ctx, registerFn, retry.Attempts(uint(retryTimes)))
	if err != nil {
		return nil, err
	}
	return ch, nil
}

// keepAlive processes the response of etcd keepAlive interface
// If keepAlive fails for unexpected error, it will send a signal to the channel.
func (s *etcdSession) keepAlive(ch <-chan *clientv3.LeaseKeepAliveResponse) (failChannel <-chan struct{}) {
	failCh := make(chan struct{})
	go func() {
		for {
			select {
			case <-s.closeCh:
				log.Info("etcd session quit")
				return
			case resp, ok := <-ch:
				if !ok {
					log.Warn("session keepalive channel closed")
					close(failCh)
					return
				}
				if resp == nil {
					log.Warn("session keepalive response failed")
					close(failCh)
					return
				}
			}
		}
	}()
	return failCh
}
