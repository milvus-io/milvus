// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/eventlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type credentialGateKV struct {
	clientv3.KV
	enabled bool
}

func (kv *credentialGateKV) Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	resp := &clientv3.GetResponse{}
	if kv.enabled {
		resp.Kvs = []*mvccpb.KeyValue{{Key: []byte("eventlog-auth/config/commonsecurityadminauthenabled"), Value: []byte("true")}}
	}
	return resp, nil
}

// Preserve the production EtcdSource publication/eviction/event code; only give
// it a distinct name so this isolated source can coexist with test init's source.
type scheduledCredentialGateSource struct {
	config.Source
	beforeDispatch func()
	name           string
}

func (s *scheduledCredentialGateSource) GetSourceName() string { return s.name }
func (s *scheduledCredentialGateSource) SetEventHandler(h config.EventHandler) {
	s.Source.SetEventHandler(config.NewHandler("eventlog-auth-source-schedule", func(e *config.Event) {
		if s.beforeDispatch != nil {
			s.beforeDispatch()
		}
		e.EventSource = s.GetSourceName()
		h.OnEvent(e)
	}))
}

func TestEventlogSecuredOnFirstConfigSourceOverride(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	key := params.CommonCfg.AdminAuthEnabled.Key
	require.False(t, params.CommonCfg.AdminAuthEnabled.GetAsBool())
	mgr := paramtable.GetBaseTable().Manager()
	kv := &credentialGateKV{}
	client := clientv3.NewCtxClient(context.Background())
	client.KV = kv
	src, err := config.NewEtcdSource(client, &config.EtcdInfo{KeyPrefix: "eventlog-auth"})
	require.NoError(t, err)
	named := &scheduledCredentialGateSource{Source: src, name: fmt.Sprintf("%s-%p", t.Name(), src)}
	require.NoError(t, mgr.AddSource(named))
	t.Cleanup(func() {
		params.Unwatch(key, config.NewHandler("eventlog-auth-mode", nil))
		params.Unwatch(key, config.NewHandler("eventlog-auth-dispatch-schedule", nil))
		named.beforeDispatch = nil
		kv.enabled = false
		require.NoError(t, src.RefreshConfigurationsLinearizable())
		require.NoError(t, eventlog.EnsureListenerMode(params.CommonCfg.AdminAuthEnabled.GetAsBool()))
		src.Close()
	})

	var mode atomic.Bool
	applied := make(chan bool, 4)
	configureEventlogListenerMode("eventlog-auth-mode", func(localOnly bool) error {
		if err := eventlog.EnsureListenerMode(localOnly); err != nil {
			return err
		}
		mode.Store(localOnly)
		applied <- localOnly
		return nil
	})
	require.False(t, <-applied)

	discover := func() map[string]any {
		recorder := httptest.NewRecorder()
		eventlog.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/eventlog", nil))
		require.Equal(t, 200, recorder.Code)
		result := make(map[string]any)
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &result))
		return result
	}
	before := discover()
	// This runs after real EtcdSource publishes the new map and evicts cache,
	// but before Manager.OnEvent has changed the key's owner from YAML to etcd.
	named.beforeDispatch = func() {
		require.False(t, params.CommonCfg.AdminAuthEnabled.GetAsBool(), "HTTP refill still reads YAML owner")
	}
	// The dispatcher invokes exact handlers before its prefix cache-reset
	// handler. Force the legal schedule in which the async mode read wins.
	barrier := config.NewHandler("eventlog-auth-dispatch-schedule", func(*config.Event) {
		select {
		case v := <-applied:
			require.True(t, v, "listener must use the new effective value even before typed-cache eviction")
		case <-time.After(2 * time.Second):
			t.Fatal("mode callback did not run")
		}
	})
	params.Watch(key, barrier)
	kv.enabled = true
	require.NoError(t, src.RefreshConfigurationsLinearizable())

	require.True(t, AdminAuthEnabled(), "after prefix reset, HTTP has the new true value")
	require.True(t, mode.Load(), "listener must be secured when HTTP authentication becomes active")
	recorder := httptest.NewRecorder()
	wrapAdminAuth(eventlog.Handler(), EventLogRouterPath, false).ServeHTTP(recorder, httptest.NewRequest("GET", "/eventlog", nil))
	require.Equal(t, http.StatusUnauthorized, recorder.Code, "HTTP discovery now rejects anonymous clients")
	require.NoError(t, src.RefreshConfigurationsLinearizable())
	require.True(t, mode.Load(), "unchanged refresh must preserve the secured mode")
	after := discover()
	require.NotEqual(t, before["port"], after["port"], "wildcard listener must be replaced")
	oldAddress := net.JoinHostPort("127.0.0.1", fmt.Sprint(int(before["port"].(float64))))
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", oldAddress, 100*time.Millisecond)
		if err != nil {
			return true
		}
		conn.Close()
		return false
	}, time.Second, time.Millisecond, "old unauthenticated listener must stop accepting connections")
}
