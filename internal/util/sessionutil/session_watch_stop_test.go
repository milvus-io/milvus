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
	"sync"
	"testing"
	"time"

	"github.com/blang/semver/v4"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestSessionWatcherStop(t *testing.T) {
	paramtable.Init()
	for _, versioned := range []bool{false, true} {
		name := "all versions"
		if versioned {
			name = "version range"
		}
		t.Run(name, func(t *testing.T) {
			for _, phase := range []string{"idle", "snapshot", "watch setup", "watch events", "retry wait", "full event queue"} {
				t.Run(phase, func(t *testing.T) {
					mockey.PatchConvey(phase, t, func() {
						parentCtx, cancelParent := context.WithCancel(context.Background())
						defer cancelParent()
						cli, err := etcd.GetRemoteEtcdClient(paramtable.Get().EtcdCfg.Endpoints.GetAsStrings())
						require.NoError(t, err)
						defer cli.Close()
						s := &Session{ctx: parentCtx, etcdCli: cli, metaRoot: "54137-stop"}
						entered := make(chan struct{})
						var enteredOnce sync.Once
						enter := func() { enteredOnce.Do(func() { close(entered) }) }
						initial := make(chan clientv3.WatchResponse, 1)
						var watchContexts []context.Context
						mockey.Mock(mockey.GetMethod(cli.Watcher, "Watch")).To(func(ctx context.Context, _ string, _ ...clientv3.OpOption) clientv3.WatchChan {
							watchContexts = append(watchContexts, ctx)
							if len(watchContexts) == 1 {
								return initial
							}
							enter()
							if phase == "watch setup" {
								<-ctx.Done()
							}
							return make(chan clientv3.WatchResponse)
						}).Build()
						mockey.Mock((*Session).GetSessions).To(func(_ *Session, ctx context.Context, _ string) (map[string]*Session, int64, error) {
							switch phase {
							case "snapshot":
								enter()
								<-ctx.Done()
								return nil, 0, ctx.Err()
							case "retry wait":
								enter()
								return nil, 0, rpctypes.ErrNoLeader
							default:
								return nil, 42, nil
							}
						}).Build()
						var watcher SessionWatcher
						if versioned {
							watcher = s.WatchServicesWithVersionRange("node", semver.MustParseRange(">=2.0.0"), 42, nil)
						} else {
							watcher = s.WatchServices("node", 42, nil)
						}
						// Cancel the parent only as test cleanup, after verifying that
						// stopping the watcher works while its session stays alive.
						defer func() { cancelParent(); watcher.Stop() }()
						switch phase {
						case "idle":
							enter()
						case "full event queue":
							w := watcher.(*sessionWatcher)
							validated := 0
							w.validate = func(*Session) bool {
								validated++
								if validated == cap(w.eventCh)+1 {
									enter()
								}
								return true
							}
							events := make([]*clientv3.Event, cap(w.eventCh)+1)
							for i := range events {
								events[i] = &clientv3.Event{Type: mvccpb.PUT, Kv: &mvccpb.KeyValue{Value: []byte(`{"ServerID":1}`)}}
							}
							initial <- clientv3.WatchResponse{Events: events}
						default:
							initial <- clientv3.WatchResponse{CompactRevision: 1}
						}
						select {
						case <-entered:
						case <-time.After(5 * time.Second):
							t.Fatal("watcher did not enter " + phase)
						}
						stopped := make(chan struct{})
						go func() { watcher.Stop(); close(stopped) }()
						select {
						case <-stopped:
						case <-time.After(time.Second):
							t.Error("Stop blocked in " + phase)
							// Unblock the baseline implementation without leaving a
							// goroutine using patches after this subtest completes.
							cancelParent()
							if phase == "full event queue" {
								<-watcher.EventChannel()
							}
							<-stopped
							return
						}
						require.NoError(t, parentCtx.Err())
						for _, ctx := range watchContexts {
							require.ErrorIs(t, ctx.Err(), context.Canceled, "underlying etcd watch was not canceled")
						}
					})
				})
			}
		})
	}
}
