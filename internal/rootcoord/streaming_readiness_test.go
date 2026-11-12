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
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func mockStreamingReadiness(t *testing.T, ready <-chan struct{}, initiallyReady bool, registrationErr error) {
	t.Helper()
	registration := mockey.Mock((*snmanager.StreamingNodeManager).RegisterStreamingEnabledListener).Return(registrationErr).Build()
	t.Cleanup(func() { registration.UnPatch() })
	initial := mockey.Mock((*snmanager.StreamingReadyNotifier).IsReady).Return(initiallyReady).Build()
	t.Cleanup(func() { initial.UnPatch() })
	notification := mockey.Mock((*snmanager.StreamingReadyNotifier).Ready).Return(ready).Build()
	t.Cleanup(func() { notification.UnPatch() })
}

func TestStreamingReadinessStopsTimeTickLoops(t *testing.T) {
	for _, loop := range []string{"ddl", "proxy"} {
		for _, state := range []string{"already_ready", "becomes_ready", "registration_error", "canceled"} {
			t.Run(loop+"/"+state, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				ready := make(chan struct{})
				var registrationErr error
				if state == "registration_error" {
					registrationErr = context.Canceled
				}
				mockStreamingReadiness(t, ready, state == "already_ready", registrationErr)
				ticks := mockey.Mock((*Core).sendMinDdlTsAsTt).Return().Build()
				t.Cleanup(func() { ticks.UnPatch() })
				done := make(chan struct{})
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer close(done)
					if loop == "ddl" {
						core := &Core{ctx: ctx}
						core.wg.Add(1)
						core.startTimeTickLoop()
						wg.Done()
					} else {
						tt := &timetickSync{ctx: ctx}
						tt.startWatch(&wg)
					}
				}()
				// Cancellation also bounds cleanup if an assertion fails.
				t.Cleanup(func() { cancel(); <-done })
				switch state {
				case "becomes_ready":
					close(ready)
				case "canceled":
					cancel()
				}
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("time tick loop did not stop")
				}
				wg.Wait()
			})
		}
	}
}

func TestTimeTickWatchForwardsBootstrapTicks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	mockStreamingReadiness(t, ready, false, nil)
	tt := &timetickSync{
		ctx: ctx, sendChan: make(chan map[typeutil.UniqueID]*chanTsMsg, 2),
		syncedTtHistogram: newTtHistogram(),
	}
	type sentTick struct {
		channel string
		ts      typeutil.Timestamp
	}
	sent := make(chan sentTick, 2)
	forward := mockey.Mock((*timetickSync).sendTimeTickToChannel).To(func(_ *timetickSync, channels []string, ts typeutil.Timestamp) error {
		sent <- sentTick{channels[0], ts}
		if channels[0] == "failed" {
			return context.Canceled
		}
		return nil
	}).Build()
	t.Cleanup(func() { forward.UnPatch() })
	tt.sendChan <- map[typeutil.UniqueID]*chanTsMsg{ddlSourceID: {chanTsMap: map[string]typeutil.Timestamp{}}}
	tt.sendChan <- map[typeutil.UniqueID]*chanTsMsg{
		ddlSourceID: {chanTsMap: map[string]typeutil.Timestamp{"ok": 100, "failed": 200}},
		1:           {chanTsMap: map[string]typeutil.Timestamp{"ok": 90, "failed": 150}},
		2:           {defaultTs: 95},
	}
	close(tt.sendChan)
	var wg sync.WaitGroup
	wg.Add(1)
	done := make(chan struct{})
	go func() { tt.startWatch(&wg); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("time tick watch did not drain its closed input")
	}
	wg.Wait()
	close(sent)
	got := make(map[string]typeutil.Timestamp)
	for tick := range sent {
		got[tick.channel] = tick.ts
	}
	require.Equal(t, map[string]typeutil.Timestamp{"ok": 90, "failed": 95}, got)
	require.EqualValues(t, 90, tt.syncedTtHistogram.get("ok"))
	require.Zero(t, tt.syncedTtHistogram.get("failed"))
}
