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

package walsummary

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
)

type strictResultHandler struct {
	done chan struct{}
	tick uint64
}

func (h *strictResultHandler) Handle(event wal.TransformLogStreamEvent) {
	if event.SyncUp != nil {
		h.tick = event.SyncUp.TimeTick
	}
}
func (h *strictResultHandler) Close() { close(h.done) }
func TestStrictReplayFailsMissingHistoryButAcceptsPreGenesisEmptyRange(t *testing.T) {
	for _, origin := range []uint64{0, 10} {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		m := NewManager(ManagerConfig{})
		m.InitLastAcked(10)
		m.SetTransformOrigin("v", origin)
		m.ObserveQueryBarrier("v", 20)
		stream := NewStream(m)
		h := &strictResultHandler{done: make(chan struct{})}
		sub, err := stream.Subscribe(ctx, wal.TransformLogSubscriptionOption{VChannel: "v", StartAfterTimeTick: 0, EndTimeTick: 20, RequireExactStart: true, Handler: h})
		require.NoError(t, err)
		select {
		case <-h.done:
		case <-ctx.Done():
			t.Fatal("strict replay did not finish")
		}
		if origin == 0 {
			require.ErrorIs(t, stream.Error(), wal.ErrTransformLogStartPointTruncated)
			require.Zero(t, h.tick)
		} else {
			require.Nil(t, stream.Error())
			require.Equal(t, uint64(20), h.tick)
		}
		_ = sub.Close()
		_ = stream.Close()
		cancel()
	}
}
