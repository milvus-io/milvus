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

package transformlog

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func TestStrictReplayRequiresExplicitPeerAcknowledgment(t *testing.T) {
	for _, confirmed := range []bool{false, true} {
		h := newRecordingHandler()
		s := &EventStream{subscriptions: make(map[int64]*eventSubscription)}
		sub := newEventSubscription(s, 1, wal.TransformLogSubscriptionOption{VChannel: "v", RequireExactStart: true, Handler: h})
		s.subscriptions[1] = sub
		event := wal.TransformLogStreamEvent{Entry: &streamingpb.TransformLogEntry{TimeTick: 10}}
		sub.handle(event)
		require.Empty(t, h.events, "strict delivery cannot precede confirmation")
		s.handleCreate(&streamingpb.CreateTransformSubscriptionResponse{SubscriptionId: 1, RequireExactStart: confirmed})
		<-sub.ready
		if confirmed {
			require.NoError(t, sub.readyErr)
			sub.handle(event)
			require.Len(t, h.events, 1)
		} else {
			require.True(t, status.AsStreamingError(sub.readyErr).IsUnrecoverable())
			sub.handle(event)
			require.Empty(t, h.events, "old peers ignoring the flag cannot supply strict history")
		}
	}
}
