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

package broadcaster

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	catalog "github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func createCreateIndexBroadcastTask(id uint64, vchannels []string) *streamingpb.BroadcastTask {
	msg := message.NewCreateIndexMessageBuilderV2().
		WithHeader(&message.CreateIndexMessageHeader{CollectionId: 1, IndexId: 2}).
		WithBody(&message.CreateIndexMessageBody{}).
		WithBroadcast(vchannels).
		MustBuildBroadcast().
		WithBroadcastID(id)
	return &streamingpb.BroadcastTask{
		Message:             msg.IntoMessageProto(),
		State:               streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING,
		AckedVchannelBitmap: make([]byte, len(vchannels)),
	}
}

func newCreateIndexAckMessage(broadcastID uint64, vchannel string, timeTick uint64) message.ImmutableMessage {
	return message.NewCreateIndexMessageBuilderV2().
		WithHeader(&message.CreateIndexMessageHeader{CollectionId: 1, IndexId: 2}).
		WithBody(&message.CreateIndexMessageBody{}).
		WithBroadcast([]string{vchannel}).
		MustBuildBroadcast().
		WithBroadcastID(broadcastID).
		SplitIntoMutableMessage()[0].
		WithTimeTick(timeTick).
		WithLastConfirmed(walimplstest.NewTestMessageID(1)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timeTick)))
}

func mustNewAny(t *testing.T, value string) *anypb.Any {
	a, err := anypb.New(wrapperspb.String(value))
	require.NoError(t, err)
	return a
}

func extraValue(t *testing.T, a *anypb.Any) string {
	require.NotNil(t, a)
	v := &wrapperspb.StringValue{}
	require.NoError(t, a.UnmarshalTo(v))
	return v.GetValue()
}

func TestBroadcastTaskAckExtra(t *testing.T) {
	registry.ResetRegistration()
	paramtable.Init()

	var saved *streamingpb.BroadcastTask
	meta := catalog.NewCataLog(nil)
	patch := mockey.Mock(mockey.GetMethod(meta, "SaveBroadcastTask")).
		To(func(_ context.Context, _ uint64, task *streamingpb.BroadcastTask) error {
			saved = proto.Clone(task).(*streamingpb.BroadcastTask)
			return nil
		}).Build()
	t.Cleanup(func() { patch.UnPatch() })
	resource.InitForTest(resource.OptStreamingCatalog(meta))

	ctx := context.Background()
	metrics := newBroadcasterMetrics()
	vchannels := []string{"v1", "v2"}
	task := newBroadcastTaskFromProto(createCreateIndexBroadcastTask(100, vchannels), metrics, newAckCallbackScheduler(mlog.With()))
	task.SetLogger(mlog.With())

	t.Run("each vchannel keeps its own extra", func(t *testing.T) {
		require.NoError(t, task.Ack(ctx, newCreateIndexAckMessage(100, "v1", 10), mustNewAny(t, "a")))
		// A second ack of an acked vchannel keeps the first extra.
		require.NoError(t, task.Ack(ctx, newCreateIndexAckMessage(100, "v1", 11), mustNewAny(t, "c")))
		require.NoError(t, task.Ack(ctx, newCreateIndexAckMessage(100, "v2", 12), mustNewAny(t, "b")))

		_, result := task.BroadcastResult()
		require.Equal(t, "a", extraValue(t, result["v1"].Extra))
		require.Equal(t, "b", extraValue(t, result["v2"].Extra))
	})

	t.Run("the extra survives the recovery and reaches the ack callback", func(t *testing.T) {
		require.NotNil(t, saved)
		b, err := proto.Marshal(saved)
		require.NoError(t, err)
		recovered := &streamingpb.BroadcastTask{}
		require.NoError(t, proto.Unmarshal(b, recovered))
		rt := newBroadcastTaskFromProto(recovered, metrics, nil)
		_, result := rt.BroadcastResult()

		callbackResults := messageAppendResults(result)
		require.Equal(t, "a", extraValue(t, callbackResults["v1"].Extra))
		require.Equal(t, "b", extraValue(t, callbackResults["v2"].Extra))
		require.Equal(t, uint64(10), callbackResults["v1"].TimeTick)
	})
}

func TestBroadcastTaskFastAckHasNoExtra(t *testing.T) {
	registry.ResetRegistration()
	paramtable.Init()

	meta := catalog.NewCataLog(nil)
	patch := mockey.Mock(mockey.GetMethod(meta, "SaveBroadcastTask")).Return(nil).Build()
	t.Cleanup(func() { patch.UnPatch() })
	resource.InitForTest(resource.OptStreamingCatalog(meta))

	task := newBroadcastTaskFromProto(createCreateIndexBroadcastTask(101, []string{"v1"}), newBroadcasterMetrics(), newAckCallbackScheduler(mlog.With()))
	task.SetLogger(mlog.With())
	require.NoError(t, task.FastAck(context.Background(), map[string]*types.AppendResult{
		"v1": {
			MessageID:              walimplstest.NewTestMessageID(3),
			LastConfirmedMessageID: walimplstest.NewTestMessageID(1),
			TimeTick:               3,
			Extra:                  mustNewAny(t, "produce"),
		},
	}))
	_, result := task.BroadcastResult()
	require.Nil(t, result["v1"].Extra)
}
