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

package recovery

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func newAckExtraCreateIndex(t *testing.T, vchannel string, timeTick uint64, indexType string) message.ImmutableMessage {
	return newBroadcastAckMessageWith(t, message.NewCreateIndexMessageBuilderV2().
		WithHeader(&message.CreateIndexMessageHeader{CollectionId: 1, FieldId: 100, IndexId: 7, IndexType: indexType}).
		WithBody(&message.CreateIndexMessageBody{}).
		WithBroadcast([]string{vchannel}), 1, timeTick)
}

func newAckExtraInsert(t *testing.T, timeTick uint64) message.ImmutableMessage {
	return message.CreateTestInsertMessage(t, 1, 1, timeTick, walimplstest.NewTestMessageID(int64(timeTick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timeTick)))
}

// newAckExtraStorage returns a recovery storage whose vchannel manager has
// observed msgs on vchannel v1.
func newAckExtraStorage(t *testing.T, msgs ...message.ImmutableMessage) *recoveryStorageImpl {
	manager, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{PChannel: "p1"})
	require.NoError(t, err)
	t.Cleanup(manager.Close)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	createID := walimplstest.NewTestMessageID(1)
	all := append([]message.ImmutableMessage{
		message.CreateTestCreateCollectionMessage(t, 1, 1, createID).IntoImmutableMessage(createID),
	}, msgs...)
	for _, msg := range all {
		owner := tracker.Track(msg)
		retained := owner.Clone()
		manager.ObserveMessage(context.Background(), retained)
		retained.Release()
		owner.Release()
	}
	return &recoveryStorageImpl{vchannelManager: manager}
}

func ackExtraOutcome(t *testing.T, extra *anypb.Any) pkindexpb.CreateIndexAckResult_Outcome {
	t.Helper()
	require.NotNil(t, extra)
	result := &pkindexpb.CreateIndexAckResult{}
	require.NoError(t, extra.UnmarshalTo(result))
	return result.GetOutcome()
}

func setNodePKIndexEnabled(t *testing.T, value string) {
	params := paramtable.Get()
	params.Save(params.StreamingCfg.PKIndexEnabled.Key, value)
	t.Cleanup(func() { params.Reset(params.StreamingCfg.PKIndexEnabled.Key) })
}

func TestBroadcastAckExtra(t *testing.T) {
	paramtable.Init()
	pk := common.PrimaryKeyIndexType

	t.Run("enabled on an empty vchannel", func(t *testing.T) {
		setNodePKIndexEnabled(t, "true")
		msg := newAckExtraCreateIndex(t, "v1", 5, pk)
		r := newAckExtraStorage(t, msg)
		require.Equal(t, pkindexpb.CreateIndexAckResult_ENABLED, ackExtraOutcome(t, r.broadcastAckExtraLocked(msg)))
	})

	t.Run("not empty after a write", func(t *testing.T) {
		setNodePKIndexEnabled(t, "true")
		msg := newAckExtraCreateIndex(t, "v1", 5, pk)
		r := newAckExtraStorage(t, newAckExtraInsert(t, 3), msg)
		require.Equal(t, pkindexpb.CreateIndexAckResult_NOT_EMPTY, ackExtraOutcome(t, r.broadcastAckExtraLocked(msg)))
	})

	t.Run("not empty when the index was enabled by another CreateIndex", func(t *testing.T) {
		setNodePKIndexEnabled(t, "true")
		r := newAckExtraStorage(t, newAckExtraCreateIndex(t, "v1", 5, pk))
		require.Equal(t, pkindexpb.CreateIndexAckResult_NOT_EMPTY,
			ackExtraOutcome(t, r.broadcastAckExtraLocked(newAckExtraCreateIndex(t, "v1", 6, pk))))
	})

	t.Run("disabled on the node", func(t *testing.T) {
		setNodePKIndexEnabled(t, "false")
		msg := newAckExtraCreateIndex(t, "v1", 5, pk)
		r := newAckExtraStorage(t, msg)
		require.Equal(t, pkindexpb.CreateIndexAckResult_DISABLED, ackExtraOutcome(t, r.broadcastAckExtraLocked(msg)))
	})

	t.Run("no extra for other messages", func(t *testing.T) {
		setNodePKIndexEnabled(t, "true")
		r := newAckExtraStorage(t)
		require.Nil(t, r.broadcastAckExtraLocked(newAckExtraCreateIndex(t, "p1_vcchan", 5, pk)))
		require.Nil(t, r.broadcastAckExtraLocked(newAckExtraCreateIndex(t, "v1", 5, "STL_SORT")))
		require.Nil(t, r.broadcastAckExtraLocked(newAckExtraInsert(t, 5)))
	})
}

func TestBroadcastAckModulePassesExtraAcrossRetry(t *testing.T) {
	scheduler := &recordingAckTaskScheduler{}
	module := newBroadcastAckModule(moduleapi.Runtime{Scheduler: scheduler})
	t.Cleanup(module.Close)
	extra, err := anypb.New(&pkindexpb.CreateIndexAckResult{Outcome: pkindexpb.CreateIndexAckResult_ENABLED})
	require.NoError(t, err)
	var got []*anypb.Any
	module.ack = func(_ context.Context, _ message.ImmutableMessage, e *anypb.Any) error {
		got = append(got, e)
		if len(got) == 1 {
			return errors.New("coordinator unavailable")
		}
		return nil
	}
	msg := newAckExtraCreateIndex(t, "v1", 10, common.PrimaryKeyIndexType)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	module.Accept(tracker.Track(msg), extra)

	require.NoError(t, scheduler.waitTask(t).Execute(context.Background()))
	require.NoError(t, scheduler.waitTaskAfter(t, 1).Execute(context.Background()))
	require.Len(t, got, 2)
	for _, e := range got {
		require.True(t, proto.Equal(extra, e))
	}
}
