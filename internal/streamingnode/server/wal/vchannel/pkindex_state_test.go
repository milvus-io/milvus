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

package vchannel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

const (
	pkStateVChannel = "v1"
	pkStateIndexID  = int64(7)
	pkStateFieldID  = int64(100)
)

var (
	never   = streamingpb.VChannelWriteHistory_VCHANNEL_WRITE_HISTORY_NEVER
	written = streamingpb.VChannelWriteHistory_VCHANNEL_WRITE_HISTORY_WRITTEN
	unknown = streamingpb.VChannelWriteHistory_VCHANNEL_WRITE_HISTORY_UNKNOWN
)

func pkStateImmutable(m message.MutableMessage, tt uint64) message.ImmutableMessage {
	return m.WithTimeTick(tt).WithLastConfirmed(walimplstest.NewTestMessageID(1)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt)))
}

func pkStateCreateCollection(t *testing.T, tt uint64) message.ImmutableMessage {
	id := walimplstest.NewTestMessageID(int64(tt))
	return message.CreateTestCreateCollectionMessage(t, 1, tt, id).IntoImmutableMessage(id)
}

func pkStateInsert(tt uint64) message.ImmutableMessage {
	return pkStateImmutable(message.NewInsertMessageBuilderV1().WithVChannel(pkStateVChannel).
		WithHeader(&message.InsertMessageHeader{CollectionId: 1}).WithBody(&msgpb.InsertRequest{}).MustBuildMutable(), tt)
}

func pkStateDelete(tt uint64) message.ImmutableMessage {
	return pkStateImmutable(message.NewDeleteMessageBuilderV1().WithVChannel(pkStateVChannel).
		WithHeader(&message.DeleteMessageHeader{CollectionId: 1}).WithBody(&msgpb.DeleteRequest{}).MustBuildMutable(), tt)
}

func pkStateImport(tt uint64) message.ImmutableMessage {
	return pkStateImmutable(message.NewImportMessageBuilderV1().WithVChannel(pkStateVChannel).
		WithHeader(&message.ImportMessageHeader{}).WithBody(&msgpb.ImportMsg{}).MustBuildMutable(), tt)
}

func pkStateCommitImport(tt uint64) message.ImmutableMessage {
	return pkStateImmutable(message.NewCommitImportMessageBuilderV2().WithVChannel(pkStateVChannel).
		WithHeader(&message.CommitImportMessageHeader{}).WithBody(&message.CommitImportMessageBody{}).MustBuildMutable(), tt)
}

func pkStateTruncate(tt uint64) message.ImmutableMessage {
	return pkStateImmutable(message.NewTruncateCollectionMessageBuilderV2().WithVChannel(pkStateVChannel).
		WithHeader(&message.TruncateCollectionMessageHeader{CollectionId: 1}).WithBody(&message.TruncateCollectionMessageBody{}).MustBuildMutable(), tt)
}

func pkStateCreateIndex(tt uint64, indexType string) message.ImmutableMessage {
	return pkStateImmutable(message.NewCreateIndexMessageBuilderV2().WithVChannel(pkStateVChannel).
		WithHeader(&message.CreateIndexMessageHeader{CollectionId: 1, FieldId: pkStateFieldID, IndexId: pkStateIndexID, IndexType: indexType}).
		WithBody(&message.CreateIndexMessageBody{}).MustBuildMutable(), tt)
}

func pkStateDropIndex(tt uint64, indexType string, indexIDs ...int64) message.ImmutableMessage {
	return pkStateImmutable(message.NewDropIndexMessageBuilderV2().WithVChannel(pkStateVChannel).
		WithHeader(&message.DropIndexMessageHeader{CollectionId: 1, IndexIds: indexIDs, IndexType: indexType}).
		WithBody(&message.DropIndexMessageBody{}).MustBuildMutable(), tt)
}

func pkStateTxn(t *testing.T, tt uint64) message.ImmutableMessage {
	txnCtx := message.TxnContext{TxnID: 1, Keepalive: time.Minute}
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel(pkStateVChannel).
		WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().WithTimeTick(tt - 1).WithLastConfirmed(walimplstest.NewTestMessageID(1)).
		WithTxnContext(txnCtx).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt) * 10))
	body := message.NewInsertMessageBuilderV1().WithVChannel(pkStateVChannel).
		WithHeader(&message.InsertMessageHeader{CollectionId: 1}).WithBody(&msgpb.InsertRequest{}).
		MustBuildMutable().WithTimeTick(tt - 1).WithLastConfirmed(walimplstest.NewTestMessageID(1)).
		WithTxnContext(txnCtx).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt)*10 + 1))
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel(pkStateVChannel).
		WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().WithTimeTick(tt).WithLastConfirmed(walimplstest.NewTestMessageID(1)).
		WithTxnContext(txnCtx).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt)*10 + 2))
	txn, err := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin)).
		Add(body).Build(message.MustAsImmutableCommitTxnMessageV2(commit))
	require.NoError(t, err)
	return txn
}

func newPKStateManager(t *testing.T, metas map[string]*streamingpb.VChannelMeta) *PChannelRecoveryManager {
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{PChannel: "p1", VChannelMetas: metas})
	require.NoError(t, err)
	t.Cleanup(manager.Close)
	return manager
}

func observePKState(t *testing.T, manager *PChannelRecoveryManager, msgs ...message.ImmutableMessage) {
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	for _, msg := range msgs {
		owner := tracker.Track(msg)
		retained := owner.Clone()
		manager.ObserveMessage(context.Background(), retained)
		retained.Release()
		owner.Release()
	}
}

// persistPKState persists every dirty vchannel snapshot and returns the last
// persisted meta of the vchannel.
func persistPKState(manager *PChannelRecoveryManager) *streamingpb.VChannelMeta {
	var meta *streamingpb.VChannelMeta
	for _, snapshot := range manager.ConsumeDirtySnapshots() {
		if snapshot.ModuleName() == moduleapi.ModuleNameVChannel {
			meta = proto.Clone(snapshot.Payload()).(*streamingpb.VChannelMeta)
		}
		snapshot.MarkPersisted()
	}
	return meta
}

func requirePKState(t *testing.T, manager *PChannelRecoveryManager, everWritten streamingpb.VChannelWriteHistory, pkIndex *streamingpb.PKIndexOfVChannel) {
	t.Helper()
	state, ok := manager.PKIndexState(pkStateVChannel)
	require.True(t, ok)
	require.Equal(t, everWritten, state.EverWritten)
	if pkIndex == nil {
		require.Nil(t, state.PKIndex)
	} else {
		require.True(t, proto.Equal(pkIndex, state.PKIndex), "got %v", state.PKIndex)
	}
	require.NotNil(t, state.Schema)
}

func TestPKIndexState(t *testing.T) {
	enabledAt5 := &streamingpb.PKIndexOfVChannel{IndexId: pkStateIndexID, FieldId: pkStateFieldID, EnableTimeTick: 5}
	pk := common.PrimaryKeyIndexType
	for _, tc := range []struct {
		name        string
		msgs        func(t *testing.T) []message.ImmutableMessage
		everWritten streamingpb.VChannelWriteHistory
		pkIndex     *streamingpb.PKIndexOfVChannel
	}{
		{"create collection", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1)}
		}, never, nil},
		{"insert", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1), pkStateInsert(3)}
		}, written, nil},
		{"txn commit", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1), pkStateTxn(t, 3)}
		}, written, nil},
		{"import", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1), pkStateImport(3)}
		}, written, nil},
		{"commit import", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1), pkStateCommitImport(3)}
		}, written, nil},
		{"delete", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1), pkStateDelete(3)}
		}, never, nil},
		{"insert then truncate", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1), pkStateInsert(3), pkStateTruncate(5)}
		}, never, nil},
		{"create primary key index on an empty vchannel", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1), pkStateCreateIndex(5, pk)}
		}, never, enabledAt5},
		{"create primary key index after an insert", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1), pkStateInsert(3), pkStateCreateIndex(5, pk)}
		}, written, nil},
		{"create another index", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1), pkStateCreateIndex(5, "STL_SORT")}
		}, never, nil},
		{"insert after the primary key index", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1), pkStateCreateIndex(5, pk), pkStateInsert(6)}
		}, written, enabledAt5},
		{"truncate keeps the primary key index", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1), pkStateCreateIndex(5, pk), pkStateInsert(6), pkStateTruncate(8)}
		}, never, enabledAt5},
		{"drop the primary key index by id", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1), pkStateCreateIndex(5, pk), pkStateDropIndex(8, pk, pkStateIndexID)}
		}, never, nil},
		{"drop all indexes", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1), pkStateCreateIndex(5, pk), pkStateDropIndex(8, pk)}
		}, never, nil},
		{"drop another index", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1), pkStateCreateIndex(5, pk), pkStateDropIndex(8, "", 9)}
		}, never, enabledAt5},
		{"drop another index id with the primary key type", func(t *testing.T) []message.ImmutableMessage {
			return []message.ImmutableMessage{pkStateCreateCollection(t, 1), pkStateCreateIndex(5, pk), pkStateDropIndex(8, pk, 9)}
		}, never, enabledAt5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := newPKStateManager(t, nil)
			observePKState(t, manager, tc.msgs(t)...)
			requirePKState(t, manager, tc.everWritten, tc.pkIndex)
		})
	}
}

func TestPKIndexStateUnknownWriteHistory(t *testing.T) {
	manager := newPKStateManager(t, nil)
	observePKState(t, manager, pkStateCreateCollection(t, 1))
	meta := persistPKState(manager)
	require.NotNil(t, meta)
	meta.CollectionInfo.EverWritten = unknown

	restarted := newPKStateManager(t, map[string]*streamingpb.VChannelMeta{pkStateVChannel: meta})
	observePKState(t, restarted, pkStateCreateIndex(5, common.PrimaryKeyIndexType))
	requirePKState(t, restarted, unknown, nil)
}

func TestPKIndexStateReplay(t *testing.T) {
	pk := common.PrimaryKeyIndexType
	t.Run("a persisted create index is not decided again", func(t *testing.T) {
		manager := newPKStateManager(t, nil)
		observePKState(t, manager, pkStateCreateCollection(t, 1), pkStateCreateIndex(5, pk), pkStateInsert(6))
		meta := persistPKState(manager)
		require.Equal(t, written, meta.GetCollectionInfo().GetEverWritten())

		restarted := newPKStateManager(t, map[string]*streamingpb.VChannelMeta{pkStateVChannel: meta})
		observePKState(t, restarted, pkStateCreateIndex(5, pk), pkStateInsert(6))
		requirePKState(t, restarted, written, &streamingpb.PKIndexOfVChannel{IndexId: pkStateIndexID, FieldId: pkStateFieldID, EnableTimeTick: 5})
	})

	t.Run("a write before a persisted truncate is skipped", func(t *testing.T) {
		manager := newPKStateManager(t, nil)
		observePKState(t, manager, pkStateCreateCollection(t, 1), pkStateInsert(3), pkStateTruncate(5))
		meta := persistPKState(manager)
		require.Equal(t, never, meta.GetCollectionInfo().GetEverWritten())

		restarted := newPKStateManager(t, map[string]*streamingpb.VChannelMeta{pkStateVChannel: meta})
		observePKState(t, restarted, pkStateInsert(3))
		requirePKState(t, restarted, never, nil)
	})
}

func TestPKIndexStateDirtyAndCheckpoint(t *testing.T) {
	pk := common.PrimaryKeyIndexType
	manager := newPKStateManager(t, nil)
	observePKState(t, manager, pkStateCreateCollection(t, 1))
	persistPKState(manager)

	observePKState(t, manager, pkStateCreateIndex(5, pk))
	meta := persistPKState(manager)
	require.NotNil(t, meta, "a create index marks the meta dirty")
	require.Equal(t, uint64(5), meta.GetCheckpointTimeTick())

	observePKState(t, manager, pkStateInsert(6))
	meta = persistPKState(manager)
	require.NotNil(t, meta, "the first write marks the meta dirty")
	require.Equal(t, written, meta.GetCollectionInfo().GetEverWritten())
	require.Equal(t, uint64(5), meta.GetCheckpointTimeTick(), "a write does not move the checkpoint")

	observePKState(t, manager, pkStateInsert(7))
	require.Nil(t, persistPKState(manager), "a later write does not mark the meta dirty")

	observePKState(t, manager, pkStateTruncate(9))
	meta = persistPKState(manager)
	require.NotNil(t, meta)
	require.Equal(t, uint64(9), meta.GetCheckpointTimeTick())
	require.Equal(t, never, meta.GetCollectionInfo().GetEverWritten())

	_, ok := manager.PKIndexState("unknown-vchannel")
	require.False(t, ok)
}

func TestPKIndexStateReturnsCopy(t *testing.T) {
	manager := newPKStateManager(t, nil)
	observePKState(t, manager, pkStateCreateCollection(t, 1), pkStateCreateIndex(5, common.PrimaryKeyIndexType))
	state, ok := manager.PKIndexState(pkStateVChannel)
	require.True(t, ok)
	state.PKIndex.IndexId = 99
	again, _ := manager.PKIndexState(pkStateVChannel)
	require.Equal(t, pkStateIndexID, again.PKIndex.GetIndexId())
}
