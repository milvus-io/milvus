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

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestInsertDispatchErrorBlocksRecoveryCompletion(t *testing.T) {
	for _, mode := range []string{"replicated", "summary-observed", "mixed-transaction"} {
		t.Run(mode, func(t *testing.T) {
			initial := &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 10}
			storage := newTestRecoveryStorage(t, initial)
			t.Cleanup(storage.metrics.Close)
			t.Cleanup(storage.taskScheduler.Close)
			t.Cleanup(storage.broadcastAck.Close)
			summary := walsummary.NewManager(walsummary.ManagerConfig{})
			summary.InitLastAcked(10)
			manager, err := vchannel.NewPChannelRecoveryManager(vchannel.PChannelManagerConfig{
				PChannel:       storage.channel.Name,
				VChannelMetas:  map[string]*streamingpb.VChannelMeta{"v1": {Vchannel: "v1"}}, // No query load/runtime.
				SummaryManager: summary,
			})
			require.NoError(t, err)
			t.Cleanup(manager.Close)
			storage.vchannelManager, storage.summaryManager = manager, summary
			insert := func(tick uint64) message.ImmutableMessage {
				msg := message.NewInsertMessageBuilderV2().WithVChannel("v1").WithHeader(&message.InsertMessageHeaderV2{
					CollectionId: 1, SchemaVersion: proto.Int32(1), Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 2, Rows: 1, SegmentAssignment: &message.SegmentAssignment{SegmentId: 3}}},
				}).WithBody(&message.InsertMessageBody{Rows: &message.InsertRows{
					PrimaryKeys:    &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}},
					RowIdsEncoding: &message.InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: []int64{8}}},
				}}).MustBuildMutable().WithTimeTick(tick).WithLastConfirmed(walimplstest.NewTestMessageID(1))
				if mode == "replicated" {
					msg = msg.WithReplicateHeader(&message.ReplicateHeader{ClusterID: "source", TimeTick: tick, MessageID: walimplstest.NewTestMessageID(2), LastConfirmedMessageID: walimplstest.NewTestMessageID(1)})
				}
				return msg.IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tick)))
			}
			raw := insert(20)
			if mode == "mixed-transaction" {
				begin := message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 1}).WithTimeTick(11).WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(11))
				commit := message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 3}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(message.TxnContext{TxnID: 1}).WithTimeTick(30).WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(30))
				builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin))
				builder.Add(insert(19))
				builder.Add(raw)
				builder.Add(newRecoveryTestDeleteMessage(t, "v1", 21))
				raw, err = builder.Build(message.MustAsImmutableCommitTxnMessageV2(commit))
				require.NoError(t, err)
			}
			injected := merr.Wrap(merr.ErrIoPermissionDenied, "dispatch decryption denied")
			calls := 0
			var original func(context.Context, message.ImmutableMessage) ([][]int64, error)
			patch := mockey.Mock(messageutil.InsertSelections).Origin(&original).To(func(ctx context.Context, msg message.ImmutableMessage) ([][]int64, error) {
				calls++
				if mode == "mixed-transaction" && calls == 1 {
					return original(ctx, msg)
				}
				// Summary has already installed readable coverage when dispatch begins.
				batch, err := summary.ReadTransform(ctx, "v1", 0, raw.TimeTick(), walsummary.ReadLimits{})
				require.NoError(t, err)
				require.Equal(t, raw.TimeTick(), batch.CoveredThrough)
				return nil, injected
			}).Build()
			defer patch.UnPatch()
			var finalizedPoison []bool
			var newOwner func(message.ImmutableMessage, func(bool)) message.OwnedImmutableMessage
			ownership := mockey.Mock(message.NewOwnedImmutableMessageWithFinalizer).Origin(&newOwner).To(func(msg message.ImmutableMessage, finalizer func(bool)) message.OwnedImmutableMessage {
				return newOwner(msg, func(poisoned bool) { finalizedPoison = append(finalizedPoison, poisoned); finalizer(poisoned) })
			}).Build()
			defer ownership.UnPatch()
			err = storage.observeMessage(t.Context(), raw)
			require.ErrorIs(t, err, injected)
			require.Equal(t, []bool{true}, finalizedPoison, "failed outer message must release its payload with poison, including a mixed transaction")
			require.EqualValues(t, 10, storage.ackTracker.CompletedPoint().TimeTick)
			// A later successful confirmation must not jump over the poisoned entry.
			require.NoError(t, storage.observeMessage(t.Context(), newAckTestTimeTickMessage(t, 40, 40)))
			require.EqualValues(t, 10, storage.ackTracker.CompletedPoint().TimeTick)
			candidate, _ := storage.ackTracker.CheckpointThrough(40)
			require.EqualValues(t, 10, candidate.TimeTick, "even fully persisted Summary must not bypass failed L1 dispatch")
			if snapshot := storage.consumeDirtySnapshot(); snapshot != nil {
				require.EqualValues(t, 10, snapshot.Checkpoint.TimeTick)
			}
			require.Positive(t, calls)
		})
	}
}

func TestDispatchErrorStopsStartupAndLiveRecovery(t *testing.T) {
	for _, live := range []bool{false, true} {
		t.Run(map[bool]string{false: "startup", true: "live"}[live], func(t *testing.T) {
			storage := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 10})
			t.Cleanup(storage.metrics.Close)
			t.Cleanup(storage.broadcastAck.Close)
			injected := merr.ErrIoPermissionDenied
			calls := 0
			patch := mockey.Mock((*recoveryStorageImpl).observeMessage).To(func(_ *recoveryStorageImpl, _ context.Context, _ message.ImmutableMessage) error {
				calls++
				return injected
			}).Build()
			defer patch.UnPatch()
			stream := &blockingRecoveryStream{ch: make(chan message.ImmutableMessage, 2), closed: make(chan struct{})}
			t.Cleanup(func() { _ = stream.Close() })
			raw := newAckTestRecoveryBarrier(t, 20, 2)
			stream.ch <- raw
			stream.ch <- newAckTestRecoveryBarrier(t, 30, 3)
			if live {
				var fatal error
				storage.onFatal = func(err error) { fatal = err }
				storage.runLiveScanner(stream)
				require.ErrorIs(t, fatal, injected)
				select {
				case <-stream.closed:
				default:
					t.Fatal("failed live scanner must close its stream")
				}
			} else {
				snapshot, err := storage.runBoundedRecovery(t.Context(), &recordingRecoveryStreamBuilder{stream: stream}, raw)
				require.ErrorIs(t, err, injected)
				require.Nil(t, snapshot)
			}
			require.Equal(t, 1, calls, "must stop before observing the next message")
		})
	}
}
