package msgdispatcher

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/mocks/github.com/milvus-io/milvus-proto/go-api/v3/mock_hook"
	"github.com/milvus-io/milvus/pkg/v3/mq/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// RegisterCipher initializes once per process. Keep the registered object stable,
// and replace its delegate for each sequential test run without retaining an old testing.T.
var replayCipher = &struct{ hook.Cipher }{}

func TestStreamingReplaySkipsDroppedCollectionBeforeDecrypt(t *testing.T) {
	for i := 0; i < 2; i++ {
		t.Run(fmt.Sprintf("run_%d", i), testStreamingReplay)
	}
}

func testStreamingReplay(t *testing.T) {
	cipher := mock_hook.NewMockCipher(t)
	encryptor := mock_hook.NewMockEncryptor(t)
	decryptor := mock_hook.NewMockDecryptor(t)
	encryptor.EXPECT().Encrypt(mock.Anything).RunAndReturn(func(payload []byte) ([]byte, error) {
		return append([]byte("encrypted:"), payload...), nil
	})
	decryptor.EXPECT().Decrypt(mock.Anything).RunAndReturn(func(payload []byte) ([]byte, error) {
		return payload[len("encrypted:"):], nil
	}).Maybe()
	cipher.EXPECT().GetEncryptor(mock.Anything, mock.Anything).Return(encryptor, []byte("test-key"), nil)
	deleted := false
	var kmsInvalid atomic.Bool
	retryStarted := make(chan struct{}, 1)
	decryptCalls := map[int64]int{}
	cipher.EXPECT().GetDecryptor(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(ezID, collectionID int64, key []byte) (hook.Decryptor, error) {
			decryptCalls[collectionID]++
			if collectionID == 2 && kmsInvalid.Load() {
				select {
				case retryStarted <- struct{}{}:
				default:
				}
				return nil, fmt.Errorf("plugin: kms key invalid for collection %d", collectionID)
			}
			if deleted && collectionID == 2 {
				return nil, fmt.Errorf("key root-coord/ez/%d not found", ezID)
			}
			return decryptor, nil
		}).Maybe()
	replayCipher.Cipher = cipher
	message.RegisterCipher(replayCipher)
	t.Cleanup(func() { replayCipher.Cipher = nil })
	restore := func(stored []byte, id int64) message.ImmutableMessage {
		wire := &messagespb.Message{}
		require.NoError(t, proto.Unmarshal(stored, wire))
		return message.NewImmutableMesasge(rmq.NewRmqID(id), wire.Payload, wire.Properties)
	}

	build := func(collectionID int64, tick uint64) []byte {
		vchannel := fmt.Sprintf("pchannel_0_%dv0", collectionID)
		msg, err := message.NewInsertMessageBuilderV1().
			WithHeader(&message.InsertMessageHeader{
				CollectionId: collectionID,
				Partitions: []*message.PartitionSegmentAssignment{{
					PartitionId: 10, Rows: 1,
					SegmentAssignment: &message.SegmentAssignment{SegmentId: 100 + collectionID},
				}},
			}).
			WithBody(&msgpb.InsertRequest{
				Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, MsgID: collectionID},
				CollectionID: collectionID, PartitionID: 10, ShardName: vchannel,
				NumRows: 1, RowIDs: []int64{42}, Timestamps: []uint64{1},
			}).
			WithVChannel(vchannel).
			WithCipher(&message.CipherConfig{EzID: collectionID, CollectionID: collectionID}).
			BuildMutable()
		require.NoError(t, err)
		stored, err := proto.Marshal(msg.WithTimeTick(tick).WithLastConfirmed(rmq.NewRmqID(1)).IntoMessageProto())
		require.NoError(t, err)
		return stored
	}

	// Keep persisted ciphertext across a database drop and a fresh consumer.
	dropped := build(2, 100)
	live := build(1, 101)
	deleted = true
	d := &Dispatcher{targets: typeutil.NewConcurrentMap[string, *target]()}
	d.AddTarget(newTarget(&StreamConfig{VChannel: "pchannel_0_1v0"}, false))

	for i, stored := range [][]byte{dropped, live} {
		replayed := restore(stored, int64(i+2))
		pack := consumeReplayMessage(t, replayed)
		packs, err := d.groupAndParseMsgs(pack, adaptor.UnmashalerDispatcher)
		require.NoError(t, err)
		got := packs["pchannel_0_1v0"]
		require.NotNil(t, got)
		require.Equal(t, uint64(100+i), got.EndTs)
		if i == 0 {
			require.Empty(t, got.Msgs)
		} else {
			require.Len(t, got.Msgs, 1)
			insert := got.Msgs[0].(*msgstream.InsertMsg)
			require.Equal(t, int64(101), insert.SegmentID)
			require.Equal(t, []uint64{101}, insert.Timestamps)
		}
	}
	require.Zero(t, decryptCalls[2], "unsubscribed ciphertext must not request the deleted EZ")
	require.Equal(t, 1, decryptCalls[1])

	t.Run("drop after prefetch", func(t *testing.T) {
		deleted = false
		d.AddTarget(newTarget(&StreamConfig{VChannel: "pchannel_0_2v0"}, false))
		pack := consumeReplayMessage(t, restore(dropped, 4))
		require.Zero(t, decryptCalls[2], "prefetch must not decrypt before the target is selected")
		d.RemoveTarget("pchannel_0_2v0")
		deleted = true
		require.NotPanics(t, func() {
			packs, err := d.groupAndParseMsgs(pack, adaptor.UnmashalerDispatcher)
			require.NoError(t, err)
			require.Empty(t, packs["pchannel_0_1v0"].Msgs)
		})
		require.Zero(t, decryptCalls[2])
	})

	t.Run("restart transaction replay", func(t *testing.T) {
		// The WAL reconstructs transactions from persisted begin/body/commit
		// messages; the dropped database's ciphertext remains in the body.
		for i, stored := range [][]byte{dropped, live} {
			body := restore(stored, int64(10+i*3))
			lastConfirmed := rmq.NewRmqID(int64(9 + i*3))
			tick := uint64(110 + i)
			txn := newReplayTransaction(t, tick, lastConfirmed, body)
			pack := consumeReplayMessage(t, txn)
			packs, err := d.groupAndParseMsgs(pack, adaptor.UnmashalerDispatcher)
			require.NoError(t, err)
			got := packs["pchannel_0_1v0"]
			require.Equal(t, tick, got.EndTs)
			require.Equal(t, tick, got.EndPositions[0].Timestamp)
			require.Equal(t, adaptor.MustGetMQWrapperIDFromMessage(lastConfirmed).Serialize(), got.EndPositions[0].MsgID)
			if i == 0 {
				require.Empty(t, got.Msgs)
			} else {
				require.Len(t, got.Msgs, 1)
				require.Equal(t, []uint64{tick}, got.Msgs[0].(*msgstream.InsertMsg).Timestamps)
			}
		}
		require.Zero(t, decryptCalls[2])
	})

	t.Run("delete replay", func(t *testing.T) {
		for _, collectionID := range []int64{2, 1} {
			vchannel := fmt.Sprintf("pchannel_0_%dv0", collectionID)
			msg, err := message.NewDeleteMessageBuilderV1().
				WithHeader(&message.DeleteMessageHeader{CollectionId: collectionID, Rows: 1}).
				WithBody(&msgpb.DeleteRequest{
					Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
					CollectionID: collectionID, ShardName: vchannel, Timestamps: []uint64{1},
				}).WithVChannel(vchannel).
				WithCipher(&message.CipherConfig{EzID: collectionID, CollectionID: collectionID}).BuildMutable()
			require.NoError(t, err)
			stored, err := proto.Marshal(msg.WithTimeTick(120).WithLastConfirmed(rmq.NewRmqID(20)).IntoMessageProto())
			require.NoError(t, err)
			pack := consumeReplayMessage(t, restore(stored, 21))
			packs, err := d.groupAndParseMsgs(pack, adaptor.UnmashalerDispatcher)
			require.NoError(t, err)
			got := packs["pchannel_0_1v0"]
			if collectionID == 2 {
				require.Empty(t, got.Msgs)
			} else {
				require.Len(t, got.Msgs, 1)
				require.Equal(t, []uint64{120}, got.Msgs[0].(*msgstream.DeleteMsg).Timestamps)
			}
		}
		require.Zero(t, decryptCalls[2])
	})

	t.Run("malformed transaction is not partially dispatched", func(t *testing.T) {
		deletedMsg, err := message.NewDeleteMessageBuilderV1().
			WithHeader(&message.DeleteMessageHeader{CollectionId: 1, Rows: 1}).
			WithBody(&msgpb.DeleteRequest{
				Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
				CollectionID: 1, ShardName: "pchannel_0_1v0", Timestamps: []uint64{1},
			}).WithVChannel("pchannel_0_1v0").
			WithCipher(&message.CipherConfig{EzID: 1, CollectionID: 1}).BuildMutable()
		require.NoError(t, err)
		deleteBody := deletedMsg.WithTimeTick(130).WithLastConfirmed(rmq.NewRmqID(30)).IntoImmutableMessage(rmq.NewRmqID(31))
		broken := restore(live, 32).IntoMessageProto()
		broken.Payload = append([]byte("encrypted:"), 0xff)
		insertBody := message.NewImmutableMesasge(rmq.NewRmqID(32), broken.Payload, broken.Properties)
		// Exercise failure after and before a valid body.
		for _, bodies := range [][]message.ImmutableMessage{{deleteBody, insertBody}, {insertBody, deleteBody}} {
			txn := newReplayTransaction(t, 131, rmq.NewRmqID(30), bodies...)
			pack := consumeReplayMessage(t, txn)
			packs, err := d.groupAndParseMsgs(pack, adaptor.UnmashalerDispatcher)
			require.Error(t, err)
			require.Empty(t, packs, "no part of a malformed transaction may be dispatched")
		}
	})

	t.Run("subscribed missing key remains an error", func(t *testing.T) {
		d.AddTarget(newTarget(&StreamConfig{VChannel: "pchannel_0_2v0"}, false))
		pack := consumeReplayMessage(t, restore(dropped, 30))
		require.PanicsWithValue(t, "can not decode message payload: key root-coord/ez/2 not found", func() {
			d.groupAndParseMsgs(pack, adaptor.UnmashalerDispatcher)
		})
		require.Equal(t, 1, decryptCalls[2])
	})

	t.Run("existing payload and base adaptor still decrypt immediately", func(t *testing.T) {
		msg := restore(dropped, 31)
		want := "can not decode message payload: key root-coord/ez/2 not found"
		require.PanicsWithValue(t, want, func() { msg.Payload() })
		require.PanicsWithValue(t, want, func() {
			adaptor.NewBaseMsgPackAdaptorHandler().GenerateMsgPack(msg)
		})
	})

	t.Run("dispatcher attempts decryption only once", func(t *testing.T) {
		kmsInvalid.Store(true)
		defer kmsInvalid.Store(false)
		before := decryptCalls[2]
		pack := consumeReplayMessage(t, restore(dropped, 32))
		packs, err := d.groupAndParseMsgs(pack, adaptor.UnmashalerDispatcher)
		// CockroachDB error marks are recognized by its errors.Is, which the
		// cipher classifier also uses, rather than the standard errors.Is.
		require.True(t, errors.Is(err, message.ErrKmsKeyInvalid))
		require.True(t, message.IsRetryableCipherError(err))
		require.Empty(t, packs)
		require.Equal(t, before+1, decryptCalls[2])
	})

	t.Run("target added after prefetch retains its message", func(t *testing.T) {
		deleted = false
		d.RemoveTarget("pchannel_0_2v0")
		pack := consumeReplayMessage(t, restore(dropped, 33))
		d.AddTarget(newTarget(&StreamConfig{VChannel: "pchannel_0_2v0"}, false))
		packs, err := d.groupAndParseMsgs(pack, adaptor.UnmashalerDispatcher)
		require.NoError(t, err)
		require.Len(t, packs["pchannel_0_2v0"].Msgs, 1)
	})

	for _, removeRevoked := range []bool{false, true} {
		t.Run(fmt.Sprintf("deregister_during_retry_remove_revoked_%t", removeRevoked), func(t *testing.T) {
			deleted = false
			kmsInvalid.Store(true)
			for len(retryStarted) > 0 {
				<-retryStarted
			}
			input := make(chan *msgstream.ConsumeMsgPack, 2)
			terminalErr := errors.New("terminal stream error after cipher retry")
			streamErrors := make(chan error, 2)
			stream := msgstream.NewMockMsgStream(t)
			stream.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)
			stream.EXPECT().Chan().Return((<-chan *msgstream.ConsumeMsgPack)(input))
			stream.EXPECT().GetUnmarshalDispatcher().Return(adaptor.UnmashalerDispatcher)
			stream.EXPECT().Close().Return()
			factory := &msgstream.MockMqFactory{NewMsgStreamFunc: func(context.Context) (msgstream.MsgStream, error) {
				return &testTerminalMsgStream{MsgStream: stream, err: terminalErr}, nil
			}}
			dispatcher, err := NewDispatcher(context.Background(), factory, 1, "pchannel_0", nil,
				common.SubscriptionPositionEarliest, 0, false)
			require.NoError(t, err)
			manager := NewDispatcherManager("pchannel_0", typeutil.QueryNodeRole, 0, factory, false).(*dispatcherManager)
			manager.mainDispatcher = dispatcher
			outputs := make(map[int]<-chan *MsgPack)
			for _, id := range []int{1, 2} {
				vchannel := fmt.Sprintf("pchannel_0_%dv0", id)
				outputs[id], err = manager.Add(context.Background(), &StreamConfig{
					VChannel: vchannel,
					OnStreamError: func(err error) {
						streamErrors <- err
					},
				})
				require.NoError(t, err)
				target, _ := manager.registeredTargets.Get(vchannel)
				dispatcher.AddTarget(target)
			}
			t.Cleanup(func() {
				kmsInvalid.Store(false)
				manager.Remove("pchannel_0_1v0")
				manager.Remove("pchannel_0_2v0")
			})
			for i, stored := range [][]byte{dropped, live} {
				input <- consumeReplayMessage(t, restore(stored, int64(40+i)))
			}
			// A closed upstream must still drain the retained pack after cipher
			// recovery and resume, then report its terminal error to the target.
			close(input)
			dispatcher.Handle(start)
			select {
			case <-retryStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("dispatcher did not attempt decryption")
			}
			require.Zero(t, dispatcher.CurTs(), "unread ciphertext must not advance the dispatcher")
			removed, remaining := 1, 2
			if removeRevoked {
				removed, remaining = 2, 1
			}
			done := make(chan struct{})
			go func() {
				manager.Remove(fmt.Sprintf("pchannel_0_%dv0", removed))
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("deregister waited for KMS recovery")
			}
			if !removeRevoked {
				kmsInvalid.Store(false)
			}
			for i := 0; i < 2; i++ {
				select {
				case pack := <-outputs[remaining]:
					require.NotNil(t, pack)
					require.Equal(t, uint64(100+i), pack.EndTs)
					if (i == 0 && !removeRevoked) || (i == 1 && removeRevoked) {
						require.Len(t, pack.Msgs, 1, "resume must deliver the retained message")
					} else {
						require.Empty(t, pack.Msgs)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("remaining subscriber did not resume")
				}
			}
			select {
			case err := <-streamErrors:
				require.ErrorIs(t, err, terminalErr)
			case <-time.After(5 * time.Second):
				t.Fatal("terminal stream error was not reported after cipher retry")
			}
			select {
			case _, ok := <-outputs[remaining]:
				require.False(t, ok, "the remaining target must close after draining pending messages")
			case <-time.After(5 * time.Second):
				t.Fatal("remaining target did not close")
			}
		})
	}
}

// Exercise the same channel adaptor as the QueryNode scanner, including prefetch.
func consumeReplayMessage(t *testing.T, msg message.ImmutableMessage) *msgstream.ConsumeMsgPack {
	t.Helper()
	handler := adaptor.NewMsgPackAdaptorHandler()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	packs := make(chan *msgstream.ConsumeMsgPack, 1)
	go func() {
		select {
		case pack := <-handler.Chan():
			packs <- pack
		case <-ctx.Done():
		}
	}()
	require.NotPanics(t, func() {
		result := handler.Handle(message.HandleParam{Ctx: ctx, Message: msg})
		require.NoError(t, result.Error)
		require.True(t, result.MessageHandled)
	})
	select {
	case pack := <-packs:
		return pack
	case <-ctx.Done():
		t.Fatal("adaptor did not emit a message pack")
		return nil
	}
}

func newReplayTransaction(t *testing.T, tick uint64, lastConfirmed message.MessageID, bodies ...message.ImmutableMessage) message.ImmutableTxnMessage {
	t.Helper()
	txnCtx := message.TxnContext{TxnID: message.TxnID(tick), Keepalive: time.Second}
	begin, err := message.NewBeginTxnMessageBuilderV2().
		WithVChannel(bodies[0].VChannel()).WithHeader(&message.BeginTxnMessageHeader{}).
		WithBody(&message.BeginTxnMessageBody{}).BuildMutable()
	require.NoError(t, err)
	commit, err := message.NewCommitTxnMessageBuilderV2().
		WithVChannel(bodies[0].VChannel()).WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).BuildMutable()
	require.NoError(t, err)
	builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(
		begin.WithTxnContext(txnCtx).WithTimeTick(tick - 1).WithLastConfirmed(lastConfirmed).IntoImmutableMessage(lastConfirmed)))
	for _, body := range bodies {
		builder.Add(body)
	}
	txn, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(
		commit.WithTxnContext(txnCtx).WithTimeTick(tick).WithLastConfirmed(lastConfirmed).IntoImmutableMessage(rmq.NewRmqID(int64(tick)))))
	require.NoError(t, err)
	return txn
}
