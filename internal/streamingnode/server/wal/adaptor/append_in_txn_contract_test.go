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

package adaptor_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/idempotency"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/lock"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/partialupdate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/registry"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// The contract tests drive txn.AppendInTxn through the production interceptor
// chain. The test interceptor sits where the primary key index interceptor will
// sit: between replicate and timetick.
const (
	inTxnKey         = "test-in-txn"
	inTxnUpgrade     = "upgrade"      // turn an autocommit insert into a transaction
	inTxnCommitExtra = "commit-extra" // append one extra body before a client CommitTxn

	inTxnCollectionID = int64(1)
	inTxnPartitionID  = int64(2)
	inTxnPKFieldID    = int64(100)
)

// inTxnObserved records what the test interceptor saw.
type inTxnObserved struct {
	mu           sync.Mutex
	attempts     int
	attemptErrs  []error
	segmentID    int64
	mainTimeTick uint64
	beginTxnIDs  []message.TxnID
	txnManager   *txn.TxnManager
}

func (o *inTxnObserved) recordAttempt() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attempts++
}

func (o *inTxnObserved) recordBeginTxnID(id message.TxnID) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.beginTxnIDs = append(o.beginTxnIDs, id)
}

func (o *inTxnObserved) snapshotBeginTxnIDs() []message.TxnID {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]message.TxnID(nil), o.beginTxnIDs...)
}

func (o *inTxnObserved) recordAttemptErr(err error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attemptErrs = append(o.attemptErrs, err)
}

func (o *inTxnObserved) recordMain(main message.ImmutableMessage) {
	o.mu.Lock()
	defer o.mu.Unlock()
	header := message.MustAsImmutableInsertMessageV1(main).Header()
	o.segmentID = header.GetPartitions()[0].GetSegmentAssignment().GetSegmentId()
	o.mainTimeTick = main.TimeTick()
}

func (o *inTxnObserved) snapshot() (attempts int, attemptErrs []error, segmentID int64, mainTimeTick uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.attempts, append([]error(nil), o.attemptErrs...), o.segmentID, o.mainTimeTick
}

type inTxnBuilder struct{ obs *inTxnObserved }

func (b *inTxnBuilder) Build(param *interceptors.InterceptorBuildParam) interceptors.Interceptor {
	b.obs.txnManager = param.TxnManager
	return &inTxnInterceptor{obs: b.obs}
}

type inTxnInterceptor struct{ obs *inTxnObserved }

func (i *inTxnInterceptor) Name() string { return "append-in-txn-contract" }

func (i *inTxnInterceptor) Close() {}

// recordBeginTxn wraps appendOp to record the TxnID of every transaction that
// AppendInTxn starts.
func (i *inTxnInterceptor) recordBeginTxn(appendOp interceptors.Append) interceptors.Append {
	return func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		msgID, err := appendOp(ctx, msg)
		if msg.MessageType() == message.MessageTypeBeginTxn {
			if txnCtx := msg.TxnContext(); txnCtx != nil {
				i.obs.recordBeginTxnID(txnCtx.TxnID)
			}
		}
		return msgID, err
	}
}

func (i *inTxnInterceptor) DoAppend(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error) {
	mode, _ := msg.Properties().Get(inTxnKey)
	switch {
	case mode == inTxnUpgrade && msg.MessageType() == message.MessageTypeInsert:
		i.obs.recordAttempt()
		result, err := txn.AppendInTxn(ctx, i.recordBeginTxn(appendOp), msg, []message.MutableMessage{newInTxnDelete()})
		if err != nil {
			i.obs.recordAttemptErr(err)
			return nil, err
		}
		i.obs.recordMain(result.Main)
		return result.Commit.MessageID(), nil
	case mode == inTxnCommitExtra && msg.MessageType() == message.MessageTypeCommitTxn:
		result, err := txn.AppendInTxn(ctx, appendOp, msg, []message.MutableMessage{newInTxnDelete()})
		if err != nil {
			return nil, err
		}
		return result.Commit.MessageID(), nil
	}
	return appendOp(ctx, msg)
}

func newInTxnInsert(props map[string]string) message.MutableMessage {
	return message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{
			CollectionId: inTxnCollectionID,
			Partitions:   []*message.PartitionSegmentAssignment{{PartitionId: inTxnPartitionID, Rows: 1, BinarySize: 100}},
		}).
		WithBody(&msgpb.InsertRequest{
			Base:    &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
			NumRows: 1,
			FieldsData: []*schemapb.FieldData{{
				Type: schemapb.DataType_Int64, FieldName: "pk", FieldId: inTxnPKFieldID,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1}}}}},
			}},
		}).
		WithVChannel(testVChannel).
		WithProperties(props).
		MustBuildMutable()
}

// newInTxnDelete builds the derived message. It is autocommit, AppendInTxn
// attaches the txn context to it.
func newInTxnDelete() message.MutableMessage {
	return message.NewDeleteMessageBuilderV1().
		WithHeader(&message.DeleteMessageHeader{CollectionId: inTxnCollectionID, Rows: 1}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: inTxnCollectionID,
			PartitionID:  inTxnPartitionID,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{1}}}},
			NumRows:      1,
			Timestamps:   []uint64{0},
		}).
		WithVChannel(testVChannel).
		MustBuildMutable()
}

// openChainWAL opens a WAL whose interceptor chain is the production one, with
// extra between the replicate and the timetick interceptor. Every interceptor is
// built while this function runs, so a caller that drives a builder by a config
// value must set it beforehand.
func openChainWAL(t *testing.T, name string, extra interceptors.InterceptorBuilder) wal.WAL {
	walimplstest.Reset()
	initResourceForTest(t)
	b := registry.MustGetBuilder(message.WALNameTest,
		idempotency.NewInterceptorBuilder(),
		redo.NewInterceptorBuilder(),
		lock.NewInterceptorBuilder(),
		replicate.NewInterceptorBuilder(),
		extra,
		timetick.NewInterceptorBuilder(),
		shard.NewInterceptorBuilder(),
		partialupdate.NewInterceptorBuilder(),
	)
	message.RegisterDefaultWALName(message.WALNameTest)
	o, err := b.Build()
	require.NoError(t, err)
	t.Cleanup(o.Close)
	w, err := o.Open(context.Background(), &wal.OpenOption{
		Channel:        types.PChannelInfo{Name: name, Term: 1},
		DisableFlusher: true,
	})
	require.NoError(t, err)
	t.Cleanup(w.Close)
	return w
}

// openInTxnWAL opens a WAL whose interceptor chain is the production one plus
// the test interceptor between replicate and timetick, and creates the
// collection the contract tests write to.
func openInTxnWAL(t *testing.T, name string, obs *inTxnObserved) wal.WAL {
	w := openChainWAL(t, name, &inTxnBuilder{obs: obs})

	create := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: inTxnCollectionID, PartitionIds: []int64{inTxnPartitionID}}).
		WithBody(&msgpb.CreateCollectionRequest{CollectionSchema: &schemapb.CollectionSchema{
			Name:   "c",
			Fields: []*schemapb.FieldSchema{{FieldID: inTxnPKFieldID, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_Int64}},
		}}).
		WithVChannel(testVChannel).
		MustBuildMutable()
	_, err := w.Append(context.Background(), create)
	require.NoError(t, err)
	return w
}

// readInTxnMessages reads the transaction messages of w from the beginning. It
// waits up to 20 seconds for the first one, then keeps reading for quiet, so a
// second transaction that must not exist is still observed.
func readInTxnMessages(t *testing.T, w wal.WAL, quiet time.Duration) []message.ImmutableTxnMessage {
	s, err := w.Read(context.Background(), wal.ReadOption{
		VChannel:      testVChannel,
		DeliverPolicy: options.DeliverPolicyAll(),
	})
	require.NoError(t, err)
	defer s.Close()

	msgs := make([]message.ImmutableTxnMessage, 0, 1)
	deadline := time.After(20 * time.Second)
	for {
		select {
		case m := <-s.Chan():
			require.NotNil(t, m, "scanner closed before a txn message was observed")
			if m.MessageType() == message.MessageTypeTxn {
				msgs = append(msgs, message.AsImmutableTxnMessage(m))
				if len(msgs) == 1 {
					deadline = time.After(quiet)
				}
			}
		case <-deadline:
			require.NotEmpty(t, msgs, "no txn message observed")
			return msgs
		}
	}
}

// TestAppendInTxnContractUpgrade checks that an autocommit insert upgraded by an
// interceptor becomes one transaction, and that the client observes the commit.
func TestAppendInTxnContractUpgrade(t *testing.T) {
	obs := &inTxnObserved{}
	w := openInTxnWAL(t, "append-in-txn-upgrade", obs)

	insert := newInTxnInsert(map[string]string{inTxnKey: inTxnUpgrade})
	r, err := w.Append(context.Background(), insert)
	require.NoError(t, err)
	require.Nil(t, insert.TxnContext(), "the caller's message must not be modified")
	require.Nil(t, r.TxnCtx, "the client did not ask for a transaction, so it must not see one")

	_, _, segmentID, mainTimeTick := obs.snapshot()
	require.NotZero(t, segmentID, "the segment assignment must be readable from the appended main message")
	require.Greater(t, r.TimeTick, mainTimeTick, "the client observes the commit time tick, not the insert one")

	txnMsgs := readInTxnMessages(t, w, time.Second)
	require.Len(t, txnMsgs, 1)
	txnMsg := txnMsgs[0]
	require.Equal(t, r.TimeTick, txnMsg.TimeTick())

	var msgTypes []message.MessageType
	require.NoError(t, txnMsg.RangeOver(func(im message.ImmutableMessage) error {
		msgTypes = append(msgTypes, im.MessageType())
		require.Equal(t, r.TimeTick, im.TimeTick(), "every body of the assembled transaction carries the commit time tick")
		if im.MessageType() == message.MessageTypeInsert {
			header := message.MustAsImmutableInsertMessageV1(im).Header()
			require.Equal(t, segmentID, header.GetPartitions()[0].GetSegmentAssignment().GetSegmentId())
		}
		return nil
	}))
	require.Equal(t, []message.MessageType{message.MessageTypeInsert, message.MessageTypeDelete}, msgTypes)
}

// TestAppendInTxnContractRedo checks that a redo of an upgraded insert leaves
// exactly one committed transaction behind.
func TestAppendInTxnContractRedo(t *testing.T) {
	obs := &inTxnObserved{}
	w := openInTxnWAL(t, "append-in-txn-redo", obs)

	// the first insert after a collection is created rotates the segment, so the
	// shard interceptor asks for a redo.
	insert := newInTxnInsert(map[string]string{inTxnKey: inTxnUpgrade})
	_, err := w.Append(context.Background(), insert)
	require.NoError(t, err)

	attempts, attemptErrs, _, _ := obs.snapshot()
	require.GreaterOrEqual(t, attempts, 2)
	require.NotEmpty(t, attemptErrs)
	require.True(t, errors.Is(attemptErrs[0], redo.ErrRedo), "the first attempt fails with ErrRedo, got %v", attemptErrs[0])
	require.Nil(t, insert.TxnContext(), "the caller's message must not be modified")

	// every transaction but the committed one must have left the in flight state,
	// otherwise it pins the last confirmed message id until its keepalive expires.
	beginTxnIDs := obs.snapshotBeginTxnIDs()
	require.GreaterOrEqual(t, len(beginTxnIDs), 2)
	require.NotNil(t, obs.txnManager)
	for _, txnID := range beginTxnIDs[:len(beginTxnIDs)-1] {
		session, err := obs.txnManager.GetSessionOfTxn(txnID)
		if err != nil {
			continue // the session is already cleaned up.
		}
		require.Equal(t, message.TxnStateRollbacked, session.State(), "txn %d was not rolled back", txnID)
	}

	require.Len(t, readInTxnMessages(t, w, time.Second), 1, "the rolled back transaction must not reach the consumer")
}

// TestAppendInTxnContractCommitExtra checks that an interceptor can append one
// more body to a client transaction when its CommitTxn passes by.
func TestAppendInTxnContractCommitExtra(t *testing.T) {
	obs := &inTxnObserved{}
	w := openInTxnWAL(t, "append-in-txn-commit-extra", obs)
	ctx := context.Background()

	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel(testVChannel).
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 5000}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable()
	br, err := w.Append(ctx, begin)
	require.NoError(t, err)
	_, err = w.Append(ctx, newInTxnInsert(nil).WithTxnContext(*br.TxnCtx))
	require.NoError(t, err)

	commit := message.NewCommitTxnMessageBuilderV2().
		WithVChannel(testVChannel).
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		WithProperties(map[string]string{inTxnKey: inTxnCommitExtra}).
		MustBuildMutable().
		WithTxnContext(*br.TxnCtx)
	cr, err := w.Append(ctx, commit)
	require.NoError(t, err)

	txnMsgs := readInTxnMessages(t, w, time.Second)
	require.Len(t, txnMsgs, 1)
	require.Equal(t, cr.TimeTick, txnMsgs[0].TimeTick())
	require.Equal(t, 2, txnMsgs[0].Size())
}

// TestAppendInTxnContractConcurrentWithExclusive checks that upgraded inserts and
// an exclusive message on the same vchannel never deadlock each other.
func TestAppendInTxnContractConcurrentWithExclusive(t *testing.T) {
	obs := &inTxnObserved{}
	w := openInTxnWAL(t, "append-in-txn-concurrent", obs)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const writers = 8
	const perWriter = 50
	done := make(chan struct{})

	// the worker goroutines only collect their errors, the test goroutine asserts them.
	insertErrs := make(chan error, writers*perWriter)
	flushErrs := make(chan error, 1024)

	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				if _, err := w.Append(ctx, newInTxnInsert(map[string]string{inTxnKey: inTxnUpgrade})); err != nil {
					insertErrs <- err
				}
			}
		}()
	}

	var flusher sync.WaitGroup
	flusher.Add(1)
	go func() {
		defer flusher.Done()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				flush := message.NewManualFlushMessageBuilderV2().
					WithVChannel(testVChannel).
					WithHeader(&message.ManualFlushMessageHeader{CollectionId: inTxnCollectionID}).
					WithBody(&message.ManualFlushMessageBody{}).
					MustBuildMutable()
				if _, err := w.Append(ctx, flush); err != nil {
					select {
					case flushErrs <- err:
					default:
					}
				}
			}
		}
	}()

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-ctx.Done():
		t.Fatal("the concurrent appends did not finish within 60 seconds")
	}
	close(done)
	flusher.Wait()

	close(insertErrs)
	close(flushErrs)
	for err := range insertErrs {
		require.NoError(t, err, "an insert must not fail while an exclusive message is appended concurrently")
	}
	for err := range flushErrs {
		require.NoError(t, err, "a manual flush must not fail while inserts are appended concurrently")
	}
}
