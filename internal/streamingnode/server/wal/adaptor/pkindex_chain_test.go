package adaptor_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The chain tests drive the primary key index interceptor through the production
// interceptor chain, in the order of walmanager.newInterceptorBuilders.
//
// The switch of the interceptor is read when the WAL opens, and it is a process
// wide config, so these tests must not run in parallel with each other.
//
// The index engine is the in-memory one that registers itself in an init() under
// the "test" build tag. This package reaches it through the pkindex interceptor.
// The index starts empty on every WAL open, so no test may assume that an entry
// survives a reopen.
const (
	pkChainCollectionID = int64(11)
	pkChainPartitionID  = int64(12)
	pkChainPKFieldID    = int64(100)
	// pkChainLastProperty marks the message that ends a read.
	pkChainLastProperty = "pkindex-chain-last"
)

// openPKIndexWAL turns the index on, opens a WAL whose chain contains the index
// interceptor and creates the collection the chain tests write to.
func openPKIndexWAL(t *testing.T, name string) wal.WAL {
	setPKIndexEnabled(t, "true")
	w := openChainWAL(t, name, pkindex.NewInterceptorBuilder())
	appendPKChainCreateCollection(t, w, pkChainCollectionID, pkChainPartitionID)
	return w
}

// setPKIndexEnabled sets the switch of the index and restores it after the test.
// It must be called before the WAL opens, the builder reads the switch once.
func setPKIndexEnabled(t *testing.T, value string) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.StreamingCfg.PKIndexEnabled.Key, value)
	t.Cleanup(func() { params.Reset(params.StreamingCfg.PKIndexEnabled.Key) })
}

// appendPKChainCreateCollection creates a collection with one user provided int64
// primary key, which is what the index interceptor asks for.
func appendPKChainCreateCollection(t *testing.T, w wal.WAL, collectionID int64, partitionID int64) {
	t.Helper()
	create := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: collectionID,
			PartitionIds: []int64{partitionID},
		}).
		WithBody(&msgpb.CreateCollectionRequest{CollectionSchema: &schemapb.CollectionSchema{
			Name: "pkindex_chain",
			Fields: []*schemapb.FieldSchema{{
				FieldID:      pkChainPKFieldID,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			}},
		}}).
		WithVChannel(testVChannel).
		MustBuildMutable()
	_, err := w.Append(context.Background(), create)
	require.NoError(t, err)
}

func appendPKChainDropCollection(t *testing.T, w wal.WAL, collectionID int64) {
	t.Helper()
	drop := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{CollectionId: collectionID}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithVChannel(testVChannel).
		MustBuildMutable()
	_, err := w.Append(context.Background(), drop)
	require.NoError(t, err)
}

func newPKChainInsert(pks []int64, props map[string]string) message.MutableMessage {
	return newPKChainInsertIn(pkChainCollectionID, pkChainPartitionID, pks, props)
}

func newPKChainInsertIn(collectionID int64, partitionID int64, pks []int64, props map[string]string) message.MutableMessage {
	return message.NewInsertMessageBuilderV1().
		WithHeader(&message.InsertMessageHeader{
			CollectionId: collectionID,
			Partitions: []*message.PartitionSegmentAssignment{{
				PartitionId: partitionID,
				Rows:        uint64(len(pks)),
				BinarySize:  uint64(8 * len(pks)),
			}},
		}).
		WithBody(&msgpb.InsertRequest{
			Base:    &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
			NumRows: uint64(len(pks)),
			FieldsData: []*schemapb.FieldData{{
				Type: schemapb.DataType_Int64, FieldName: "pk", FieldId: pkChainPKFieldID,
				Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
				}},
			}},
		}).
		WithVChannel(testVChannel).
		WithProperties(props).
		MustBuildMutable()
}

func newPKChainDelete(pks []int64) message.MutableMessage {
	return message.NewDeleteMessageBuilderV1().
		WithHeader(&message.DeleteMessageHeader{
			CollectionId: pkChainCollectionID,
			Rows:         uint64(len(pks)),
		}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: pkChainCollectionID,
			PartitionID:  pkChainPartitionID,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}},
			NumRows:      int64(len(pks)),
			Timestamps:   make([]uint64, len(pks)),
		}).
		WithVChannel(testVChannel).
		MustBuildMutable()
}

func pkChainLastMarker() map[string]string {
	return map[string]string{pkChainLastProperty: "true"}
}

// readPKChainMessages reads the data messages of w from the beginning, in arrival
// order, up to and including the one that carries the last marker.
func readPKChainMessages(t *testing.T, w wal.WAL) []message.ImmutableMessage {
	t.Helper()
	s, err := w.Read(context.Background(), wal.ReadOption{
		VChannel:      testVChannel,
		DeliverPolicy: options.DeliverPolicyAll(),
	})
	require.NoError(t, err)
	defer s.Close()

	msgs := make([]message.ImmutableMessage, 0, 8)
	deadline := time.After(60 * time.Second)
	for {
		select {
		case m := <-s.Chan():
			require.NotNil(t, m, "the scanner closed before the last message was read")
			switch m.MessageType() {
			case message.MessageTypeInsert, message.MessageTypeDelete, message.MessageTypeTxn:
				msgs = append(msgs, m)
			default:
				continue
			}
			if isPKChainLastMessage(m) {
				return msgs
			}
		case <-deadline:
			t.Fatalf("the last message was not read within 60 seconds, read %d messages", len(msgs))
			return nil
		}
	}
}

func isPKChainLastMessage(m message.ImmutableMessage) bool {
	if value, ok := m.Properties().Get(pkChainLastProperty); ok && value == "true" {
		return true
	}
	if m.MessageType() != message.MessageTypeTxn {
		return false
	}
	last := false
	_ = message.AsImmutableTxnMessage(m).RangeOver(func(body message.ImmutableMessage) error {
		if value, ok := body.Properties().Get(pkChainLastProperty); ok && value == "true" {
			last = true
		}
		return nil
	})
	return last
}

func pkChainTypesOf(msgs []message.ImmutableMessage) []message.MessageType {
	types := make([]message.MessageType, 0, len(msgs))
	for _, m := range msgs {
		types = append(types, m.MessageType())
	}
	return types
}

// pkChainTxnBodies returns the bodies of an assembled transaction message.
func pkChainTxnBodies(t *testing.T, m message.ImmutableMessage) []message.ImmutableMessage {
	t.Helper()
	require.Equal(t, message.MessageTypeTxn, m.MessageType())
	txnMsg := message.AsImmutableTxnMessage(m)
	bodies := make([]message.ImmutableMessage, 0, txnMsg.Size())
	require.NoError(t, txnMsg.RangeOver(func(body message.ImmutableMessage) error {
		bodies = append(bodies, body)
		return nil
	}))
	require.Len(t, bodies, txnMsg.Size())
	return bodies
}

// pkChainDeleteKeys returns the primary keys carried by a delete message.
func pkChainDeleteKeys(t *testing.T, m message.ImmutableMessage) []int64 {
	t.Helper()
	body, err := message.MustAsImmutableDeleteMessageV1(m).Body(context.Background())
	require.NoError(t, err)
	return body.GetPrimaryKeys().GetIntId().GetData()
}

// requireCompanionDeleteTxn checks that m is the transaction of an insert that hit
// existing keys: the insert, the companion delete of keys, and every body at the
// time tick of the commit.
func requireCompanionDeleteTxn(t *testing.T, m message.ImmutableMessage, keys []int64) {
	t.Helper()
	txnMsg := message.AsImmutableTxnMessage(m)
	require.Equal(t, 2, txnMsg.Size())
	bodies := pkChainTxnBodies(t, m)
	require.Equal(t, []message.MessageType{message.MessageTypeInsert, message.MessageTypeDelete}, pkChainTypesOf(bodies))
	for _, body := range bodies {
		require.Equal(t, m.TimeTick(), body.TimeTick(), "every body of the transaction carries the commit time tick")
	}
	require.Equal(t, keys, pkChainDeleteKeys(t, bodies[1]))
	require.Equal(t, uint64(len(keys)), message.MustAsImmutableDeleteMessageV1(bodies[1]).Header().GetRows())
}

func requireAscendingTimeTicks(t *testing.T, msgs []message.ImmutableMessage) {
	t.Helper()
	for i := 1; i < len(msgs); i++ {
		require.Greater(t, msgs[i].TimeTick(), msgs[i-1].TimeTick(), "the consumer reads message %d out of time tick order", i)
	}
}

// TestPKIndexChainDuplicateInsert checks that the second insert of a key becomes a
// transaction that also deletes the old row, and that the client observes the
// commit time tick of it.
func TestPKIndexChainDuplicateInsert(t *testing.T) {
	w := openPKIndexWAL(t, "pkindex-duplicate-insert")
	ctx := context.Background()

	first, err := w.Append(ctx, newPKChainInsert([]int64{1}, nil))
	require.NoError(t, err)
	second, err := w.Append(ctx, newPKChainInsert([]int64{1}, nil))
	require.NoError(t, err)
	third, err := w.Append(ctx, newPKChainInsert([]int64{2}, pkChainLastMarker()))
	require.NoError(t, err)

	msgs := readPKChainMessages(t, w)
	require.Equal(t, []message.MessageType{
		message.MessageTypeInsert, message.MessageTypeTxn, message.MessageTypeInsert,
	}, pkChainTypesOf(msgs))
	requireAscendingTimeTicks(t, msgs)

	require.Equal(t, first.TimeTick, msgs[0].TimeTick(), "the first insert misses the index and stays autocommit")
	require.Equal(t, second.TimeTick, msgs[1].TimeTick(), "the client observes the commit time tick of the transaction")
	require.Equal(t, third.TimeTick, msgs[2].TimeTick(), "an insert of another key misses the index")
	requireCompanionDeleteTxn(t, msgs[1], []int64{1})
}

// TestPKIndexChainClientTxn checks that the companion delete of a write inside a
// client transaction is appended as a body of that transaction.
func TestPKIndexChainClientTxn(t *testing.T) {
	w := openPKIndexWAL(t, "pkindex-client-txn")
	ctx := context.Background()

	_, err := w.Append(ctx, newPKChainInsert([]int64{5}, nil))
	require.NoError(t, err)

	begin, err := w.Append(ctx, newPKChainBeginTxn())
	require.NoError(t, err)
	_, err = w.Append(ctx, newPKChainInsert([]int64{5}, nil).WithTxnContext(*begin.TxnCtx))
	require.NoError(t, err)
	commit, err := w.Append(ctx, newPKChainCommitTxn(*begin.TxnCtx))
	require.NoError(t, err)

	_, err = w.Append(ctx, newPKChainInsert([]int64{7}, pkChainLastMarker()))
	require.NoError(t, err)

	msgs := readPKChainMessages(t, w)
	require.Equal(t, []message.MessageType{
		message.MessageTypeInsert, message.MessageTypeTxn, message.MessageTypeInsert,
	}, pkChainTypesOf(msgs))
	requireAscendingTimeTicks(t, msgs)

	require.Equal(t, commit.TimeTick, msgs[1].TimeTick())
	requireCompanionDeleteTxn(t, msgs[1], []int64{5})
}

// TestPKIndexChainUpsertLikeTxn checks that a transaction that deletes a key and
// inserts it again gets no companion delete of its own.
func TestPKIndexChainUpsertLikeTxn(t *testing.T) {
	w := openPKIndexWAL(t, "pkindex-upsert-like-txn")
	ctx := context.Background()

	_, err := w.Append(ctx, newPKChainInsert([]int64{6}, nil))
	require.NoError(t, err)

	begin, err := w.Append(ctx, newPKChainBeginTxn())
	require.NoError(t, err)
	_, err = w.Append(ctx, newPKChainDelete([]int64{6}).WithTxnContext(*begin.TxnCtx))
	require.NoError(t, err)
	_, err = w.Append(ctx, newPKChainInsert([]int64{6}, nil).WithTxnContext(*begin.TxnCtx))
	require.NoError(t, err)
	commit, err := w.Append(ctx, newPKChainCommitTxn(*begin.TxnCtx))
	require.NoError(t, err)

	_, err = w.Append(ctx, newPKChainInsert([]int64{7}, pkChainLastMarker()))
	require.NoError(t, err)

	msgs := readPKChainMessages(t, w)
	require.Equal(t, []message.MessageType{
		message.MessageTypeInsert, message.MessageTypeTxn, message.MessageTypeInsert,
	}, pkChainTypesOf(msgs))
	requireAscendingTimeTicks(t, msgs)

	require.Equal(t, commit.TimeTick, msgs[1].TimeTick())
	bodies := pkChainTxnBodies(t, msgs[1])
	require.Equal(t, 2, message.AsImmutableTxnMessage(msgs[1]).Size(), "the transaction keeps the two bodies of the client")
	require.Equal(t, []message.MessageType{message.MessageTypeDelete, message.MessageTypeInsert}, pkChainTypesOf(bodies))
	for _, body := range bodies {
		require.Equal(t, commit.TimeTick, body.TimeTick())
	}
}

// TestPKIndexChainDropCollection checks that the index of a vchannel is dropped
// with its collection, so a key of the dropped collection is not a duplicate for
// the next one.
func TestPKIndexChainDropCollection(t *testing.T) {
	w := openPKIndexWAL(t, "pkindex-drop-collection")
	ctx := context.Background()

	_, err := w.Append(ctx, newPKChainInsert([]int64{1}, nil))
	require.NoError(t, err)

	// a drop collection conflicts with an in flight transaction, the same wait as
	// in wal_test.go.
	time.Sleep(2 * time.Second)
	appendPKChainDropCollection(t, w, pkChainCollectionID)

	const nextCollectionID = int64(21)
	const nextPartitionID = int64(22)
	appendPKChainCreateCollection(t, w, nextCollectionID, nextPartitionID)
	last, err := w.Append(ctx, newPKChainInsertIn(nextCollectionID, nextPartitionID, []int64{1}, pkChainLastMarker()))
	require.NoError(t, err)

	msgs := readPKChainMessages(t, w)
	require.Equal(t, []message.MessageType{
		message.MessageTypeInsert, message.MessageTypeInsert,
	}, pkChainTypesOf(msgs))
	requireAscendingTimeTicks(t, msgs)
	require.Equal(t, last.TimeTick, msgs[1].TimeTick(), "the key of the dropped collection is not in the index of the new one")
}

// TestPKIndexChainConcurrentSameKey checks that the decisions of one key are
// serialized from the probe to the index write: of all the concurrent inserts of
// one key, exactly one finds the index empty.
func TestPKIndexChainConcurrentSameKey(t *testing.T) {
	w := openPKIndexWAL(t, "pkindex-concurrent-same-key")

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	const writers = 16
	const perWriter = 20
	const total = writers * perWriter

	// the worker goroutines only collect their errors, the test goroutine asserts them.
	errs := make(chan error, total)
	var wg sync.WaitGroup
	wg.Add(writers)
	for i := 0; i < writers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				if _, err := w.Append(ctx, newPKChainInsert([]int64{1}, nil)); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err, "every concurrent insert of the same key must succeed")
	}

	_, err := w.Append(ctx, newPKChainInsert([]int64{2}, pkChainLastMarker()))
	require.NoError(t, err)

	msgs := readPKChainMessages(t, w)
	require.Len(t, msgs, total+1)
	requireAscendingTimeTicks(t, msgs)
	require.Equal(t, message.MessageTypeInsert, msgs[total].MessageType(), "the last insert is of another key")

	withCompanion := 0
	withoutCompanion := 0
	for i, m := range msgs[:total] {
		switch m.MessageType() {
		case message.MessageTypeInsert:
			withoutCompanion++
		case message.MessageTypeTxn:
			requireCompanionDeleteTxn(t, m, []int64{1})
			withCompanion++
		default:
			t.Fatalf("message %d has unexpected type %s", i, m.MessageType())
		}
	}
	require.Equal(t, 1, withoutCompanion, "exactly one insert may find the key absent")
	require.Equal(t, total-1, withCompanion)
}

// TestPKIndexChainDisabled checks that the interceptor changes nothing while its
// switch is off.
// TestPKIndexChainDeleteReachesTheWALUnchanged checks that the index never narrows
// a delete: a key that the index does not know may still exist in the data.
func TestPKIndexChainDeleteReachesTheWALUnchanged(t *testing.T) {
	w := openPKIndexWAL(t, "pkindex-delete-unchanged")
	ctx := context.Background()

	_, err := w.Append(ctx, newPKChainInsert([]int64{1}, nil))
	require.NoError(t, err)
	// key 1 is in the index, key 9 is not.
	_, err = w.Append(ctx, newPKChainDelete([]int64{1, 9}))
	require.NoError(t, err)
	// none of the keys is in the index.
	_, err = w.Append(ctx, newPKChainDelete([]int64{8, 9}))
	require.NoError(t, err)
	// key 1 was removed from the index by the delete, so this insert misses.
	_, err = w.Append(ctx, newPKChainInsert([]int64{1}, pkChainLastMarker()))
	require.NoError(t, err)

	msgs := readPKChainMessages(t, w)
	require.Equal(t, []message.MessageType{
		message.MessageTypeInsert, message.MessageTypeDelete, message.MessageTypeDelete, message.MessageTypeInsert,
	}, pkChainTypesOf(msgs))
	requireAscendingTimeTicks(t, msgs)
	require.Equal(t, []int64{1, 9}, pkChainDeleteKeys(t, msgs[1]))
	require.Equal(t, uint64(2), message.MustAsImmutableDeleteMessageV1(msgs[1]).Header().GetRows())
	require.Equal(t, []int64{8, 9}, pkChainDeleteKeys(t, msgs[2]))
	require.Equal(t, uint64(2), message.MustAsImmutableDeleteMessageV1(msgs[2]).Header().GetRows())
}

func TestPKIndexChainDisabled(t *testing.T) {
	setPKIndexEnabled(t, "false")
	w := openChainWAL(t, "pkindex-disabled", pkindex.NewInterceptorBuilder())
	appendPKChainCreateCollection(t, w, pkChainCollectionID, pkChainPartitionID)
	ctx := context.Background()

	first, err := w.Append(ctx, newPKChainInsert([]int64{1}, nil))
	require.NoError(t, err)
	second, err := w.Append(ctx, newPKChainInsert([]int64{1}, pkChainLastMarker()))
	require.NoError(t, err)

	msgs := readPKChainMessages(t, w)
	require.Equal(t, []message.MessageType{
		message.MessageTypeInsert, message.MessageTypeInsert,
	}, pkChainTypesOf(msgs))
	require.Equal(t, first.TimeTick, msgs[0].TimeTick())
	require.Equal(t, second.TimeTick, msgs[1].TimeTick())
}

func newPKChainBeginTxn() message.MutableMessage {
	return message.NewBeginTxnMessageBuilderV2().
		WithVChannel(testVChannel).
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 5000}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable()
}

func newPKChainCommitTxn(txnCtx message.TxnContext) message.MutableMessage {
	return message.NewCommitTxnMessageBuilderV2().
		WithVChannel(testVChannel).
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx)
}
