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

package pkindex

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	snpkindex "github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const (
	testVChannel     = "by-dev-rootcoord-dml_0_1v0"
	testCollectionID = int64(1)
	testPKFieldID    = int64(100)
	testPartitionID  = int64(2)
	testPChannel     = "by-dev-rootcoord-dml_0"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	// The cipher is registered once per process. No other test of the package
	// builds an encrypted message.
	message.RegisterCipher(testCipher{})
	m.Run()
}

// testCipher marks a payload as encrypted with a prefix.
type testCipher struct{}

const testCipherPrefix = "enc:"

func (testCipher) Init(map[string]string) error { return nil }

func (testCipher) GetEncryptor(int64, int64) (hook.Encryptor, []byte, error) {
	return testCipher{}, []byte("key"), nil
}

func (testCipher) GetDecryptor(int64, int64, []byte) (hook.Decryptor, error) {
	return testCipher{}, nil
}

func (testCipher) GetUnsafeKey(int64, int64) []byte { return nil }

func (testCipher) Encrypt(plain []byte) ([]byte, error) {
	return append([]byte(testCipherPrefix), plain...), nil
}

func (testCipher) Decrypt(cipherText []byte) ([]byte, error) {
	return bytes.TrimPrefix(cipherText, []byte(testCipherPrefix)), nil
}

// ===== test doubles =====

// testEngine is a memory engine that a test can read back and that counts probes.
type testIndexEnv struct {
	mgr       *snpkindex.Manager
	committer *snpkindex.FakeCommitter
	root      string

	mu     sync.Mutex
	fatals []error
}

func newTestIndexEnv(t *testing.T) *testIndexEnv {
	return newTestIndexEnvAt(t, t.TempDir())
}

// newTestIndexEnvAt creates the env with root as the local root directory.
func newTestIndexEnvAt(t *testing.T, root string) *testIndexEnv {
	env := snpkindex.NewFakeEnv(t, root)
	return &testIndexEnv{mgr: env.Manager, committer: env.Committer, root: root}
}

// openChannel opens the index Channel of the test pchannel.
func (env *testIndexEnv) openChannel(seed uint64) *snpkindex.PChannelIndexManager {
	ch := env.mgr.New(snpkindex.PChannelConfig{PChannel: types.PChannelInfo{Name: testPChannel, Term: 1}, Seed: seed, OnFatal: env.onFatal})
	ch.Start()
	return ch
}

func (env *testIndexEnv) onFatal(err error) {
	env.mu.Lock()
	defer env.mu.Unlock()
	env.fatals = append(env.fatals, err)
}

func (env *testIndexEnv) fatalList() []error {
	env.mu.Lock()
	defer env.mu.Unlock()
	return append([]error(nil), env.fatals...)
}

// testEngine reads and seeds the engine of the Index of one vchannel.
type testEngine struct {
	idx *index.Index
}

// put seeds the index of one primary key, as an earlier insert would have done.
func (e *testEngine) put(t *testing.T, pk int64, segmentID int64) {
	t.Helper()
	e.putPK(t, dedup.Int64PK(pk), segmentID)
}

func (e *testEngine) putPK(t *testing.T, pk dedup.PK, segmentID int64) {
	t.Helper()
	value := codec.EncodePKEntry(codec.PKEntry{SegmentID: segmentID})
	require.NoError(t, e.idx.EngineForTest().Write(context.Background(), []engine.Mutation{
		{Key: pk.Encode(), Value: value},
	}))
}

// segmentOf returns the segment of a primary key and whether it is in the index.
func (e *testEngine) segmentOf(t *testing.T, pk int64) (int64, bool) {
	t.Helper()
	return e.segmentOfPK(t, dedup.Int64PK(pk))
}

func (e *testEngine) segmentOfPK(t *testing.T, pk dedup.PK) (int64, bool) {
	t.Helper()
	values, err := e.idx.EngineForTest().MultiGet(context.Background(), [][]byte{pk.Encode()})
	require.NoError(t, err)
	if values[0] == nil {
		return 0, false
	}
	entry, err := codec.DecodePKEntry(values[0])
	require.NoError(t, err)
	return entry.SegmentID, true
}

// testEngines finds the Index of a vchannel in the Channel of an interceptor.
type testEngines struct {
	ch *snpkindex.PChannelIndexManager
}

func (e *testEngines) of(t *testing.T, vchannel string) *testEngine {
	t.Helper()
	idx := e.ch.IndexForTest(vchannel)
	require.NotNil(t, idx, "no index was created for %s", vchannel)
	return &testEngine{idx: idx}
}

// fakeAppender imitates the chain below the interceptor: it assigns a time tick,
// a txn context to a BeginTxn and a segment to an insert.
type fakeAppender struct {
	types     []message.MessageType
	appended  []message.MutableMessage
	ids       []message.MessageID
	segmentID int64
	nextTT    uint64
	nextTxnID int64
	failAt    int // fail the n-th call (0-based), -1 never
	failErr   error
	// failAfterTimeTick makes the failed call assign a time tick first, as the
	// timetick interceptor does when the chain below it fails.
	failAfterTimeTick bool
}

func newFakeAppender() *fakeAppender {
	return &fakeAppender{segmentID: 500, nextTT: 100, nextTxnID: 7, failAt: -1}
}

func (f *fakeAppender) append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	idx := len(f.types)
	f.types = append(f.types, msg.MessageType())
	f.appended = append(f.appended, msg)
	if idx == f.failAt && !f.failAfterTimeTick {
		f.ids = append(f.ids, nil)
		return nil, f.failErr
	}
	f.nextTT++
	msg.WithTimeTick(f.nextTT).WithLastConfirmedUseMessageID()
	if idx == f.failAt {
		f.ids = append(f.ids, nil)
		return nil, f.failErr
	}
	if msg.MessageType() == message.MessageTypeBeginTxn {
		msg.WithTxnContext(message.TxnContext{TxnID: message.TxnID(f.nextTxnID), Keepalive: time.Hour})
	}
	if msg.MessageType() == message.MessageTypeInsert {
		insert := message.MustAsMutableInsertMessageV1(msg)
		header := insert.Header()
		header.Partitions[0].SegmentAssignment = &message.SegmentAssignment{SegmentId: f.segmentID}
		insert.OverwriteHeader(header)
	}
	if utility.GetExtraAppendResult(ctx) != nil {
		utility.ReplaceAppendResultTxnContext(ctx, msg.TxnContext())
	}
	id := walimplstest.NewTestMessageID(int64(f.nextTT))
	f.ids = append(f.ids, id)
	return id, nil
}

func (f *fakeAppender) lastID() message.MessageID {
	return f.ids[len(f.ids)-1]
}

// deleteBodyAt returns the body of the n-th appended message, which must be a delete.
func (f *fakeAppender) deleteBodyAt(t *testing.T, idx int) *msgpb.DeleteRequest {
	t.Helper()
	del, err := message.AsMutableDeleteMessageV1(f.appended[idx])
	require.NoError(t, err)
	body, err := del.Body(context.Background())
	require.NoError(t, err)
	return body
}

// ===== message builders =====

func testSchema(autoID bool) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: testPKFieldID, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_Int64, AutoID: autoID},
			{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector},
		},
	}
}

func newInsert(vchannel string, pks ...int64) message.MutableMessage {
	return newInsertInPartition(vchannel, testPartitionID, pks...)
}

func newInsertInPartition(vchannel string, partitionID int64, pks ...int64) message.MutableMessage {
	return newEncryptedInsertInPartition(vchannel, partitionID, nil, pks...)
}

func newEncryptedInsert(vchannel string, cipher *message.CipherConfig, pks ...int64) message.MutableMessage {
	return newEncryptedInsertInPartition(vchannel, testPartitionID, cipher, pks...)
}

// newEncryptedInsertInPartition returns an insert into partitionID encrypted
// with cipher, plain when cipher is nil.
func newEncryptedInsertInPartition(vchannel string, partitionID int64, cipher *message.CipherConfig, pks ...int64) message.MutableMessage {
	return message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: testCollectionID,
			Partitions: []*message.PartitionSegmentAssignment{
				{PartitionId: partitionID, Rows: uint64(len(pks)), BinarySize: 1024},
			},
		}).
		WithBody(&msgpb.InsertRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
			CollectionID: testCollectionID,
			PartitionID:  partitionID,
			NumRows:      uint64(len(pks)),
			FieldsData: []*schemapb.FieldData{
				{
					Type:    schemapb.DataType_Int64,
					FieldId: testPKFieldID,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
					}},
				},
			},
		}).
		WithCipher(cipher).
		MustBuildMutable()
}

func testVarCharSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: testPKFieldID, Name: "id", IsPrimaryKey: true, DataType: schemapb.DataType_VarChar},
			{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector},
		},
	}
}

func newVarCharInsert(vchannel string, pks ...string) message.MutableMessage {
	return message.NewInsertMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{
			CollectionId: testCollectionID,
			Partitions: []*message.PartitionSegmentAssignment{
				{PartitionId: testPartitionID, Rows: uint64(len(pks)), BinarySize: 1024},
			},
		}).
		WithBody(&msgpb.InsertRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
			CollectionID: testCollectionID,
			PartitionID:  testPartitionID,
			NumRows:      uint64(len(pks)),
			FieldsData: []*schemapb.FieldData{
				{
					Type:    schemapb.DataType_VarChar,
					FieldId: testPKFieldID,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: pks}},
					}},
				},
			},
		}).
		MustBuildMutable()
}

func newVarCharDelete(vchannel string, pks ...string) message.MutableMessage {
	return message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DeleteMessageHeader{CollectionId: testCollectionID, Rows: uint64(len(pks))}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: testCollectionID,
			PartitionID:  testPartitionID,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: pks}}},
			NumRows:      int64(len(pks)),
			Timestamps:   make([]uint64, len(pks)),
		}).
		MustBuildMutable()
}

func newDelete(vchannel string, pks ...int64) message.MutableMessage {
	return newDeleteInPartition(vchannel, testPartitionID, pks...)
}

func newDeleteInPartition(vchannel string, partitionID int64, pks ...int64) message.MutableMessage {
	return message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DeleteMessageHeader{CollectionId: testCollectionID, Rows: uint64(len(pks))}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: testCollectionID,
			PartitionID:  partitionID,
			PrimaryKeys:  &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}},
			NumRows:      int64(len(pks)),
			Timestamps:   make([]uint64, len(pks)),
		}).
		MustBuildMutable()
}

func newDeleteByExpression(vchannel string) message.MutableMessage {
	return message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DeleteMessageHeader{CollectionId: testCollectionID, Rows: 0}).
		WithBody(&msgpb.DeleteRequest{
			Base:               &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID:       testCollectionID,
			PartitionID:        testPartitionID,
			SerializedExprPlan: []byte("id > 0"),
			NumRows:            0,
			Timestamps:         []uint64{1},
		}).
		MustBuildMutable()
}

func newCommit(vchannel string, txnCtx message.TxnContext) message.MutableMessage {
	return message.NewCommitTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CommitTxnMessageHeader{}).
		WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx)
}

func newRollback(vchannel string, txnCtx message.TxnContext) message.MutableMessage {
	return message.NewRollbackTxnMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.RollbackTxnMessageHeader{}).
		WithBody(&message.RollbackTxnMessageBody{}).
		MustBuildMutable().
		WithTxnContext(txnCtx)
}

func newCreateCollection(vchannel string, schema *schemapb.CollectionSchema) message.MutableMessage {
	return message.NewCreateCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: testCollectionID,
			PartitionIds: []int64{testPartitionID},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			Base:             &commonpb.MsgBase{MsgType: commonpb.MsgType_CreateCollection},
			CollectionID:     testCollectionID,
			CollectionName:   "collection",
			CollectionSchema: schema,
		}).
		MustBuildMutable()
}

func newDropCollection(vchannel string) message.MutableMessage {
	return message.NewDropCollectionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DropCollectionMessageHeader{CollectionId: testCollectionID}).
		WithBody(&msgpb.DropCollectionRequest{
			Base:           &commonpb.MsgBase{MsgType: commonpb.MsgType_DropCollection},
			CollectionID:   testCollectionID,
			CollectionName: "collection",
		}).
		MustBuildMutable()
}

// ===== harness =====

// newTestInterceptor builds the interceptor directly, with a target registered
// for testVChannel. A real index over a real engine is used.
func newTestInterceptor(t *testing.T, sessions txnSessions) (*appendInterceptor, *testEngines) {
	return newTestInterceptorOfSchema(t, sessions, testSchema(false))
}

func newTestInterceptorOfSchema(t *testing.T, sessions txnSessions, schema *schemapb.CollectionSchema) (*appendInterceptor, *testEngines) {
	i, engines := newEmptyTestInterceptor(t, newTestIndexEnv(t), sessions)
	created, err := i.registry.add(context.Background(), testVChannel, testCollectionID, schema)
	require.NoError(t, err)
	require.True(t, created)
	require.NotNil(t, i.registry.get(testVChannel))
	return i, engines
}

// newEmptyTestInterceptor builds the interceptor directly, with no target.
func newEmptyTestInterceptor(t *testing.T, env *testIndexEnv, sessions txnSessions) (*appendInterceptor, *testEngines) {
	ch := env.openChannel(0)
	i := &appendInterceptor{
		ch:       ch,
		registry: newRegistry(ch),
		sessions: sessions,
		metrics:  newMetrics(),
		onFatal:  env.onFatal,
	}
	t.Cleanup(i.Close)
	return i, &testEngines{ch: ch}
}

// requireDoAppendReturns fails when DoAppend does not return within five
// seconds. A decision whose registration was never filled in or removed blocks
// every later write of the same keys forever, and this is what that looks like
// from the outside.
func requireDoAppendReturns(t *testing.T, i *appendInterceptor, msg message.MutableMessage, appendOp interceptors.Append) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := i.DoAppend(newTestAppendContext(), msg, appendOp)
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the append did not return, an earlier decision still holds its keys")
		return nil
	}
}

// newTestAppendContext returns the context an append chain provides.
func newTestAppendContext() context.Context {
	return utility.WithExtraAppendResult(context.Background(), &utility.ExtraAppendResult{})
}

func requireInner(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INNER, status.AsStreamingError(err).Code)
}

func requireOnShutdown(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN, status.AsStreamingError(err).Code)
}

// presentOf reports whether a key exists for the write path of testVChannel:
// in the in-flight set or in the engine.
func presentOf(t *testing.T, i *appendInterceptor, pk dedup.PK) bool {
	t.Helper()
	present, err := dedup.PresentForTest(context.Background(), i.ch.Decider(testVChannel), pk)
	require.NoError(t, err)
	return present
}

func inflightLen(i *appendInterceptor) int {
	return dedup.InflightLenForTest(i.ch.Decider(testVChannel))
}

// ===== the switch =====

func TestBuildWithoutASnapshotIsPassthrough(t *testing.T) {
	var i interceptors.Interceptor
	require.NotPanics(t, func() {
		i = NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
			ChannelInfo: types.PChannelInfo{Name: testPChannel},
		})
	})
	defer i.Close()
	require.IsType(t, passthroughInterceptor{}, i)

	f := newFakeAppender()
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeInsert}, f.types)
}

// TestBuildWithoutAnIndexInTheSnapshotIsPassthrough checks that the
// interceptor passes the messages through when the recovery snapshot has no
// index: the index is off, or the WAL recovery did not build one.
func TestBuildWithoutAnIndexInTheSnapshotIsPassthrough(t *testing.T) {
	i := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ChannelInfo:            types.PChannelInfo{Name: testPChannel},
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{},
	})
	defer i.Close()
	require.IsType(t, passthroughInterceptor{}, i)
}

func TestBuildUsesTheIndexOfTheSnapshot(t *testing.T) {
	env := newTestIndexEnv(t)
	ch := env.openChannel(0)

	i := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ChannelInfo:            types.PChannelInfo{Name: testPChannel},
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{PKIndex: ch},
	})
	defer i.Close()
	require.IsType(t, &appendInterceptor{}, i)
	require.Same(t, ch, i.(*appendInterceptor).ch)
}

// TestBuildRestoresThePendingWritesOfOpenTransactions checks that the bodies
// of a transaction that was open at the recovery barrier reach the pending
// set of the index, so that its commit is decided over them.
func TestBuildRestoresThePendingWritesOfOpenTransactions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cipher *message.CipherConfig
	}{
		{name: "plain"},
		{name: "encrypted", cipher: &message.CipherConfig{EzID: 1, CollectionID: testCollectionID}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestIndexEnv(t)
			ch := env.openChannel(0)
			txnCtx := message.TxnContext{TxnID: 48, Keepalive: time.Hour}
			beginID := walimplstest.NewTestMessageID(3)
			begin := message.NewBeginTxnMessageBuilderV2().
				WithVChannel(testVChannel).
				WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: time.Hour.Milliseconds()}).
				WithBody(&message.BeginTxnMessageBody{}).
				MustBuildMutable().
				WithTxnContext(txnCtx).
				WithTimeTick(3).
				WithLastConfirmed(beginID).
				IntoImmutableMessage(beginID)
			body := newEncryptedInsert(testVChannel, tc.cipher, 1).
				WithTxnContext(txnCtx).
				WithTimeTick(4).
				WithLastConfirmed(beginID).
				IntoImmutableMessage(walimplstest.NewTestMessageID(4))
			buf := utility.NewTxnBuffer(mlog.With(), metricsutil.NewScanMetrics(types.PChannelInfo{}).NewScannerMetrics())
			buf.HandleImmutableMessages([]message.ImmutableMessage{begin, body}, 4)
			require.Len(t, buf.GetUncommittedMessageBuilder(), 1)

			i := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
				ChannelInfo:            types.PChannelInfo{Name: testPChannel},
				InitialRecoverSnapshot: &recovery.RecoverySnapshot{PKIndex: ch, TxnBuffer: buf},
				OnFatal:                func(err error) { t.Errorf("OnFatal must not be called: %v", err) },
				ShardManager: fakeSchemaLister{infos: map[int64]shards.CollectionSchemaInfo{
					testCollectionID: {VChannel: testVChannel, Schema: testSchema(false)},
				}},
			})
			defer i.Close()
			ai := i.(*appendInterceptor)
			require.Equal(t, []int64{48}, dedup.PendingTxnIDsForTest(ai.ch.Decider(testVChannel)))

			// pk 1 reached the index before the restart, so the commit of the
			// open transaction gets a companion delete for it. The commit is the
			// first message of the vchannel in this WAL lifetime, so the cipher
			// config of the delete comes from the restored body.
			(&testEngine{idx: ai.ch.IndexForTest(testVChannel)}).put(t, 1, 100)
			f := newFakeAppender()
			_, err := ai.DoAppend(newTestAppendContext(), newCommit(testVChannel, txnCtx), f.append)
			require.NoError(t, err)
			require.Equal(t, []message.MessageType{message.MessageTypeDelete, message.MessageTypeCommitTxn}, f.types)
			require.Equal(t, tc.cipher, message.CipherConfigOf(f.appended[0]))
		})
	}
}

// fakeSchemaLister is the part of the shard manager Build reads at WAL open.
type fakeSchemaLister struct {
	shards.ShardManager
	infos map[int64]shards.CollectionSchemaInfo
}

func (l fakeSchemaLister) GetAllCollectionSchemaInfos() map[int64]shards.CollectionSchemaInfo {
	return l.infos
}

func TestBuildCreatesTheIndexOfExistingCollections(t *testing.T) {
	env := newTestIndexEnv(t)
	ch := env.openChannel(0)

	i := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ChannelInfo:            types.PChannelInfo{Name: testPChannel},
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{PKIndex: ch},
		ShardManager: fakeSchemaLister{infos: map[int64]shards.CollectionSchemaInfo{
			testCollectionID:     {VChannel: testVChannel, Schema: testSchema(false)},
			testCollectionID + 1: {VChannel: testVChannel + "_autoid", Schema: testSchema(true)},
		}},
	})
	defer i.Close()

	ai := i.(*appendInterceptor)
	require.NotNil(t, ai.registry.get(testVChannel))
	require.Same(t, ai.ch.IndexForTest(testVChannel).Decider(), ai.ch.Decider(testVChannel))
	require.Nil(t, ai.registry.get(testVChannel+"_autoid"))
	require.Nil(t, ai.ch.IndexForTest(testVChannel+"_autoid"), "an autoID collection gets no index")
}

// newBrokenRoot returns a local root directory under which no engine can be
// opened: the root is a regular file.
func newBrokenRoot(t *testing.T) string {
	root := filepath.Join(t.TempDir(), "root")
	require.NoError(t, os.WriteFile(root, nil, 0o600))
	return root
}

// TestBuildMarksTheWALUnavailableWhenTheIndexCanNotBeOpened checks that a
// local disk error at WAL open goes to param.OnFatal and does not panic.
func TestBuildMarksTheWALUnavailableWhenTheIndexCanNotBeOpened(t *testing.T) {
	env := newTestIndexEnvAt(t, newBrokenRoot(t))
	ch := env.openChannel(0)

	var fatals []error
	var i interceptors.Interceptor
	require.NotPanics(t, func() {
		i = NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
			ChannelInfo:            types.PChannelInfo{Name: testPChannel},
			InitialRecoverSnapshot: &recovery.RecoverySnapshot{PKIndex: ch},
			OnFatal:                func(err error) { fatals = append(fatals, err) },
			ShardManager: fakeSchemaLister{infos: map[int64]shards.CollectionSchemaInfo{
				testCollectionID: {VChannel: testVChannel, Schema: testSchema(false)},
			}},
		})
	})
	defer i.Close()

	require.Len(t, fatals, 1)
	require.True(t, errors.Is(fatals[0], errOpenIndex), "the error is marked as an index open error")
	require.ErrorContains(t, fatals[0], "not a directory")
	require.Nil(t, i.(*appendInterceptor).registry.get(testVChannel))
}

func TestBuildPanicsWhenAnExistingCollectionHasAnUnsupportedPrimaryKey(t *testing.T) {
	env := newTestIndexEnv(t)
	ch := env.openChannel(0)
	schema := testSchema(false)
	schema.Fields[0].DataType = schemapb.DataType_Float

	defer func() {
		value := recover()
		require.NotNil(t, value, "Build must panic")
		require.Contains(t, value.(string), "at wal open")
		require.Contains(t, value.(string), "unsupported data type")
	}()
	NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ChannelInfo:            types.PChannelInfo{Name: testPChannel},
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{PKIndex: ch},
		OnFatal:                func(err error) { t.Errorf("OnFatal must not be called: %v", err) },
		ShardManager: fakeSchemaLister{infos: map[int64]shards.CollectionSchemaInfo{
			testCollectionID: {VChannel: testVChannel, Schema: schema},
		}},
	})
}

// ===== autocommit insert =====

// corruptedEngine stands for an engine that finds a damaged table file: every
// read fails with an error marked ErrCorrupted.
type corruptedEngine struct {
	engine.RW
}

func (corruptedEngine) MultiGet(context.Context, [][]byte) ([][]byte, error) {
	return nil, errors.Mark(errors.New("damaged table file"), pkroot.ErrCorrupted)
}

func TestInsertOnCorruptedIndexMarksTheWALUnavailable(t *testing.T) {
	env := newTestIndexEnv(t)
	i, engines := newEmptyTestInterceptor(t, env, nil)
	created, err := i.registry.add(context.Background(), testVChannel, testCollectionID, testSchema(false))
	require.NoError(t, err)
	require.True(t, created)
	e := engines.of(t, testVChannel)
	e.idx.SetDeciderEngineForTest(corruptedEngine{RW: e.idx.EngineForTest()})
	f := newFakeAppender()

	_, err = i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.Error(t, err)
	require.Empty(t, f.types, "nothing is appended after a failed decision")
	fatals := env.fatalList()
	require.Len(t, fatals, 1)
	// errors.Mark of cockroachdb is not visible to the errors.Is of the standard library
	require.True(t, errors.Is(fatals[0], pkroot.ErrCorrupted), "%v", fatals[0])
}

func TestInsertMiss(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	f := newFakeAppender()

	id, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.NoError(t, err)
	require.Equal(t, f.lastID(), id)
	require.Equal(t, []message.MessageType{message.MessageTypeInsert}, f.types)

	// the shell does not write the engine, observe does. The key is present
	// through the in-flight set.
	_, ok := engines.of(t, testVChannel).segmentOf(t, 1)
	require.False(t, ok)
	require.True(t, presentOf(t, i, dedup.Int64PK(1)))
}

func TestInsertHit(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engine := engines.of(t, testVChannel)
	engine.put(t, 1, 100)
	f := newFakeAppender()

	id, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{
		message.MessageTypeBeginTxn,
		message.MessageTypeInsert,
		message.MessageTypeDelete,
		message.MessageTypeCommitTxn,
	}, f.types)
	// the client observes the commit of the transaction built here.
	require.Equal(t, f.lastID(), id)

	body := f.deleteBodyAt(t, 2)
	require.Equal(t, []int64{1}, body.GetPrimaryKeys().GetIntId().GetData())
	require.Equal(t, common.AllPartitionsID, body.GetPartitionID())
	require.Equal(t, testCollectionID, body.GetCollectionID())
	require.Equal(t, int64(1), body.GetNumRows())
	require.Len(t, body.GetTimestamps(), 1)
	require.Equal(t, uint64(1), message.MustAsMutableDeleteMessageV1(f.appended[2]).Header().GetRows())

	segmentID, ok := engine.segmentOf(t, 1)
	require.True(t, ok)
	require.Equal(t, int64(100), segmentID, "the shell does not write the engine")
	require.Equal(t, 1, inflightLen(i))
}

func TestInsertHitOfSomeKeys(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engine := engines.of(t, testVChannel)
	engine.put(t, 1, 100)
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1, 2), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{
		message.MessageTypeBeginTxn,
		message.MessageTypeInsert,
		message.MessageTypeDelete,
		message.MessageTypeCommitTxn,
	}, f.types)
	require.Equal(t, []int64{1}, f.deleteBodyAt(t, 2).GetPrimaryKeys().GetIntId().GetData())

	for _, pk := range []int64{1, 2} {
		require.True(t, presentOf(t, i, dedup.Int64PK(pk)))
	}
}

func TestInsertRedoIsRetriedFromScratch(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engine := engines.of(t, testVChannel)
	engine.put(t, 1, 100)

	f := newFakeAppender()
	f.failAt, f.failErr = 1, redo.ErrRedo
	msg := newInsert(testVChannel, 1)

	_, err := i.DoAppend(newTestAppendContext(), msg, f.append)
	require.ErrorIs(t, err, redo.ErrRedo)
	require.Equal(t, []message.MessageType{
		message.MessageTypeBeginTxn,
		message.MessageTypeInsert,
		message.MessageTypeRollbackTxn,
	}, f.types)
	// the caller's message must stay autocommit, the redo appends it again.
	require.Nil(t, msg.TxnContext())
	segmentID, ok := engine.segmentOf(t, 1)
	require.True(t, ok)
	require.Equal(t, int64(100), segmentID, "a failed append must not change the index")
	require.Equal(t, 0, inflightLen(i), "a redo is known not to be in the WAL, its registration is removed")

	// the redo interceptor runs the whole chain again with the same message.
	f2 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), msg, f2.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{
		message.MessageTypeBeginTxn,
		message.MessageTypeInsert,
		message.MessageTypeDelete,
		message.MessageTypeCommitTxn,
	}, f2.types)
	require.Equal(t, 1, inflightLen(i))
}

// A plain append that fails with an unknown outcome may be in the WAL at the time
// tick it was assigned. Its key stays present until observe passes that time
// tick, so the next insert of the key gets a companion delete, and it no longer
// blocks that insert.
func TestFailedInsertWithAnUnknownOutcomeKeepsItsKey(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	f := newFakeAppender()
	f.failAt, f.failErr, f.failAfterTimeTick = 0, errors.New("boom"), true

	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.Error(t, err)
	require.True(t, presentOf(t, i, dedup.Int64PK(1)))

	f2 := newFakeAppender()
	f2.nextTT = 200
	require.NoError(t, requireDoAppendReturns(t, i, newInsert(testVChannel, 1), f2.append))
	require.Equal(t, []message.MessageType{
		message.MessageTypeBeginTxn,
		message.MessageTypeInsert,
		message.MessageTypeDelete,
		message.MessageTypeCommitTxn,
	}, f2.types)

	// the failed insert got time tick 101 and was not in the WAL: observe passes
	// 101 without meeting it, and only the second insert stays.
	auth := i.ch.Decider(testVChannel)
	require.Equal(t, 2, inflightLen(i))
	dedup.ObserveTimeTickForTest(auth, 100)
	require.Equal(t, 2, inflightLen(i))
	dedup.ObserveTimeTickForTest(auth, 101)
	require.Equal(t, 1, inflightLen(i))
}

// An append that fails before the timetick interceptor assigned a time tick
// wrote nothing, so its registration is removed.
func TestFailedInsertWithoutATimeTickRemovesItsKey(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	f := newFakeAppender()
	f.failAt, f.failErr = 0, errors.New("allocate timestamp failed")

	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.Error(t, err)
	require.Equal(t, 0, inflightLen(i))
	require.False(t, presentOf(t, i, dedup.Int64PK(1)))

	// a redo or a retry passes the message again with the time tick of an earlier
	// attempt still in it. That time tick does not count.
	msg := newInsert(testVChannel, 2).WithTimeTick(50)
	f2 := newFakeAppender()
	f2.failAt, f2.failErr = 0, errors.New("allocate timestamp failed")
	_, err = i.DoAppend(newTestAppendContext(), msg, f2.append)
	require.Error(t, err)
	require.Equal(t, 0, inflightLen(i))
}

// The transaction built for an insert hit is rolled back after a failed body,
// so it is known not to be in the WAL and its registration is removed.
func TestFailedCompanionDeleteRemovesTheRegistration(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engines.of(t, testVChannel).put(t, 1, 100)

	f := newFakeAppender()
	// BeginTxn, the insert, then the companion delete.
	f.failAt, f.failErr = 2, errors.New("boom")
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.Error(t, err)
	require.Equal(t, []message.MessageType{
		message.MessageTypeBeginTxn,
		message.MessageTypeInsert,
		message.MessageTypeDelete,
		message.MessageTypeRollbackTxn,
	}, f.types)
	require.Equal(t, 0, inflightLen(i))

	require.NoError(t, requireDoAppendReturns(t, i, newInsert(testVChannel, 1), newFakeAppender().append))
}

func TestFailedCommitOfAnInsertHitLeavesTheIndexUnchanged(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engine := engines.of(t, testVChannel)
	engine.put(t, 1, 100)

	f := newFakeAppender()
	boom := errors.New("boom")
	// BeginTxn, the insert, the companion delete, then the CommitTxn.
	f.failAt, f.failErr = 3, boom
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.ErrorIs(t, err, boom)
	require.Equal(t, []message.MessageType{
		message.MessageTypeBeginTxn,
		message.MessageTypeInsert,
		message.MessageTypeDelete,
		message.MessageTypeCommitTxn,
		message.MessageTypeRollbackTxn,
	}, f.types)

	segmentID, ok := engine.segmentOf(t, 1)
	require.True(t, ok)
	require.Equal(t, int64(100), segmentID, "a failed commit must not change the index")
	require.Equal(t, 0, inflightLen(i), "the persisted rollback proves the commit did not take effect")
}

// A failed CommitTxn whose rollback is rejected may have been persisted, so the
// keys of the insert stay present.
func TestFailedCommitOfAnInsertHitWithAnUnknownOutcomeKeepsItsKeys(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engines.of(t, testVChannel).put(t, 1, 100)

	boom := errors.New("boom")
	f := newFakeAppender()
	appendOp := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		switch msg.MessageType() {
		case message.MessageTypeCommitTxn:
			return nil, boom
		case message.MessageTypeRollbackTxn:
			return nil, status.NewInvalidTransactionState("Rollback", message.TxnStateInFlight, message.TxnStateOnCommit)
		}
		return f.append(ctx, msg)
	}
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1, 2), appendOp)
	require.ErrorIs(t, err, boom)
	require.Equal(t, 2, inflightLen(i))
	require.True(t, presentOf(t, i, dedup.Int64PK(2)))
}

// TestInsertFailsWhenTheIndexIsUnavailable covers a write that finds the engine
// of its index closed: the index closes only with the WAL, so the client sees
// the on-shutdown error and retries on the next owner of the channel.
func TestInsertFailsWhenTheIndexIsUnavailable(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	require.NoError(t, engines.of(t, testVChannel).idx.EngineForTest().Close())
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	requireOnShutdown(t, err)
	require.Empty(t, f.types, "nothing may be appended when the index lookup failed")
}

// TestInsertFailsWhenTheIndexIsRemoved covers a write that still finds the
// target of an index that the Channel already closed.
func TestInsertFailsWhenTheIndexIsRemoved(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	i.ch.RemoveForTest(context.Background(), testVChannel)
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	requireOnShutdown(t, err)
	require.Empty(t, f.types, "nothing may be appended when the index lookup failed")
}

func TestDecideErrorClassification(t *testing.T) {
	require.ErrorIs(t, decideError(context.Canceled), context.Canceled)
	require.ErrorIs(t, decideError(errors.Wrap(context.DeadlineExceeded, "lookup")), context.DeadlineExceeded)
	requireOnShutdown(t, decideError(errors.Wrap(engine.ErrClosed, "lookup")))
	requireInner(t, decideError(errors.New("disk read failed")))
}

// A decider whose writes wait for observe makes DoAppend return
// WaitRequiredError and append nothing. Its Wait fails when the decider closes,
// and returns once observe reaches the time tick, and the append then succeeds.
// An autocommit delete needs no decision, so it does not wait.
func TestWriteThatWaitsReturnsWaitRequired(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	decider := i.ch.Decider(testVChannel)
	dedup.WaitUntilForTest(decider, 50)
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	var wait *interceptors.WaitRequiredError
	require.ErrorAs(t, err, &wait)
	require.Empty(t, f.types, "nothing may be appended while the write waits")
	require.Equal(t, 0, inflightLen(i))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, wait.Wait(ctx), context.Canceled)

	_, err = i.DoAppend(newTestAppendContext(), newDelete(testVChannel, 1), f.append)
	require.NoError(t, err, "an autocommit delete does not wait")
	require.Equal(t, []message.MessageType{message.MessageTypeDelete}, f.types)
	require.Equal(t, 0, inflightLen(i))

	_, err = i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.ErrorAs(t, err, &wait)
	waitDone := make(chan error, 1)
	go func() { waitDone <- wait.Wait(context.Background()) }()
	decider.Close()
	requireOnShutdown(t, <-waitDone)

	i, _ = newTestInterceptor(t, nil)
	decider = i.ch.Decider(testVChannel)
	dedup.WaitUntilForTest(decider, 50)
	f = newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.ErrorAs(t, err, &wait)
	dedup.ObserveTimeTickForTest(decider, 50)
	require.NoError(t, wait.Wait(context.Background()))
	_, err = i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeInsert}, f.types)
}

// ===== autocommit delete =====

func TestDeleteOfPresentKeys(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engine := engines.of(t, testVChannel)
	engine.put(t, 1, 100)
	engine.put(t, 2, 100)
	f := newFakeAppender()

	msg := newDelete(testVChannel, 1, 2)
	_, err := i.DoAppend(newTestAppendContext(), msg, f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeDelete}, f.types)
	require.Equal(t, []int64{1, 2}, f.deleteBodyAt(t, 0).GetPrimaryKeys().GetIntId().GetData())

	// The delete is not registered. observe applies it, until then the engine
	// still holds the keys.
	for _, pk := range []int64{1, 2} {
		_, ok := engine.segmentOf(t, pk)
		require.True(t, ok)
	}
	require.Equal(t, 0, inflightLen(i))

	// A later insert of a deleted key finds it in the engine and gets a
	// companion delete that deletes nothing.
	_, err = i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.NoError(t, err)
	require.Equal(t, message.MessageTypeDelete, f.types[3])
	require.Equal(t, []int64{1}, f.deleteBodyAt(t, 3).GetPrimaryKeys().GetIntId().GetData())
}

// A key that the index does not know may still exist in the data, so the delete
// reaches the WAL unchanged.
func TestDeleteOfSomePresentKeysIsAppendedUnchanged(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engine := engines.of(t, testVChannel)
	engine.put(t, 1, 100)
	f := newFakeAppender()

	msg := newDelete(testVChannel, 1, 9)
	_, err := i.DoAppend(newTestAppendContext(), msg, f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeDelete}, f.types)

	body := f.deleteBodyAt(t, 0)
	require.Equal(t, []int64{1, 9}, body.GetPrimaryKeys().GetIntId().GetData())
	require.Equal(t, int64(2), body.GetNumRows())
	require.Len(t, body.GetTimestamps(), 2)
	require.Equal(t, uint64(2), message.MustAsMutableDeleteMessageV1(msg).Header().GetRows())
}

func TestDeleteOfAbsentKeysIsStillAppended(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newDelete(testVChannel, 8, 9), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeDelete}, f.types)

	body := f.deleteBodyAt(t, 0)
	require.Equal(t, []int64{8, 9}, body.GetPrimaryKeys().GetIntId().GetData())
	require.Equal(t, int64(2), body.GetNumRows())
}

func TestFailedDeleteLeavesTheIndexUnchanged(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engine := engines.of(t, testVChannel)
	engine.put(t, 1, 100)

	boom := errors.New("boom")
	f := newFakeAppender()
	f.failAt, f.failErr = 0, boom
	_, err := i.DoAppend(newTestAppendContext(), newDelete(testVChannel, 1), f.append)
	require.ErrorIs(t, err, boom)

	segmentID, ok := engine.segmentOf(t, 1)
	require.True(t, ok)
	require.Equal(t, int64(100), segmentID, "a failed delete must not change the index")

	require.NoError(t, requireDoAppendReturns(t, i, newDelete(testVChannel, 1), newFakeAppender().append))
}

// ===== varchar primary keys =====

func TestVarCharInsertMiss(t *testing.T) {
	i, engines := newTestInterceptorOfSchema(t, nil, testVarCharSchema())
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newVarCharInsert(testVChannel, "a"), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeInsert}, f.types)

	require.True(t, presentOf(t, i, dedup.VarCharPK("a")))
	_, ok := engines.of(t, testVChannel).segmentOfPK(t, dedup.VarCharPK("a"))
	require.False(t, ok)
}

func TestVarCharInsertHit(t *testing.T) {
	i, engines := newTestInterceptorOfSchema(t, nil, testVarCharSchema())
	engine := engines.of(t, testVChannel)
	engine.putPK(t, dedup.VarCharPK("a"), 100)
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newVarCharInsert(testVChannel, "a"), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{
		message.MessageTypeBeginTxn,
		message.MessageTypeInsert,
		message.MessageTypeDelete,
		message.MessageTypeCommitTxn,
	}, f.types)

	body := f.deleteBodyAt(t, 2)
	require.Equal(t, []string{"a"}, body.GetPrimaryKeys().GetStrId().GetData())
	require.Nil(t, body.GetPrimaryKeys().GetIntId())
	require.Equal(t, int64(1), body.GetNumRows())
	require.True(t, presentOf(t, i, dedup.VarCharPK("a")))
}

func TestVarCharDeleteOfPresentKeys(t *testing.T) {
	i, engines := newTestInterceptorOfSchema(t, nil, testVarCharSchema())
	engine := engines.of(t, testVChannel)
	engine.putPK(t, dedup.VarCharPK("a"), 100)
	engine.putPK(t, dedup.VarCharPK("b"), 100)
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newVarCharDelete(testVChannel, "a", "b"), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeDelete}, f.types)
	require.Equal(t, []string{"a", "b"}, f.deleteBodyAt(t, 0).GetPrimaryKeys().GetStrId().GetData())

	for _, pk := range []string{"a", "b"} {
		require.True(t, presentOf(t, i, dedup.VarCharPK(pk)))
	}
}

func TestDeleteByExpressionPanics(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	f := newFakeAppender()

	require.PanicsWithValue(t, "a delete by expression reached the primary key index, the index does not support it", func() {
		i.DoAppend(newTestAppendContext(), newDeleteByExpression(testVChannel), f.append)
	})
	require.Empty(t, f.types, "the delete must not reach the WAL")
}

// ===== client transactions =====

func TestTxnCommitHit(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engine := engines.of(t, testVChannel)
	engine.put(t, 1, 100)
	txnCtx := message.TxnContext{TxnID: 42, Keepalive: time.Hour}

	f := newFakeAppender()
	body := newInsert(testVChannel, 1).WithTxnContext(txnCtx)
	_, err := i.DoAppend(newTestAppendContext(), body, f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeInsert}, f.types)
	require.Equal(t, 0, inflightLen(i), "a transaction body registers nothing")

	f2 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newCommit(testVChannel, txnCtx), f2.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeDelete, message.MessageTypeCommitTxn}, f2.types)
	require.Equal(t, []int64{1}, f2.deleteBodyAt(t, 0).GetPrimaryKeys().GetIntId().GetData())
	require.Equal(t, txnCtx.TxnID, f2.appended[0].TxnContext().TxnID)
	require.Equal(t, 1, inflightLen(i))
}

func TestTxnCommitMiss(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	txnCtx := message.TxnContext{TxnID: 43, Keepalive: time.Hour}

	f := newFakeAppender()
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 5).WithTxnContext(txnCtx), f.append)
	require.NoError(t, err)

	f2 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newCommit(testVChannel, txnCtx), f2.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeCommitTxn}, f2.types)
	require.True(t, presentOf(t, i, dedup.Int64PK(5)))
}

// An upsert without partition key mode deletes the key in its target partition
// only. The row of the key in another partition survives that delete, so the
// commit still gives the key a companion delete that covers all partitions.
func TestTxnOwnDeleteOfOnePartitionKeepsTheCompanionDelete(t *testing.T) {
	const partitionB = testPartitionID + 1
	i, _ := newTestInterceptor(t, nil)

	// key 7 exists in partition A.
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 7), newFakeAppender().append)
	require.NoError(t, err)

	txnCtx := message.TxnContext{TxnID: 44, Keepalive: time.Hour}
	f := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newDeleteInPartition(testVChannel, partitionB, 7).WithTxnContext(txnCtx), f.append)
	require.NoError(t, err)
	_, err = i.DoAppend(newTestAppendContext(), newInsertInPartition(testVChannel, partitionB, 7).WithTxnContext(txnCtx), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeDelete, message.MessageTypeInsert}, f.types)

	f2 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newCommit(testVChannel, txnCtx), f2.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeDelete, message.MessageTypeCommitTxn}, f2.types)
	companion := f2.deleteBodyAt(t, 0)
	require.Equal(t, []int64{7}, companion.GetPrimaryKeys().GetIntId().GetData())
	require.Equal(t, common.AllPartitionsID, companion.GetPartitionID())
	require.Equal(t, txnCtx.TxnID, f2.appended[0].TxnContext().TxnID)

	// the delete of the client reaches the WAL unchanged.
	own := f.deleteBodyAt(t, 0)
	require.Equal(t, []int64{7}, own.GetPrimaryKeys().GetIntId().GetData())
	require.Equal(t, partitionB, own.GetPartitionID())
}

func TestTxnRollbackDropsThePendingWrites(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	txnCtx := message.TxnContext{TxnID: 44, Keepalive: time.Hour}

	f := newFakeAppender()
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 6).WithTxnContext(txnCtx), f.append)
	require.NoError(t, err)

	f2 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newRollback(testVChannel, txnCtx), f2.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeRollbackTxn}, f2.types)
	require.False(t, presentOf(t, i, dedup.Int64PK(6)))

	// a commit of the same transaction finds nothing left to apply.
	f3 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newCommit(testVChannel, txnCtx), f3.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeCommitTxn}, f3.types)
	require.False(t, presentOf(t, i, dedup.Int64PK(6)))
}

// A failed CommitTxn of a client transaction may have been persisted, so its keys
// stay present. The pending writes are kept for a retried commit.
func TestFailedCommitKeepsThePendingWrites(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	txnCtx := message.TxnContext{TxnID: 45, Keepalive: time.Hour}

	f := newFakeAppender()
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 9).WithTxnContext(txnCtx), f.append)
	require.NoError(t, err)

	boom := errors.New("boom")
	f2 := newFakeAppender()
	f2.failAt, f2.failErr, f2.failAfterTimeTick = 0, boom, true
	_, err = i.DoAppend(newTestAppendContext(), newCommit(testVChannel, txnCtx), f2.append)
	require.ErrorIs(t, err, boom)
	require.True(t, presentOf(t, i, dedup.Int64PK(9)), "the commit may be in the WAL")

	// the pending writes survive the failure, so the retried commit decides on
	// them again, and it returns because the failed decision no longer holds its
	// keys. The first commit may have taken effect, so the retry deletes the key.
	f3 := newFakeAppender()
	require.NoError(t, requireDoAppendReturns(t, i, newCommit(testVChannel, txnCtx), f3.append))
	require.Equal(t, []message.MessageType{message.MessageTypeDelete, message.MessageTypeCommitTxn}, f3.types)
}

func TestFailedRollbackKeepsThePendingWrites(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	txnCtx := message.TxnContext{TxnID: 46, Keepalive: time.Hour}

	f := newFakeAppender()
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 10).WithTxnContext(txnCtx), f.append)
	require.NoError(t, err)

	boom := errors.New("boom")
	f2 := newFakeAppender()
	f2.failAt, f2.failErr = 0, boom
	_, err = i.DoAppend(newTestAppendContext(), newRollback(testVChannel, txnCtx), f2.append)
	require.ErrorIs(t, err, boom)
	require.False(t, presentOf(t, i, dedup.Int64PK(10)), "a failed rollback must not change the index")

	f3 := newFakeAppender()
	require.NoError(t, requireDoAppendReturns(t, i, newCommit(testVChannel, txnCtx), f3.append))
	require.True(t, presentOf(t, i, dedup.Int64PK(10)), "a failed rollback must not drop the pending writes")
}

func TestCommitFailsWhenTheIndexIsUnavailable(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engine := engines.of(t, testVChannel)
	txnCtx := message.TxnContext{TxnID: 47, Keepalive: time.Hour}

	f := newFakeAppender()
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 11).WithTxnContext(txnCtx), f.append)
	require.NoError(t, err)
	require.NoError(t, engine.idx.EngineForTest().Close())

	f2 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newCommit(testVChannel, txnCtx), f2.append)
	requireOnShutdown(t, err)
	require.Empty(t, f2.types, "nothing may be appended when the index lookup failed")
}

func TestCommitFailsWhenTheChannelIsClosed(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	txnCtx := message.TxnContext{TxnID: 47, Keepalive: time.Hour}

	f := newFakeAppender()
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 11).WithTxnContext(txnCtx), f.append)
	require.NoError(t, err)
	i.ch.Close()

	f2 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newCommit(testVChannel, txnCtx), f2.append)
	requireOnShutdown(t, err)
	require.Empty(t, f2.types, "nothing may be appended when the index lookup failed")
}

// newTestTxnSession starts one real transaction session in a real manager.
func newTestTxnSession(t *testing.T) (*txn.TxnManager, *txn.TxnSession) {
	t.Helper()
	resource.InitForTest(t)
	manager := txn.NewTxnManager(types.PChannelInfo{Name: testPChannel}, nil)
	<-manager.RecoverDone()
	begin := message.NewBeginTxnMessageBuilderV2().
		WithVChannel(testVChannel).
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: time.Hour.Milliseconds()}).
		WithBody(&message.BeginTxnMessageBody{}).
		MustBuildMutable().
		WithTimeTick(1)
	beginMsg, err := message.AsMutableBeginTxnMessageV2(begin)
	require.NoError(t, err)
	session, err := manager.BeginNewTxn(context.Background(), beginMsg)
	require.NoError(t, err)
	return manager, session
}

// newTimeTick builds the TimeTick message the timetick interceptor appends.
func newTimeTick(ts uint64) message.MutableMessage {
	return message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{Base: &commonpb.MsgBase{MsgType: commonpb.MsgType_TimeTick, Timestamp: ts}}).
		WithAllVChannel().
		MustBuildMutable().
		WithTimeTick(ts)
}

// TestTimeTickDropsThePendingWritesOfEndedTxns covers a transaction that ends
// without a CommitTxn or RollbackTxn reaching this interceptor: expiry, a force
// fail by an exclusive DDL, or a force promote. The manager forgets the session,
// and the next TimeTick drops the pending writes.
func TestTimeTickDropsThePendingWritesOfEndedTxns(t *testing.T) {
	manager, session := newTestTxnSession(t)
	txnCtx := session.TxnContext()
	txnID := int64(txnCtx.TxnID)

	i, _ := newTestInterceptor(t, manager)
	target := i.ch.Decider(testVChannel)

	for _, pk := range []int64{7, 8} {
		_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, pk).WithTxnContext(txnCtx), newFakeAppender().append)
		require.NoError(t, err)
	}
	require.Equal(t, []int64{txnID}, dedup.PendingTxnIDsForTest(target))

	// the session is alive, so a TimeTick keeps the pending writes.
	f := newFakeAppender()
	_, err := i.DoAppend(newTestAppendContext(), newTimeTick(20), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeTimeTick}, f.types, "the TimeTick passes through")
	require.Equal(t, []int64{txnID}, dedup.PendingTxnIDsForTest(target))

	// an exclusive DDL fails the transaction. The manager forgets the session.
	manager.FailTxnAtVChannel(testVChannel)
	_, err = manager.GetSessionOfTxn(txnCtx.TxnID)
	require.Error(t, err)

	_, err = i.DoAppend(newTestAppendContext(), newTimeTick(21), newFakeAppender().append)
	require.NoError(t, err)
	require.Empty(t, dedup.PendingTxnIDsForTest(target), "the pending writes of an ended transaction must be dropped")

	// a commit that still arrives has nothing to apply.
	f2 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newCommit(testVChannel, txnCtx), f2.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeCommitTxn}, f2.types)
	for _, pk := range []int64{7, 8} {
		require.False(t, presentOf(t, i, dedup.Int64PK(pk)), "the pending writes of an ended transaction must not reach the index")
	}
}

// TestTimeTickWithoutSessionsIsPassthrough covers the interceptor without a
// transaction manager, which only tests build.
func TestTimeTickWithoutSessionsIsPassthrough(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	f := newFakeAppender()
	_, err := i.DoAppend(newTestAppendContext(), newTimeTick(20), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeTimeTick}, f.types)
}

// ===== passthrough =====

func TestReplicatedMessagePassesThrough(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	f := newFakeAppender()

	msg := newInsert(testVChannel, 1).WithReplicateHeader(&message.ReplicateHeader{
		ClusterID:              "primary",
		MessageID:              walimplstest.NewTestMessageID(1),
		LastConfirmedMessageID: walimplstest.NewTestMessageID(1),
		TimeTick:               1,
		VChannel:               testVChannel,
	})
	_, err := i.DoAppend(newTestAppendContext(), msg, f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeInsert}, f.types)
	require.False(t, presentOf(t, i, dedup.Int64PK(1)))
}

func TestVChannelWithoutTargetPassesThrough(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newInsert("by-dev-rootcoord-dml_0_2v0", 1), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeInsert}, f.types)
}

// ===== collection lifecycle =====

func TestCreateAndDropCollectionMaintainTheRegistry(t *testing.T) {
	i, _ := newEmptyTestInterceptor(t, newTestIndexEnv(t), nil)
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newCreateCollection(testVChannel, testSchema(false)), f.append)
	require.NoError(t, err)
	require.NotNil(t, i.registry.get(testVChannel))

	_, err = i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.NoError(t, err)
	require.True(t, presentOf(t, i, dedup.Int64PK(1)))

	_, err = i.DoAppend(newTestAppendContext(), newDropCollection(testVChannel), f.append)
	require.NoError(t, err)
	require.Nil(t, i.registry.get(testVChannel))
	idx := i.ch.IndexForTest(testVChannel)
	require.NotNil(t, idx, "the observe of the WAL recovery closes the index, not the append of the drop")
	// the observe reaches the insert before the drop and still finds the engine open
	require.NoError(t, i.ch.Apply(context.Background(), testVChannel, index.Write{Puts: []index.Put{{PK: dedup.Int64PK(3), SegmentID: 9}}, TimeTick: 50}))

	before := len(f.types)
	_, err = i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 2), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeInsert}, f.types[before:])
}

func TestCreateCollectionAfterDropBuildsAFreshIndex(t *testing.T) {
	i, _ := newEmptyTestInterceptor(t, newTestIndexEnv(t), nil)
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newCreateCollection(testVChannel, testSchema(false)), f.append)
	require.NoError(t, err)
	_, err = i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.NoError(t, err)
	_, err = i.DoAppend(newTestAppendContext(), newDropCollection(testVChannel), f.append)
	require.NoError(t, err)
	require.Nil(t, i.registry.get(testVChannel))
	// the observe of the WAL recovery closes the index of the dropped collection
	require.NoError(t, i.ch.DropCollection(context.Background(), testVChannel))

	_, err = i.DoAppend(newTestAppendContext(), newCreateCollection(testVChannel, testSchema(false)), f.append)
	require.NoError(t, err)
	require.NotNil(t, i.registry.get(testVChannel))

	f2 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f2.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeInsert}, f2.types,
		"the new index is empty, so the same key is a miss again")
}

func TestCreateCollectionOfAutoIDHasNoTarget(t *testing.T) {
	i, _ := newEmptyTestInterceptor(t, newTestIndexEnv(t), nil)
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newCreateCollection(testVChannel, testSchema(true)), f.append)
	require.NoError(t, err)
	require.Nil(t, i.registry.get(testVChannel))
}

func TestCreateCollectionRejectedWhenTheIndexCanNotBeCreated(t *testing.T) {
	i, _ := newEmptyTestInterceptor(t, newTestIndexEnvAt(t, newBrokenRoot(t)), nil)
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newCreateCollection(testVChannel, testSchema(false)), f.append)
	require.Error(t, err)
	require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE, status.AsStreamingError(err).Code)
	require.ErrorContains(t, err, "not a directory")
	require.Empty(t, f.types, "the create collection message must not reach the WAL")
	require.Nil(t, i.registry.get(testVChannel))
}

func TestCreateCollectionAppendFailureRemovesTheTarget(t *testing.T) {
	env := newTestIndexEnv(t)
	i, _ := newEmptyTestInterceptor(t, env, nil)

	appendErr := errors.New("wal is closed")
	_, err := i.DoAppend(newTestAppendContext(), newCreateCollection(testVChannel, testSchema(false)),
		func(context.Context, message.MutableMessage) (message.MessageID, error) { return nil, appendErr })
	require.ErrorIs(t, err, appendErr)
	require.Nil(t, i.registry.get(testVChannel))
	require.Nil(t, i.ch.IndexForTest(testVChannel), "the index of a collection that was not created is closed")
	require.NoDirExists(t, filepath.Join(env.root, testVChannel), "a retry of the create opens an index with no leftover")
}

func TestDuplicateCreateCollectionAppendFailureKeepsTheTarget(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engines.of(t, testVChannel).put(t, 1, 100)
	existing := i.registry.get(testVChannel)
	existingIdx := i.ch.IndexForTest(testVChannel)

	// The shard interceptor lets a duplicate CreateCollection pass, so it reaches
	// this interceptor while the target of the collection is in use.
	appendErr := errors.New("wal is closed")
	_, err := i.DoAppend(newTestAppendContext(), newCreateCollection(testVChannel, testSchema(false)),
		func(context.Context, message.MutableMessage) (message.MessageID, error) { return nil, appendErr })
	require.ErrorIs(t, err, appendErr)
	require.Same(t, existing, i.registry.get(testVChannel))
	require.Same(t, existingIdx, i.ch.IndexForTest(testVChannel), "the index in use stays open")
	seg, ok := engines.of(t, testVChannel).segmentOf(t, 1)
	require.True(t, ok, "the index keeps its data")
	require.EqualValues(t, 100, seg)

	f := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{
		message.MessageTypeBeginTxn,
		message.MessageTypeInsert,
		message.MessageTypeDelete,
		message.MessageTypeCommitTxn,
	}, f.types)
	require.Equal(t, []int64{1}, f.deleteBodyAt(t, 2).GetPrimaryKeys().GetIntId().GetData())
}

func TestRegistryAddRejectsAnUnusableSchema(t *testing.T) {
	r := newRegistry(newTestIndexEnv(t).openChannel(0))
	defer r.close()

	noPK := testSchema(false)
	noPK.Fields[0].IsPrimaryKey = false
	_, err := r.add(context.Background(), testVChannel, testCollectionID, noPK)
	require.Error(t, err)

	floatPK := testSchema(false)
	floatPK.Fields[0].DataType = schemapb.DataType_Float
	_, err = r.add(context.Background(), testVChannel, testCollectionID, floatPK)
	require.Error(t, err)
	require.Nil(t, r.get(testVChannel))
	require.Nil(t, r.ch.IndexForTest(testVChannel), "a rejected schema opens no index")
}

func TestRegistryAddIsIdempotent(t *testing.T) {
	r := newRegistry(newTestIndexEnv(t).openChannel(0))
	defer r.close()

	created, err := r.add(context.Background(), testVChannel, testCollectionID, testSchema(false))
	require.NoError(t, err)
	require.True(t, created)
	first := r.get(testVChannel)
	firstIdx := r.ch.IndexForTest(testVChannel)
	created, err = r.add(context.Background(), testVChannel, testCollectionID, testSchema(false))
	require.NoError(t, err)
	require.False(t, created)
	require.Same(t, first, r.get(testVChannel))
	require.Same(t, firstIdx, r.ch.IndexForTest(testVChannel), "a second add opens no second index")
}

func TestCreateCollectionOnControlChannelHasNoTarget(t *testing.T) {
	i, _ := newEmptyTestInterceptor(t, newTestIndexEnv(t), nil)
	f := newFakeAppender()

	controlChannel := funcutil.GetControlChannel(testPChannel)
	require.True(t, funcutil.IsControlChannel(controlChannel))
	_, err := i.DoAppend(newTestAppendContext(), newCreateCollection(controlChannel, testSchema(false)), f.append)
	require.NoError(t, err)
	require.Nil(t, i.registry.get(controlChannel))
	require.Equal(t, []message.MessageType{message.MessageTypeCreateCollection}, f.types)
}

// BenchmarkDecodeKeysOf1000Rows measures the key decoding of the write path for
// one insert of 1000 rows: the body decode, the primary key column and the
// conversion to index keys. The vector case adds a 128-dim float vector column,
// which the body decode must also read.
func BenchmarkDecodeKeysOf1000Rows(b *testing.B) {
	pks := make([]int64, 1000)
	for i := range pks {
		pks[i] = int64(i)
	}
	withVector := func(msg message.MutableMessage) message.MutableMessage {
		insert := message.MustAsMutableInsertMessageV1(msg)
		body := insert.MustBody()
		const dim = 128
		body.FieldsData = append(body.FieldsData, &schemapb.FieldData{
			Type:    schemapb.DataType_FloatVector,
			FieldId: testPKFieldID + 1,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  dim,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, dim*len(pks))}},
			}},
		})
		insert.OverwriteBody(body)
		return msg
	}
	for _, c := range []struct {
		name string
		msg  message.MutableMessage
	}{
		{"pk_only", newInsert(testVChannel, pks...)},
		{"with_128d_vector", withVector(newInsert(testVChannel, pks...))},
	} {
		b.Run(c.name, func(b *testing.B) {
			ctx := context.Background()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				body, err := utility.DecodeInsertBody(ctx, c.msg)
				if err != nil {
					b.Fatal(err)
				}
				keys, err := primarykey.KeysOfInsert(body, testPKFieldID, schemapb.DataType_Int64)
				if err != nil {
					b.Fatal(err)
				}
				if len(snpkindex.PKsOf(keys)) != len(pks) {
					b.Fatal("wrong key count")
				}
			}
		})
	}
}
