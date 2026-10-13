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
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/mock_recovery"
	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/index"
	"github.com/milvus-io/milvus/internal/pkindex/index/indextest"
	"github.com/milvus-io/milvus/internal/pkindex/snapshot"
	snpkindex "github.com/milvus-io/milvus/internal/streamingnode/server/pkindex"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/common"
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
	m.Run()
}

// ===== test doubles =====

// testIndexEnv is an index Manager over a real engine in a temporary
// directory. No size or tail ever starts a handover.
type testIndexEnv struct {
	mgr       *index.Manager
	committer *indextest.Committer
	root      string

	mu     sync.Mutex
	fatals []error
}

func newTestIndexEnv(t *testing.T) *testIndexEnv {
	return newTestIndexEnvAt(t, t.TempDir())
}

// newTestIndexEnvAt creates the env with root as the local root directory.
func newTestIndexEnvAt(t *testing.T, root string) *testIndexEnv {
	env := indextest.NewEnv(t, root)
	return &testIndexEnv{mgr: env.Manager, committer: env.Committer, root: root}
}

// openChannel opens the index Channel of the test pchannel.
func (env *testIndexEnv) openChannel(seed uint64) *index.Channel {
	return env.mgr.OpenChannel(types.PChannelInfo{Name: testPChannel, Term: 1}, seed, nil, env.onFatal)
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

// useResource makes the env the index Manager of the streaming node resource.
func (env *testIndexEnv) useResource(t *testing.T) {
	resource.InitForTest(t, resource.OptPKIndexManager(env.mgr))
}

// testEngine reads and seeds the engine of the Index of one vchannel.
type testEngine struct {
	idx *index.Index
}

// put seeds the index of one primary key, as an earlier insert would have done.
func (e *testEngine) put(t *testing.T, pk int64, segmentID int64) {
	t.Helper()
	e.putPK(t, snpkindex.Int64PK(pk), segmentID)
}

func (e *testEngine) putPK(t *testing.T, pk snpkindex.PK, segmentID int64) {
	t.Helper()
	value := codec.EncodePKEntry(codec.PKEntry{SegmentID: segmentID})
	require.NoError(t, e.idx.EngineForTest().Write(context.Background(), []engine.Mutation{
		{Key: pk.Encode(), Value: value},
	}))
}

// segmentOf returns the segment of a primary key and whether it is in the index.
func (e *testEngine) segmentOf(t *testing.T, pk int64) (int64, bool) {
	t.Helper()
	return e.segmentOfPK(t, snpkindex.Int64PK(pk))
}

func (e *testEngine) segmentOfPK(t *testing.T, pk snpkindex.PK) (int64, bool) {
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
	ch *index.Channel
}

func (e *testEngines) of(t *testing.T, vchannel string) *testEngine {
	t.Helper()
	idx := e.ch.Get(vchannel)
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
}

func newFakeAppender() *fakeAppender {
	return &fakeAppender{segmentID: 500, nextTT: 100, nextTxnID: 7, failAt: -1}
}

func (f *fakeAppender) append(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
	idx := len(f.types)
	f.types = append(f.types, msg.MessageType())
	f.appended = append(f.appended, msg)
	if idx == f.failAt {
		f.ids = append(f.ids, nil)
		return nil, f.failErr
	}
	f.nextTT++
	// A TimeTick message carries its own time tick, the timetick interceptor
	// does not assign one.
	if msg.MessageType() != message.MessageTypeTimeTick {
		msg.WithTimeTick(f.nextTT).WithLastConfirmedUseMessageID()
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
					Type:    schemapb.DataType_Int64,
					FieldId: testPKFieldID,
					Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: pks}},
					}},
				},
			},
		}).
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
	return message.NewDeleteMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DeleteMessageHeader{CollectionId: testCollectionID, Rows: uint64(len(pks))}).
		WithBody(&msgpb.DeleteRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Delete},
			CollectionID: testCollectionID,
			PartitionID:  testPartitionID,
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
	}
	t.Cleanup(i.Close)
	return i, &testEngines{ch: ch}
}

// requireDoAppendReturns fails when DoAppend does not return within five
// seconds. A decision that kept its stripes blocks every later write of the
// same keys forever, and this is what that looks like from the outside.
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
		t.Fatal("the append did not return, the stripes of an earlier decision are still held")
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

// ===== the switch =====

func TestBuildWithoutIndexManagerIsPassthrough(t *testing.T) {
	resource.InitForTest(t)

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

func TestBuildWithIndexManagerUsesTheIndex(t *testing.T) {
	newTestIndexEnv(t).useResource(t)

	i := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ChannelInfo: types.PChannelInfo{Name: testPChannel},
	})
	defer i.Close()
	require.IsType(t, &appendInterceptor{}, i)
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
	newTestIndexEnv(t).useResource(t)

	i := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ChannelInfo: types.PChannelInfo{Name: testPChannel},
		ShardManager: fakeSchemaLister{infos: map[int64]shards.CollectionSchemaInfo{
			testCollectionID:     {VChannel: testVChannel, Schema: testSchema(false)},
			testCollectionID + 1: {VChannel: testVChannel + "_autoid", Schema: testSchema(true)},
		}},
	})
	defer i.Close()

	ai := i.(*appendInterceptor)
	require.NotNil(t, ai.registry.get(testVChannel))
	require.Same(t, ai.ch.Get(testVChannel), ai.registry.get(testVChannel).idx)
	require.Nil(t, ai.registry.get(testVChannel+"_autoid"))
	require.Nil(t, ai.ch.Get(testVChannel+"_autoid"), "an autoID collection gets no index")
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
	newTestIndexEnvAt(t, newBrokenRoot(t)).useResource(t)

	var fatals []error
	var i interceptors.Interceptor
	require.NotPanics(t, func() {
		i = NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
			ChannelInfo: types.PChannelInfo{Name: testPChannel},
			OnFatal:     func(err error) { fatals = append(fatals, err) },
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
	newTestIndexEnv(t).useResource(t)
	schema := testSchema(false)
	schema.Fields[0].DataType = schemapb.DataType_Float

	defer func() {
		value := recover()
		require.NotNil(t, value, "Build must panic")
		require.Contains(t, value.(string), "at wal open")
		require.Contains(t, value.(string), "unsupported data type")
	}()
	NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ChannelInfo: types.PChannelInfo{Name: testPChannel},
		OnFatal:     func(err error) { t.Errorf("OnFatal must not be called: %v", err) },
		ShardManager: fakeSchemaLister{infos: map[int64]shards.CollectionSchemaInfo{
			testCollectionID: {VChannel: testVChannel, Schema: schema},
		}},
	})
}

// TestBuildPassesOnFatalToTheIndex checks that a fenced claim marks the WAL
// unavailable through param.OnFatal.
func TestBuildPassesOnFatalToTheIndex(t *testing.T) {
	env := newTestIndexEnv(t)
	env.committer.SetClaimErr(errors.Wrap(snapshot.ErrFenced, testVChannel))
	env.useResource(t)

	fatal := make(chan error, 1)
	i := NewInterceptorBuilder().Build(&interceptors.InterceptorBuildParam{
		ChannelInfo: types.PChannelInfo{Name: testPChannel, Term: 1},
		OnFatal: func(err error) {
			select {
			case fatal <- err:
			default:
			}
		},
		ShardManager: fakeSchemaLister{infos: map[int64]shards.CollectionSchemaInfo{
			testCollectionID: {VChannel: testVChannel, Schema: testSchema(false)},
		}},
	})
	defer i.Close()

	select {
	case err := <-fatal:
		require.ErrorIs(t, err, snapshot.ErrFenced)
	case <-time.After(10 * time.Second):
		t.Fatal("OnFatal was not called after a fenced claim")
	}
}

// ===== autocommit insert =====

func TestInsertMiss(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	f := newFakeAppender()

	id, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.NoError(t, err)
	require.Equal(t, f.lastID(), id)
	require.Equal(t, []message.MessageType{message.MessageTypeInsert}, f.types)

	segmentID, ok := engines.of(t, testVChannel).segmentOf(t, 1)
	require.True(t, ok)
	require.Equal(t, f.segmentID, segmentID)
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
	require.Equal(t, f.segmentID, segmentID)
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
		segmentID, ok := engine.segmentOf(t, pk)
		require.True(t, ok)
		require.Equal(t, f.segmentID, segmentID)
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
	segmentID, ok = engine.segmentOf(t, 1)
	require.True(t, ok)
	require.Equal(t, f2.segmentID, segmentID)
}

// TestPanicBetweenDecideAndApplyReleasesTheStripes checks the deferred guard of
// the handlers. Without it the stripes of the decision stay locked and every
// later write of the same key blocks for the life of the wal.
func TestFailedInsertReleasesTheStripes(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	f := newFakeAppender()
	f.failAt, f.failErr = 0, errors.New("boom")

	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.Error(t, err)

	require.NoError(t, requireDoAppendReturns(t, i, newInsert(testVChannel, 1), newFakeAppender().append))
}

func TestFailedCompanionDeleteReleasesTheStripes(t *testing.T) {
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
}

// TestInsertFailsWhenTheIndexIsRemoved covers a write that still finds the
// target of an index that the Channel already closed.
func TestInsertFailsWhenTheIndexIsRemoved(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	i.ch.Remove(context.Background(), testVChannel)
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

	for _, pk := range []int64{1, 2} {
		_, ok := engine.segmentOf(t, pk)
		require.False(t, ok)
	}
}

// A key that the index does not know may still exist in the data, so the delete
// reaches the WAL unchanged. Only the index update is narrowed to the known keys.
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

	_, ok := engine.segmentOf(t, 1)
	require.False(t, ok)
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
	_, ok = engine.segmentOf(t, 1)
	require.False(t, ok)
}

// ===== varchar primary keys =====

func TestVarCharInsertMiss(t *testing.T) {
	i, engines := newTestInterceptorOfSchema(t, nil, testVarCharSchema())
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newVarCharInsert(testVChannel, "a"), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeInsert}, f.types)

	segmentID, ok := engines.of(t, testVChannel).segmentOfPK(t, snpkindex.VarCharPK("a"))
	require.True(t, ok)
	require.Equal(t, f.segmentID, segmentID)
}

func TestVarCharInsertHit(t *testing.T) {
	i, engines := newTestInterceptorOfSchema(t, nil, testVarCharSchema())
	engine := engines.of(t, testVChannel)
	engine.putPK(t, snpkindex.VarCharPK("a"), 100)
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

	segmentID, ok := engine.segmentOfPK(t, snpkindex.VarCharPK("a"))
	require.True(t, ok)
	require.Equal(t, f.segmentID, segmentID)
}

func TestVarCharDeleteOfPresentKeys(t *testing.T) {
	i, engines := newTestInterceptorOfSchema(t, nil, testVarCharSchema())
	engine := engines.of(t, testVChannel)
	engine.putPK(t, snpkindex.VarCharPK("a"), 100)
	engine.putPK(t, snpkindex.VarCharPK("b"), 100)
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newVarCharDelete(testVChannel, "a", "b"), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeDelete}, f.types)
	require.Equal(t, []string{"a", "b"}, f.deleteBodyAt(t, 0).GetPrimaryKeys().GetStrId().GetData())

	for _, pk := range []string{"a", "b"} {
		_, ok := engine.segmentOfPK(t, snpkindex.VarCharPK(pk))
		require.False(t, ok)
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
	segmentID, ok := engine.segmentOf(t, 1)
	require.True(t, ok)
	require.Equal(t, int64(100), segmentID, "a transaction body must not change the index")

	f2 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newCommit(testVChannel, txnCtx), f2.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeDelete, message.MessageTypeCommitTxn}, f2.types)
	require.Equal(t, []int64{1}, f2.deleteBodyAt(t, 0).GetPrimaryKeys().GetIntId().GetData())
	require.Equal(t, txnCtx.TxnID, f2.appended[0].TxnContext().TxnID)

	segmentID, ok = engine.segmentOf(t, 1)
	require.True(t, ok)
	require.Equal(t, f.segmentID, segmentID)
}

func TestTxnCommitMiss(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engine := engines.of(t, testVChannel)
	txnCtx := message.TxnContext{TxnID: 43, Keepalive: time.Hour}

	f := newFakeAppender()
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 5).WithTxnContext(txnCtx), f.append)
	require.NoError(t, err)

	f2 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newCommit(testVChannel, txnCtx), f2.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeCommitTxn}, f2.types)

	segmentID, ok := engine.segmentOf(t, 5)
	require.True(t, ok)
	require.Equal(t, f.segmentID, segmentID)
}

func TestTxnRollbackDropsThePendingWrites(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engine := engines.of(t, testVChannel)
	txnCtx := message.TxnContext{TxnID: 44, Keepalive: time.Hour}

	f := newFakeAppender()
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 6).WithTxnContext(txnCtx), f.append)
	require.NoError(t, err)

	f2 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newRollback(testVChannel, txnCtx), f2.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeRollbackTxn}, f2.types)
	_, ok := engine.segmentOf(t, 6)
	require.False(t, ok)

	// a commit of the same transaction finds nothing left to apply.
	f3 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newCommit(testVChannel, txnCtx), f3.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeCommitTxn}, f3.types)
	_, ok = engine.segmentOf(t, 6)
	require.False(t, ok)
}

func TestFailedCommitKeepsThePendingWrites(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engine := engines.of(t, testVChannel)
	txnCtx := message.TxnContext{TxnID: 45, Keepalive: time.Hour}

	f := newFakeAppender()
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 9).WithTxnContext(txnCtx), f.append)
	require.NoError(t, err)

	boom := errors.New("boom")
	f2 := newFakeAppender()
	f2.failAt, f2.failErr = 0, boom
	_, err = i.DoAppend(newTestAppendContext(), newCommit(testVChannel, txnCtx), f2.append)
	require.ErrorIs(t, err, boom)
	_, ok := engine.segmentOf(t, 9)
	require.False(t, ok, "a failed commit must not change the index")

	// the pending writes survive the failure, so the retried commit still applies
	// them, and it returns because the failed decision released its stripes.
	f3 := newFakeAppender()
	require.NoError(t, requireDoAppendReturns(t, i, newCommit(testVChannel, txnCtx), f3.append))
	segmentID, ok := engine.segmentOf(t, 9)
	require.True(t, ok)
	require.Equal(t, f.segmentID, segmentID)
}

func TestFailedRollbackKeepsThePendingWrites(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engine := engines.of(t, testVChannel)
	txnCtx := message.TxnContext{TxnID: 46, Keepalive: time.Hour}

	f := newFakeAppender()
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 10).WithTxnContext(txnCtx), f.append)
	require.NoError(t, err)

	boom := errors.New("boom")
	f2 := newFakeAppender()
	f2.failAt, f2.failErr = 0, boom
	_, err = i.DoAppend(newTestAppendContext(), newRollback(testVChannel, txnCtx), f2.append)
	require.ErrorIs(t, err, boom)
	_, ok := engine.segmentOf(t, 10)
	require.False(t, ok, "a failed rollback must not change the index")

	f3 := newFakeAppender()
	require.NoError(t, requireDoAppendReturns(t, i, newCommit(testVChannel, txnCtx), f3.append))
	segmentID, ok := engine.segmentOf(t, 10)
	require.True(t, ok, "a failed rollback must not drop the pending writes")
	require.Equal(t, f.segmentID, segmentID)
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

	i, engines := newTestInterceptor(t, manager)
	engine := engines.of(t, testVChannel)
	target := i.registry.get(testVChannel).idx

	for _, pk := range []int64{7, 8} {
		_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, pk).WithTxnContext(txnCtx), newFakeAppender().append)
		require.NoError(t, err)
	}
	require.Equal(t, []int64{txnID}, target.PendingTxnIDs())

	// the session is alive, so a TimeTick keeps the pending writes.
	f := newFakeAppender()
	_, err := i.DoAppend(newTestAppendContext(), newTimeTick(20), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeTimeTick}, f.types, "the TimeTick passes through")
	require.Equal(t, []int64{txnID}, target.PendingTxnIDs())

	// an exclusive DDL fails the transaction. The manager forgets the session.
	manager.FailTxnAtVChannel(testVChannel)
	_, err = manager.GetSessionOfTxn(txnCtx.TxnID)
	require.Error(t, err)

	_, err = i.DoAppend(newTestAppendContext(), newTimeTick(21), newFakeAppender().append)
	require.NoError(t, err)
	require.Empty(t, target.PendingTxnIDs(), "the pending writes of an ended transaction must be dropped")

	// a commit that still arrives has nothing to apply.
	f2 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newCommit(testVChannel, txnCtx), f2.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeCommitTxn}, f2.types)
	for _, pk := range []int64{7, 8} {
		_, ok := engine.segmentOf(t, pk)
		require.False(t, ok, "the pending writes of an ended transaction must not reach the index")
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

// ===== the applied time tick watermark =====

// TestHeldInsertKeepsTheWatermark holds an insert inside its append, after
// Decide and before Apply. A TimeTick appended meanwhile closes the epoch of
// the insert, but the watermark moves only after the insert applies.
func TestHeldInsertKeepsTheWatermark(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	idx := i.registry.get(testVChannel).idx
	require.Equal(t, uint64(0), idx.LastAppliedTimeTick())

	entered := make(chan struct{})
	release := make(chan struct{})
	f := newFakeAppender()
	held := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		close(entered)
		<-release
		return f.append(ctx, msg)
	}
	done := make(chan error, 1)
	go func() {
		_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), held)
		done <- err
	}()
	<-entered

	_, err := i.DoAppend(newTestAppendContext(), newTimeTick(200), newFakeAppender().append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), idx.LastAppliedTimeTick(), "a writer in flight holds the watermark back")

	close(release)
	require.NoError(t, <-done)
	require.Equal(t, uint64(200), idx.LastAppliedTimeTick(), "the apply releases the watermark")
}

// TestDiscardLeavesNoWriterInFlight checks that every failed append releases
// its writer, so a later TimeTick moves the watermark at once.
func TestDiscardLeavesNoWriterInFlight(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	idx := i.registry.get(testVChannel).idx
	engines.of(t, testVChannel).put(t, 1, 100)

	// the append of an autocommit insert fails
	f := newFakeAppender()
	f.failAt, f.failErr = 0, errors.New("boom")
	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 2), f.append)
	require.Error(t, err)

	// the companion delete of an insert hit fails
	f = newFakeAppender()
	f.failAt, f.failErr = 2, errors.New("boom")
	_, err = i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.Error(t, err)

	// the append of a delete fails
	f = newFakeAppender()
	f.failAt, f.failErr = 0, errors.New("boom")
	_, err = i.DoAppend(newTestAppendContext(), newDelete(testVChannel, 1), f.append)
	require.Error(t, err)

	_, err = i.DoAppend(newTestAppendContext(), newTimeTick(200), newFakeAppender().append)
	require.NoError(t, err)
	require.Equal(t, uint64(200), idx.LastAppliedTimeTick())
}

func TestTxnBodyDoesNotMarkTheIndexDirty(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	idx := i.registry.get(testVChannel).idx
	txnCtx := message.TxnContext{TxnID: 48, Keepalive: time.Hour}

	_, err := i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1).WithTxnContext(txnCtx), newFakeAppender().append)
	require.NoError(t, err)
	require.False(t, idx.DirtyForTest(), "a transaction body writes nothing to the engine")

	_, err = i.DoAppend(newTestAppendContext(), newCommit(testVChannel, txnCtx), newFakeAppender().append)
	require.NoError(t, err)
	require.True(t, idx.DirtyForTest(), "the commit writes the pending writes to the engine")
}

func TestFailedTimeTickDoesNotTick(t *testing.T) {
	i, _ := newTestInterceptor(t, nil)
	idx := i.registry.get(testVChannel).idx

	f := newFakeAppender()
	f.failAt, f.failErr = 0, errors.New("boom")
	_, err := i.DoAppend(newTestAppendContext(), newTimeTick(200), f.append)
	require.Error(t, err)
	require.Equal(t, uint64(0), idx.LastAppliedTimeTick())

	_, err = i.DoAppend(newTestAppendContext(), newTimeTick(300), newFakeAppender().append)
	require.NoError(t, err)
	require.Equal(t, uint64(300), idx.LastAppliedTimeTick())
}

// ===== passthrough =====

func TestReplicatedMessagePassesThrough(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
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
	_, ok := engines.of(t, testVChannel).segmentOf(t, 1)
	require.False(t, ok)
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
	env := newTestIndexEnv(t)
	i, engines := newEmptyTestInterceptor(t, env, nil)
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newCreateCollection(testVChannel, testSchema(false)), f.append)
	require.NoError(t, err)
	require.NotNil(t, i.registry.get(testVChannel))

	_, err = i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.NoError(t, err)
	segmentID, ok := engines.of(t, testVChannel).segmentOf(t, 1)
	require.True(t, ok)
	require.Equal(t, f.segmentID, segmentID)

	_, err = i.DoAppend(newTestAppendContext(), newDropCollection(testVChannel), f.append)
	require.NoError(t, err)
	require.Nil(t, i.registry.get(testVChannel))
	require.Nil(t, i.ch.Get(testVChannel), "a dropped collection closes its index")
	require.NoDirExists(t, filepath.Join(env.root, testVChannel), "a dropped collection removes the local files of its index")

	before := len(f.types)
	_, err = i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 2), f.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeInsert}, f.types[before:])
}

// TestCreateCollectionAfterDropBuildsAFreshIndex checks that a new create
// opens a new, empty Index.
func TestCreateCollectionAfterDropBuildsAFreshIndex(t *testing.T) {
	i, engines := newEmptyTestInterceptor(t, newTestIndexEnv(t), nil)
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newCreateCollection(testVChannel, testSchema(false)), f.append)
	require.NoError(t, err)
	first := i.ch.Get(testVChannel)
	require.NotNil(t, first)
	_, err = i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 1), f.append)
	require.NoError(t, err)
	_, err = i.DoAppend(newTestAppendContext(), newDropCollection(testVChannel), f.append)
	require.NoError(t, err)
	require.Nil(t, i.registry.get(testVChannel))

	_, err = i.DoAppend(newTestAppendContext(), newCreateCollection(testVChannel, testSchema(false)), f.append)
	require.NoError(t, err)
	require.NotNil(t, i.registry.get(testVChannel))
	second := i.ch.Get(testVChannel)
	require.NotNil(t, second)
	require.NotSame(t, first, second)
	require.Same(t, second, i.registry.get(testVChannel).idx)
	_, ok := engines.of(t, testVChannel).segmentOf(t, 1)
	require.False(t, ok, "the new index does not see the keys of the dropped collection")

	f2 := newFakeAppender()
	_, err = i.DoAppend(newTestAppendContext(), newInsert(testVChannel, 2), f2.append)
	require.NoError(t, err)
	require.Equal(t, []message.MessageType{message.MessageTypeInsert}, f2.types)
}

func TestCreateCollectionOfAutoIDHasNoTarget(t *testing.T) {
	i, _ := newEmptyTestInterceptor(t, newTestIndexEnv(t), nil)
	f := newFakeAppender()

	_, err := i.DoAppend(newTestAppendContext(), newCreateCollection(testVChannel, testSchema(true)), f.append)
	require.NoError(t, err)
	require.Nil(t, i.registry.get(testVChannel))
	require.Nil(t, i.ch.Get(testVChannel))
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
	require.Nil(t, i.ch.Get(testVChannel), "the index of a collection that was not created is closed")
	require.NoDirExists(t, filepath.Join(env.root, testVChannel), "a retry of the create opens an index with no leftover")
}

func TestDuplicateCreateCollectionAppendFailureKeepsTheTarget(t *testing.T) {
	i, engines := newTestInterceptor(t, nil)
	engines.of(t, testVChannel).put(t, 1, 100)
	existing := i.registry.get(testVChannel)
	existingIdx := i.ch.Get(testVChannel)

	// The shard interceptor lets a duplicate CreateCollection pass, so it reaches
	// this interceptor while the target of the collection is in use.
	appendErr := errors.New("wal is closed")
	_, err := i.DoAppend(newTestAppendContext(), newCreateCollection(testVChannel, testSchema(false)),
		func(context.Context, message.MutableMessage) (message.MessageID, error) { return nil, appendErr })
	require.ErrorIs(t, err, appendErr)
	require.Same(t, existing, i.registry.get(testVChannel))
	require.Same(t, existingIdx, i.ch.Get(testVChannel), "the index in use stays open")
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
	require.Nil(t, r.ch.Get(testVChannel), "a rejected schema opens no index")
}

func TestRegistryAddIsIdempotent(t *testing.T) {
	r := newRegistry(newTestIndexEnv(t).openChannel(0))
	defer r.close()

	created, err := r.add(context.Background(), testVChannel, testCollectionID, testSchema(false))
	require.NoError(t, err)
	require.True(t, created)
	first := r.get(testVChannel)
	firstIdx := r.ch.Get(testVChannel)
	created, err = r.add(context.Background(), testVChannel, testCollectionID, testSchema(false))
	require.NoError(t, err)
	require.False(t, created)
	require.Same(t, first, r.get(testVChannel))
	require.Same(t, firstIdx, r.ch.Get(testVChannel), "a second add opens no second index")
	require.Same(t, firstIdx, first.idx)
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

func TestTailOfReadsTheRecoveryMetrics(t *testing.T) {
	require.Equal(t, index.WALTail{}, tailOf(nil)())

	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().Metrics().Return(recovery.RecoveryMetrics{RecoveryTimeTick: 42, RecoveryTailBytes: 1000})
	require.Equal(t, index.WALTail{Bytes: 1000, RecoveryTimeTick: 42}, tailOf(rs)())
}
