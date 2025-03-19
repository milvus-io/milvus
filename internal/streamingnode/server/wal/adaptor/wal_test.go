package adaptor_test

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/remeh/sizedwaitgroup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/registry"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/idalloc"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

const testVChannel = "v1"

type walTestFramework struct {
	b            wal.OpenerBuilder
	t            *testing.T
	messageCount int
}

func TestWAL(t *testing.T) {
	initResourceForTest(t)
	b := registry.MustGetBuilder(walimplstest.WALName,
		redo.NewInterceptorBuilder(),
		// TODO: current flusher interceptor cannot work well with the walimplstest.
		// flusher.NewInterceptorBuilder(),
		timetick.NewInterceptorBuilder(),
		segment.NewInterceptorBuilder(),
	)
	f := &walTestFramework{
		b:            b,
		t:            t,
		messageCount: 1000,
	}
	f.Run()
}

func initResourceForTest(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.StreamingCfg.WALWriteAheadBufferKeepalive.Key, "500ms")
	params.Save(params.StreamingCfg.WALWriteAheadBufferCapacity.Key, "10k")

	rc := idalloc.NewMockRootCoordClient(t)
	rc.EXPECT().GetPChannelInfo(mock.Anything, mock.Anything).Return(&rootcoordpb.GetPChannelInfoResponse{}, nil)

	fRootCoordClient := syncutil.NewFuture[internaltypes.RootCoordClient]()
	fRootCoordClient.Set(rc)

	dc := mocks.NewMockDataCoordClient(t)
	dc.EXPECT().AllocSegment(mock.Anything, mock.Anything).Return(&datapb.AllocSegmentResponse{}, nil)

	fDataCoordClient := syncutil.NewFuture[internaltypes.DataCoordClient]()
	fDataCoordClient.Set(dc)

	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().ListSegmentAssignment(mock.Anything, mock.Anything).Return(nil, nil)
	catalog.EXPECT().SaveSegmentAssignments(mock.Anything, mock.Anything, mock.Anything).Return(nil)

	resource.InitForTest(
		t,
		resource.OptRootCoordClient(fRootCoordClient),
		resource.OptDataCoordClient(fDataCoordClient),
		resource.OptStreamingNodeCatalog(catalog),
	)
}

func (f *walTestFramework) Run() {
	wg := sync.WaitGroup{}
	loopCnt := 3
	wg.Add(loopCnt)
	o, err := f.b.Build()
	assert.NoError(f.t, err)
	assert.NotNil(f.t, o)
	defer o.Close()

	for i := 0; i < loopCnt; i++ {
		go func(i int) {
			defer wg.Done()
			f.runOnce(fmt.Sprintf("pchannel-%d", i), o)
		}(i)
	}
	wg.Wait()
}

func (f *walTestFramework) runOnce(pchannel string, o wal.Opener) {
	f2 := &testOneWALFramework{
		t:            f.t,
		opener:       o,
		pchannel:     pchannel,
		messageCount: f.messageCount,
		term:         1,
	}
	f2.Run()
}

type testOneWALFramework struct {
	t            *testing.T
	opener       wal.Opener
	written      []message.ImmutableMessage
	pchannel     string
	messageCount int
	term         int
}

func (f *testOneWALFramework) Run() {
	ctx := context.Background()

	for ; f.term <= 3; f.term++ {
		pChannel := types.PChannelInfo{
			Name: f.pchannel,
			Term: int64(f.term),
		}
		rwWAL, err := f.opener.Open(ctx, &wal.OpenOption{
			Channel: pChannel,
		})
		assert.NoError(f.t, err)
		assert.NotNil(f.t, rwWAL)
		assert.Equal(f.t, pChannel.Name, rwWAL.Channel().Name)

		pChannel.AccessMode = types.AccessModeRO
		roWAL, err := f.opener.Open(ctx, &wal.OpenOption{
			Channel: pChannel,
		})
		assert.NoError(f.t, err)
		f.testReadAndWrite(ctx, rwWAL, roWAL)
		// close the wal
		rwWAL.Close()
		roWAL.Close()
	}
}

func (f *testOneWALFramework) testReadAndWrite(ctx context.Context, rwWAL wal.WAL, roWAL wal.ROWAL) {
	f.testSendCreateCollection(ctx, rwWAL)
	defer f.testSendDropCollection(ctx, rwWAL)

	// Test read and write.
	wg := sync.WaitGroup{}
	wg.Add(5)

	var newWritten []message.ImmutableMessage
	var read1, read2, read3 []message.ImmutableMessage
	appendDone := make(chan struct{})
	go func() {
		defer wg.Done()
		lastMVCC, err := rwWAL.GetLatestMVCCTimestamp(context.Background(), testVChannel)
		assert.NoError(f.t, err)
		for {
			select {
			case <-appendDone:
				return
			case <-time.After(time.Duration(rand.Int31n(100)) * time.Millisecond):
				newMVCC, err := rwWAL.GetLatestMVCCTimestamp(context.Background(), testVChannel)
				assert.NoError(f.t, err)
				assert.GreaterOrEqual(f.t, newMVCC, lastMVCC)
				lastMVCC = newMVCC
			}
		}
	}()
	go func() {
		defer func() {
			close(appendDone)
			wg.Done()
		}()
		var err error
		newWritten, err = f.testAppend(ctx, rwWAL)
		assert.NoError(f.t, err)
	}()
	go func() {
		defer wg.Done()
		var err error
		read1, err = f.testRead(ctx, rwWAL)
		assert.NoError(f.t, err)
	}()
	go func() {
		defer wg.Done()
		var err error
		read3, err = f.testRead(ctx, roWAL)
		assert.NoError(f.t, err)
	}()
	go func() {
		defer wg.Done()
		var err error
		read2, err = f.testRead(ctx, rwWAL)
		assert.NoError(f.t, err)
	}()
	wg.Wait()

	// read result should be sorted by timetick.
	f.assertSortByTimeTickMessageList(read1)
	f.assertSortByTimeTickMessageList(read2)
	f.assertSortByTimeTickMessageList(read3)

	// all written messages should be read.
	sort.Sort(sortByMessageID(newWritten))
	f.written = append(f.written, newWritten...)
	sort.Sort(sortByMessageID(read1))
	sort.Sort(sortByMessageID(read2))
	sort.Sort(sortByMessageID(read3))
	f.assertEqualMessageList(f.written, read1)
	f.assertEqualMessageList(f.written, read2)
	f.assertEqualMessageList(f.written, read3)

	// test read with option
	wg.Add(2)
	go func() {
		defer wg.Done()
		f.testReadWithOption(ctx, rwWAL)
	}()
	go func() {
		defer wg.Done()
		f.testReadWithOption(ctx, roWAL)
	}()
	wg.Wait()
}

func (f *testOneWALFramework) testSendCreateCollection(ctx context.Context, w wal.WAL) {
	// create collection before start test
	createMsg, err := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{2},
		}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		WithVChannel(testVChannel).
		BuildMutable()
	assert.NoError(f.t, err)

	msgID, err := w.Append(ctx, createMsg)
	assert.NoError(f.t, err)
	assert.NotNil(f.t, msgID)
}

func (f *testOneWALFramework) testSendDropCollection(ctx context.Context, w wal.WAL) {
	// Here, the drop colllection is conflict with the txn message, so we need to wait for a while.
	// TODO: fix it in the redo interceptor.
	time.Sleep(2 * time.Second)
	// drop collection after test
	dropMsg, err := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{
			CollectionId: 1,
		}).
		WithBody(&msgpb.DropCollectionRequest{}).
		WithVChannel(testVChannel).
		BuildMutable()
	assert.NoError(f.t, err)

	msgID, err := w.Append(ctx, dropMsg)
	assert.NoError(f.t, err)
	assert.NotNil(f.t, msgID)
}

func (f *testOneWALFramework) testAppend(ctx context.Context, w wal.WAL) ([]message.ImmutableMessage, error) {
	messages := make([]message.ImmutableMessage, f.messageCount)
	swg := sizedwaitgroup.New(10)
	for i := 0; i < f.messageCount-1; i++ {
		swg.Add()
		go func(i int) {
			defer swg.Done()
			time.Sleep(time.Duration(5+rand.Int31n(10)) * time.Millisecond)

			createPartOfTxn := func() (*message.ImmutableTxnMessageBuilder, *message.TxnContext) {
				msg, err := message.NewBeginTxnMessageBuilderV2().
					WithVChannel(testVChannel).
					WithHeader(&message.BeginTxnMessageHeader{
						KeepaliveMilliseconds: 1000,
					}).
					WithBody(&message.BeginTxnMessageBody{}).
					BuildMutable()
				assert.NoError(f.t, err)
				assert.NotNil(f.t, msg)
				appendResult, err := w.Append(ctx, msg)
				assert.NoError(f.t, err)
				assert.NotNil(f.t, appendResult)

				immutableMsg := msg.IntoImmutableMessage(appendResult.MessageID)
				begin, err := message.AsImmutableBeginTxnMessageV2(immutableMsg)
				assert.NoError(f.t, err)
				b := message.NewImmutableTxnMessageBuilder(begin)
				txnCtx := appendResult.TxnCtx
				for i := 0; i < int(rand.Int31n(5)); i++ {
					msg = message.CreateTestEmptyInsertMesage(int64(i), map[string]string{})
					msg.WithTxnContext(*txnCtx)
					appendResult, err = w.Append(ctx, msg)
					assert.NoError(f.t, err)
					assert.NotNil(f.t, msg)
					b.Add(msg.IntoImmutableMessage(appendResult.MessageID))
				}

				return b, txnCtx
			}

			if rand.Int31n(2) == 0 {
				// ...rocksmq has a dirty implement of properties,
				// without commonpb.MsgHeader, it can not work.
				msg := message.CreateTestEmptyInsertMesage(int64(i), map[string]string{
					"id":    fmt.Sprintf("%d", i),
					"const": "t",
				})
				appendResult, err := w.Append(ctx, msg)
				assert.NoError(f.t, err)
				assert.NotNil(f.t, appendResult)
				messages[i] = msg.IntoImmutableMessage(appendResult.MessageID)
			} else {
				b, txnCtx := createPartOfTxn()

				msg, err := message.NewCommitTxnMessageBuilderV2().
					WithVChannel("v1").
					WithHeader(&message.CommitTxnMessageHeader{}).
					WithBody(&message.CommitTxnMessageBody{}).
					WithProperties(map[string]string{
						"id":    fmt.Sprintf("%d", i),
						"const": "t",
					}).
					BuildMutable()
				assert.NoError(f.t, err)
				assert.NotNil(f.t, msg)
				appendResult, err := w.Append(ctx, msg.WithTxnContext(*txnCtx))
				assert.NoError(f.t, err)
				assert.NotNil(f.t, appendResult)

				immutableMsg := msg.IntoImmutableMessage(appendResult.MessageID)
				commit, err := message.AsImmutableCommitTxnMessageV2(immutableMsg)
				assert.NoError(f.t, err)
				txn, err := b.Build(commit)
				assert.NoError(f.t, err)
				messages[i] = txn
			}

			if rand.Int31n(3) == 0 {
				// produce a rollback or expired message.
				_, txnCtx := createPartOfTxn()
				if rand.Int31n(2) == 0 {
					msg, err := message.NewRollbackTxnMessageBuilderV2().
						WithVChannel("v1").
						WithHeader(&message.RollbackTxnMessageHeader{}).
						WithBody(&message.RollbackTxnMessageBody{}).
						BuildMutable()
					assert.NoError(f.t, err)
					assert.NotNil(f.t, msg)
					appendResult, err := w.Append(ctx, msg.WithTxnContext(*txnCtx))
					assert.NoError(f.t, err)
					assert.NotNil(f.t, appendResult)
				}
			}
		}(i)
	}
	swg.Wait()

	msg := message.CreateTestEmptyInsertMesage(int64(f.messageCount-1), map[string]string{
		"id":    fmt.Sprintf("%d", f.messageCount-1),
		"const": "t",
		"term":  strconv.FormatInt(int64(f.term), 10),
	})
	appendResult, err := w.Append(ctx, msg)
	assert.NoError(f.t, err)
	messages[f.messageCount-1] = msg.IntoImmutableMessage(appendResult.MessageID)
	return messages, nil
}

func (f *testOneWALFramework) testRead(ctx context.Context, w wal.ROWAL) ([]message.ImmutableMessage, error) {
	s, err := w.Read(ctx, wal.ReadOption{
		VChannel:      testVChannel,
		DeliverPolicy: options.DeliverPolicyAll(),
		MessageFilter: []options.DeliverFilter{
			options.DeliverFilterMessageType(message.MessageTypeInsert),
		},
	})
	assert.NoError(f.t, err)
	defer s.Close()

	expectedCnt := f.messageCount + len(f.written)
	msgs := make([]message.ImmutableMessage, 0, expectedCnt)
	cnt := 5
	for {
		msg, ok := <-s.Chan()
		// make a random slow down to trigger cache expire.
		if rand.Int31n(10) == 0 && cnt > 0 {
			cnt--
			time.Sleep(time.Duration(rand.Int31n(500)+100) * time.Millisecond)
		}
		if msg.MessageType() != message.MessageTypeInsert && msg.MessageType() != message.MessageTypeTxn {
			continue
		}
		assert.NotNil(f.t, msg)
		assert.True(f.t, ok)
		msgs = append(msgs, msg)
		termString, ok := msg.Properties().Get("term")
		if !ok {
			continue
		}
		term, err := strconv.ParseInt(termString, 10, 64)
		if err != nil {
			panic(err)
		}
		if int(term) == f.term {
			break
		}
	}
	return msgs, nil
}

func (f *testOneWALFramework) testReadWithOption(ctx context.Context, w wal.ROWAL) {
	loopCount := 5
	wg := sync.WaitGroup{}
	wg.Add(loopCount)
	for i := 0; i < loopCount; i++ {
		go func() {
			defer wg.Done()
			idx := rand.Int31n(int32(len(f.written)))
			// Test other read options.
			// Test start from some message and timetick is gte than it.
			readFromMsg := f.written[idx]
			s, err := w.Read(ctx, wal.ReadOption{
				VChannel:      testVChannel,
				DeliverPolicy: options.DeliverPolicyStartFrom(readFromMsg.LastConfirmedMessageID()),
				MessageFilter: []options.DeliverFilter{
					options.DeliverFilterTimeTickGTE(readFromMsg.TimeTick()),
					options.DeliverFilterMessageType(message.MessageTypeInsert),
				},
			})
			assert.NoError(f.t, err)
			maxTimeTick := f.maxTimeTickWritten()
			msgCount := 0
			lastTimeTick := readFromMsg.TimeTick() - 1
			for {
				msg, ok := <-s.Chan()
				if msg.MessageType() != message.MessageTypeInsert && msg.MessageType() != message.MessageTypeTxn {
					continue
				}
				msgCount++
				assert.NotNil(f.t, msg)
				assert.True(f.t, ok)
				assert.Greater(f.t, msg.TimeTick(), lastTimeTick)
				lastTimeTick = msg.TimeTick()
				if msg.TimeTick() >= maxTimeTick {
					break
				}
			}

			// shouldn't lost any message.
			assert.Equal(f.t, f.countTheTimeTick(readFromMsg.TimeTick()), msgCount)
			s.Close()
		}()
	}
	wg.Wait()
}

func (f *testOneWALFramework) assertSortByTimeTickMessageList(msgs []message.ImmutableMessage) {
	for i := 1; i < len(msgs); i++ {
		assert.Less(f.t, msgs[i-1].TimeTick(), msgs[i].TimeTick())
	}
}

func (f *testOneWALFramework) assertEqualMessageList(msgs1 []message.ImmutableMessage, msgs2 []message.ImmutableMessage) {
	assert.Equal(f.t, len(msgs2), len(msgs1))
	for i := 0; i < len(msgs1); i++ {
		assert.Equal(f.t, msgs1[i].MessageType(), msgs2[i].MessageType())
		if msgs1[i].MessageType() == message.MessageTypeInsert {
			assert.True(f.t, msgs1[i].MessageID().EQ(msgs2[i].MessageID()))
			// assert.True(f.t, bytes.Equal(msgs1[i].Payload(), msgs2[i].Payload()))
			id1, ok1 := msgs1[i].Properties().Get("id")
			id2, ok2 := msgs2[i].Properties().Get("id")
			assert.True(f.t, ok1)
			assert.True(f.t, ok2)
			assert.Equal(f.t, id1, id2)
			id1, ok1 = msgs1[i].Properties().Get("const")
			id2, ok2 = msgs2[i].Properties().Get("const")
			assert.True(f.t, ok1)
			assert.True(f.t, ok2)
			assert.Equal(f.t, id1, id2)
		}
		if msgs1[i].MessageType() == message.MessageTypeTxn {
			txn1 := message.AsImmutableTxnMessage(msgs1[i])
			txn2 := message.AsImmutableTxnMessage(msgs2[i])
			assert.Equal(f.t, txn1.Size(), txn2.Size())
			id1, ok1 := txn1.Commit().Properties().Get("id")
			id2, ok2 := txn2.Commit().Properties().Get("id")
			assert.True(f.t, ok1)
			assert.True(f.t, ok2)
			assert.Equal(f.t, id1, id2)
			id1, ok1 = txn1.Commit().Properties().Get("const")
			id2, ok2 = txn2.Commit().Properties().Get("const")
			assert.True(f.t, ok1)
			assert.True(f.t, ok2)
			assert.Equal(f.t, id1, id2)
		}
	}
}

func (f *testOneWALFramework) countTheTimeTick(begin uint64) int {
	cnt := 0
	for _, m := range f.written {
		if m.TimeTick() >= begin {
			cnt++
		}
	}
	return cnt
}

func (f *testOneWALFramework) maxTimeTickWritten() uint64 {
	maxTimeTick := uint64(0)
	for _, m := range f.written {
		if m.TimeTick() > maxTimeTick {
			maxTimeTick = m.TimeTick()
		}
	}
	return maxTimeTick
}

type sortByMessageID []message.ImmutableMessage

func (a sortByMessageID) Len() int {
	return len(a)
}

func (a sortByMessageID) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a sortByMessageID) Less(i, j int) bool {
	return a[i].MessageID().LT(a[j].MessageID())
}
