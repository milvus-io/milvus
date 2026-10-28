package utility

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/metricsutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	rmqwal "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/registry"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type historicalTestScanner struct {
	walimpls.ScannerImpls
	ch  chan message.ImmutableMessage
	err error
}

func (s *historicalTestScanner) Chan() <-chan message.ImmutableMessage { return s.ch }
func (s *historicalTestScanner) Error() error                          { return s.err }
func (s *historicalTestScanner) Close() error                          { return nil }

type historicalTestOpener struct {
	walimpls.OpenerImpls
	open func(*walimpls.OpenOption) (walimpls.WALImpls, error)
}

func (o historicalTestOpener) Open(_ context.Context, opt *walimpls.OpenOption) (walimpls.WALImpls, error) {
	return o.open(opt)
}

type historicalTestWAL struct {
	walimpls.WALImpls
	read  func(walimpls.ReadOption) (walimpls.ScannerImpls, error)
	close func()
}

func (w historicalTestWAL) WALName() message.WALName { return message.WALNameTest }
func (w historicalTestWAL) Read(_ context.Context, opt walimpls.ReadOption) (walimpls.ScannerImpls, error) {
	return w.read(opt)
}
func (w historicalTestWAL) Close() { w.close() }

type historicalTestHandler struct {
	messages []message.ImmutableMessage
	closed   bool
	handle   func(message.HandleParam) message.HandleResult
}

func (h *historicalTestHandler) Handle(p message.HandleParam) message.HandleResult {
	if h.handle != nil {
		return h.handle(p)
	}
	h.messages = append(h.messages, p.Message)
	return message.HandleResult{MessageHandled: true}
}
func (h *historicalTestHandler) Close() { h.closed = true }

func newHistoricalTestReader(t *testing.T, filters ...options.DeliverFilter) *historicalReader {
	t.Helper()
	metrics := metricsutil.NewScanMetrics(types.PChannelInfo{Name: "p"}).NewScannerMetrics()
	t.Cleanup(metrics.Close)
	return newHistoricalReader("p", "v1", mlog.With(), metrics, filters)
}

func historyMessage(msg message.MutableMessage, ts uint64) message.ImmutableMessage {
	return msg.WithTimeTick(ts).WithLastConfirmed(walimplstest.NewTestMessageID(0)).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(ts)))
}

func historyInsert(ts uint64, vchannel string, txn *message.TxnContext) message.ImmutableMessage {
	msg := message.NewInsertMessageBuilderV1().WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{}).WithBody(&msgpb.InsertRequest{}).MustBuildMutable()
	if txn != nil {
		msg = msg.WithTxnContext(*txn)
	}
	return historyMessage(msg, ts)
}

func historyAlter(ts uint64) message.ImmutableMessage {
	return historyMessage(message.NewAlterWALMessageBuilderV2().WithAllVChannel().
		WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker}).
		WithBody(&message.AlterWALMessageBody{}).MustBuildMutable(), ts)
}

func historyInput(messages ...message.ImmutableMessage) *historicalTestScanner {
	ch := make(chan message.ImmutableMessage, len(messages))
	for _, msg := range messages {
		ch <- msg
	}
	// Deliberately keep the channel open: completion cannot depend on EOF or TT.
	return &historicalTestScanner{ch: ch}
}

func TestHistoricalReaderEndsAtAlterWALWithoutTimeTick(t *testing.T) {
	r := newHistoricalTestReader(t, options.DeliverFilterTimeTickGT(10), options.DeliverFilterMessageType(message.MessageTypeInsert))
	h := &historicalTestHandler{}
	first, second := historyInsert(20, "v1", nil), historyInsert(30, "v1", nil)
	input := historyInput(second, historyInsert(25, "other", nil), historyInsert(5, "v1", nil), first, first, historyAlter(40), historyInsert(50, "v1", nil))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	cut, err := r.Read(ctx, input, h)
	require.NoError(t, err)
	require.EqualValues(t, 40, cut)
	require.Equal(t, []message.ImmutableMessage{first, second}, h.messages)
	require.Empty(t, r.TxnBuffer.GetUncommittedMessageBuilder())
	require.Zero(t, r.PendingQueue.Len())
	require.Len(t, input.ch, 1, "must stop at AlterWAL, without reading its suffix")
	require.False(t, h.closed)
}

func TestHistoricalReaderReusesTxnAssemblyAndBoundaryAbort(t *testing.T) {
	r := newHistoricalTestReader(t)
	h := &historicalTestHandler{}
	committed := message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}
	unfinished := message.TxnContext{TxnID: 2, Keepalive: message.TxnKeepaliveInfinite}
	begin := func(txn message.TxnContext, ts uint64) message.ImmutableMessage {
		return historyMessage(message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
			WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
			MustBuildMutable().WithTxnContext(txn), ts)
	}
	commit := historyMessage(message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(committed), 30)
	input := historyInput(commit, begin(unfinished, 21), historyInsert(22, "v1", &unfinished), historyInsert(12, "v1", &committed), begin(committed, 11), historyAlter(40))
	cut, err := r.Read(t.Context(), input, h)
	require.NoError(t, err)
	require.EqualValues(t, 40, cut)
	require.Len(t, h.messages, 1)
	require.Equal(t, message.MessageTypeTxn, h.messages[0].MessageType())
	require.EqualValues(t, 30, h.messages[0].TimeTick())
	require.Empty(t, r.TxnBuffer.GetUncommittedMessageBuilder())
}

func TestHistoricalReaderWaitsForDeliveryAndHonorsCancellation(t *testing.T) {
	r := newHistoricalTestReader(t)
	entered := make(chan struct{})
	h := &historicalTestHandler{handle: func(p message.HandleParam) message.HandleResult {
		close(entered)
		<-p.Ctx.Done()
		return message.HandleResult{Error: p.Ctx.Err()}
	}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		cut, err := r.Read(ctx, historyInput(historyInsert(10, "v1", nil), historyAlter(20)), h)
		if cut != 0 {
			done <- errors.New("premature boundary success")
			return
		}
		done <- err
	}()
	<-entered
	select {
	case err := <-done:
		t.Fatalf("reader completed before delivery: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("reader did not cancel")
	}
	require.False(t, h.closed)
	require.Equal(t, 2, r.PendingQueue.Len(), "cancel must not turn pending output into success")
}

func TestHistoricalReaderFailureBeforeBoundary(t *testing.T) {
	expected := errors.New("backend read failed")
	for _, err := range []error{nil, expected} {
		r := newHistoricalTestReader(t)
		input := historyInput(historyInsert(10, "v1", nil))
		input.err = err
		close(input.ch)
		cut, got := r.Read(t.Context(), input, &historicalTestHandler{})
		require.Zero(t, cut)
		require.Error(t, got)
		if err != nil {
			require.ErrorIs(t, got, expected)
		} else {
			require.True(t, status.AsStreamingError(got).IsUnrecoverable())
		}
	}
}

func TestHistoricalHelperOwnsRetryProgress(t *testing.T) {
	paramtable.Init()
	for _, cancelRetry := range []bool{false, true} {
		name := "complete"
		if cancelRetry {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			h := &historicalTestHandler{}
			if cancelRetry {
				h.handle = func(p message.HandleParam) message.HandleResult {
					h.messages = append(h.messages, p.Message)
					if p.Message.TimeTick() == 20 {
						time.AfterFunc(20*time.Millisecond, cancel)
					}
					return message.HandleResult{MessageHandled: true}
				}
			}
			tick := historyMessage(message.NewTimeTickMessageBuilderV1().WithAllVChannel().
				WithHeader(&message.TimeTickMessageHeader{}).WithBody(&msgpb.TimeTickMsg{}).MustBuildMutable(), 20)
			opens, closes := 0, 0
			opener := historicalTestOpener{open: func(opt *walimpls.OpenOption) (walimpls.WALImpls, error) {
				opens++
				require.Equal(t, types.AccessModeRO, opt.Channel.AccessMode)
				return historicalTestWAL{close: func() { closes++ }, read: func(opt walimpls.ReadOption) (walimpls.ScannerImpls, error) {
					require.Equal(t, options.DeliverPolicyStartFrom(walimplstest.NewTestMessageID(0)), opt.DeliverPolicy)
					if opens == 1 {
						input := historyInput(historyInsert(10, "v1", nil), tick)
						input.err = status.NewUnrecoverableError("history failed after delivering a prefix")
						close(input.ch)
						return input, nil
					}
					return historyInput(historyInsert(10, "v1", nil), tick, historyInsert(30, "v1", nil), historyAlter(40)), nil
				}}, nil
			}}
			opts := HistoricalReadOptions{
				PChannel: "p", VChannel: "v1", MessageHandler: h,
				DeliverPolicy:  options.DeliverPolicyStartFrom(walimplstest.NewTestMessageID(0)),
				DeliverFilters: []options.DeliverFilter{options.DeliverFilterTimeTickGT(1), options.DeliverFilterMessageType(message.MessageTypeInsert)},
			}
			cut, err := readHistoricalWAL(ctx, opts, nil, opener)
			if cancelRetry {
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, cut)
				require.Equal(t, 1, opens)
				require.Len(t, h.messages, 2)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, 40, cut)
				require.Equal(t, 2, opens)
				require.Len(t, h.messages, 3, "retry must not repeat the handled prefix")
				require.EqualValues(t, 30, h.messages[2].TimeTick())
			}
			require.Equal(t, opens, closes)
			require.False(t, h.closed)
			require.Len(t, opts.DeliverFilters, 2)
			require.True(t, options.GetFilterFunc(opts.DeliverFilters)(historyInsert(10, "v1", nil)), "caller filters must remain unchanged")
		})
	}
}

func TestHistoricalReaderStartAfterStillObservesBoundary(t *testing.T) {
	for _, start := range []message.ImmutableMessage{historyInsert(10, "v1", nil), historyAlter(20)} {
		r := newHistoricalTestReader(t)
		r.ExcludeMessageID = start.MessageID()
		h := &historicalTestHandler{}
		input := historyInput(start)
		if start.MessageType() != message.MessageTypeAlterWAL {
			input = historyInput(start, historyAlter(20))
		}
		cut, err := r.Read(t.Context(), input, h)
		require.NoError(t, err)
		require.EqualValues(t, 20, cut)
		require.Empty(t, h.messages)
	}
}

func TestHistoricalReaderLegacyMessagesShareTimeTick(t *testing.T) {
	r := newHistoricalTestReader(t)
	h := &historicalTestHandler{}
	input := historyInput()
	input.ch = make(chan message.ImmutableMessage, 3)
	for _, id := range []int64{1, 2} {
		payload, err := proto.Marshal(&msgpb.InsertRequest{
			Base:         &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert, Timestamp: 10},
			CollectionID: 1, PartitionID: 2, SegmentID: 3, ShardName: "v1", NumRows: 1,
			Timestamps: []uint64{10}, RowIDs: []int64{id},
		})
		require.NoError(t, err)
		input.ch <- message.NewImmutableMesasge(walimplstest.NewTestMessageID(id), payload, map[string]string{})
	}
	input.ch <- historyAlter(20)
	cut, err := r.Read(t.Context(), input, h)
	require.NoError(t, err)
	require.EqualValues(t, 20, cut)
	require.Len(t, h.messages, 2, "legacy records sharing a TimeTick are distinct messages")
	for _, msg := range h.messages {
		require.Equal(t, message.VersionOld, msg.Version())
		require.EqualValues(t, 10, msg.TimeTick())
	}
}

func TestHistoricalReaderReassemblesChunksBeforeBoundary(t *testing.T) {
	r := newHistoricalTestReader(t)
	h := &historicalTestHandler{}
	mutable := message.NewDeleteMessageBuilderV1().WithVChannel("v1").WithHeader(&message.DeleteMessageHeader{}).
		WithBody(&msgpb.DeleteRequest{CollectionID: 1, Int64PrimaryKeys: []int64{1, 2, 3, 4, 5}}).MustBuildMutable().
		WithTimeTick(10).WithLastConfirmed(walimplstest.NewTestMessageID(0))
	parts := message.SplitIntoChunks(mutable, 3)
	require.Greater(t, len(parts), 1)
	input := historyInput()
	input.ch = make(chan message.ImmutableMessage, len(parts)+1)
	for i, part := range parts {
		input.ch <- part.IntoImmutableMessage(walimplstest.NewTestMessageID(int64(i + 1)))
	}
	input.ch <- historyAlter(20)
	_, err := r.Read(t.Context(), input, h)
	require.NoError(t, err)
	require.Len(t, h.messages, 1)
	require.Equal(t, mutable.IntoMessageProto().Payload, h.messages[0].Payload())
}

func TestHistoricalReaderRejectsCorruptChunks(t *testing.T) {
	r := newHistoricalTestReader(t)
	corrupted := message.NewMutableMessageBeforeAppend([]byte("broken"), map[string]string{
		"_t": "x", "_tt": "!", "_ci": "0", "_ct": "2", "_v": message.VersionV1.String(),
	}).IntoImmutableMessage(walimplstest.NewTestMessageID(1))
	cut, err := r.Read(t.Context(), historyInput(corrupted, historyAlter(20)), &historicalTestHandler{})
	require.Zero(t, cut)
	require.Error(t, err)
	require.True(t, status.AsStreamingError(err).IsUnrecoverable())
}

func TestHistoricalReaderReadsStandaloneRocksMQAfterWriterClose(t *testing.T) {
	paramtable.Init()
	oldRole := paramtable.GetRole()
	paramtable.SetRole(typeutil.StandaloneRole)
	t.Cleanup(func() { paramtable.SetRole(oldRole) })
	rmq, err := server.NewRocksMQ(t.TempDir())
	require.NoError(t, err)
	previous := server.Rmq
	server.Rmq = rmq
	defer func() { server.Rmq = previous; rmq.Close() }()
	opener, err := registry.MustGetBuilder(message.WALNameRocksmq).Build()
	require.NoError(t, err)
	defer opener.Close()
	channel := types.PChannelInfo{Name: "historical-boundary-no-tt", AccessMode: types.AccessModeRW}
	writer, err := opener.Open(t.Context(), &walimpls.OpenOption{Channel: channel})
	require.NoError(t, err)
	var firstID message.MessageID
	for _, tick := range []uint64{20, 10} {
		msg := message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(&message.InsertMessageHeader{}).
			WithBody(&msgpb.InsertRequest{}).MustBuildMutable().WithTimeTick(tick).WithLastConfirmedUseMessageID()
		id, err := writer.Append(t.Context(), msg)
		require.NoError(t, err)
		if firstID == nil {
			firstID = id
		}
	}
	marker := message.NewAlterWALMessageBuilderV2().WithAllVChannel().WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker}).WithBody(&message.AlterWALMessageBody{}).MustBuildMutable().WithTimeTick(30).WithLastConfirmedUseMessageID()
	_, err = writer.Append(t.Context(), marker)
	require.NoError(t, err)
	writer.Close()
	h := &historicalTestHandler{}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	cut, err := ReadHistoricalWAL(ctx, HistoricalReadOptions{
		PChannel: channel.Name, VChannel: "v1", DeliverPolicy: options.DeliverPolicyStartFrom(firstID), MessageHandler: h,
	})
	require.NoError(t, err)
	require.EqualValues(t, 30, cut)
	require.Len(t, h.messages, 2)
	require.EqualValues(t, 10, h.messages[0].TimeTick())
	require.EqualValues(t, 20, h.messages[1].TimeTick())
	require.False(t, h.closed)
}

func TestHistoricalReaderConcurrentRocksMQWritesEndAtAlterWAL(t *testing.T) {
	paramtable.Init()
	oldRole := paramtable.GetRole()
	paramtable.SetRole(typeutil.StandaloneRole)
	t.Cleanup(func() { paramtable.SetRole(oldRole) })
	rmq, err := server.NewRocksMQ(t.TempDir())
	require.NoError(t, err)
	previous := server.Rmq
	server.Rmq = rmq
	defer func() { server.Rmq = previous; rmq.Close() }()
	opener, err := registry.MustGetBuilder(message.WALNameRocksmq).Build()
	require.NoError(t, err)
	defer opener.Close()
	channel := types.PChannelInfo{Name: "concurrent-historical-boundary", AccessMode: types.AccessModeRW}
	writer, err := opener.Open(t.Context(), &walimpls.OpenOption{Channel: channel})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	h := &historicalTestHandler{}
	type outcome struct {
		cut uint64
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		cut, err := ReadHistoricalWAL(ctx, HistoricalReadOptions{
			PChannel: channel.Name, VChannel: "v1", DeliverPolicy: options.DeliverPolicyStartFrom(rmqwal.NewRmqID(-1)), MessageHandler: h,
		})
		done <- outcome{cut, err}
	}()
	var tick atomic.Uint64
	var producers sync.WaitGroup
	appendErrors := make(chan error, 4)
	for range 4 {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for i := range 16 {
				msg := message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(&message.InsertMessageHeader{}).
					WithBody(&msgpb.InsertRequest{}).MustBuildMutable().WithTimeTick(tick.Add(1)).WithLastConfirmedUseMessageID()
				if _, err := writer.Append(ctx, msg); err != nil {
					appendErrors <- err
					return
				}
				if i%2 == 0 {
					msg := message.NewDeleteMessageBuilderV1().WithVChannel("v1").WithHeader(&message.DeleteMessageHeader{}).
						WithBody(&msgpb.DeleteRequest{}).MustBuildMutable().WithTimeTick(tick.Add(1)).WithLastConfirmedUseMessageID()
					if _, err := writer.Append(ctx, msg); err != nil {
						appendErrors <- err
						return
					}
				}
			}
		}()
	}
	producers.Wait()
	close(appendErrors)
	for err := range appendErrors {
		require.NoError(t, err)
	}
	select {
	case result := <-done:
		t.Fatalf("reader completed before AlterWAL: %+v", result)
	default:
	}
	cut := tick.Add(1)
	marker := message.NewAlterWALMessageBuilderV2().WithAllVChannel().
		WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker}).
		WithBody(&message.AlterWALMessageBody{}).MustBuildMutable().WithTimeTick(cut).WithLastConfirmedUseMessageID()
	_, err = writer.Append(ctx, marker)
	require.NoError(t, err)
	writer.Close()
	select {
	case result := <-done:
		require.NoError(t, result.err)
		require.Equal(t, cut, result.cut)
	case <-ctx.Done():
		t.Fatal("reader did not finish at AlterWAL")
	}
	require.Len(t, h.messages, 96)
	counts := make(map[message.MessageType]int)
	for i, msg := range h.messages {
		require.EqualValues(t, i+1, msg.TimeTick(), "every concurrent write must be delivered once in order")
		counts[msg.MessageType()]++
	}
	require.Equal(t, 64, counts[message.MessageTypeInsert])
	require.Equal(t, 32, counts[message.MessageTypeDelete])
	require.False(t, h.closed)
}
