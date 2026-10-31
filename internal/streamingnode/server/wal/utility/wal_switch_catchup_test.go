package utility

import (
	"context"
	"strings"
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

type catchUpTestScanner struct {
	walimpls.ScannerImpls
	ch  chan message.ImmutableMessage
	err error
}

func (s *catchUpTestScanner) Chan() <-chan message.ImmutableMessage { return s.ch }
func (s *catchUpTestScanner) Error() error                          { return s.err }
func (s *catchUpTestScanner) Close() error                          { return nil }

type catchUpTestOpener struct {
	walimpls.OpenerImpls
	open func(*walimpls.OpenOption) (walimpls.WALImpls, error)
}

func (o catchUpTestOpener) Open(_ context.Context, opt *walimpls.OpenOption) (walimpls.WALImpls, error) {
	return o.open(opt)
}

type catchUpTestWAL struct {
	walimpls.WALImpls
	read  func(walimpls.ReadOption) (walimpls.ScannerImpls, error)
	close func()
}

func (w catchUpTestWAL) WALName() message.WALName { return message.WALNameTest }
func (w catchUpTestWAL) Read(_ context.Context, opt walimpls.ReadOption) (walimpls.ScannerImpls, error) {
	return w.read(opt)
}
func (w catchUpTestWAL) Close() { w.close() }

type catchUpTestHandler struct {
	messages []message.ImmutableMessage
	closed   bool
	handle   func(message.HandleParam) message.HandleResult
}

func (h *catchUpTestHandler) Handle(p message.HandleParam) message.HandleResult {
	if h.handle != nil {
		return h.handle(p)
	}
	h.messages = append(h.messages, p.Message)
	return message.HandleResult{MessageHandled: true}
}
func (h *catchUpTestHandler) Close() { h.closed = true }

func newCatchUpTestReader(t *testing.T, filters ...options.DeliverFilter) *walSwitchCatchUpReader {
	t.Helper()
	metrics := metricsutil.NewScanMetrics(types.PChannelInfo{Name: "p"}).NewScannerMetrics()
	t.Cleanup(metrics.Close)
	return newWALSwitchCatchUpReader("p", "v1", mlog.With(), metrics, filters)
}

func catchUpMessage(msg message.MutableMessage, ts uint64) message.ImmutableMessage {
	return msg.WithTimeTick(ts).WithLastConfirmed(walimplstest.NewTestMessageID(0)).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(ts)))
}

func catchUpInsert(ts uint64, vchannel string, txn *message.TxnContext) message.ImmutableMessage {
	msg := message.NewInsertMessageBuilderV1().WithVChannel(vchannel).
		WithHeader(&message.InsertMessageHeader{}).WithBody(&msgpb.InsertRequest{}).MustBuildMutable()
	if txn != nil {
		msg = msg.WithTxnContext(*txn)
	}
	return catchUpMessage(msg, ts)
}

func catchUpAlterWAL(ts uint64) message.ImmutableMessage {
	return catchUpMessage(message.NewAlterWALMessageBuilderV2().WithAllVChannel().
		WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker}).
		WithBody(&message.AlterWALMessageBody{}).MustBuildMutable(), ts)
}

func newCatchUpTestInput(messages ...message.ImmutableMessage) *catchUpTestScanner {
	ch := make(chan message.ImmutableMessage, len(messages))
	for _, msg := range messages {
		ch <- msg
	}
	// Deliberately keep the channel open: completion cannot depend on EOF or TT.
	return &catchUpTestScanner{ch: ch}
}

func TestWALSwitchCatchUpStopsAtAlterWALWithoutTrailingTimeTick(t *testing.T) {
	r := newCatchUpTestReader(t, options.DeliverFilterTimeTickGT(10), options.DeliverFilterMessageType(message.MessageTypeInsert))
	h := &catchUpTestHandler{}
	first, second := catchUpInsert(20, "v1", nil), catchUpInsert(30, "v1", nil)
	input := newCatchUpTestInput(second, catchUpInsert(25, "other", nil), catchUpInsert(5, "v1", nil), first, first, catchUpAlterWAL(40), catchUpInsert(50, "v1", nil))
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	alterWALTimeTick, err := r.Read(ctx, input, h)
	require.NoError(t, err)
	require.EqualValues(t, 40, alterWALTimeTick)
	require.Equal(t, []message.ImmutableMessage{first, second}, h.messages)
	require.Empty(t, r.TxnBuffer.GetUncommittedMessageBuilder())
	require.Zero(t, r.PendingQueue.Len())
	require.Len(t, input.ch, 1, "must stop at AlterWAL, without reading its suffix")
	require.False(t, h.closed)
}

func TestWALSwitchCatchUpAssemblesCommittedTransactionsAndAbortsUnfinished(t *testing.T) {
	r := newCatchUpTestReader(t)
	h := &catchUpTestHandler{}
	committed := message.TxnContext{TxnID: 1, Keepalive: message.TxnKeepaliveInfinite}
	unfinished := message.TxnContext{TxnID: 2, Keepalive: message.TxnKeepaliveInfinite}
	begin := func(txn message.TxnContext, ts uint64) message.ImmutableMessage {
		return catchUpMessage(message.NewBeginTxnMessageBuilderV2().WithVChannel("v1").
			WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).
			MustBuildMutable().WithTxnContext(txn), ts)
	}
	commit := catchUpMessage(message.NewCommitTxnMessageBuilderV2().WithVChannel("v1").
		WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).
		MustBuildMutable().WithTxnContext(committed), 30)
	input := newCatchUpTestInput(commit, begin(unfinished, 21), catchUpInsert(22, "v1", &unfinished), catchUpInsert(12, "v1", &committed), begin(committed, 11), catchUpAlterWAL(40))
	alterWALTimeTick, err := r.Read(t.Context(), input, &catchUpProgressHandler{Handler: h, walSwitchCatchUpStats: r.stats})
	require.NoError(t, err)
	require.EqualValues(t, 40, alterWALTimeTick)
	require.Len(t, h.messages, 1)
	require.Equal(t, message.MessageTypeTxn, h.messages[0].MessageType())
	require.EqualValues(t, 30, h.messages[0].TimeTick())
	require.Empty(t, r.TxnBuffer.GetUncommittedMessageBuilder())
	require.EqualValues(t, 6, r.stats.readMessages)
	require.EqualValues(t, 1, r.stats.handledMessages, "a committed transaction is one logical delivery; unfinished transactions are not delivered")
}

func TestWALSwitchCatchUpWaitsForDeliveryAndHonorsCancellation(t *testing.T) {
	r := newCatchUpTestReader(t)
	entered := make(chan struct{})
	h := &catchUpTestHandler{handle: func(p message.HandleParam) message.HandleResult {
		close(entered)
		<-p.Ctx.Done()
		return message.HandleResult{Error: p.Ctx.Err()}
	}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		alterWALTimeTick, err := r.Read(ctx, newCatchUpTestInput(catchUpInsert(10, "v1", nil), catchUpAlterWAL(20)), h)
		if alterWALTimeTick != 0 {
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

func TestWALSwitchCatchUpFailsWithoutAlterWAL(t *testing.T) {
	expected := errors.New("backend read failed")
	for _, err := range []error{nil, expected} {
		r := newCatchUpTestReader(t)
		input := newCatchUpTestInput(catchUpInsert(10, "v1", nil))
		input.err = err
		close(input.ch)
		alterWALTimeTick, got := r.Read(t.Context(), input, &catchUpTestHandler{})
		require.Zero(t, alterWALTimeTick)
		require.Error(t, got)
		if err != nil {
			require.ErrorIs(t, got, expected)
		} else {
			require.True(t, status.AsStreamingError(got).IsUnrecoverable())
		}
	}
}

func TestWALSwitchCatchUpRetriesFromHandledProgress(t *testing.T) {
	paramtable.Init()
	for _, cancelRetry := range []bool{false, true} {
		name := "resume_without_replaying_handled_messages"
		if cancelRetry {
			name = "cancel_during_retry_backoff"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			h := &catchUpTestHandler{}
			if cancelRetry {
				h.handle = func(p message.HandleParam) message.HandleResult {
					h.messages = append(h.messages, p.Message)
					if p.Message.TimeTick() == 20 {
						time.AfterFunc(20*time.Millisecond, cancel)
					}
					return message.HandleResult{MessageHandled: true}
				}
			}
			tick := catchUpMessage(message.NewTimeTickMessageBuilderV1().WithAllVChannel().
				WithHeader(&message.TimeTickMessageHeader{}).WithBody(&msgpb.TimeTickMsg{}).MustBuildMutable(), 20)
			opens, closes := 0, 0
			opener := catchUpTestOpener{open: func(opt *walimpls.OpenOption) (walimpls.WALImpls, error) {
				opens++
				require.Equal(t, types.AccessModeRO, opt.Channel.AccessMode)
				return catchUpTestWAL{close: func() { closes++ }, read: func(opt walimpls.ReadOption) (walimpls.ScannerImpls, error) {
					require.Equal(t, options.DeliverPolicyStartFrom(walimplstest.NewTestMessageID(0)), opt.DeliverPolicy)
					if opens == 1 {
						input := newCatchUpTestInput(catchUpInsert(10, "v1", nil), tick)
						input.err = status.NewUnrecoverableError("history failed after delivering a prefix")
						close(input.ch)
						return input, nil
					}
					return newCatchUpTestInput(catchUpInsert(10, "v1", nil), tick, catchUpInsert(30, "v1", nil), catchUpAlterWAL(40)), nil
				}}, nil
			}}
			opts := WALSwitchCatchUpOptions{
				PChannel: "p", VChannel: "v1", MessageHandler: h,
				DeliverPolicy:  options.DeliverPolicyStartFrom(walimplstest.NewTestMessageID(0)),
				DeliverFilters: []options.DeliverFilter{options.DeliverFilterTimeTickGT(1), options.DeliverFilterMessageType(message.MessageTypeInsert)},
			}
			stats := &walSwitchCatchUpStats{}
			alterWALTimeTick, err := catchUpWALWithRetry(ctx, opts, nil, opener, stats)
			if cancelRetry {
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, alterWALTimeTick)
				require.Equal(t, 1, opens)
				require.Len(t, h.messages, 2)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, 40, alterWALTimeTick)
				require.Equal(t, 2, opens)
				require.Len(t, h.messages, 3, "retry must not repeat the handled prefix")
				require.EqualValues(t, 30, h.messages[2].TimeTick())
				require.EqualValues(t, 6, stats.readMessages, "include records reread and filtered on retry")
				require.EqualValues(t, 3, stats.handledMessages, "exclude retry duplicates and AlterWAL")
				require.Greater(t, stats.readBytes, stats.handledBytes)
				require.Positive(t, stats.handledBytes)
				require.Equal(t, catchUpAlterWAL(40).MessageID(), stats.endMessageID)
			}
			require.Equal(t, opens, stats.attempts)
			require.Equal(t, opens, closes)
			require.False(t, h.closed)
			require.Len(t, opts.DeliverFilters, 2)
			require.True(t, options.GetFilterFunc(opts.DeliverFilters)(catchUpInsert(10, "v1", nil)), "caller filters must remain unchanged")
		})
	}
}

func TestWALSwitchCatchUpStartAfterStillReadsAlterWAL(t *testing.T) {
	for _, start := range []message.ImmutableMessage{catchUpInsert(10, "v1", nil), catchUpAlterWAL(20)} {
		r := newCatchUpTestReader(t)
		r.StartAfterMessageID = start.MessageID()
		h := &catchUpTestHandler{}
		input := newCatchUpTestInput(start)
		if start.MessageType() != message.MessageTypeAlterWAL {
			input = newCatchUpTestInput(start, catchUpAlterWAL(20))
		}
		alterWALTimeTick, err := r.Read(t.Context(), input, h)
		require.NoError(t, err)
		require.EqualValues(t, 20, alterWALTimeTick)
		require.Empty(t, h.messages)
	}
}

func TestWALSwitchCatchUpPreservesLegacyMessagesSharingTimeTick(t *testing.T) {
	r := newCatchUpTestReader(t)
	h := &catchUpTestHandler{}
	input := newCatchUpTestInput()
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
	input.ch <- catchUpAlterWAL(20)
	alterWALTimeTick, err := r.Read(t.Context(), input, h)
	require.NoError(t, err)
	require.EqualValues(t, 20, alterWALTimeTick)
	require.Len(t, h.messages, 2, "legacy records sharing a TimeTick are distinct messages")
	for _, msg := range h.messages {
		require.Equal(t, message.VersionOld, msg.Version())
		require.EqualValues(t, 10, msg.TimeTick())
	}
}

func TestWALSwitchCatchUpReadsRocksMQHistoryAfterWriterClose(t *testing.T) {
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
	markerID, err := writer.Append(t.Context(), marker)
	require.NoError(t, err)
	writer.Close()
	h := &catchUpTestHandler{}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "info", DisableTimestamp: true})
	alterWALTimeTick, err := CatchUpWALAfterSwitch(ctx, WALSwitchCatchUpOptions{
		PChannel: channel.Name, VChannel: "v1", DeliverPolicy: options.DeliverPolicyStartFrom(firstID), MessageHandler: h,
	})
	require.NoError(t, err)
	require.EqualValues(t, 30, alterWALTimeTick)
	require.Len(t, h.messages, 2)
	require.EqualValues(t, 10, h.messages[0].TimeTick())
	require.EqualValues(t, 20, h.messages[1].TimeTick())
	require.False(t, h.closed)
	logs := sink.String()
	require.Contains(t, logs, "[INFO]")
	require.Contains(t, logs, "historical WAL catch-up started")
	var finished string
	for _, line := range strings.Split(logs, "\n") {
		if strings.Contains(line, "historical WAL catch-up finished") {
			finished = line
		}
	}
	for _, field := range []string{
		"[catchUpID=", "[pchannel=" + channel.Name + "]", "[vchannel=v1]", "[walName=rocksmq]",
		"[startMessageID=" + firstID.String() + "]", "[startAfter=false]", "[completed=true]",
		"[endMessageID=" + markerID.String() + "]", "[endTimeTick=30]", "[lastReadMessageID=" + markerID.String() + "]",
		"[readMessages=3]", "[handledMessages=2]", "[estimatedReadBytes=", "[estimatedHandledBytes=", "[attempts=1]", "[elapsed=",
	} {
		require.Contains(t, finished, field)
	}
}

func TestWALSwitchCatchUpLogsFailureBeforeOpeningHistory(t *testing.T) {
	paramtable.Init()
	oldRole := paramtable.GetRole()
	paramtable.SetRole(typeutil.QueryNodeRole)
	t.Cleanup(func() { paramtable.SetRole(oldRole) })
	sink := mlog.CaptureGlobalLogs(t, &mlog.Config{Level: "info", DisableTimestamp: true})
	_, err := CatchUpWALAfterSwitch(t.Context(), WALSwitchCatchUpOptions{
		PChannel: "unavailable-history", VChannel: "v1", DeliverPolicy: options.DeliverPolicyStartAfter(rmqwal.NewRmqID(10)),
	})
	require.Error(t, err)
	logs := sink.String()
	for _, field := range []string{
		"historical WAL catch-up started", "historical WAL catch-up finished",
		"[startAfter=true]", "[completed=false]", "[readMessages=0]", "[handledMessages=0]", "[attempts=0]", "[error=", "[elapsed=",
	} {
		require.Contains(t, logs, field)
	}
	require.NotContains(t, logs, "[endMessageID=")
}

func TestWALSwitchCatchUpOrdersConcurrentRocksMQWritesThroughAlterWAL(t *testing.T) {
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
	h := &catchUpTestHandler{}
	type outcome struct {
		alterWALTimeTick uint64
		err              error
	}
	done := make(chan outcome, 1)
	go func() {
		alterWALTimeTick, err := CatchUpWALAfterSwitch(ctx, WALSwitchCatchUpOptions{
			PChannel: channel.Name, VChannel: "v1", DeliverPolicy: options.DeliverPolicyStartFrom(rmqwal.NewRmqID(-1)), MessageHandler: h,
		})
		done <- outcome{alterWALTimeTick, err}
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
	alterWALTimeTick := tick.Add(1)
	marker := message.NewAlterWALMessageBuilderV2().WithAllVChannel().
		WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker}).
		WithBody(&message.AlterWALMessageBody{}).MustBuildMutable().WithTimeTick(alterWALTimeTick).WithLastConfirmedUseMessageID()
	_, err = writer.Append(ctx, marker)
	require.NoError(t, err)
	writer.Close()
	select {
	case result := <-done:
		require.NoError(t, result.err)
		require.Equal(t, alterWALTimeTick, result.alterWALTimeTick)
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
