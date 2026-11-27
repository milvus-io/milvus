package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

// Keep the real write-side MVCC, pack adaptor, pipeline and delegator wait.
// Only the transport and the application operation are patched. Data-result
// correctness is additionally exercised by the SDK E2E suite.
func TestRetainedMVCCPipelineWait(t *testing.T) {
	for _, operation := range []string{"insert", "delete", "schema"} {
		t.Run(operation, func(t *testing.T) {
			mockey.PatchConvey("retained target waits for application", t, func() {
				paramtable.Init()
				const collectionID int64 = 54366
				const channel = "retained_54366v0"
				// Shift the logical 90/100/120/140/300 sequence into the current
				// physical clock so the real delegator can finish startup catchup.
				base := tsoutil.ComposeTSByTime(time.Now())
				ts := func(n uint64) uint64 { return base + n }
				schema := mock_segcore.GenTestCollectionSchema("retained", schemapb.DataType_Int64, false)
				manager := segments.NewManager()
				require.NoError(t, manager.Collection.PutOrRef(collectionID, schema, &segcorepb.CollectionIndexMeta{}, &querypb.LoadMetaInfo{LoadType: querypb.LoadType_LoadCollection, PartitionIDs: []int64{1}}))
				defer manager.Collection.Unref(collectionID, 1)
				sd, err := delegator.NewShardDelegator(context.Background(), collectionID, 1, channel, 1, nil, manager, nil, ts(99), nil, nil, delegator.NewChannelQueryView(nil, nil, nil, -1), nil)
				require.NoError(t, err)
				sd.Start()
				defer sd.Close()
				sd.UpdateTSafe(ts(100))
				require.False(t, sd.CatchingUpStreamingData())
				cm := mvcc.NewMVCCManager(ts(100))
				cm.ApplyRecoveryBarrier(channel, ts(100))
				oldWAL := streaming.WAL()
				streaming.SetupNoopWALForTest()
				defer streaming.SetWALForTest(oldWAL)
				mockey.Mock(mockey.GetMethod(streaming.WAL().Local(), "GetLatestMVCCTimestampIfLocal")).To(func(_ context.Context, vc string) (uint64, error) {
					return cm.GetMVCCOfVChannel(vc).Timetick, nil
				}).Build()
				query := func(guarantee uint64, timeout time.Duration) (*querypb.QueryRequest, error) {
					ctx, cancel := context.WithTimeout(context.Background(), timeout)
					defer cancel()
					req := &querypb.QueryRequest{DmlChannels: []string{channel}, Req: &internalpb.RetrieveRequest{CollectionID: collectionID, ConsistencyLevel: commonpb.ConsistencyLevel_Strong, GuaranteeTimestamp: guarantee}}
					_, err := sd.Query(ctx, req)
					return req, err
				}
				seed, err := query(ts(90), time.Second)
				require.Error(t, err) // no query distribution is installed
				require.Equal(t, ts(100), seed.Req.MvccTimestamp)
				require.Equal(t, ts(90), sd.GetLatestRequiredMVCCTimeTick())

				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				block := func() { close(entered); <-release }
				var write, second message.MutableMessage
				target := ts(120)
				switch operation {
				case "insert":
					row := buildInsertMsg(collectionID, 1, 1, channel, 1)
					row.FieldsData = genFiledDataWithSchema(schema, 1)
					write = message.NewInsertMessageBuilderV1().WithVChannel(channel).WithHeader(&message.InsertMessageHeader{CollectionId: collectionID, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 1, SegmentAssignment: &message.SegmentAssignment{SegmentId: 1}}}}).WithBody(row.InsertRequest).MustBuildMutable().WithTimeTick(ts(120))
					mockey.Mock(mockey.GetMethod(sd, "ProcessInsert")).To(func(data map[int64]*delegator.InsertData) {
						require.Equal(t, []uint64{ts(120)}, data[1].Timestamps)
						block()
					}).Build()
				case "delete":
					target = ts(140)
					row := buildDeleteMsg(collectionID, 1, channel, 1)
					write = message.NewDeleteMessageBuilderV1().WithVChannel(channel).WithHeader(&message.DeleteMessageHeader{CollectionId: collectionID}).WithBody(row.DeleteRequest).MustBuildMutable().WithTimeTick(ts(120))
					second = message.NewDeleteMessageBuilderV1().WithVChannel(channel).WithHeader(&message.DeleteMessageHeader{CollectionId: collectionID}).WithBody(row.DeleteRequest).MustBuildMutable().WithTimeTick(ts(140)).WithLastConfirmed(rmq.NewRmqID(0))
					mockey.Mock(mockey.GetMethod(sd, "ProcessDeleteBatches")).To(func(batches []delegator.DeleteBatch) {
						require.Len(t, batches, 2)
						require.Equal(t, ts(140), batches[1].Ts)
						require.Equal(t, ts(120), batches[0].Ts)
						require.Equal(t, []uint64{ts(120)}, batches[0].Data[0].Timestamps)
						block()
					}).Build()
				case "schema":
					write = message.NewSchemaChangeMessageBuilderV2().WithVChannel(channel).WithHeader(&message.SchemaChangeMessageHeader{CollectionId: collectionID}).WithBody(&message.SchemaChangeMessageBody{Schema: schema}).MustBuildMutable().WithTimeTick(ts(120))
					mockey.Mock(mockey.GetMethod(sd, "UpdateSchema")).To(func(_ context.Context, got *schemapb.CollectionSchema, barrier uint64) error {
						require.Equal(t, ts(120), barrier)
						require.Equal(t, schema.GetName(), got.GetName())
						block()
						return nil
					}).Build()
				}
				write.WithLastConfirmed(rmq.NewRmqID(0))
				cm.UpdateMVCC(write)
				if second != nil {
					cm.UpdateMVCC(second)
				}
				tick := message.NewTimeTickMessageBuilderV1().WithAllVChannel().WithHeader(&message.TimeTickMessageHeader{}).WithBody(&message.TimeTickMsg{}).MustBuildMutable().WithTimeTick(ts(300))
				cm.UpdateMVCC(tick)
				require.Equal(t, mvcc.VChannelMVCC{Timetick: target, Confirmed: true}, cm.GetMVCCOfVChannel(channel))
				input := make(chan *msgstream.MsgPack, 4)
				dispatcher := msgdispatcher.NewClient(nil, "retained-test", 54366)
				mockey.Mock(mockey.GetMethod(dispatcher, "Register")).Return((<-chan *msgstream.MsgPack)(input), nil).Build()
				mockey.Mock(mockey.GetMethod(dispatcher, "Deregister")).Return().Build()
				pipeline, err := NewPipeLine(manager.Collection.Get(collectionID), channel, manager, dispatcher, sd)
				require.NoError(t, err)
				require.NoError(t, pipeline.ConsumeMsgStream(context.Background(), &msgpb.MsgPosition{ChannelName: channel}))
				defer func() { unblock(); pipeline.Close() }()
				input <- &msgstream.MsgPack{BeginTs: ts(100), EndTs: ts(100)}
				pack, err := adaptor.NewMsgPackFromMessage(write.IntoImmutableMessage(rmq.NewRmqID(1)))
				require.NoError(t, err)
				require.Equal(t, ts(120), pack.EndTs)
				input <- pack
				if second != nil {
					pack, err = adaptor.NewMsgPackFromMessage(second.IntoImmutableMessage(rmq.NewRmqID(2)))
					require.NoError(t, err)
					input <- pack
				}
				require.NoError(t, pipeline.Start())
				select {
				case <-entered:
				case <-time.After(5 * time.Second):
					t.Fatal("application was not reached")
				}
				if operation == "insert" {
					// Original issue: finish the write, filter tick300 while
					// required90 is satisfied, THEN issue the next strong read.
					filtered := metrics.WALDelegatorEmptyTimeTickFilteredTotal.WithLabelValues(paramtable.GetStringNodeID(), funcutil.ToPhysicalChannel(channel))
					before := testutil.ToFloat64(filtered)
					input <- &msgstream.MsgPack{BeginTs: ts(300), EndTs: ts(300)}
					unblock()
					require.Eventually(t, func() bool { return testutil.ToFloat64(filtered) > before }, 5*time.Second, time.Millisecond)
				} else {
					// C=300 is already published on the write side, but hold its
					// empty consumer pack back. The data application alone must
					// wake the real wait without receiving any further tick.
					req, err := query(ts(1000), 30*time.Millisecond)
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.Zero(t, req.Req.MvccTimestamp)
					require.Equal(t, ts(100), sd.GetTSafe())
					type readResult struct {
						req *querypb.QueryRequest
						err error
					}
					pending := make(chan readResult, 1)
					go func() {
						req, err := query(ts(1000), 5*time.Second)
						pending <- readResult{req: req, err: err}
					}()
					select {
					case <-pending:
						t.Fatal("query passed before application completed")
					case <-time.After(20 * time.Millisecond):
					}
					unblock()
					select {
					case result := <-pending:
						require.Error(t, result.err) // absent distribution, after wait
						require.NotErrorIs(t, result.err, context.DeadlineExceeded)
						require.Equal(t, target, result.req.Req.MvccTimestamp)
					case <-time.After(5 * time.Second):
						t.Fatal("application did not wake query")
					}
				}
				// A cluster-wide flush changes storage state, not the visible rows.
				// The legacy scanner does not deliver it to this pipeline, so it
				// must not force the next read to wait for another consumer tick.
				pchannel := funcutil.ToPhysicalChannel(channel)
				flush := message.NewFlushAllMessageBuilderV2().
					WithHeader(&message.FlushAllMessageHeader{}).
					WithBody(&message.FlushAllMessageBody{}).
					WithClusterLevelBroadcast(message.ClusterChannels{Channels: []string{pchannel}, ControlChannel: pchannel + "_vcchan"}).
					MustBuildBroadcast()
				for _, msg := range flush.WithBroadcastID(1).SplitIntoMutableMessage() {
					cm.UpdateMVCC(msg.WithTimeTick(ts(200)))
				}
				// No next tick is supplied. The real wait passes at the applied target; the deliberately
				// absent query distribution then rejects execution AFTER setting MVCC.
				req, err := query(ts(1000), time.Second)
				require.Error(t, err)
				require.NotErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, target, req.Req.GuaranteeTimestamp)
				require.Equal(t, target, req.Req.MvccTimestamp)
				require.Equal(t, target, sd.GetTSafe())
				// The delegator subscription excludes these storage-only messages.
				// Feed them to the real write-side manager, but not the consumer.
				// Neither may introduce another tSafe wait after data application.
				for _, storage := range []message.MutableMessage{
					message.NewCreateSegmentMessageBuilderV2().WithVChannel(channel).
						WithHeader(&message.CreateSegmentMessageHeader{CollectionId: collectionID, PartitionId: 1, SegmentId: 2}).
						WithBody(&message.CreateSegmentMessageBody{}).MustBuildMutable().WithTimeTick(ts(210)),
					message.NewFlushMessageBuilderV2().WithVChannel(channel).
						WithHeader(&message.FlushMessageHeader{CollectionId: collectionID, PartitionId: 1, SegmentId: 1}).
						WithBody(&message.FlushMessageBody{}).MustBuildMutable().WithTimeTick(ts(220)),
				} {
					cm.UpdateMVCC(storage)
					req, err = query(ts(1000), time.Second)
					require.Error(t, err) // no query distribution, after the real wait
					require.NotErrorIs(t, err, context.DeadlineExceeded, storage.MessageType().String())
					require.Equal(t, target, req.Req.GuaranteeTimestamp)
					require.Equal(t, target, req.Req.MvccTimestamp)
					require.Equal(t, target, sd.GetTSafe())
				}
			})
		})
	}
}

func TestRetainedMVCCTransactionPack(t *testing.T) {
	const channel = "retained_54366v0"
	cm := mvcc.NewMVCCManager(100)
	cm.ApplyRecoveryBarrier(channel, 100)
	txnCtx := message.TxnContext{TxnID: 1, Keepalive: time.Second}
	begin := message.NewBeginTxnMessageBuilderV2().WithVChannel(channel).WithHeader(&message.BeginTxnMessageHeader{}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable().WithTimeTick(80).WithTxnContext(txnCtx).WithLastConfirmed(rmq.NewRmqID(0))
	// These body timestamps predate the new recovery fence. They become visible
	// at the commit, and their row timestamps must be rewritten accordingly.
	insert := buildInsertMsg(54366, 1, 1, channel, 1)
	body := message.NewInsertMessageBuilderV1().WithVChannel(channel).WithHeader(&message.InsertMessageHeader{CollectionId: 54366, Partitions: []*message.PartitionSegmentAssignment{{PartitionId: 1, SegmentAssignment: &message.SegmentAssignment{SegmentId: 1}}}}).WithBody(insert.InsertRequest).MustBuildMutable().WithTimeTick(90).WithTxnContext(txnCtx).WithLastConfirmed(rmq.NewRmqID(0))
	deletion := buildDeleteMsg(54366, 1, channel, 1)
	deleted := message.NewDeleteMessageBuilderV1().WithVChannel(channel).WithHeader(&message.DeleteMessageHeader{CollectionId: 54366}).WithBody(deletion.DeleteRequest).MustBuildMutable().WithTimeTick(95).WithTxnContext(txnCtx).WithLastConfirmed(rmq.NewRmqID(0))
	commit := message.NewCommitTxnMessageBuilderV2().WithVChannel(channel).WithHeader(&message.CommitTxnMessageHeader{}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTimeTick(200).WithTxnContext(txnCtx).WithLastConfirmed(rmq.NewRmqID(0))
	for _, m := range []message.MutableMessage{begin, body, deleted} {
		cm.UpdateMVCC(m)
	}
	require.Equal(t, uint64(100), cm.GetMVCCOfVChannel(channel).Timetick)
	cm.UpdateMVCC(commit)
	txn, err := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(begin.IntoImmutableMessage(rmq.NewRmqID(1)))).Add(body.IntoImmutableMessage(rmq.NewRmqID(2))).Add(deleted.IntoImmutableMessage(rmq.NewRmqID(3))).Build(message.MustAsImmutableCommitTxnMessageV2(commit.IntoImmutableMessage(rmq.NewRmqID(4))))
	require.NoError(t, err)
	pack, err := adaptor.NewMsgPackFromMessage(txn)
	require.NoError(t, err)
	require.Equal(t, cm.GetMVCCOfVChannel(channel).Timetick, pack.EndTs)
	require.Equal(t, uint64(200), pack.EndTs)
	require.Len(t, pack.Msgs, 2)
	require.Equal(t, []uint64{200}, pack.Msgs[0].(*msgstream.InsertMsg).Timestamps)
	require.Equal(t, []uint64{200}, pack.Msgs[1].(*msgstream.DeleteMsg).Timestamps)
	for _, msg := range pack.Msgs {
		require.Equal(t, uint64(200), msg.EndTs())
	}
}
