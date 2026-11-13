package recovery

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/wp"
)

func TestAlterWALSummaryGatesPublishedCheckpoint(t *testing.T) {
	for _, hot := range []bool{false, true} {
		for _, replay := range []bool{false, true} {
			t.Run(map[bool]string{false: "legacy/", true: "hot/"}[hot]+map[bool]string{false: "live", true: "startup_replay"}[replay], func(t *testing.T) {
				resource.InitForTest(t)
				ctx := context.Background()
				rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{
					MessageID: walimplstest.NewTestMessageID(1), TimeTick: 10,
				})
				defer rs.metrics.Close()
				rs.SetLogger(mlog.With())
				store := walsummary.NewStore(storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir())), rs.channel.Name, 1)
				summary := walsummary.NewManager(walsummary.ManagerConfig{
					PChannel: rs.channel.Name, Term: 1, Store: store, FlushMaxBytes: 1 << 30,
					Runtime: moduleapi.Runtime{Scheduler: rs.taskScheduler},
				})
				summary.InitLastAcked(10)
				rs.summaryManager = summary
				// Isolate the Summary gate: all VChannel work is already complete.
				observe := mockey.Mock((*vchannel.PChannelRecoveryManager).ObserveMessage).Return().Build()
				defer observe.UnPatch()
				var failSummary atomic.Bool
				failSummary.Store(true)
				origin := (*walsummary.Store).WriteManifest
				write := mockey.Mock((*walsummary.Store).WriteManifest).Origin(&origin).To(func(s *walsummary.Store, ctx context.Context, manifest *streamingpb.PChannelSummaryManifest) error {
					if failSummary.Load() {
						return context.DeadlineExceeded
					}
					return origin(s, ctx, manifest)
				}).Build()
				defer write.UnPatch()
				// Stop tasks before removing patches used by the scheduler workers.
				defer rs.taskScheduler.Close()
				alter := message.NewAlterWALMessageBuilderV2().WithVChannel("").
					WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker}).
					WithBody(&message.AlterWALMessageBody{}).MustBuildMutable().WithTimeTick(30).
					WithLastConfirmed(walimplstest.NewTestMessageID(3))
				if hot {
					message.SetWALSwitchStart(alter, wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 0}))
				}
				immutable := alter.IntoImmutableMessage(walimplstest.NewTestMessageID(4))
				expectedStage := streamingpb.AlterWALStage_FLUSHING
				if hot {
					expectedStage = streamingpb.AlterWALStage_NONE
				}
				messages := []message.ImmutableMessage{newRecoveryTestDeleteMessage(t, "v1", 20), immutable}
				if replay {
					barrier := newAckTestRecoveryBarrier(t, 40, 5)
					stream := &blockingRecoveryStream{ch: make(chan message.ImmutableMessage, 3), closed: make(chan struct{})}
					defer stream.Close()
					for _, msg := range append(messages, barrier) {
						stream.ch <- msg
					}
					var builder RecoveryStreamBuilder = &recordingRecoveryStreamBuilder{stream: stream}
					if hot {
						targetBarrier := recoveryProbe(50, wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 1}))
						target := recoveryLeg(t, targetBarrier)
						builder = &physicalRecoveryBuilder{}
						leg := 0
						build := mockey.Mock((*physicalRecoveryBuilder).Build).To(func(BuildRecoveryStreamParam) RecoveryStream {
							leg++
							if leg == 1 {
								return stream
							}
							return target
						}).Build()
						defer build.UnPatch()
						switchWAL := mockey.Mock((*physicalRecoveryBuilder).SwitchWAL).Return(targetBarrier, nil).Build()
						defer switchWAL.UnPatch()
					}
					snapshot, err := rs.runBoundedRecovery(ctx, builder, barrier)
					require.NoError(t, err)
					require.Equal(t, expectedStage, snapshot.PChannelControl.GetAlterWalState().GetStage())
				} else {
					for _, msg := range messages {
						rs.observeMessage(ctx, msg)
					}
				}
				require.GreaterOrEqual(t, rs.ackTracker.CompletedPoint().TimeTick, uint64(30))
				require.Equal(t, uint64(10), summary.LastAcked())
				require.Equal(t, uint64(10), rs.GetCheckpoint(ctx).TimeTick)
				var failCatalog bool
				save := mockey.Mock((*recoveryStorageImpl).saveRecoverySnapshot).To(func(_ *recoveryStorageImpl, _ context.Context, _ *metastore.WALRecoverySnapshot) error {
					if failCatalog {
						return context.DeadlineExceeded
					}
					return nil
				}).Build()
				defer save.UnPatch()
				require.NoError(t, rs.persistDirtySnapshot(ctx, mlog.DebugLevel))
				require.Equal(t, uint64(10), rs.GetCheckpoint(ctx).TimeTick,
					"control state may publish but failed Summary still pins the global position")
				failSummary.Store(false)
				require.Eventually(t, func() bool { return summary.LastAcked() >= 30 && !summary.HasPendingWork() },
					10*time.Second, time.Millisecond)
				require.Equal(t, uint64(10), rs.GetCheckpoint(ctx).TimeTick, "Summary completion alone is not catalog publication")
				failCatalog = true
				require.ErrorIs(t, rs.persistDirtySnapshot(ctx, mlog.DebugLevel), context.DeadlineExceeded)
				require.Equal(t, uint64(10), rs.GetCheckpoint(ctx).TimeTick)
				failCatalog = false
				require.NoError(t, rs.persistDirtySnapshot(ctx, mlog.DebugLevel))
				require.GreaterOrEqual(t, rs.GetCheckpoint(ctx).TimeTick, uint64(30))
				require.Equal(t, expectedStage, rs.GetCheckpoint(ctx).AlterWalState.GetStage(),
					"the opener, not Summary, owns the remaining WAL-switch stages")
			})
		}
	}
}
