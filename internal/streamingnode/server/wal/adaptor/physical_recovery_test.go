package adaptor

import (
	"context"
	"fmt"
	"testing"
	"time"

	pulsarclient "github.com/apache/pulsar-client-go/pulsar"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	wplog "github.com/zilliztech/woodpecker/woodpecker/log"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/replica"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/autowalimpls"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/wp"
)

type recoveryPhysicalWAL struct {
	walimpls.WALImpls
	name message.WALName
}

func (w *recoveryPhysicalWAL) WALName() message.WALName { return w.name }
func (*recoveryPhysicalWAL) Channel() types.PChannelInfo {
	return types.PChannelInfo{Name: "physical-recovery", Term: 1, AccessMode: types.AccessModeRW}
}
func (*recoveryPhysicalWAL) Close() { panic("mockey") }

func TestRecoveryBuilderWriterOwnership(t *testing.T) {
	for _, scenario := range []string{"success", "open_error", "barrier_error"} {
		t.Run(scenario, func(t *testing.T) {
			mockey.PatchConvey(scenario, t, func() {
				resource.InitForTest(t)
				old := &recoveryPhysicalWAL{name: message.WALNamePulsar}
				target := &recoveryPhysicalWAL{name: message.WALNameWoodpecker}
				closed := map[message.WALName]int{}
				mockey.Mock((*recoveryPhysicalWAL).Close).To(func(w *recoveryPhysicalWAL) { closed[w.name]++ }).Build()
				opens, probes := 0, 0
				auto := autowalimpls.New(old, func(_ context.Context, name message.WALName, mode types.AccessMode) (walimpls.WALImpls, error) {
					opens++
					require.Equal(t, target.name, name)
					require.Equal(t, types.AccessModeRW, mode)
					if scenario == "open_error" {
						return nil, context.DeadlineExceeded
					}
					return target, nil
				})
				ro := adaptImplsToROWAL(auto, func() {})
				resources := &walOpenResources{roWAL: ro}
				defer resources.Close()
				builder := newRecoveryStreamBuilder(ro, nil, make(chan struct{}))
				start := wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 0})
				barrier := recoveryTestMessage(t, message.MessageTypeRecoveryBarrier, 100).WithLastConfirmed(start).IntoImmutableMessage(wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 1}))
				mockey.Mock(sendFirstTimeTick).To(func(_ context.Context, w walimpls.WALImpls, last message.MessageID) (message.ImmutableMessage, error) {
					require.Equal(t, 1, opens, "writer acquired before allocating barrier")
					require.Same(t, auto, w)
					require.Equal(t, target.name, w.WALName())
					require.True(t, start.EQ(last))
					probes++
					if scenario == "barrier_error" {
						return nil, context.DeadlineExceeded
					}
					return barrier, nil
				}).Build()
				result, err := builder.SwitchWAL(context.Background(), start)
				if scenario != "success" {
					require.Error(t, err)
					require.Nil(t, result)
					require.Same(t, auto, builder.RWWALImpls())
					resources.Close()
					if scenario == "barrier_error" {
						require.Equal(t, 1, closed[target.name])
					} else {
						require.Zero(t, probes)
						require.Zero(t, closed[target.name])
					}
					require.Equal(t, 1, closed[old.name], "the opener closes all owned handles through auto")
					return
				}
				require.NoError(t, err)
				require.Same(t, barrier, result)
				require.Same(t, auto, builder.RWWALImpls())
				_, err = builder.SwitchWAL(context.Background(), start)
				require.NoError(t, err)
				require.Equal(t, 1, opens, "revisits reuse the owned writer")
				require.Equal(t, 2, probes, "every leg gets a fresh barrier")
				require.Equal(t, target.name, auto.WALName())
				resources.Release()
				resources.Close()
				require.Empty(t, closed)
				ro.Close()
				require.Equal(t, map[message.WALName]int{old.name: 1, target.name: 1}, closed)
			})
		})
	}
}

func TestRecoveryScannerStopsAtSwitchWithAuto(t *testing.T) {
	resource.InitForTest(t)
	l := newRecoveryTestWAL(t)
	first := appendRecoveryTestMessage(t, l, message.MessageTypeTimeTick, 10)
	target := wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 0})
	marker := message.NewAlterWALMessageBuilderV2().WithHeader(&message.AlterWALMessageHeader{TargetWalName: commonpb.WALName_WoodPecker}).WithBody(&message.AlterWALMessageBody{}).WithAllVChannel().MustBuildMutable().WithTimeTick(20).WithLastConfirmed(first.MessageID())
	message.SetWALSwitchStart(marker, target)
	for {
		if _, err := l.Append(context.Background(), marker); err == nil {
			break
		} else {
			require.EqualError(t, err, "random error")
		}
	}
	probe := appendRecoveryTestMessage(t, l, message.MessageTypeRecoveryBarrier, 100)
	auto := autowalimpls.New(l, func(ctx context.Context, _ message.WALName, mode types.AccessMode) (walimpls.WALImpls, error) {
		require.Equal(t, types.AccessModeRO, mode, "reader traversal must not acquire the next writer")
		<-ctx.Done()
		return nil, ctx.Err()
	})
	ro := adaptImplsToROWAL(auto, func() {})
	defer ro.Close()
	stream := newRecoveryStreamBuilder(ro, nil, make(chan struct{})).Build(recovery.BuildRecoveryStreamParam{
		StartCheckpoint: first.MessageID(), RecoveryBarrier: probe, StopAtWALSwitch: true,
	})
	defer stream.Close()
	require.EqualValues(t, 10, receiveRecoveryTestMessage(t, stream).TimeTick())
	require.EqualValues(t, 20, receiveRecoveryTestMessage(t, stream).TimeTick())
	select {
	case m := <-stream.Chan():
		t.Fatalf("old probe leaked past AlterWAL: %v", m)
	case <-time.After(30 * time.Millisecond):
	}
	require.Nil(t, stream.TxnBuffer(), "old probe must not capture final recovery snapshot")
}

type recoveryPhysicalOpener struct{ walimpls.OpenerImpls }

func (*recoveryPhysicalOpener) Open(context.Context, *walimpls.OpenOption) (walimpls.WALImpls, error) {
	panic("mockey")
}

func TestRecoveryBackendOpenerPreservesAccessMode(t *testing.T) {
	mockey.PatchConvey("historical reads never acquire a writer; open failures propagate", t, func() {
		original := types.PChannelInfo{Name: "physical-recovery", Term: 42, AccessMode: types.AccessModeRW}
		target := &recoveryPhysicalWAL{name: message.WALNameWoodpecker}
		physical := &recoveryPhysicalOpener{}
		resolve := mockey.Mock((*openerAdaptorImpl).getOrCreateOpenerImpl).To(func(_ *openerAdaptorImpl, _ context.Context, name message.WALName) (walimpls.OpenerImpls, error) {
			require.Equal(t, target.name, name)
			return physical, nil
		}).Build()
		var got types.PChannelInfo
		open := mockey.Mock((*recoveryPhysicalOpener).Open).To(func(_ context.Context, opt *walimpls.OpenOption) (walimpls.WALImpls, error) {
			got = opt.Channel
			return target, nil
		}).Build()
		route := (&openerAdaptorImpl{}).backendOpener(original)
		for _, mode := range []types.AccessMode{types.AccessModeRO, types.AccessModeRW} {
			result, err := route(context.Background(), target.name, mode)
			require.NoError(t, err)
			require.Same(t, target, result)
			expected := original
			expected.AccessMode = mode
			require.Equal(t, expected, got)
		}
		open.UnPatch()
		mockey.Mock((*recoveryPhysicalOpener).Open).Return(nil, context.Canceled).Build()
		_, err := route(context.Background(), target.name, types.AccessModeRW)
		require.ErrorIs(t, err, context.Canceled)
		resolve.UnPatch()
		mockey.Mock((*openerAdaptorImpl).getOrCreateOpenerImpl).Return(nil, context.DeadlineExceeded).Build()
		_, err = route(context.Background(), target.name, types.AccessModeRW)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestRecoveredWALAcceptsHistoricalReadOptions(t *testing.T) {
	target := &recoveryPhysicalWAL{name: message.WALNameWoodpecker}
	auto := autowalimpls.New(target, nil)
	reader := &roWALAdaptorImpl{roWALImpls: auto}
	id := pulsar.NewPulsarID(pulsarclient.NewMessageID(1, 0, 0, 0))
	for _, policy := range []options.DeliverPolicy{options.DeliverPolicyStartFrom(id), options.DeliverPolicyStartAfter(id)} {
		require.NoError(t, reader.checkReadOptWALName(wal.ReadOption{DeliverPolicy: policy}))
	}
}

func TestReplicaPromotionFollowsDurableSwitch(t *testing.T) {
	for _, caughtUp := range []bool{false, true} {
		t.Run(fmt.Sprintf("reader_caught_up_%t", caughtUp), func(t *testing.T) {
			mockey.PatchConvey("select the writer witnessed by the live scanner", t, func() {
				old := &recoveryPhysicalWAL{name: message.WALNamePulsar}
				target := &recoveryPhysicalWAL{name: message.WALNameWoodpecker}
				start := wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 0})
				buf := replica.NewBuffer(10)
				defer buf.Close()
				cursor, _, err := buf.Pin(true)
				require.NoError(t, err)
				defer cursor.Close()
				if caughtUp {
					buf.ObserveSwitch(20, start)
					buf.Truncate(30)
				}
				auto := autowalimpls.New(old, func(_ context.Context, name message.WALName, mode types.AccessMode) (walimpls.WALImpls, error) {
					require.Equal(t, target.name, name)
					require.Equal(t, types.AccessModeRW, mode)
					return target, nil
				})
				builder := newRecoveryStreamBuilder(&roWALAdaptorImpl{roWALImpls: auto}, nil, nil)
				var probes []message.WALName
				mockey.Mock(sendFirstTimeTick).To(func(_ context.Context, l walimpls.WALImpls, anchor message.MessageID) (message.ImmutableMessage, error) {
					probes = append(probes, l.WALName())
					if l.WALName() == old.name {
						// The old probe was appended past the link. It never appears
						// on this logical scanner, even though Append succeeded.
						buf.ObserveSwitch(20, start)
						return recoveryTestMessage(t, message.MessageTypeRecoveryBarrier, 30).IntoImmutableMessage(pulsar.NewPulsarID(pulsarclient.NewMessageID(1, 3, 0, 0))), nil
					}
					require.True(t, start.EQ(anchor))
					probe := recoveryTestMessage(t, message.MessageTypeRecoveryBarrier, 40).IntoImmutableMessage(wp.NewWpID(&wplog.LogMessageId{SegmentId: 1, EntryId: 2}))
					buf.ObserveRecoveryBarrier(probe, utility.NewTxnBuffer(nil, nil))
					buf.Observe([]message.ImmutableMessage{probe}, 40)
					return probe, nil
				}).Build()
				result, err := prepareReplicaWriter(t.Context(), builder, cursor, buf, nil)
				require.NoError(t, err)
				require.Equal(t, target.name, result.WALName())
				if caughtUp {
					require.Equal(t, []message.WALName{target.name}, probes)
				} else {
					require.Equal(t, []message.WALName{old.name, target.name}, probes)
				}
				require.Equal(t, target.name, builder.RWWALImpls().WALName())
			})
		})
	}
}

// A failed staging or authority check must leave the read side usable and
// release the recovery pin so that the next checkpoint can truncate the tail.
func TestReplicaPromotionReleasesPin(t *testing.T) {
	for _, staleCheckpoint := range []bool{false, true} {
		t.Run(fmt.Sprintf("stale_checkpoint_%t", staleCheckpoint), func(t *testing.T) {
			mockey.PatchConvey("keep the reader on a failed promotion", t, func() {
				const floor = 10
				buffer := replica.NewBuffer(floor)
				reader := &roWALAdaptorImpl{replicaBuffer: buffer}
				checkpoint := &utility.WALCheckpoint{TimeTick: floor}
				if staleCheckpoint {
					checkpoint.TimeTick--
				}
				mockey.Mock(recovery.ReadFrozenSnapshot).Return(&recovery.FrozenSnapshot{Checkpoint: checkpoint}, nil).Build()
				buffer.Observe(nil, floor+1)
				var activated bool
				writer, err := reader.Promote(t.Context(), types.PChannelInfo{}, true, func(context.Context) error {
					activated = true
					return context.Canceled
				})
				require.Error(t, err)
				require.Nil(t, writer)
				require.Equal(t, !staleCheckpoint, activated)
				buffer.Truncate(floor + 1)
				cursor, actual, err := buffer.Pin(false)
				require.NoError(t, err)
				defer cursor.Close()
				require.EqualValues(t, floor+1, actual)
			})
		})
	}
}
