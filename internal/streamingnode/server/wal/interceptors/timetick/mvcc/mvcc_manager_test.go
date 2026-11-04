package mvcc

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestRetainedMVCC(t *testing.T) {
	cm := NewMVCCManager(100)
	cm.ApplyRecoveryBarrier("a", 100)
	cm.ApplyRecoveryBarrier("b", 100)
	update := func(tt uint64, vc string, kind message.MessageType) {
		cm.UpdateMVCC(createQueryTestMessage(t, tt, vc, kind, false, true))
	}
	update(120, "a", message.MessageTypeInsert)
	require.Equal(t, VChannelMVCC{Timetick: 120}, cm.GetMVCCOfVChannel("a"))
	update(300, "", message.MessageTypeTimeTick)
	require.Equal(t, VChannelMVCC{Timetick: 120, Confirmed: true}, cm.GetMVCCOfVChannel("a"))
	require.Equal(t, VChannelMVCC{Timetick: 100, Confirmed: true}, cm.GetMVCCOfVChannel("b"))
	require.Equal(t, VChannelMVCC{Timetick: 100, Confirmed: true}, cm.GetMVCCOfVChannel("unknown"))
	// Ack and sync may finish before the successful append's deferred update.
	update(140, "a", message.MessageTypeDelete)
	update(130, "a", message.MessageTypeInsert)
	require.Equal(t, VChannelMVCC{Timetick: 140, Confirmed: true}, cm.GetMVCCOfVChannel("a"))
	update(500, "b", message.MessageTypeInsert)
	require.Equal(t, VChannelMVCC{Timetick: 140, Confirmed: true}, cm.GetMVCCOfVChannel("a"))
	require.Equal(t, uint64(300), cm.GetConfirmedTimeTick())
	update(200, "", message.MessageTypeTimeTick)
	cm.UpdateMVCC(createQueryTestMessage(t, 1000, "", message.MessageTypeTimeTick, false, false))
	require.Equal(t, uint64(300), cm.GetConfirmedTimeTick())
	update(600, "", message.MessageTypeTimeTick)
	require.Equal(t, VChannelMVCC{Timetick: 500, Confirmed: true}, cm.GetMVCCOfVChannel("b"))
}

func TestRetainedMVCCTransactionAndRecovery(t *testing.T) {
	cm := NewMVCCManager(100)
	cm.ApplyRecoveryBarrier("a", 100)
	for _, kind := range []message.MessageType{message.MessageTypeBeginTxn, message.MessageTypeInsert, message.MessageTypeDelete, message.MessageTypeRollbackTxn} {
		cm.UpdateMVCC(createQueryTestMessage(t, 150, "a", kind, true, true))
		require.Equal(t, VChannelMVCC{Timetick: 100, Confirmed: true}, cm.GetMVCCOfVChannel("a"))
	}
	// A recovered transaction can have bodies before the new opening fence,
	// but only its post-open commit becomes visible.
	cm.UpdateMVCC(createQueryTestMessage(t, 90, "a", message.MessageTypeInsert, true, true))
	cm.UpdateMVCC(createQueryTestMessage(t, 200, "a", message.MessageTypeCommitTxn, true, true))
	cm.ApplyRecoveryBarrier("a", 100)
	require.Equal(t, VChannelMVCC{Timetick: 200}, cm.GetMVCCOfVChannel("a"))
	cm.UpdateMVCC(createQueryTestMessage(t, 300, "", message.MessageTypeTimeTick, false, true))
	require.Equal(t, VChannelMVCC{Timetick: 200, Confirmed: true}, cm.GetMVCCOfVChannel("a"))
	// Ownership change rebuilds from the new opening fence, never an old L.
	restarted := NewMVCCManager(400)
	restarted.ApplyRecoveryBarrier("a", 400)
	restarted.UpdateMVCC(createQueryTestMessage(t, 600, "", message.MessageTypeTimeTick, false, true))
	require.Equal(t, VChannelMVCC{Timetick: 400, Confirmed: true}, restarted.GetMVCCOfVChannel("a"))
}

func TestRetainedMVCCVisibilityBarriers(t *testing.T) {
	for _, kind := range []message.MessageType{message.MessageTypeSchemaChange, message.MessageTypeAlterCollection, message.MessageTypeCommitImport, message.MessageTypeManualFlush, message.MessageTypeDropPartition, message.MessageTypeTruncateCollection} {
		t.Run(kind.String(), func(t *testing.T) {
			cm := NewMVCCManager(100)
			cm.ApplyRecoveryBarrier("a", 100)
			msg := createQueryTestMessage(t, 120, "a", kind, false, true)
			if kind == message.MessageTypeAlterCollection {
				msg = message.NewAlterCollectionMessageBuilderV2().WithVChannel("a").WithHeader(&message.AlterCollectionMessageHeader{}).WithBody(&message.AlterCollectionMessageBody{}).MustBuildMutable().WithTimeTick(120)
			}
			cm.UpdateMVCC(msg)
			cm.UpdateMVCC(createQueryTestMessage(t, 300, "", message.MessageTypeTimeTick, false, true))
			require.Equal(t, VChannelMVCC{Timetick: 120, Confirmed: true}, cm.GetMVCCOfVChannel("a"))
		})
	}
}

func TestRetainedMVCCIgnoresSegmentStorageMessages(t *testing.T) {
	for _, kind := range []message.MessageType{message.MessageTypeCreateSegment, message.MessageTypeFlush} {
		t.Run(kind.String(), func(t *testing.T) {
			cm := NewMVCCManager(100)
			cm.ApplyRecoveryBarrier("a", 100)
			cm.UpdateMVCC(createQueryTestMessage(t, 120, "a", message.MessageTypeInsert, false, true))
			cm.UpdateMVCC(createQueryTestMessage(t, 140, "a", kind, false, true))
			require.Equal(t, VChannelMVCC{Timetick: 120}, cm.GetMVCCOfVChannel("a"))
			require.Equal(t, uint64(100), cm.GetConfirmedTimeTick())
			// The independent query-plan protocol must still observe Flush.
			transformTarget := uint64(100)
			if kind == message.MessageTypeFlush {
				transformTarget = 140
			}
			require.Equal(t, max(uint64(120), transformTarget), cm.QueryMVCC().Minimum("a", true))
			cm.UpdateMVCC(createQueryTestMessage(t, 300, "", message.MessageTypeTimeTick, false, true))
			cm.UpdateMVCC(createQueryTestMessage(t, 400, "a", kind, false, true))
			require.Equal(t, VChannelMVCC{Timetick: 120, Confirmed: true}, cm.GetMVCCOfVChannel("a"))
			require.Equal(t, uint64(300), cm.GetConfirmedTimeTick())
			// Excluding storage operations must not suppress the next visible write.
			cm.UpdateMVCC(createQueryTestMessage(t, 500, "a", message.MessageTypeDelete, false, true))
			require.Equal(t, VChannelMVCC{Timetick: 500}, cm.GetMVCCOfVChannel("a"))
			cm.UpdateMVCC(createQueryTestMessage(t, 600, "", message.MessageTypeTimeTick, false, true))
			require.Equal(t, VChannelMVCC{Timetick: 500, Confirmed: true}, cm.GetMVCCOfVChannel("a"))
		})
	}
}

func TestRetainedMVCCLifecycle(t *testing.T) {
	cm := NewMVCCManager(100)
	for i := 0; i < 1000; i++ {
		vc := fmt.Sprintf("p_%dv0", i)
		cm.UpdateMVCC(createQueryTestMessage(t, 120, vc, message.MessageTypeCreateCollection, false, true))
		cm.UpdateMVCC(createQueryTestMessage(t, 140, vc, message.MessageTypeInsert, false, true))
		require.Equal(t, uint64(140), cm.GetMVCCOfVChannel(vc).Timetick)
		cm.UpdateMVCC(createQueryTestMessage(t, 160, vc, message.MessageTypeDropCollection, false, true))
		// A delayed callback/internal message cannot recreate a retired identity.
		cm.UpdateMVCC(createQueryTestMessage(t, 130, vc, message.MessageTypeInsert, false, true))
		cm.UpdateMVCC(createQueryTestMessage(t, 180, vc, message.MessageTypeFlush, false, true))
	}
	require.Empty(t, cm.vchannelMVCCTimestamps)
	require.Equal(t, VChannelMVCC{Timetick: 160}, cm.GetMVCCOfVChannel("p_0v0"))
	cm.ApplyRecoveryBarrier("active", 100)
	require.Equal(t, VChannelMVCC{Timetick: 100, Confirmed: true}, cm.GetMVCCOfVChannel("active"))
	cm.UpdateMVCC(createQueryTestMessage(t, 200, "p_1001v0", message.MessageTypeCreateCollection, false, true))
	require.Len(t, cm.vchannelMVCCTimestamps, 2)
	require.Equal(t, VChannelMVCC{Timetick: 200}, cm.GetMVCCOfVChannel("p_1001v0"))
	cm.UpdateMVCC(createQueryTestMessage(t, 210, "p_vcchan", message.MessageTypeCreateCollection, false, true))
	require.Len(t, cm.vchannelMVCCTimestamps, 2)
}

func TestRetainedMVCCConcurrentConfirmation(t *testing.T) {
	cm := NewMVCCManager(100)
	cm.ApplyRecoveryBarrier("a", 100)
	var wg sync.WaitGroup
	for i := uint64(101); i <= 500; i++ {
		write := createQueryTestMessage(t, i, "a", message.MessageTypeInsert, false, true)
		tick := createQueryTestMessage(t, i, "", message.MessageTypeTimeTick, false, true)
		wg.Add(2)
		go func() { defer wg.Done(); cm.UpdateMVCC(write); cm.GetMVCCOfVChannel("a") }()
		go func() { defer wg.Done(); cm.UpdateMVCC(tick); cm.GetConfirmedTimeTick() }()
	}
	wg.Wait()
	require.Equal(t, VChannelMVCC{Timetick: 500, Confirmed: true}, cm.GetMVCCOfVChannel("a"))
}

func BenchmarkRetainedMVCC(b *testing.B) {
	for _, n := range []int{1, 1000, 10000, 100000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			cm := NewMVCCManager(100)
			for i := 0; i < n; i++ {
				cm.ApplyRecoveryBarrier(fmt.Sprintf("p_%dv0", i), 100)
			}
			tick := message.NewTimeTickMessageBuilderV1().WithAllVChannel().WithHeader(&message.TimeTickMessageHeader{}).WithBody(&message.TimeTickMsg{}).MustBuildMutable().WithTimeTick(300)
			write := message.NewInsertMessageBuilderV1().WithVChannel("p_0v0").WithHeader(&message.InsertMessageHeader{}).WithBody(&message.InsertRequest{}).MustBuildMutable().WithTimeTick(200)
			b.Run("confirm", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					cm.UpdateMVCC(tick.WithTimeTick(uint64(301 + i)))
				}
			})
			b.Run("get", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					cm.GetMVCCOfVChannel("p_0v0")
				}
			})
			b.Run("update", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					cm.UpdateMVCC(write.WithTimeTick(uint64(1_000_000_000 + i)))
				}
			})
		})
	}
}

// PChannel operations do not change the legacy read snapshot. Query-plan
// transform frontiers still track storage publication independently.
func TestRetainedMVCCClusterBroadcast(t *testing.T) {
	channels := message.ClusterChannels{Channels: []string{"p1", "p2"}, ControlChannel: "p1_vcchan"}
	for _, broadcast := range []message.BroadcastMutableMessage{
		message.NewFlushAllMessageBuilderV2().WithHeader(&message.FlushAllMessageHeader{}).WithBody(&message.FlushAllMessageBody{}).WithClusterLevelBroadcast(channels).MustBuildBroadcast(),
		message.NewAlterWALMessageBuilderV2().WithHeader(&message.AlterWALMessageHeader{}).WithBody(&message.AlterWALMessageBody{}).WithClusterLevelBroadcast(channels).MustBuildBroadcast(),
		message.NewAlterReplicateConfigMessageBuilderV2().WithHeader(&message.AlterReplicateConfigMessageHeader{}).WithBody(&message.AlterReplicateConfigMessageBody{}).WithClusterLevelBroadcast(channels).MustBuildBroadcast(),
	} {
		t.Run(broadcast.MessageType().String(), func(t *testing.T) {
			for _, msg := range broadcast.WithBroadcastID(1).SplitIntoMutableMessage() {
				cm := NewMVCCManager(100)
				cm.ApplyRecoveryBarrier("a", 100)
				cm.ApplyRecoveryBarrier("b", 100)
				cm.UpdateMVCC(createQueryTestMessage(t, 120, "a", message.MessageTypeInsert, false, true))
				require.True(t, msg.IsPChannelLevel())
				require.NotEmpty(t, msg.VChannel())
				cm.UpdateMVCC(msg.WithTimeTick(200))
				require.Equal(t, VChannelMVCC{Timetick: 120}, cm.GetMVCCOfVChannel("a"))
				for _, vc := range []string{"b", "unknown"} {
					require.Equal(t, VChannelMVCC{Timetick: 100, Confirmed: true}, cm.GetMVCCOfVChannel(vc))
				}
				transformTarget := uint64(200)
				if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
					transformTarget = 100
				}
				require.Equal(t, max(uint64(120), transformTarget), cm.QueryMVCC().Minimum("a", true))
				cm.UpdateMVCC(createQueryTestMessage(t, 300, "", message.MessageTypeTimeTick, false, true))
				require.Equal(t, VChannelMVCC{Timetick: 120, Confirmed: true}, cm.GetMVCCOfVChannel("a"))
				require.Len(t, cm.vchannelMVCCTimestamps, 2)
			}
		})
	}
}

// newSplitShardTestReplicas builds the source and target replicas of one
// SplitShard broadcast, each addressed to its own vchannel, at the given ticks.
func newSplitShardTestReplicas(t *testing.T, sourceTick, targetTick uint64) (source, target message.MutableMessage) {
	t.Helper()
	broadcast := message.NewSplitShardMessageBuilderV2().
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    1,
			SplitTaskId:     2,
			SourceVchannel:  "p0_1v0",
			TargetVchannels: []string{"p1_1v1", "p2_1v2"},
		}).
		WithBody(&message.SplitShardMessageBody{}).
		WithBroadcast([]string{"p0_1v0", "p1_1v1", "p2_1v2"}, message.OptBuildBroadcastAppendFirst("p0_1v0")).
		MustBuildBroadcast().
		WithBroadcastID(1)
	for _, replica := range broadcast.SplitIntoMutableMessage() {
		switch replica.VChannel() {
		case "p0_1v0":
			source = replica.WithTimeTick(sourceTick)
		case "p1_1v1":
			target = replica.WithTimeTick(targetTick)
		}
	}
	require.NotNil(t, source)
	require.NotNil(t, target)
	return source, target
}

// TestRetainedMVCCSplitShardTargetGenesis: the target replica of a SplitShard
// is a brand-new vchannel's genesis and registers it live, exactly as
// CreateCollection does, so a Strong read of a split target is answered at the
// target's own progress rather than at the retired/recovery fence. The source
// replica is an ordinary record of its live vchannel.
func TestRetainedMVCCSplitShardTargetGenesis(t *testing.T) {
	cm := NewMVCCManager(100)
	cm.UpdateMVCC(createQueryTestMessage(t, 120, "p0_1v0", message.MessageTypeCreateCollection, false, true))
	source, target := newSplitShardTestReplicas(t, 150, 160)

	// Before the genesis the target is unknown: it reads as the recovery fence.
	require.Equal(t, uint64(100), cm.GetMVCCOfVChannel("p1_1v1").Timetick)

	cm.UpdateMVCC(source)
	cm.UpdateMVCC(target)
	require.Equal(t, uint64(150), cm.GetMVCCOfVChannel("p0_1v0").Timetick, "the source replica advances the live source")
	require.Equal(t, uint64(160), cm.GetMVCCOfVChannel("p1_1v1").Timetick, "the target replica registers the target live")
	require.Equal(t, uint64(100), cm.GetMVCCOfVChannel("p2_1v2").Timetick, "the other target's replica has not been appended here")

	// From then on the target's own writes move it, as on any live vchannel.
	cm.UpdateMVCC(createQueryTestMessage(t, 170, "p1_1v1", message.MessageTypeDelete, false, true))
	require.Equal(t, uint64(170), cm.GetMVCCOfVChannel("p1_1v1").Timetick)
	cm.UpdateMVCC(createQueryTestMessage(t, 180, "", message.MessageTypeTimeTick, false, true))
	require.True(t, cm.GetMVCCOfVChannel("p1_1v1").Confirmed)
}
