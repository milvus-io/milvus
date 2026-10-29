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
			require.Equal(t, QueryVChannelMVCC{GrowingTimetick: 120, TransformingTimetick: transformTarget}, cm.GetQueryMVCCOfVChannel("a"))
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
				require.Equal(t, transformTarget, cm.GetQueryMVCCOfVChannel("a").TransformingTimetick)
				cm.UpdateMVCC(createQueryTestMessage(t, 300, "", message.MessageTypeTimeTick, false, true))
				require.Equal(t, VChannelMVCC{Timetick: 120, Confirmed: true}, cm.GetMVCCOfVChannel("a"))
				require.Len(t, cm.vchannelMVCCTimestamps, 2)
			}
		})
	}
}
