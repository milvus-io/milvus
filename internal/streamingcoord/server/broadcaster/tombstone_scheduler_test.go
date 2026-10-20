package broadcaster

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/metastore"
	coordcatalog "github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	nodecatalog "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func setupTombstoneGC(t *testing.T) (metastore.StreamingCoordCataLog, metastore.StreamingNodeCataLog) {
	t.Helper()
	paramtable.Init()
	cfg := &paramtable.Get().StreamingCfg
	for _, item := range []*paramtable.ParamItem{&cfg.WALBroadcasterTombstoneCheckInternal, &cfg.WALBroadcasterTombstoneMaxLifetime} {
		old := item.SwapTempValue("1h")
		t.Cleanup(func() { item.SwapTempValue(old) })
	}
	old := cfg.WALBroadcasterTombstoneMaxCount.SwapTempValue("1")
	t.Cleanup(func() { cfg.WALBroadcasterTombstoneMaxCount.SwapTempValue(old) })
	client, _ := kvfactory.GetEtcdAndPath()
	kv := etcdkv.NewEtcdKV(client, "tombstone-gc-"+uuid.NewString()+"/meta")
	t.Cleanup(func() { require.NoError(t, kv.RemoveWithPrefix(context.Background(), "")) })
	coord, node := coordcatalog.NewCataLog(kv), nodecatalog.NewCataLog(kv)
	resource.InitForTest(resource.OptStreamingCatalog(coord), resource.OptStreamingNodeCatalog(node))
	for _, name := range []string{"p0", "p1", "p2"} {
		require.NoError(t, coord.SavePChannels(t.Context(), []*streamingpb.PChannelMeta{{Channel: &streamingpb.PChannelInfo{Name: name, Term: 1}}}))
	}
	return coord, node
}

func saveGCCheckpoint(t *testing.T, catalog metastore.StreamingNodeCataLog, channel string, tick uint64, term int64) {
	t.Helper()
	require.NoError(t, catalog.SaveRecoverySnapshot(t.Context(), channel, &metastore.WALRecoverySnapshot{
		ConsumeCheckpoint: &streamingpb.WALCheckpoint{TimeTick: tick, Term: term},
	}))
}

func gcTestMessages(id uint64, channels []string, replicated bool) []message.ImmutableMessage {
	msg := createNewBroadcastMsg(channels).WithBroadcastID(id)
	messages := make([]message.ImmutableMessage, 0, len(channels))
	for _, part := range msg.SplitIntoMutableMessage() {
		// Header construction can reorder channels; assign positions by the
		// caller's channel list so replay copies preserve their original LSNs.
		i := slices.Index(channels, part.VChannel())
		if replicated {
			// The source LSN deliberately differs from the receiving WAL's LSN.
			part.WithReplicateHeader(&message.ReplicateHeader{
				ClusterID: "source", TimeTick: 9000,
				MessageID: walimplstest.NewTestMessageID(9), LastConfirmedMessageID: walimplstest.NewTestMessageID(8),
			})
		}
		messages = append(messages, part.WithTimeTick(uint64(100+i*10)).WithLastConfirmed(walimplstest.NewTestMessageID(1)).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(i+2))))
	}
	return messages
}

func gcTestTombstone(id uint64, channels []string, replicated bool) *streamingpb.BroadcastTask {
	messages := gcTestMessages(id, channels, replicated)
	task := &streamingpb.BroadcastTask{Message: messages[0].IntoBroadcastMutableMessage().IntoMessageProto(), State: streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE}
	// Split order need not equal header order.
	for _, channel := range messages[0].BroadcastHeader().VChannels {
		for _, msg := range messages {
			if msg.VChannel() == channel {
				task.AckedVchannelBitmap = append(task.AckedVchannelBitmap, 1)
				task.AckedCheckpoints = append(task.AckedCheckpoints, &streamingpb.AckedCheckpoint{TimeTick: msg.TimeTick(), MessageId: msg.MessageID().IntoProto(), LastConfirmedMessageId: msg.LastConfirmedMessageID().IntoProto()})
			}
		}
	}
	return task
}

// A separately driven scheduler makes TTL deterministic without mutating the
// running manager's scheduler or sleeping for a configured lifetime.
func gcTestScheduler(bm *broadcastTaskManager, ids ...uint64) *tombstoneScheduler {
	s := newTombstoneScheduler(mlog.With())
	s.bm = bm
	for _, id := range ids {
		s.tombstones = append(s.tombstones, tombstoneItem{broadcastID: id, createTime: time.Now().Add(-2 * time.Hour)})
	}
	return s
}

func TestTombstoneGCTTLAndCheckpoint(t *testing.T) {
	for _, replicated := range []bool{false, true} {
		name := "primary"
		if replicated {
			name = "secondary"
		}
		t.Run(name, func(t *testing.T) {
			coord, node := setupTombstoneGC(t)
			blocked := gcTestTombstone(1, []string{"p0_vcchan", "p1_1v0", "p1_1v1"}, replicated)
			safe := gcTestTombstone(2, []string{"p2_1v0"}, replicated)
			for i, task := range []*streamingpb.BroadcastTask{blocked, safe} {
				require.NoError(t, coord.SaveBroadcastTask(t.Context(), uint64(i+1), task))
			}
			bc, err := RecoverBroadcaster(t.Context())
			require.NoError(t, err)
			defer bc.Close()
			bm := bc.(*broadcastTaskManager)
			s := gcTestScheduler(bm, 1, 2)
			s.tombstones[1].createTime = time.Now()
			saveGCCheckpoint(t, node, "p0", 1000, 1)
			saveGCCheckpoint(t, node, "p2", 1000, 1)
			s.triggerGCTombstone()
			require.Len(t, s.tombstones, 2, "blocked tombstones must not count toward the eligible quota")
			s.tombstones[1].createTime = time.Now().Add(-2 * time.Hour)
			s.triggerGCTombstone()
			require.Len(t, s.tombstones, 1, "blocked oldest entry must not prevent unrelated GC")
			require.Equal(t, uint64(1), s.tombstones[0].broadcastID)
			// The checkpoint must pass the latest of both copies on p1.
			var latest uint64
			for _, ack := range gcTestMessages(1, []string{"p0_vcchan", "p1_1v0", "p1_1v1"}, replicated) {
				if ack.PChannel() == "p1" && ack.TimeTick() > latest {
					latest = ack.TimeTick()
				}
			}
			for _, tick := range []uint64{latest - 1, latest} {
				saveGCCheckpoint(t, node, "p1", tick, 1)
				s.triggerGCTombstone()
				require.Len(t, s.tombstones, 1, "behind/equal cannot collect")
			}
			saveGCCheckpoint(t, node, "p1", latest+1, 2)
			s.triggerGCTombstone()
			require.Empty(t, s.tombstones, "next pass must read the newly persisted checkpoint")
			tasks, err := coord.ListBroadcastTask(t.Context())
			require.NoError(t, err)
			require.Empty(t, tasks)
		})
	}
}

func TestTombstoneGCEligibleMaxCount(t *testing.T) {
	for _, replicated := range []bool{false, true} {
		name := "primary"
		if replicated {
			name = "secondary"
		}
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				maxCount string
				expired  []uint64
				retained []uint64
			}{
				{name: "evict_smallest_ids", maxCount: "1", retained: []uint64{30}},
				{name: "exact_limit", maxCount: "3", retained: []uint64{30, 10, 20}},
				{name: "below_limit", maxCount: "4", retained: []uint64{30, 10, 20}},
				{name: "ttl_before_count", maxCount: "2", expired: []uint64{30}, retained: []uint64{10, 20}},
				{name: "ttl_and_count", maxCount: "1", expired: []uint64{30}, retained: []uint64{20}},
				{name: "zero_limit", maxCount: "0"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					coord, node := setupTombstoneGC(t)
					paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.SwapTempValue(tc.maxCount)
					// Callback completion/enqueue order need not follow BroadcastID.
					ids := []uint64{30, 10, 20, 1, 2, 3}
					for _, id := range ids {
						channel := "p0_vcchan"
						switch id {
						case 1:
							channel = "p1_vcchan" // Missing checkpoint.
						case 2:
							channel = "p2_vcchan" // Equal checkpoint.
						}
						task := gcTestTombstone(id, []string{channel}, replicated)
						if id == 3 {
							task.AckedCheckpoints = nil // Legacy positions.
						}
						require.NoError(t, coord.SaveBroadcastTask(t.Context(), id, task))
					}
					saveGCCheckpoint(t, node, "p0", 101, 2)
					saveGCCheckpoint(t, node, "p2", 100, 2)
					bc, err := RecoverBroadcaster(t.Context())
					require.NoError(t, err)
					defer bc.Close()
					bm := bc.(*broadcastTaskManager)
					s := gcTestScheduler(bm, ids...)
					for i := range s.tombstones {
						if !slices.Contains(tc.expired, s.tombstones[i].broadcastID) {
							s.tombstones[i].createTime = time.Now()
						}
					}
					s.triggerGCTombstone()
					expected := append(slices.Clone(tc.retained), 1, 2, 3)
					var retained []uint64
					for _, item := range s.tombstones {
						retained = append(retained, item.broadcastID)
					}
					require.Equal(t, expected, retained, "retain enqueue order for the TTL shortcut")
					persisted, err := coord.ListBroadcastTask(t.Context())
					require.NoError(t, err)
					require.Len(t, persisted, len(expected))
					for _, id := range ids {
						_, exists := bm.getBroadcastTaskByID(id)
						require.Equal(t, slices.Contains(expected, id), exists)
					}
				})
			}
		})
	}
}

func TestTombstoneGCSkipsCatalogWithoutAgeOrCountPressure(t *testing.T) {
	coord, node := setupTombstoneGC(t)
	paramtable.Get().StreamingCfg.WALBroadcasterTombstoneMaxCount.SwapTempValue("2")
	listReads, checkpointReads := 0, 0
	listPatch := mockey.Mock(mockey.GetMethod(coord, "ListPChannel")).To(func(context.Context) ([]*streamingpb.PChannelMeta, error) {
		listReads++
		return []*streamingpb.PChannelMeta{{Channel: &streamingpb.PChannelInfo{Name: "p0"}}}, nil
	}).Build()
	defer listPatch.UnPatch()
	checkpointPatch := mockey.Mock(mockey.GetMethod(node, "GetConsumeCheckpoint")).To(func(context.Context, string) (*streamingpb.WALCheckpoint, error) {
		checkpointReads++
		return nil, context.Canceled // Stop before accessing the task manager.
	}).Build()
	defer checkpointPatch.UnPatch()
	for _, tc := range []struct {
		name    string
		count   int
		expired bool
		reads   int
	}{
		{name: "empty"},
		{name: "below_limit", count: 1},
		{name: "at_limit", count: 2},
		{name: "count_only", count: 3, reads: 1},
		{name: "ttl_only", count: 1, expired: true, reads: 1},
		{name: "ttl_and_count", count: 3, expired: true, reads: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listReads, checkpointReads = 0, 0
			s := newTombstoneScheduler(mlog.With())
			for i := 0; i < tc.count; i++ {
				created := time.Now()
				if tc.expired && i == 0 {
					created = created.Add(-2 * time.Hour)
				}
				s.tombstones = append(s.tombstones, tombstoneItem{broadcastID: uint64(i + 1), createTime: created})
			}
			s.triggerGCTombstone()
			require.Equal(t, tc.reads, listReads)
			require.Equal(t, tc.reads, checkpointReads)
			require.Len(t, s.tombstones, tc.count)
		})
	}
}

func TestTombstoneGCFailuresAndLegacyPositions(t *testing.T) {
	coord, node := setupTombstoneGC(t)
	tasks := []*streamingpb.BroadcastTask{gcTestTombstone(1, []string{"p0_vcchan"}, true), gcTestTombstone(2, []string{"p0_vcchan"}, false)}
	tasks[1].AckedCheckpoints = nil // Older persisted task format.
	for i, task := range tasks {
		require.NoError(t, coord.SaveBroadcastTask(t.Context(), uint64(i+1), task))
	}
	bc, err := RecoverBroadcaster(t.Context())
	require.NoError(t, err)
	defer bc.Close()
	bm := bc.(*broadcastTaskManager)
	s := gcTestScheduler(bm, 1, 2)
	saveGCCheckpoint(t, node, "p0", 101, 1)
	for _, target := range []struct {
		receiver any
		method   string
	}{{coord, "ListPChannel"}, {node, "GetConsumeCheckpoint"}, {coord, "SaveBroadcastTask"}} {
		builder := mockey.Mock(mockey.GetMethod(target.receiver, target.method))
		if target.method == "SaveBroadcastTask" {
			builder.To(func(ctx context.Context, _ uint64, _ *streamingpb.BroadcastTask) error {
				s.notifier.Cancel()
				return ctx.Err()
			})
		} else {
			builder.Return(nil, context.Canceled)
		}
		patch := builder.Build()
		s.triggerGCTombstone()
		patch.UnPatch()
		require.Len(t, s.tombstones, 2)
		persisted, err := coord.ListBroadcastTask(t.Context())
		require.NoError(t, err)
		require.Len(t, persisted, 2)
	}
	s = gcTestScheduler(bm, 1, 2) // Retry with a live scheduler context.
	// Deletion failure left DONE in memory: a duplicate still must not be processed.
	_, process := bm.getOrCreateBroadcastTask(gcTestMessages(1, []string{"p0_vcchan"}, true)[0])
	require.False(t, process)
	s.triggerGCTombstone()
	require.Len(t, s.tombstones, 1)
	require.Equal(t, uint64(2), s.tombstones[0].broadcastID, "legacy position is retained")
	dropped, err := bm.DropTombstone(t.Context(), 99, nil)
	require.NoError(t, err)
	require.True(t, dropped)
}

func TestTombstoneGCReplayAndRecovery(t *testing.T) {
	coord, node := setupTombstoneGC(t)
	var callbacks atomic.Int64
	patch := mockey.Mock(registry.CallMessageAckCallback).To(func(context.Context, message.BroadcastMutableMessage, map[string]*message.AppendResult) error {
		callbacks.Add(1)
		return nil
	}).Build()
	defer patch.UnPatch()
	oncePatch := mockey.Mock(registry.CallMessageAckOnceCallbacks).Return(nil).Build()
	defer oncePatch.UnPatch()
	bc, err := RecoverBroadcaster(t.Context())
	require.NoError(t, err)
	bm := bc.(*broadcastTaskManager)
	defer func() {
		if bm != nil {
			bm.Close()
		}
	}()
	acks := gcTestMessages(10, []string{"p0_vcchan", "p1_1v0"}, true)
	for _, ack := range acks {
		require.NoError(t, bm.Ack(t.Context(), ack))
	}
	require.Eventually(t, func() bool {
		tasks, err := coord.ListBroadcastTask(t.Context())
		return err == nil && len(tasks) == 1 && tasks[0].State == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(1), callbacks.Load())
	s := gcTestScheduler(bm, 10)
	// Consumption completed, but checkpoint publication is still behind.
	for _, channel := range []string{"p0", "p1"} {
		saveGCCheckpoint(t, node, channel, 50, 1)
	}
	s.triggerGCTombstone()
	require.Len(t, s.tombstones, 1)
	original, _ := bm.getBroadcastTaskByID(10)
	require.NoError(t, bm.Ack(t.Context(), acks[0]), "partial replay")
	for _, ack := range acks {
		require.NoError(t, bm.Ack(t.Context(), ack))
	}
	got, _ := bm.getBroadcastTaskByID(10)
	require.Same(t, original, got)
	require.Equal(t, int64(1), callbacks.Load())

	bm.Close()
	restartAt := time.Now()
	bc, err = RecoverBroadcaster(t.Context())
	require.NoError(t, err)
	bm = bc.(*broadcastTaskManager)
	// Handoff claims a new term at the unchanged published logical position.
	for _, channel := range []string{"p0", "p1"} {
		saveGCCheckpoint(t, node, channel, 50, 2)
	}
	for _, ack := range acks {
		require.NoError(t, bm.Ack(t.Context(), ack))
	}
	s = gcTestScheduler(bm, 10)
	s.triggerGCTombstone()
	require.Len(t, s.tombstones, 1)
	require.Equal(t, int64(1), callbacks.Load(), "recovery and all-channel replay must not reschedule callback")
	for _, channel := range []string{"p0", "p1"} {
		saveGCCheckpoint(t, node, channel, 1000, 2)
	}
	s.tombstones[0].createTime = time.Now() // Recovery restarts the TTL clock.
	s.triggerGCTombstone()
	require.Len(t, s.tombstones, 1)
	s.tombstones[0].createTime = time.Now().Add(-2 * time.Hour)
	s.triggerGCTombstone()
	require.Empty(t, s.tombstones)
	persisted, err := coord.ListBroadcastTask(t.Context())
	require.NoError(t, err)
	require.Empty(t, persisted)
	fresh := createNewBroadcastMsg([]string{"p0_vcchan", "p1_1v0"}).WithBroadcastID(11)
	for _, channel := range fresh.BroadcastHeader().VChannels {
		require.NoError(t, bm.Ack(t.Context(), schemaTestAck(fresh, channel, 2000)))
	}
	require.Eventually(t, func() bool { return callbacks.Load() == 2 }, 5*time.Second, 10*time.Millisecond, "a new replicated task is still reconstructed")
	bm.Close()
	// The actual recovered scheduler also resets its clock (inspect after Close
	// joins its background goroutine, avoiding a test-only data race).
	recovered := bm.ackScheduler.tombstoneScheduler
	bm = nil
	require.NotEmpty(t, recovered.tombstones)
	for _, item := range recovered.tombstones {
		require.False(t, item.createTime.Before(restartAt))
	}
}

func TestTombstoneGCCollectsCheckpointsBeforeDeletion(t *testing.T) {
	coord, node := setupTombstoneGC(t)
	for _, id := range []uint64{1, 2} {
		require.NoError(t, coord.SaveBroadcastTask(t.Context(), id, gcTestTombstone(id, []string{"p0_vcchan"}, true)))
	}
	bc, err := RecoverBroadcaster(t.Context())
	require.NoError(t, err)
	defer bc.Close()
	bm := bc.(*broadcastTaskManager)
	s := gcTestScheduler(bm, 1, 2)
	reads := make(map[string]int)
	failLastRead := true
	patch := mockey.Mock(mockey.GetMethod(node, "GetConsumeCheckpoint")).To(func(_ context.Context, name string) (*streamingpb.WALCheckpoint, error) {
		reads[name]++
		// Even the checkpoint for a PChannel unrelated to these tombstones is
		// fetched before any deletion. No task/manager lock surrounds the I/O.
		for _, id := range []uint64{1, 2} {
			task, found := bm.getBroadcastTaskByID(id)
			require.True(t, found)
			require.Equal(t, streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_TOMBSTONE, task.State())
		}
		if failLastRead && len(reads) == 3 {
			return nil, context.DeadlineExceeded
		}
		return &streamingpb.WALCheckpoint{TimeTick: 1000}, nil
	}).Build()
	defer patch.UnPatch()
	s.triggerGCTombstone()
	require.Equal(t, map[string]int{"p0": 1, "p1": 1, "p2": 1}, reads)
	require.Len(t, s.tombstones, 2, "a partial checkpoint map must not authorize deletion")
	reads = make(map[string]int)
	failLastRead = false
	s.triggerGCTombstone()
	require.Equal(t, map[string]int{"p0": 1, "p1": 1, "p2": 1}, reads, "one fresh read per channel, not per tombstone")
	require.Empty(t, s.tombstones)
}
