package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestWALSwitchCapabilityWaitsForPublication(t *testing.T) {
	rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 10, Magic: utility.RecoveryMagicRecoveryStorageV2})
	defer rs.metrics.Close()
	rs.SetLogger(mlog.With())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- rs.EnableWALSwitch(ctx) }()
	require.Eventually(t, func() bool { rs.mu.Lock(); defer rs.mu.Unlock(); return rs.walSwitchReady != nil }, time.Second, time.Millisecond)
	fail := true
	save := mockey.Mock((*recoveryStorageImpl).saveRecoverySnapshot).To(func(_ *recoveryStorageImpl, _ context.Context, s *metastore.WALRecoverySnapshot) error {
		require.Equal(t, utility.RecoveryMagicHotWALSwitch, s.ConsumeCheckpoint.GetRecoveryMagic())
		require.Equal(t, uint64(10), s.ConsumeCheckpoint.GetTimeTick())
		if fail {
			return context.DeadlineExceeded
		}
		return nil
	}).Build()
	defer save.UnPatch()
	require.ErrorIs(t, rs.persistDirtySnapshot(ctx, mlog.DebugLevel), context.DeadlineExceeded)
	require.Equal(t, utility.RecoveryMagicRecoveryStorageV2, rs.GetCheckpoint(ctx).Magic)
	select {
	case err := <-done:
		t.Fatalf("switch allowed before durable publication: %v", err)
	default:
	}
	fail = false
	require.NoError(t, rs.persistDirtySnapshot(ctx, mlog.DebugLevel))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("publication did not release switch")
	}
	require.Equal(t, uint64(10), rs.GetCheckpoint(ctx).TimeTick)
	require.NoError(t, rs.EnableWALSwitch(ctx))
	require.Nil(t, rs.consumeDirtySnapshot())
	_, err := rs.migrateLegacyRecoveryInfo(ctx, nil, nil)
	require.NoError(t, err)
	require.NoError(t, rs.alignCollectionSchema(ctx, nil))
	require.Equal(t, utility.RecoveryMagicHotWALSwitch, rs.GetCheckpoint(ctx).Magic)
}

func TestWALSwitchCapabilityCancellation(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		rs := newTestRecoveryStorage(t, &utility.WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1), TimeTick: 10, Magic: utility.RecoveryMagicRecoveryStorageV2})
		ctx, cancel := context.WithCancel(context.Background())
		if stopped {
			rs.backgroundTaskNotifier.Finish(struct{}{})
		} else {
			cancel()
		}
		require.Error(t, rs.EnableWALSwitch(ctx))
		require.Equal(t, utility.RecoveryMagicRecoveryStorageV2, rs.GetCheckpoint(ctx).Magic)
		cancel()
		rs.metrics.Close()
	}
}
