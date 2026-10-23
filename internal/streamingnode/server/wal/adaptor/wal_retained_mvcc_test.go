package adaptor_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/lock"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/redo"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/replicate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/registry"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestRetainedMVCCAppendAfterConfirmation(t *testing.T) {
	mockey.PatchConvey("append publishes its target even when confirmation wins the race", t, func() {
		acked := make(chan uint64, 1)
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseAppend := func() { releaseOnce.Do(func() { close(release) }) }
		var pauseInsert atomic.Bool
		// Delay the callback after the real Acker.Ack has completed. Always return
		// false from the condition so every call runs the original UpdateMVCC;
		// only scheduling is controlled, never the MVCC state or getter result.
		mockey.Mock((*mvcc.MVCCManager).UpdateMVCC).When(func(_ *mvcc.MVCCManager, msg message.MutableMessage, _ bool) bool {
			if pauseInsert.Load() && msg.MessageType() == message.MessageTypeInsert {
				acked <- msg.TimeTick()
				<-release
			}
			return false
		}).Return().Build()

		initResourceForTest(t)
		walimplstest.Reset()
		defaultWAL := message.GetDefaultWALName()
		message.RegisterDefaultWALName(message.WALNameTest)
		defer message.RegisterDefaultWALName(defaultWAL)
		opener, err := registry.MustGetBuilder(message.WALNameTest,
			redo.NewInterceptorBuilder(),
			lock.NewInterceptorBuilder(),
			replicate.NewInterceptorBuilder(),
			timetick.NewInterceptorBuilder(),
			shard.NewInterceptorBuilder(),
		).Build()
		require.NoError(t, err)
		defer opener.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		w, err := opener.Open(ctx, &wal.OpenOption{
			Channel:        types.PChannelInfo{Name: t.Name(), Term: 1},
			DisableFlusher: true,
		})
		require.NoError(t, err)
		defer w.Close()
		require.Equal(t, message.WALNameTest, w.WALName())
		(&testOneWALFramework{t: t}).testSendCreateCollection(ctx, w)
		// Initialize segment assignment before injecting the race: CreateSegment
		// also legitimately advances this VChannel's retained target.
		_, err = w.Append(ctx, message.CreateTestEmptyInsertMesage(0, nil))
		require.NoError(t, err)
		inspector := resource.Resource().TimeTickInspector()
		cm := inspector.MustGetOperator(w.Channel()).MVCCManager()
		previousTarget := cm.GetMVCCOfVChannel(testVChannel).Timetick

		pauseInsert.Store(true)
		defer releaseAppend() // Also unblock WAL shutdown if an assertion fails.

		type appendOutcome struct {
			result *wal.AppendResult
			err    error
		}
		appended := make(chan appendOutcome, 1)
		go func() {
			result, err := w.Append(ctx, message.CreateTestEmptyInsertMesage(1, nil))
			appended <- appendOutcome{result, err}
		}()
		var writeTimeTick uint64
		select {
		case writeTimeTick = <-acked:
		case <-ctx.Done():
			t.Fatal("insert did not reach its ack", ctx.Err())
		}
		require.Greater(t, writeTimeTick, previousTarget)
		// Use the real inspector and WAL Append for confirmation while the
		// acknowledged insert has not yet entered its MVCC update callback.
		require.Eventually(t, func() bool {
			inspector.TriggerSync(w.Channel(), true)
			return cm.GetConfirmedTimeTick() >= writeTimeTick
		}, 10*time.Second, 10*time.Millisecond)
		require.Equal(t, previousTarget, cm.GetMVCCOfVChannel(testVChannel).Timetick)
		releaseAppend()
		select {
		case outcome := <-appended:
			require.NoError(t, outcome.err)
			require.Equal(t, writeTimeTick, outcome.result.TimeTick)
		case <-ctx.Done():
			t.Fatal("append did not return after releasing its MVCC callback", ctx.Err())
		}
		target, err := w.GetLatestMVCCTimestamp(ctx, testVChannel)
		require.NoError(t, err)
		require.Equal(t, writeTimeTick, target)
		require.True(t, cm.GetMVCCOfVChannel(testVChannel).Confirmed)

		for i := 0; i < 3; i++ {
			confirmed := cm.GetConfirmedTimeTick()
			require.Eventually(t, func() bool {
				inspector.TriggerSync(w.Channel(), true)
				return cm.GetConfirmedTimeTick() > confirmed
			}, 10*time.Second, 10*time.Millisecond)
			target, err = w.GetLatestMVCCTimestamp(ctx, testVChannel)
			require.NoError(t, err)
			require.Equal(t, writeTimeTick, target, "later PChannel ticks must not raise the retained target")
		}
	})
}
