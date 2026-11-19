package viewfeedback

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestReplicaFeedbackAcceptsConsecutiveNodeWindows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := New(&loadmgr.LoadConfigStore{}, testDataViews{})
		defer store.Close()
		store.onLoadConfig(1, &loadmgr.LoadConfig{Replicas: []*loadmgr.ReplicaAssignment{{ReplicaID: 10}}}, 1)
		h := &snview.SNQueryViewHandler{}
		id := qviews.ShardID{ReplicaID: 10, VChannel: "p_1v0"}
		h.RecordShardAccess(id)
		time.Sleep(time.Second)
		first := h.TakeViewFeedback()
		store.OnViewFeedback(first)
		time.Sleep(2 * time.Second)
		h.RecordShardAccess(id)
		time.Sleep(3 * time.Second)
		second := h.TakeViewFeedback()
		require.Greater(t, second.StartUnixNanos, first.EndUnixNanos)
		store.OnViewFeedback(second)
		got, ok := store.GetViewFeedback(10)
		require.True(t, ok)
		require.EqualValues(t, second.Shards[id].LastAccessUnixNanos, got.LastAccessUnixNanos)
		require.Equal(t, 2.0/6, got.RequestRates.Last2Windows, "both consecutive SN windows must be accepted")

		// No send loop takes feedback while the stream is disconnected.
		time.Sleep(3 * time.Minute)
		h.RecordShardAccess(id)
		time.Sleep(time.Second)
		reconnected := h.TakeViewFeedback()
		require.Greater(t, reconnected.EndUnixNanos-reconnected.StartUnixNanos, int64(retention))
		store.OnViewFeedback(reconnected)
		got, ok = store.GetViewFeedback(10)
		require.True(t, ok)
		require.Equal(t, reconnected.Shards[id].LastAccessUnixNanos, got.LastAccessUnixNanos)
		rate := 1 / time.Duration(reconnected.EndUnixNanos-reconnected.StartUnixNanos).Seconds()
		require.Equal(t, RequestRates{rate, rate, rate, rate}, got.RequestRates, "long windows use their full duration, without clipping Start")
		lastAccess := got.LastAccessUnixNanos
		time.Sleep(retention + time.Second)
		synctest.Wait()
		got, _ = store.GetViewFeedback(10)
		require.Equal(t, &Snapshot{LastAccessUnixNanos: lastAccess}, got, "expiration is by End, and retains LastAccess")
	})
}
