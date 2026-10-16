package walsummary

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueryBarrierIsScopedAndNeverDurable(t *testing.T) {
	m := NewManager(ManagerConfig{})
	defer m.WatchTransform("a")()
	defer m.WatchTransform("b")()
	m.ObserveMessage(t.Context(), newTestDeleteMessage(t, "a", 10, 1, 1))
	a, b := readNotification(t, m, "a"), readNotification(t, m, "b")
	lastObserved, lastAcked, sealed := m.lastObserved, m.lastAcked, m.sealedThrough
	m.ObserveQueryBarrier("a", 100)
	requireNotification(t, a.TransformChanged, true)
	requireNotification(t, b.TransformChanged, false)
	a = readNotification(t, m, "a")
	require.EqualValues(t, 100, a.CoveredThrough)
	require.Len(t, a.Entries, 1)
	require.EqualValues(t, 10, a.Entries[0].TimeTick)
	require.EqualValues(t, 10, readNotification(t, m, "b").CoveredThrough)
	require.Equal(t, lastObserved, m.lastObserved)
	require.Equal(t, lastAcked, m.lastAcked)
	require.Equal(t, sealed, m.sealedThrough)
	m.ObserveQueryBarrier("a", 90)
	requireNotification(t, a.TransformChanged, false)
	// A reader created after notification still sees the complete interval.
	require.EqualValues(t, 100, readNotification(t, m, "a").CoveredThrough)
	m.AdvanceGCTimeTick("a", DroppedVChannelTimeTick)
	require.NotContains(t, m.queryBarriers, "a")
}
