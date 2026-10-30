package snview

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
)

func TestDirectorySnapshotsAndCoalescing(t *testing.T) {
	d := newReplicaDirectory()
	defer d.Close()
	d.Set("p_1v0", 2, &qviews.QueryViewVersion{}, false)
	d.Set("p_1v0", 1, &qviews.QueryViewVersion{}, false)
	w := d.Watch()
	defer w.Close()
	w.Subscribe("p_1v0")
	require.Equal(t, testReplicas(1, 2), qviews.ReplicasFromProto(w.Pop().GetReplicas()))
	require.Nil(t, w.Pop())
	d.Set("p_1v0", 1, &qviews.QueryViewVersion{}, false)
	require.Nil(t, w.Pop(), "unchanged eligibility does not publish")
	for i := 0; i < 1000; i++ {
		d.Set("p_1v0", 3, testUpVersion(i%2 == 0), false)
	}
	require.Len(t, w.pending, 1)
	require.Equal(t, testReplicas(1, 2), qviews.ReplicasFromProto(w.Pop().GetReplicas()))
	w.Subscribe("p_2v0")
	snapshot := w.Pop()
	require.Equal(t, "p_2v0", snapshot.GetVchannel())
	require.Empty(t, qviews.ReplicasFromProto(snapshot.GetReplicas()))
	d.Set("p_2v0", 5, &qviews.QueryViewVersion{}, false)
	w.Unsubscribe("p_2v0")
	require.Nil(t, w.Pop(), "unsubscribe discards queued snapshots")
	w.Subscribe("p_2v0")
	require.Equal(t, testReplicas(5), qviews.ReplicasFromProto(w.Pop().GetReplicas()))
	w.Close()
	require.Empty(t, d.listeners)
	d.Close()
	select {
	case <-w.Done():
	default:
		t.Fatal("owner close must wake watchers")
	}
	w.Subscribe("p_3v0")
	require.Nil(t, w.Pop())
}

func TestDirectoryRegistrationDoesNotLosePublication(t *testing.T) {
	for i := 0; i < 100; i++ {
		d := newReplicaDirectory()
		w := d.Watch()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); w.Subscribe("p_1v0") }()
		go func() { defer wg.Done(); d.Set("p_1v0", 7, &qviews.QueryViewVersion{}, false) }()
		wg.Wait()
		require.Equal(t, testReplicas(7), qviews.ReplicasFromProto(w.Pop().GetReplicas()))
		w.Close()
		d.Close()
	}
}

func testUpVersion(up bool) *qviews.QueryViewVersion {
	if up {
		return &qviews.QueryViewVersion{}
	}
	return nil
}

func testReplicas(ids ...int64) []qviews.Replica {
	replicas := make([]qviews.Replica, len(ids))
	for i, id := range ids {
		replicas[i].ReplicaID = id
	}
	return replicas
}

func TestDirectoryVersionChangesAndOwnership(t *testing.T) {
	d := newReplicaDirectory()
	defer d.Close()
	w := d.Watch()
	defer w.Close()
	w.Subscribe("p_1v0")
	require.Empty(t, w.Pop().GetReplicas())
	version := qviews.QueryViewVersion{DataVersion: qviews.DataVersion{StreamingVersion: 1, CompactVersion: 2}, QueryVersion: 3}
	original := version
	d.Set("p_1v0", 7, &version, false)
	version.QueryVersion++
	first := w.Pop()
	require.Equal(t, original, qviews.FromProtoQueryViewVersion(first.Replicas[0].MaxUpViewVersion), "publication copies the input")
	d.Set("p_1v0", 7, &version, false)
	require.Equal(t, version, qviews.FromProtoQueryViewVersion(w.Pop().Replicas[0].MaxUpViewVersion), "same replica with new version must publish")
	d.Set("p_1v0", 7, &version, false)
	require.Nil(t, w.Pop(), "same version must not publish")
	// Wire snapshots must not alias directory metadata.
	first.Replicas[0].MaxUpViewVersion.DataVersion.StreamingVersion = 99
	w.Unsubscribe("p_1v0")
	w.Subscribe("p_1v0")
	require.Equal(t, version, qviews.FromProtoQueryViewVersion(w.Pop().Replicas[0].MaxUpViewVersion))
	d.Set("p_1v0", 7, nil, false)
	require.Empty(t, w.Pop().GetReplicas())
}
