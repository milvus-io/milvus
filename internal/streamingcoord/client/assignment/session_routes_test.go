package assignment

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/json"
	kvfactory "github.com/milvus-io/milvus/internal/util/dependency/kv"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/discoverer"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Exercise the same list/watch and projection as the WAL client, with no Coord.
func TestSessionRoutesFollowAdvertisements(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	etcd, root := kvfactory.GetEtcdAndPath()
	prefix := root + "/tests/wal-routes/" + funcutil.RandomString(16) + "/"
	lease, err := etcd.Grant(ctx, 30)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = etcd.Delete(cleanup, prefix, clientv3.WithPrefix())
	})
	a := &sessionutil.SessionRaw{ServerID: 1, Address: "localhost:1", Version: "3.0.0", WALReplicas: []*streamingpb.WALAdvertisement{{Pchannel: "p", WalReplicaId: 11, WriterTerm: proto.Int64(7), IsCchannel: true}}}
	b := &sessionutil.SessionRaw{ServerID: 2, Address: "localhost:2", Version: "3.0.0", WALReplicas: []*streamingpb.WALAdvertisement{{Pchannel: "p", WalReplicaId: 22}}}
	put := func(key string, s *sessionutil.SessionRaw, opts ...clientv3.OpOption) int64 {
		raw, e := json.Marshal(s)
		require.NoError(t, e)
		resp, e := etcd.Put(ctx, prefix+key, string(raw), opts...)
		require.NoError(t, e)
		return resp.Header.Revision
	}
	put("a", a)
	revision := put("b", b, clientv3.WithLease(lease.ID))
	rb := resolver.NewSessionBuilder(etcd, discoverer.OptSDPrefix(prefix), discoverer.OptSDVersionRange(">=2.6.0-dev"))
	defer rb.Close()
	service := &AssignmentServiceImpl{ctx: ctx, lifetime: typeutil.NewLifetime(), routes: rb.Resolver()}
	updates := make(chan *types.VersionedStreamingNodeAssignments, 16)
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	done := make(chan error, 1)
	go func() {
		done <- service.AssignmentDiscover(watchCtx, func(s *types.VersionedStreamingNodeAssignments) error { updates <- s; return nil })
	}()
	at := func(rev int64) *types.VersionedStreamingNodeAssignments {
		t.Helper()
		for {
			select {
			case s := <-updates:
				if s.Version.Global >= rev {
					return s
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
	}
	s := at(revision)
	require.Equal(t, "p", s.PChannelOfCChannel())
	require.EqualValues(t, 7, s.Assignments[1].Channels["p"].Term)
	require.Empty(t, s.Assignments[2].Channels)
	// Across Seal the only live advertisements can be read-only.
	a.WALReplicas[0].WriterTerm = nil
	a.WALReplicas[0].IsCchannel = false
	s = at(put("a", a))
	require.Empty(t, s.Assignments[1].Channels)
	require.Empty(t, s.Assignments[2].Channels)
	require.Empty(t, s.PChannelOfCChannel())
	a.WALReplicas[0].WriterTerm = proto.Int64(7)
	a.WALReplicas[0].IsCchannel = true
	s = at(put("a", a))
	require.EqualValues(t, 7, s.Assignments[1].Channels["p"].Term)
	// Conflicting writer claims cannot be resolved by map iteration or term comparison.
	b.WALReplicas[0].WriterTerm = proto.Int64(8)
	b.WALReplicas[0].IsCchannel = true
	s = at(put("b", b, clientv3.WithLease(lease.ID)))
	require.Empty(t, s.PChannelOfCChannel())
	require.Empty(t, s.Assignments[1].Channels)
	require.Empty(t, s.Assignments[2].Channels)
	a.WALReplicas[0].WriterTerm = nil
	a.WALReplicas[0].IsCchannel = false
	s = at(put("a", a))
	require.Empty(t, s.Assignments[1].Channels)
	require.EqualValues(t, 8, s.Assignments[2].Channels["p"].Term)
	require.Equal(t, "p", s.PChannelOfCChannel())
	malformed, err := etcd.Put(ctx, prefix+"b", "{", clientv3.WithLease(lease.ID))
	require.NoError(t, err)
	s = at(malformed.Header.Revision)
	require.NotContains(t, s.Assignments, int64(2))
	require.Empty(t, s.PChannelOfCChannel())
	b.WALReplicas = append(b.WALReplicas, nil, &streamingpb.WALAdvertisement{Pchannel: "invalid", WalReplicaId: 23, WriterTerm: proto.Int64(0)})
	s = at(put("b", b, clientv3.WithLease(lease.ID)))
	require.Len(t, s.Assignments[2].Channels, 1)
	require.Equal(t, "p", s.PChannelOfCChannel())
	a.Stopping = true
	s = at(put("a", a))
	require.NotContains(t, s.Assignments, int64(1))
	resp, err := etcd.Revoke(ctx, lease.ID)
	require.NoError(t, err)
	s = at(resp.Header.Revision)
	require.Empty(t, s.Assignments)
	require.Empty(t, s.PChannelOfCChannel())
	latest, err := service.GetLatestAssignments(ctx)
	require.NoError(t, err)
	require.Equal(t, s, latest)
	stopWatch()
	require.Error(t, <-done)
}
