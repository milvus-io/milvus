package handler

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	grpcresolver "google.golang.org/grpc/resolver"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
)

type sessionTestResolver struct{ resolver.Resolver }

func (*sessionTestResolver) Watch(context.Context, func(resolver.VersionedState) error) error {
	panic("mockey")
}

func TestSessionGoneRequiresObservedRemoval(t *testing.T) {
	s := &sessionChanges{changed: make(chan struct{}), members: make(map[int64]struct{}), gone: make(map[int64]time.Time)}
	empty := resolver.VersionedState{}
	s.update(empty)
	require.False(t, s.Gone(11), "initial absence is not confirmed deletion")
	present := resolver.VersionedState{State: grpcresolver.State{Addresses: []grpcresolver.Address{{Attributes: attributes.WithServerID(new(attributes.Attributes), 11)}}}}
	s.update(present)
	require.False(t, s.Gone(11))
	s.update(empty)
	require.True(t, s.Gone(11))
	s.update(present)
	require.False(t, s.Gone(11), "a contradictory fresh membership must not keep an old tombstone")
	s.gone[12] = time.Now().Add(-time.Second)
	require.False(t, s.Gone(12))
	s.update(present)
	require.Empty(t, s.gone)
}

func TestSessionChangesBroadcastAndJoin(t *testing.T) {
	updates := make(chan struct{})
	patch := mockey.Mock((*sessionTestResolver).Watch).To(func(_ *sessionTestResolver, ctx context.Context, cb func(resolver.VersionedState) error) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-updates:
				if err := cb(resolver.VersionedState{}); err != nil {
					return err
				}
			}
		}
	}).Build()
	defer patch.UnPatch()
	changes := newSessionChanges(&sessionTestResolver{})
	first := changes.Changed()
	updates <- struct{}{}
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("missed revision")
	}
	second := changes.Changed()
	require.NotEqual(t, first, second)
	changes.Close()
	changes.Close()
	select {
	case <-second:
	default:
		t.Fatal("close must wake waiters")
	}
	select {
	case <-changes.done:
	default:
		t.Fatal("close must join observer")
	}
}
