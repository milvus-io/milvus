package handler

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
)

type sessionTestResolver struct{ resolver.Resolver }

func (*sessionTestResolver) Watch(context.Context, func(resolver.VersionedState) error) error {
	panic("mockey")
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
