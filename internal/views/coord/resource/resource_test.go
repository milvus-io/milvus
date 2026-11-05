package resource

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/coord/readiness"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestResourceLifecycle(t *testing.T) {
	previous := Resource()
	t.Cleanup(func() { Init(previous) })
	first := &Resources{Readiness: readiness.NewNotifications()}
	Init(first)
	require.Same(t, first, Resource())
	first.Readiness.Close()
	require.ErrorIs(t, Resource().Refresh(t.Context(), 1), merr.ErrServiceUnavailable)
	require.ErrorIs(t, Resource().AddPartition(t.Context(), 1, 2), merr.ErrServiceUnavailable)
	second := &Resources{Readiness: readiness.NewNotifications()}
	Init(second)
	t.Cleanup(second.Readiness.Close)
	first.Readiness.Close()
	require.Same(t, second, Resource(), "old lifecycle cleanup must not unpublish its successor")
}
