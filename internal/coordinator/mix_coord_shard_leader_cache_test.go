package coordinator

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/proxyutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/proxypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// recordingProxyClientManager captures the invalidation requests the
// coordinator fans out. Every other method is inherited from the generated
// mock with no expectations set, so a call to one of them fails the test
// rather than passing quietly.
type recordingProxyClientManager struct {
	proxyutil.ProxyClientManagerInterface

	seen []*proxypb.InvalidateShardLeaderCacheRequest
	err  error
}

func (m *recordingProxyClientManager) InvalidateShardLeaderCache(_ context.Context, req *proxypb.InvalidateShardLeaderCacheRequest) error {
	m.seen = append(m.seen, req)
	return m.err
}

// The collection the engine released is the collection whose shard leaders
// every proxy must forget. Fanning out a different id, or an empty list, would
// leave the proxies routing queries at query nodes that no longer serve it.
func TestInvalidateShardLeaderCacheNamesTheCollection(t *testing.T) {
	manager := &recordingProxyClientManager{}
	s := &mixCoordImpl{proxyClientManager: manager}

	require.NoError(t, s.InvalidateShardLeaderCache(context.Background(), 42))

	require.Len(t, manager.seen, 1, "the coordinator must fan out exactly one invalidation")
	assert.Equal(t, []int64{42}, manager.seen[0].GetCollectionIDs())
}

func TestInvalidateShardLeaderCacheReportsTheFanOutFailure(t *testing.T) {
	want := errors.New("proxy unreachable")
	s := &mixCoordImpl{proxyClientManager: &recordingProxyClientManager{err: want}}

	assert.ErrorIs(t, s.InvalidateShardLeaderCache(context.Background(), 42), want,
		"a caller that cannot tell a landed invalidation from a lost one cannot decide what to do next")
}

// Before initInternal wires the manager, the call must FAIL, not silently
// succeed: a caller that just released a collection treats a nil error as
// "every proxy has been told", and a skipped fan-out leaves proxies routing
// to leaders that are gone. (The manager is assigned in initInternal, right
// after rootcoord's Init builds it - the same source fileResourceObserver
// draws from.)
func TestInvalidateShardLeaderCacheWithoutAProxyClientManagerFails(t *testing.T) {
	s := &mixCoordImpl{}
	err := s.InvalidateShardLeaderCache(context.Background(), 42)
	require.Error(t, err, "an invalidation that went nowhere must not read as delivered")
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)
}
