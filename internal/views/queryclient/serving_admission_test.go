package queryclient

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type admissionMetadata struct {
	mu        sync.Mutex
	owners    []string
	split     bool
	err       error
	refreshes int
	onRefresh func(*admissionMetadata)
	onRead    func()
}

func (m *admissionMetadata) GetCollectionVChannels(context.Context, int64) ([]string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.onRead != nil {
		m.onRead()
	}
	return append([]string(nil), m.owners...), m.split, m.err
}

func (m *admissionMetadata) RefreshShardRouting(context.Context, int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.refreshes++
	if m.onRefresh != nil {
		m.onRefresh(m)
	}
}

type admissionLoadInfo struct {
	mu    sync.Mutex
	info  *querypb.GetQueryViewLoadInfoResponse
	err   error
	calls int
	seen  chan struct{}
	once  sync.Once
	onGet func()
}

func (c *admissionLoadInfo) GetQueryViewLoadInfo(ctx context.Context, _ *querypb.GetQueryViewLoadInfoRequest, _ ...grpc.CallOption) (*querypb.GetQueryViewLoadInfoResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	c.once.Do(func() { close(c.seen) })
	if c.onGet != nil {
		c.onGet()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.err != nil {
		return nil, c.err
	}
	return proto.Clone(c.info).(*querypb.GetQueryViewLoadInfoResponse), nil
}

func (c *admissionLoadInfo) publish(info *querypb.GetQueryViewLoadInfoResponse, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.info, c.err = info, err
}

func admissionInfo(epoch uint64, names ...string) *querypb.GetQueryViewLoadInfoResponse {
	info := &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success(), CollectionID: 100, ShardSetEpoch: epoch}
	for _, name := range names {
		info.ServingShards = append(info.ServingShards, &viewpb.DataViewOfShard{Vchannel: name})
	}
	return info
}

type admissionResult struct {
	shards []resolver.Shard
	err    error
}

func startAdmission(t *testing.T, meta *admissionMetadata, client *admissionLoadInfo) (context.CancelFunc, <-chan admissionResult) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	t.Cleanup(cancel)
	r := withServingResolver(resolver.NewShardResolverImpl(meta), client)
	done := make(chan admissionResult, 1)
	go func() { shards, err := resolver.ResolveShards(ctx, r, 100); done <- admissionResult{shards, err} }()
	select {
	case <-client.seen:
	case <-ctx.Done():
		t.Fatal("load info was not queried")
	}
	return cancel, done
}

func admissionBlocked(t *testing.T, done <-chan admissionResult) {
	t.Helper()
	select {
	case got := <-done:
		t.Fatalf("returned before required owners were published: %+v", got)
	case <-time.After(40 * time.Millisecond):
	}
}

func TestServingAdmissionWaitsForEachSplitGeneration(t *testing.T) {
	for _, tc := range []struct {
		name               string
		oldEpoch, newEpoch uint64
		source, other      string
		targets            []string
	}{
		{"first", 0, 2, "s", "", []string{"t1", "t2"}},
		{"second", 3, 4, "t1", "t2", []string{"t11", "t12"}},
		{"third", 5, 6, "t12", "t11", []string{"t121", "t122"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owners := append([]string(nil), tc.targets...)
			oldNames := []string{tc.source}
			if tc.other != "" {
				owners = append(owners, tc.other)
				oldNames = append(oldNames, tc.other)
			}
			meta := &admissionMetadata{owners: owners, split: true}
			client := &admissionLoadInfo{info: admissionInfo(tc.oldEpoch, oldNames...), seen: make(chan struct{})}
			_, done := startAdmission(t, meta, client)
			admissionBlocked(t, done)
			published := admissionInfo(tc.newEpoch, append([]string{tc.source}, owners...)...)
			published.ServingShards[0].Lineage = &viewpb.QueryViewLineage{Vchannels: tc.targets, SwitchTimetick: 100}
			client.publish(published, nil)
			got := <-done
			require.NoError(t, got.err)
			require.Len(t, got.shards, len(owners)+1)
			require.Equal(t, tc.newEpoch, got.shards[0].ShardSetEpoch)
			require.Equal(t, tc.targets, got.shards[0].Lineage.VChannels)
		})
	}
}

func TestServingAdmissionRefreshesOwnersWhileWaiting(t *testing.T) {
	meta := &admissionMetadata{owners: []string{"old", "other"}, split: true}
	meta.onRefresh = func(m *admissionMetadata) {
		// The refresh may join a pre-invalidation fill once. Once a split has
		// been observed, that older snapshot must not send the read back to g0.
		if m.refreshes == 1 {
			m.split = false
			m.owners = []string{"old"}
			return
		}
		m.split = true
		m.owners = []string{"new1", "new2", "other"}
	}
	client := &admissionLoadInfo{info: admissionInfo(5, "new1", "new2", "other"), seen: make(chan struct{})}
	_, done := startAdmission(t, meta, client)
	got := <-done
	require.NoError(t, got.err)
	require.Equal(t, []resolver.Shard{{ShardSetEpoch: 5, VChannel: "new1"}, {ShardSetEpoch: 5, VChannel: "new2"}, {ShardSetEpoch: 5, VChannel: "other"}}, got.shards)
	meta.mu.Lock()
	defer meta.mu.Unlock()
	require.GreaterOrEqual(t, meta.refreshes, 2)
}

func TestServingAdmissionAfterStaleUnsplitRefresh(t *testing.T) {
	meta := &admissionMetadata{owners: []string{"s"}}
	meta.onRefresh = func(m *admissionMetadata) { m.split = true; m.owners = []string{"t1", "t2"} }
	client := &admissionLoadInfo{info: admissionInfo(0, "s"), seen: make(chan struct{})}
	r := withServingResolver(resolver.NewShardResolverImpl(meta), client)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	attempts := 0
	var seen [][]resolver.Shard
	go func() {
		done <- retryServingGeneration(ctx, r, 2, 100, func() error {
			attempts++
			shards, err := resolver.ResolveShards(ctx, r, 100)
			if err != nil {
				return err
			}
			seen = append(seen, shards)
			if attempts == 1 {
				return viewerror.ConvertViewError("GetQueryPlan", viewerror.NewGRPCStatusFromViewError(viewerror.NewViewInvalidated("source fenced")).Err())
			}
			return nil
		})
	}()
	select {
	case <-client.seen:
	case <-ctx.Done():
		t.Fatal("refresh did not enter serving resolver")
	}
	select {
	case err := <-done:
		t.Fatalf("retry budget expired during safe-generation wait: %v", err)
	case <-time.After(80 * time.Millisecond):
	}
	info := admissionInfo(2, "s", "t1", "t2")
	info.ServingShards[0].Lineage = &viewpb.QueryViewLineage{Vchannels: []string{"t1", "t2"}, SwitchTimetick: 100}
	client.publish(info, nil)
	require.NoError(t, <-done)
	require.Equal(t, 2, attempts)
	require.Equal(t, []resolver.Shard{{VChannel: "s"}}, seen[0])
	require.Len(t, seen[1], 3)
}

func TestServingAdmissionCancellationAndRelease(t *testing.T) {
	for _, phase := range []string{"cancel", "release", "transport", "metadata"} {
		t.Run(phase, func(t *testing.T) {
			meta := &admissionMetadata{owners: []string{"t1", "t2"}, split: true}
			client := &admissionLoadInfo{info: admissionInfo(0, "s"), seen: make(chan struct{})}
			cancel, done := startAdmission(t, meta, client)
			admissionBlocked(t, done)
			switch phase {
			case "cancel":
				cancel()
			case "release":
				client.publish(&querypb.GetQueryViewLoadInfoResponse{Status: merr.Status(merr.ErrCollectionNotLoaded)}, nil)
			case "transport":
				client.publish(nil, merr.ErrIoFailed)
			case "metadata":
				meta.mu.Lock()
				meta.err = merr.ErrCollectionNotFound
				meta.mu.Unlock()
			}
			want := map[string]error{"cancel": context.Canceled, "release": merr.ErrCollectionNotLoaded, "transport": merr.ErrIoFailed, "metadata": merr.ErrCollectionNotFound}[phase]
			require.ErrorIs(t, (<-done).err, want)
		})
	}
}

func TestServingAdmissionStaticRestore(t *testing.T) {
	client := &admissionLoadInfo{info: admissionInfo(0, "wrong"), seen: make(chan struct{})}
	meta := &admissionMetadata{owners: []string{"a", "b", "c"}}
	r := withServingResolver(resolver.NewShardResolverImpl(meta), client)
	shards, err := resolver.ResolveShards(t.Context(), r, 100)
	require.NoError(t, err)
	require.Equal(t, []resolver.Shard{{VChannel: "a"}, {VChannel: "b"}, {VChannel: "c"}}, shards)
	require.Zero(t, client.calls, "a restored static routing table has no local split lineage")
}

func TestServingAdmissionUsesOriginalContextAndBackoff(t *testing.T) {
	t.Run("already canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		client := &admissionLoadInfo{seen: make(chan struct{})}
		r := withServingResolver(resolver.NewShardResolverImpl(&admissionMetadata{split: true}), client)
		_, err := resolver.ResolveShards(ctx, r, 100)
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, client.calls)
	})
	for _, point := range []string{"metadata", "refresh", "rpc"} {
		t.Run("canceled during "+point, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			meta := &admissionMetadata{owners: []string{"t1", "t2"}, split: true}
			client := &admissionLoadInfo{info: admissionInfo(0, "s"), seen: make(chan struct{})}
			reads := 0
			meta.onRead = func() {
				reads++
				if (point == "metadata" && reads == 1) || (point == "refresh" && reads == 2) {
					cancel(merr.ErrServiceUnavailable)
				}
			}
			if point == "rpc" {
				client.onGet = func() { cancel(merr.ErrServiceUnavailable) }
			}
			r := withServingResolver(resolver.NewShardResolverImpl(meta), client)
			_, err := resolver.ResolveShards(ctx, r, 100)
			require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		})
	}
	t.Run("deadline and bounded polling", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 350*time.Millisecond)
		defer cancel()
		meta := &admissionMetadata{owners: []string{"t1", "t2"}, split: true}
		client := &admissionLoadInfo{info: admissionInfo(0, "s"), seen: make(chan struct{})}
		r := withServingResolver(resolver.NewShardResolverImpl(meta), client)
		_, err := resolver.ResolveShards(ctx, r, 100)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Greater(t, client.calls, 1)
		require.Less(t, client.calls, 12, "waiting readers must back off control-plane polling")
	})
	t.Run("initial metadata error", func(t *testing.T) {
		meta := &admissionMetadata{err: merr.ErrCollectionNotFound}
		client := &admissionLoadInfo{seen: make(chan struct{})}
		_, err := resolver.ResolveShards(t.Context(), withServingResolver(resolver.NewShardResolverImpl(meta), client), 100)
		require.ErrorIs(t, err, merr.ErrCollectionNotFound)
	})
}

func TestServingAdmissionPreservesCustomResolverBoundary(t *testing.T) {
	client := &admissionLoadInfo{seen: make(chan struct{})}
	custom := &legacyResolver{vchannels: []string{"custom"}}
	wrapped := withServingResolver(custom, client)
	shards, err := resolver.ResolveShards(t.Context(), wrapped, 100)
	require.NoError(t, err)
	require.Equal(t, []resolver.Shard{{VChannel: "custom"}}, shards)
	calls := 0
	err = retryServingGeneration(t.Context(), wrapped, 3, 100, func() error {
		calls++
		return viewerror.NewViewInvalidated("custom resolver owns its topology")
	})
	require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
	require.Equal(t, 1, calls)
	require.Zero(t, client.calls)
}
