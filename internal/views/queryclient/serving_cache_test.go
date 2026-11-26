package queryclient

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type servingCacheClient struct {
	get func(context.Context, int64) (*querypb.GetQueryViewLoadInfoResponse, error)
}

func (c *servingCacheClient) GetQueryViewLoadInfo(ctx context.Context, req *querypb.GetQueryViewLoadInfoRequest, _ ...grpc.CallOption) (*querypb.GetQueryViewLoadInfoResponse, error) {
	return c.get(ctx, req.CollectionID)
}

func servingCacheInfo(load uint64, schema int32) *querypb.GetQueryViewLoadInfoResponse {
	return &querypb.GetQueryViewLoadInfoResponse{
		Status: merr.Success(), CollectionID: 100, Version: load, ReadSchemaVersion: schema, ShardSetEpoch: 2,
		ServingShards: []*viewpb.DataViewOfShard{
			{Vchannel: "s", Lineage: &viewpb.QueryViewLineage{Vchannels: []string{"t1", "t2"}, SwitchTimetick: 100}},
			{Vchannel: "t1"},
			{Vchannel: "t2"},
		},
	}
}

func TestServingCacheHitsOwnTheirLineage(t *testing.T) {
	var calls atomic.Int64
	r := withServingResolver(nil, &servingCacheClient{get: func(context.Context, int64) (*querypb.GetQueryViewLoadInfoResponse, error) {
		calls.Add(1)
		return servingCacheInfo(11, 7), nil
	}})
	first, err := resolver.ResolveShards(t.Context(), r, 100)
	require.NoError(t, err)
	first[0].VChannel = "changed"
	first[0].Lineage.VChannels[0] = "changed"
	second, err := resolver.ResolveShards(t.Context(), r, 100)
	require.NoError(t, err)
	require.Equal(t, "s", second[0].VChannel)
	require.Equal(t, []string{"t1", "t2"}, second[0].Lineage.VChannels)
	for _, shard := range second {
		require.EqualValues(t, 11, shard.LoadInfoVersion)
		require.EqualValues(t, 7, shard.ReadSchemaVersion)
		require.EqualValues(t, 2, shard.ShardSetEpoch)
	}
	require.EqualValues(t, 1, calls.Load(), "cache hit must not query Coordinator")
}

func TestServingCacheInvalidationRejectsLateFill(t *testing.T) {
	for _, id := range []int64{100, 0} {
		t.Run(map[int64]string{100: "collection", 0: "all"}[id], func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int64
			r := withServingResolver(nil, &servingCacheClient{get: func(context.Context, int64) (*querypb.GetQueryViewLoadInfoResponse, error) {
				if calls.Add(1) == 1 {
					close(started)
					<-release
					return servingCacheInfo(11, 7), nil
				}
				return servingCacheInfo(12, 8), nil
			}}).(*servingResolver)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			done := make(chan []resolver.Shard, 1)
			go func() { shards, _ := r.ResolveShards(ctx, 100); done <- shards }()
			<-started
			r.invalidate(id)
			current, err := r.ResolveShards(ctx, 100)
			require.NoError(t, err)
			require.EqualValues(t, 12, current[0].LoadInfoVersion)
			close(release)
			late := <-done
			require.NotEmpty(t, late)
			require.EqualValues(t, 12, late[0].LoadInfoVersion)
			cached, err := r.ResolveShards(ctx, 100)
			require.NoError(t, err)
			require.EqualValues(t, 8, cached[0].ReadSchemaVersion)
			require.EqualValues(t, 2, calls.Load())
		})
	}
}

func TestServingCacheCancellationKeepsOtherWaiters(t *testing.T) {
	started := make(chan struct{})
	var calls atomic.Int64
	r := withServingResolver(nil, &servingCacheClient{get: func(ctx context.Context, _ int64) (*querypb.GetQueryViewLoadInfoResponse, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return servingCacheInfo(11, 7), nil
	}})
	ownerCtx, cancelOwner := context.WithCancel(t.Context())
	defer cancelOwner()
	owner := make(chan error, 1)
	go func() { _, err := resolver.ResolveShards(ownerCtx, r, 100); owner <- err }()
	<-started
	waiterCtx, cancelWaiter := context.WithTimeout(t.Context(), time.Second)
	defer cancelWaiter()
	waiter := make(chan error, 1)
	go func() { _, err := resolver.ResolveShards(waiterCtx, r, 100); waiter <- err }()
	cancelOwner()
	require.ErrorIs(t, <-owner, context.Canceled)
	require.NoError(t, <-waiter)
	require.EqualValues(t, 2, calls.Load())
}

func TestServingCacheWaiterKeepsItsDeadline(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	r := withServingResolver(nil, &servingCacheClient{get: func(context.Context, int64) (*querypb.GetQueryViewLoadInfoResponse, error) {
		close(started)
		<-release
		return servingCacheInfo(11, 7), nil
	}})
	owner := make(chan error, 1)
	go func() { _, err := resolver.ResolveShards(t.Context(), r, 100); owner <- err }()
	<-started
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err := resolver.ResolveShards(ctx, r, 100)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	close(release)
	require.NoError(t, <-owner)
}

func TestServingCacheMissingPublicationWaitsForDeadline(t *testing.T) {
	for _, notReady := range []bool{false, true} {
		var calls atomic.Int64
		r := withServingResolver(nil, &servingCacheClient{get: func(context.Context, int64) (*querypb.GetQueryViewLoadInfoResponse, error) {
			calls.Add(1)
			if notReady {
				return nil, merr.WrapErrServiceNotReadyMsg("publication pending")
			}
			return &querypb.GetQueryViewLoadInfoResponse{Status: merr.Success()}, nil
		}})
		ctx, cancel := context.WithTimeout(t.Context(), 65*time.Millisecond)
		_, err := resolver.ResolveShards(ctx, r, 100)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.GreaterOrEqual(t, calls.Load(), int64(2))
	}
}

func TestServingCacheBoundedLRUAndCollectionIsolation(t *testing.T) {
	var calls atomic.Int64
	r := withServingResolver(nil, &servingCacheClient{get: func(context.Context, int64) (*querypb.GetQueryViewLoadInfoResponse, error) {
		calls.Add(1)
		return servingCacheInfo(11, 7), nil
	}}).(*servingResolver)
	r.publications.Resize(2)
	for _, id := range []int64{100, 101, 100, 102, 101} {
		_, err := r.ResolveShards(t.Context(), id)
		require.NoError(t, err)
	}
	require.EqualValues(t, 4, calls.Load())
	require.Equal(t, 2, r.publications.Len())
	other := withServingResolver(nil, r.client)
	_, err := resolver.ResolveShards(t.Context(), other, 101)
	require.NoError(t, err)
	require.EqualValues(t, 5, calls.Load(), "clients from different clusters cannot share entries")
}

func TestServingLoadProbeChecksReloadAndSchemaIdentity(t *testing.T) {
	for _, info := range []*querypb.GetQueryViewLoadInfoResponse{servingCacheInfo(12, 7), servingCacheInfo(11, 8)} {
		call := &loadStateCall{info: info}
		ctx := context.WithValue(t.Context(), servingShardContextKey{}, resolver.Shard{VChannel: "s", ShardSetEpoch: 2, LoadInfoVersion: 11, ReadSchemaVersion: 7})
		_, err := call.result(ctx)
		require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
	}
	call := &loadStateCall{info: &querypb.GetQueryViewLoadInfoResponse{
		ShardSetEpoch: 1, ServingShards: []*viewpb.DataViewOfShard{{Vchannel: "s"}},
	}}
	ctx := context.WithValue(t.Context(), servingShardContextKey{}, resolver.Shard{VChannel: "s"})
	_, err := call.result(ctx)
	require.True(t, viewerror.AsViewError(err).IsViewInvalidated(), "an epoch-zero plan cannot cross a published epoch fence")
}

func TestServingLoadProbeDoesNotRetainAnOlderLoadIdentity(t *testing.T) {
	var calls atomic.Int64
	checks := newCollectionLoadChecks(&servingCacheClient{get: func(context.Context, int64) (*querypb.GetQueryViewLoadInfoResponse, error) {
		return servingCacheInfo(uint64(10+calls.Add(1)), 7), nil
	}})
	check := checks.acquire(100)
	defer check.release()
	ctx := context.WithValue(t.Context(), servingShardContextKey{}, resolver.Shard{VChannel: "s", ShardSetEpoch: 2, LoadInfoVersion: 11, ReadSchemaVersion: 7})
	_, err := check.check(ctx)
	require.NoError(t, err)
	ctx = context.WithValue(t.Context(), servingShardContextKey{}, resolver.Shard{VChannel: "s", ShardSetEpoch: 2, LoadInfoVersion: 12, ReadSchemaVersion: 7})
	_, err = check.check(ctx)
	require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
	_, err = check.check(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 2, calls.Load())
}

func TestServingLoadProbeInvalidationRejectsLateFill(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	checks := newCollectionLoadChecks(&servingCacheClient{get: func(context.Context, int64) (*querypb.GetQueryViewLoadInfoResponse, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			return servingCacheInfo(11, 7), nil
		}
		return servingCacheInfo(12, 7), nil
	}})
	check := checks.acquire(100)
	defer check.release()
	owner := make(chan error, 1)
	go func() { _, err := check.check(t.Context()); owner <- err }()
	<-started
	checks.invalidate(100)
	_, err := check.check(t.Context())
	require.NoError(t, err)
	close(release)
	require.NoError(t, <-owner)
	require.EqualValues(t, 12, check.last.info.GetVersion())
	require.EqualValues(t, 2, calls.Load())
}

func TestServingCachePublicInvalidationClearsTopologyAndLoadProbe(t *testing.T) {
	var calls atomic.Int64
	info := &servingCacheClient{get: func(context.Context, int64) (*querypb.GetQueryViewLoadInfoResponse, error) {
		calls.Add(1)
		return servingCacheInfo(11, 7), nil
	}}
	r := withServingResolver(nil, info)
	checks := newCollectionLoadChecks(info)
	check := checks.acquire(100)
	defer check.release()
	otherCheck := checks.acquire(101)
	defer otherCheck.release()
	_, err := otherCheck.check(t.Context())
	require.NoError(t, err)
	shardClient := &shardViewQueryClient{loadChecks: checks}
	clients := []Client{
		&legacyOnlyClient{legacy: &legacyClient{shardResolver: r, shardClient: shardClient}},
		&viewQueryClientImpl{shardResolver: r, shardClient: shardClient},
	}
	for _, client := range clients {
		_, err := resolver.ResolveShards(t.Context(), r, 100)
		require.NoError(t, err)
		_, err = check.check(t.Context())
		require.NoError(t, err)
		before := calls.Load()
		InvalidateServingGeneration(client, 100)
		_, err = resolver.ResolveShards(t.Context(), r, 100)
		require.NoError(t, err)
		_, err = check.check(t.Context())
		require.NoError(t, err)
		_, err = otherCheck.check(t.Context())
		require.NoError(t, err)
		require.Equal(t, before+2, calls.Load())
		InvalidateServingGeneration(client, 0)
		_, err = otherCheck.check(t.Context())
		require.NoError(t, err)
		require.Equal(t, before+3, calls.Load())
	}
	InvalidateServingGeneration(nil, 100)
	fallback := &legacyResolver{vchannels: []string{"s"}}
	require.Same(t, fallback, withServingResolver(fallback, nil))
}

func TestServingCacheRetryReloadsIdentityAtTheSameEpoch(t *testing.T) {
	for _, attempts := range []int{1, 2} {
		var calls atomic.Int64
		r := withServingResolver(nil, &servingCacheClient{get: func(context.Context, int64) (*querypb.GetQueryViewLoadInfoResponse, error) {
			return servingCacheInfo(uint64(calls.Add(1)), 7), nil
		}})
		n := 0
		err := retryServingGeneration(t.Context(), r, attempts, 100, func() error {
			n++
			shards, err := resolver.ResolveShards(t.Context(), r, 100)
			require.NoError(t, err)
			require.EqualValues(t, n, shards[0].LoadInfoVersion)
			if n == 1 {
				return viewerror.NewViewInvalidated("collection released and reloaded")
			}
			return nil
		})
		if attempts == 1 {
			require.True(t, viewerror.AsViewError(err).IsViewInvalidated())
		} else {
			require.NoError(t, err)
		}
		shards, err := resolver.ResolveShards(t.Context(), r, 100)
		require.NoError(t, err)
		require.EqualValues(t, 2, shards[0].LoadInfoVersion, "even the last rejected attempt must evict its publication")
		require.EqualValues(t, 2, calls.Load())
	}
}

func TestServingCacheCoalescesParallelFills(t *testing.T) {
	const count = 12
	var calls atomic.Int64
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	r := withServingResolver(nil, &servingCacheClient{get: func(ctx context.Context, _ int64) (*querypb.GetQueryViewLoadInfoResponse, error) {
		calls.Add(1)
		once.Do(func() { close(started) })
		select {
		case <-release:
			return servingCacheInfo(11, 7), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}})
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	results := make(chan error, count)
	for range count {
		go func() { _, err := resolver.ResolveShards(ctx, r, 100); results <- err }()
	}
	<-started
	close(release)
	for range count {
		require.NoError(t, <-results)
	}
	require.EqualValues(t, 1, calls.Load())
}

func TestServingCacheTransientWholeRequestRetryKeepsPublication(t *testing.T) {
	for _, transient := range []error{
		viewerror.NewOnShutdownError("owner is moving"),
		viewerror.NewNotPrimaryError("owner changed"),
		status.Error(codes.Unavailable, "connection closed"),
		&workNodeFailure{err: status.Error(codes.Unavailable, "worker connection closed")},
	} {
		t.Run(transient.Error(), func(t *testing.T) {
			var calls atomic.Int64
			meta := &admissionMetadata{split: true, owners: []string{"t1", "t2"}}
			r := withServingResolver(resolver.NewShardResolverImpl(meta), &servingCacheClient{get: func(context.Context, int64) (*querypb.GetQueryViewLoadInfoResponse, error) {
				if calls.Add(1) > 1 {
					return nil, merr.WrapErrServiceUnavailable("Coordinator unavailable")
				}
				return servingCacheInfo(11, 7), nil
			}})
			attempts := 0
			err := retryServingGeneration(t.Context(), r, 3, 100, func() error {
				attempts++
				_, err := resolver.ResolveShards(t.Context(), r, 100)
				if err != nil {
					return err
				}
				if attempts < 3 {
					return transient
				}
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, 3, attempts)
			require.EqualValues(t, 1, calls.Load(), "transport retry must work with Coordinator unavailable")
			require.Zero(t, meta.refreshes)
		})
	}
}

func TestServingCacheTransientRetryUsesOriginalDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Millisecond)
	defer cancel()
	var calls atomic.Int64
	r := withServingResolver(nil, &servingCacheClient{get: func(actual context.Context, _ int64) (*querypb.GetQueryViewLoadInfoResponse, error) {
		require.Same(t, ctx, actual)
		calls.Add(1)
		return servingCacheInfo(11, 7), nil
	}})
	attempts := 0
	err := retryServingGeneration(ctx, r, 100, 100, func() error {
		attempts++
		_, err := resolver.ResolveShards(ctx, r, 100)
		if err != nil {
			return err
		}
		return viewerror.NewNotPrimaryError("owner changed")
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Greater(t, attempts, 1)
	require.Less(t, attempts, 100)
	require.EqualValues(t, 1, calls.Load())
}
