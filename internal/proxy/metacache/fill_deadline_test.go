package metacache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCacheMissHonorsDeadlineBehindSharedWork(t *testing.T) {
	for _, resource := range []string{"collection", "partition", "database"} {
		t.Run(resource, func(t *testing.T) {
			cache, err := NewMetaCache(nil)
			require.NoError(t, err)
			defer cache.Close()
			blocked, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			var calls atomic.Int32
			hold := func() {
				if calls.Add(1) == 1 {
					close(blocked)
				}
				<-release
			}
			var lookup func(context.Context) error
			switch resource {
			case "collection":
				p := mockey.Mock((*MetaCache).describeCollection).To(func(*MetaCache, context.Context, string, string, int64) (*milvuspb.DescribeCollectionResponse, error) {
					hold()
					return nil, merr.ErrServiceUnavailable
				}).Build()
				defer p.UnPatch()
				lookup = func(ctx context.Context) error {
					_, err := cache.GetCollectionInfo(ctx, "", "", 101)
					return err
				}
			case "partition":
				schema, err := NewSchemaInfo(&schemapb.CollectionSchema{Name: "c"})
				require.NoError(t, err)
				cache.collections[101] = &CollectionInfo{CollID: 101, DBName: "db", Schema: schema}
				cache.nameIdx["db"] = map[string]int64{"c": 101}
				p := mockey.Mock((*MetaCache).showPartitions).To(func(*MetaCache, context.Context, string, string, int64) (*milvuspb.ShowPartitionsResponse, error) {
					hold()
					return nil, merr.ErrServiceUnavailable
				}).Build()
				defer p.UnPatch()
				lookup = func(ctx context.Context) error {
					_, err := cache.GetPartitionInfos(ctx, "db", "c")
					return err
				}
			case "database":
				p := mockey.Mock((*MetaCache).describeDatabase).To(func(*MetaCache, context.Context, string) (*rootcoordpb.DescribeDatabaseResponse, error) {
					hold()
					return nil, merr.ErrServiceUnavailable
				}).Build()
				defer p.UnPatch()
				lookup = func(ctx context.Context) error {
					_, err := cache.GetDatabaseInfo(ctx, "db")
					return err
				}
			}

			leader := make(chan error, 1)
			go func() { leader <- lookup(t.Context()) }()
			<-blocked
			defer func() {
				unblock()
				require.ErrorIs(t, <-leader, merr.ErrServiceUnavailable)
				require.EqualValues(t, 1, calls.Load())
			}()
			expectCacheDeadline(t, lookup, unblock)
		})
	}
}

func TestCacheMissHonorsDeadlineBehindInvalidation(t *testing.T) {
	cache, err := NewMetaCache(nil)
	require.NoError(t, err)
	defer cache.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	p := mockey.Mock((*MetaCache).describeCollection).To(func(_ *MetaCache, _ context.Context, _, _ string, id int64) (*milvuspb.DescribeCollectionResponse, error) {
		if id == 101 {
			close(entered)
			<-release
		}
		return nil, merr.ErrServiceUnavailable
	}).Build()
	defer p.UnPatch()
	oldFill := make(chan error, 1)
	go func() {
		_, err := cache.GetCollectionInfo(t.Context(), "", "", 101)
		oldFill <- err
	}()
	<-entered
	evicted := make(chan struct{})
	go func() {
		cache.RemoveCollectionsByID(t.Context(), 101)
		close(evicted)
	}()
	defer func() {
		unblock()
		require.ErrorIs(t, <-oldFill, merr.ErrServiceUnavailable)
		<-evicted
	}()
	// The unrelated RPC holds a read permit. Another reader can enter until
	// the invalidation has queued for all permits.
	require.Eventually(t, func() bool {
		if cache.fillMu.TryAcquire(1) {
			cache.fillMu.Release(1)
			return false
		}
		return true
	}, time.Second, time.Millisecond)
	expectCacheDeadline(t, func(ctx context.Context) error {
		_, err := cache.GetCollectionInfo(ctx, "", "", 202)
		return err
	}, unblock)
}

func expectCacheDeadline(t *testing.T, lookup func(context.Context) error, unblock func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- lookup(ctx) }()
	<-ctx.Done()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Error("cache miss is still waiting after the caller's deadline")
		unblock()
		<-result
	}
}
