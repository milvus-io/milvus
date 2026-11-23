package queryview

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	tikvclient "github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv"

	"github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type failSecondRegionDeleteClient struct {
	tikvclient.Client
	first, second uint64
	firstHandled  chan struct{}
	signalFirst   sync.Once
	cancel        context.CancelFunc
	failDelete    atomic.Bool
}

func (c *failSecondRegionDeleteClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if !c.failDelete.Load() || (req.Type != tikvrpc.CmdPrewrite && req.Type != tikvrpc.CmdDeleteRange) {
		return c.Client.SendRequest(ctx, addr, req, timeout)
	}
	if req.RegionId == c.second {
		select {
		case <-c.firstHandled:
			c.cancel()
			return nil, context.Canceled
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	resp, err := c.Client.SendRequest(ctx, addr, req, timeout)
	if req.RegionId == c.first && err == nil {
		regionErr, decodeErr := resp.GetRegionError()
		if regionErr == nil && decodeErr == nil {
			c.signalFirst.Do(func() { close(c.firstHandled) })
		}
	}
	return resp, err
}

func TestCollectionLoadInfoRemoveCollectionAcrossTiKVRegions(t *testing.T) {
	paramtable.Init()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	removeCtx, cancelRemove := context.WithCancel(ctx)
	defer cancelRemove()

	client, cluster, pd, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	_, _, firstRegion := testutils.BootstrapWithSingleStore(cluster)
	secondRegion, secondPeer := cluster.AllocID(), cluster.AllocID()
	fault := &failSecondRegionDeleteClient{
		first: firstRegion, second: secondRegion, firstHandled: make(chan struct{}), cancel: cancelRemove,
	}
	store, err := tikvclient.NewTestTiKVStore(client, pd, func(client tikvclient.Client) tikvclient.Client {
		fault.Client = client
		return fault
	}, nil, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	storage := tikv.NewTiKV(&txnkv.Client{KVStore: store}, "qv-load-info")
	boundary := storage.GetPath(collectionLoadInfoVersionKey(1, 2))
	cluster.Split(firstRegion, secondRegion, []byte(boundary), []uint64{secondPeer}, secondPeer)
	catalog := NewCollectionLoadInfoCatalog(storage)
	for _, version := range []uint64{1, 2} {
		require.NoError(t, catalog.Save(ctx, &viewpb.CollectionLoadConfig{CollectionID: 1, Version: version}))
	}

	fault.failDelete.Store(true)
	require.Error(t, catalog.RemoveCollection(removeCtx, 1))
	require.ErrorIs(t, removeCtx.Err(), context.Canceled, "the second Region must fail after the first handles the request")
	fault.failDelete.Store(false)
	records, err := catalog.List(ctx)
	require.NoError(t, err)
	require.Len(t, records, 2, "a failed cross-Region deletion must leave both versions visible")

	require.NoError(t, catalog.RemoveCollection(ctx, 1))
	records, err = catalog.List(ctx)
	require.NoError(t, err)
	require.Empty(t, records)
}
