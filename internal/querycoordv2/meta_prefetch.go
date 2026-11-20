package querycoordv2

import (
	"bytes"
	"context"
	"sync"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// metaPrefetchPrefixes are the prefixes QueryCoord walks in full while it
// initializes: load configs and QueryViews.
var metaPrefetchPrefixes = []string{
	queryview.VersionedLoadInfoPrefix + "/",
	"coord/qv/",
}

type prefetchedKV struct {
	key   []byte
	value []byte
}

// metaPrefetchKV walks the QueryCoord recovery prefixes in the background
// before QueryCoord initializes, so the cold etcd reads overlap DataCoord
// recovery, and serves later walks of those prefixes from that read.
//
// The prefetched state stays exact until QueryCoord writes: nothing else writes
// these keys before MixCoord registers the DDL callbacks and serves RPCs, which
// it does only after QueryCoord has initialized. Any write through this kv
// therefore drops every prefetched walk, and release, called once QueryCoord
// has initialized, makes it a plain pass-through.
type metaPrefetchKV struct {
	kv.MetaKv
	mu    sync.Mutex
	walks map[string]*conc.Future[[]prefetchedKV]
}

// newMetaPrefetchKV starts walking metaPrefetchPrefixes on base.
func newMetaPrefetchKV(ctx context.Context, base kv.MetaKv) *metaPrefetchKV {
	pageSize := paramtable.Get().MetaStoreCfg.PaginationSize.GetAsInt()
	walks := make(map[string]*conc.Future[[]prefetchedKV], len(metaPrefetchPrefixes))
	for _, prefix := range metaPrefetchPrefixes {
		walks[prefix] = conc.Go(func() ([]prefetchedKV, error) {
			var pairs []prefetchedKV
			err := base.WalkWithPrefix(ctx, prefix, pageSize, func(key, value []byte) error {
				pairs = append(pairs, prefetchedKV{key: bytes.Clone(key), value: bytes.Clone(value)})
				return nil
			})
			return pairs, err
		})
	}
	return &metaPrefetchKV{MetaKv: base, walks: walks}
}

func (p *metaPrefetchKV) prefetched(prefix string) *conc.Future[[]prefetchedKV] {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.walks[prefix]
}

// release stops serving prefetched walks.
func (p *metaPrefetchKV) release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.walks = nil
}

// WalkWithPrefix replays a prefetched walk of prefix if there is one and it
// succeeded; otherwise it walks prefix now, as it would have without prefetch.
func (p *metaPrefetchKV) WalkWithPrefix(ctx context.Context, prefix string, paginationSize int, fn func([]byte, []byte) error) error {
	if walk := p.prefetched(prefix); walk != nil {
		select {
		case <-walk.Inner():
			if pairs, err := walk.Await(); err == nil {
				for _, pair := range pairs {
					if err := fn(pair.key, pair.value); err != nil {
						return err
					}
				}
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return p.MetaKv.WalkWithPrefix(ctx, prefix, paginationSize, fn)
}

func (p *metaPrefetchKV) Save(ctx context.Context, key, value string) error {
	p.release()
	return p.MetaKv.Save(ctx, key, value)
}

func (p *metaPrefetchKV) MultiSave(ctx context.Context, kvs map[string]string) error {
	p.release()
	return p.MetaKv.MultiSave(ctx, kvs)
}

func (p *metaPrefetchKV) Remove(ctx context.Context, key string) error {
	p.release()
	return p.MetaKv.Remove(ctx, key)
}

func (p *metaPrefetchKV) MultiRemove(ctx context.Context, keys []string) error {
	p.release()
	return p.MetaKv.MultiRemove(ctx, keys)
}

func (p *metaPrefetchKV) RemoveWithPrefix(ctx context.Context, key string) error {
	p.release()
	return p.MetaKv.RemoveWithPrefix(ctx, key)
}

func (p *metaPrefetchKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	p.release()
	return p.MetaKv.MultiSaveAndRemove(ctx, saves, removals, preds...)
}

func (p *metaPrefetchKV) MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	p.release()
	return p.MetaKv.MultiSaveAndRemoveWithPrefix(ctx, saves, removals, preds...)
}

func (p *metaPrefetchKV) CompareVersionAndSwap(ctx context.Context, key string, version int64, target string) (bool, error) {
	p.release()
	return p.MetaKv.CompareVersionAndSwap(ctx, key, version, target)
}
