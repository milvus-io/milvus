package walsummary

import (
	"container/list"
	"context"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// chunkIndex mirrors the retained manifest. Membership and snapshots are guarded
// by Manager.mu; payloads have a separate lock so object I/O never holds it.
type chunkIndex struct {
	chunks []*indexedChunk
	cache  chunkCache
}

type indexedChunk struct {
	*streamingpb.PChannelSummaryChunkIndexEntry
	payload *chunkPayload
	loading *chunkLoad
	recent  *list.Element
	retired bool
	// Protected by cache.mu. The embedded manifest index is immutable.
	verified *chunkValidation
}

type chunkLoad struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int // Protected by cache.mu; zero means this load cannot be joined.
	// Published by closing done.
	payload *chunkPayload
	err     error
}

type chunkCache struct {
	mu       sync.Mutex
	recent   list.List
	bytes    uint64
	capacity uint64
	// Bound simultaneous cold object downloads per PChannel; waiters can cancel.
	loadSlots chan struct{}
}

func newChunkIndex(capacity uint64) *chunkIndex {
	return &chunkIndex{cache: chunkCache{capacity: capacity, loadSlots: make(chan struct{}, 4)}}
}

func (i *chunkIndex) newChunk(index *streamingpb.PChannelSummaryChunkIndexEntry, payload *chunkPayload) *indexedChunk {
	chunk := &indexedChunk{PChannelSummaryChunkIndexEntry: index}
	if payload != nil {
		i.cache.mu.Lock()
		i.cache.putLocked(chunk, payload)
		i.cache.mu.Unlock()
	}
	return chunk
}

func (i *chunkIndex) snapshot(after, through uint64) []*indexedChunk {
	start := sort.Search(len(i.chunks), func(n int) bool { return i.chunks[n].GetEndTimetick() > after })
	end := start + sort.Search(len(i.chunks)-start, func(n int) bool { return i.chunks[start+n].GetStartTimeTick() > through })
	return append([]*indexedChunk(nil), i.chunks[start:end]...)
}

func (i *chunkIndex) remove(generation uint64) {
	n := sort.Search(len(i.chunks), func(n int) bool { return i.chunks[n].GetGeneration() >= generation })
	if n == len(i.chunks) || i.chunks[n].GetGeneration() != generation {
		return
	}
	chunk := i.chunks[n]
	i.cache.mu.Lock()
	chunk.retired = true
	chunk.verified = nil
	i.cache.evictLocked(chunk)
	i.cache.mu.Unlock()
	copy(i.chunks[n:], i.chunks[n+1:])
	i.chunks[len(i.chunks)-1] = nil
	i.chunks = i.chunks[:len(i.chunks)-1]
}

func (c *chunkCache) read(ctx context.Context, store *Store, chunk *indexedChunk) (*chunkPayload, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.mu.Lock()
		if payload := chunk.payload; payload != nil {
			c.recent.MoveToFront(chunk.recent)
			c.mu.Unlock()
			return payload, nil
		}
		loading := chunk.loading
		if loading != nil && loading.waiters == 0 {
			// The last waiter canceled this load. Join its cleanup before
			// starting a new one, rather than attaching to a canceled context.
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-loading.done:
				continue
			}
		}
		if loading == nil {
			loadCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
			loading = &chunkLoad{done: make(chan struct{}), cancel: cancel}
			chunk.loading = loading
			go func(load *chunkLoad, verified *chunkValidation) {
				defer cancel()
				c.load(loadCtx, store, chunk, load, verified)
			}(loading, chunk.verified)
		}
		loading.waiters++
		c.mu.Unlock()
		defer c.releaseLoad(loading)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-loading.done:
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return loading.payload, loading.err
		}
	}
}

func (c *chunkCache) releaseLoad(loading *chunkLoad) {
	c.mu.Lock()
	loading.waiters--
	last := loading.waiters == 0
	if last {
		loading.cancel()
	}
	c.mu.Unlock()
	if last {
		// The caller still owns its ReadTransform/idempotency snapshot pin.
		// Do not let GC reclaim its objects until the storage read has exited.
		<-loading.done
	}
}

func (c *chunkCache) load(ctx context.Context, store *Store, chunk *indexedChunk, loading *chunkLoad, verified *chunkValidation) {
	loading.payload, loading.err = c.readPayload(ctx, store, chunk, verified)
	c.mu.Lock()
	if loading.err == nil && loading.waiters != 0 && !chunk.retired {
		chunk.verified = loading.payload.verified
		c.putLocked(chunk, loading.payload)
	}
	chunk.loading = nil
	close(loading.done)
	c.mu.Unlock()
}

// readPayload owns one retry sequence for all waiters. Joining or leaving a
// load never resets its backoff; the last waiter cancels both I/O and backoff.
func (c *chunkCache) readPayload(ctx context.Context, store *Store, chunk *indexedChunk, verified *chunkValidation) (*chunkPayload, error) {
	var retryBackoff *backoff.ExponentialBackOff
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case c.loadSlots <- struct{}{}:
		}
		if err := ctx.Err(); err != nil {
			<-c.loadSlots
			return nil, err
		}
		payload, err := store.readChunkPayload(ctx, chunk.PChannelSummaryChunkIndexEntry, verified)
		<-c.loadSlots
		if err == nil {
			return payload, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// Preserve retries for generic I/O and operation-local failures, but
		// never hide a missing, malformed, or inaccessible retained object.
		if errors.IsAny(err, ErrStoreCorrupted, merr.ErrDataIntegrity, io.ErrUnexpectedEOF) || merr.IsNonRetryableErr(err) {
			return nil, err
		}
		if retryBackoff == nil {
			retryBackoff = backoff.NewExponentialBackOff()
			retryBackoff.InitialInterval = 100 * time.Millisecond
			retryBackoff.MaxInterval = 10 * time.Second
			retryBackoff.MaxElapsedTime = 0
			retryBackoff.Reset()
		}
		delay := retryBackoff.NextBackOff()
		mlog.RatedWarn(ctx, 1, "retrying shared summary chunk read",
			mlog.FieldPChannel(store.PChannel()), mlog.Uint64("generation", chunk.GetGeneration()),
			mlog.Duration("backoff", delay), mlog.Err(err))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (c *chunkCache) putLocked(chunk *indexedChunk, payload *chunkPayload) {
	size := uint64(cap(payload.bytes))
	// Oversized chunks remain readable but do not evict the entire working set.
	if size > c.capacity || c.capacity == 0 {
		return
	}
	for c.bytes > c.capacity-size {
		c.evictLocked(c.recent.Back().Value.(*indexedChunk))
	}
	chunk.payload = payload
	chunk.recent = c.recent.PushFront(chunk)
	c.bytes += size
}

func (c *chunkCache) evictLocked(chunk *indexedChunk) {
	if chunk.payload == nil {
		return
	}
	c.bytes -= uint64(cap(chunk.payload.bytes))
	c.recent.Remove(chunk.recent)
	chunk.payload, chunk.recent = nil, nil
}
