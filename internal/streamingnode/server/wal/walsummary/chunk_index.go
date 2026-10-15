package walsummary

import (
	"container/list"
	"context"
	"errors"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
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
	done     chan struct{}
	payload  *chunkPayload
	err      error
	canceled bool
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
		if loading := chunk.loading; loading != nil {
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-loading.done:
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				// A canceled loader must not cancel other subscriptions. They
				// retry ownership using their own context; other failures propagate.
				if loading.canceled {
					continue
				}
				return loading.payload, loading.err
			}
		}
		loading := &chunkLoad{done: make(chan struct{})}
		chunk.loading = loading
		verified := chunk.verified
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			loading.err = ctx.Err()
		case c.loadSlots <- struct{}{}:
			loading.payload, loading.err = store.readChunkPayload(ctx, chunk.PChannelSummaryChunkIndexEntry, verified)
			<-c.loadSlots
		}
		loading.canceled = ctx.Err() != nil && errors.Is(loading.err, ctx.Err())
		c.mu.Lock()
		if loading.err == nil && !chunk.retired {
			chunk.verified = loading.payload.verified
			c.putLocked(chunk, loading.payload)
		}
		chunk.loading = nil
		close(loading.done)
		c.mu.Unlock()
		return loading.payload, loading.err
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
