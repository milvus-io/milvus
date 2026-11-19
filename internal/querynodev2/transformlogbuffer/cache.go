// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transformlogbuffer

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// SetCacheLimits sets the node and per-stream recent-cache bounds. Call before
// publishing the Buffer. Logical guards/pins do not change when RAM is evicted.
func (b *Buffer) SetCacheLimits(nodeBytes, streamBytes int64) {
	b.cacheLimit = max(0, nodeBytes)
	b.streamCacheLimit = max(0, streamBytes)
}
func (b *Buffer) CachedBytes() int64 { return b.cacheBytes.Load() }
func (b *vchannelBuffer) releaseCacheEntryLocked(entry *streamingpb.TransformLogEntry) {
	bytes := wal.TransformEntryMemory(entry)
	// Unit fixtures may pre-populate entries without reserving payload memory.
	bytes = min(bytes, b.cacheBytes)
	if bytes > 0 {
		b.cacheBytes -= bytes
		b.owner.cacheBytes.Add(-bytes)
		wal.TransformMemory.Release(bytes)
	}
}

func (b *vchannelBuffer) clearCacheLocked() {
	for _, entry := range b.entries {
		b.releaseCacheEntryLocked(entry)
	}
	clear(b.entries)
	b.entries = nil
}

func (b *vchannelBuffer) evictFirstLocked() {
	entry := b.entries[0]
	b.cacheStart = max(b.cacheStart, entry.GetTimeTick())
	b.releaseCacheEntryLocked(entry)
	copy(b.entries, b.entries[1:])
	b.entries[len(b.entries)-1] = nil
	b.entries = b.entries[:len(b.entries)-1]
}

func (b *vchannelBuffer) cacheEntryLocked(entry *streamingpb.TransformLogEntry) {
	bytes := wal.TransformEntryMemory(entry)
	for len(b.entries) > 0 && (b.cacheBytes+bytes > b.owner.streamCacheLimit || b.owner.cacheBytes.Load()+bytes > b.owner.cacheLimit) {
		b.evictFirstLocked()
	}
	reserve := false
	if bytes <= b.owner.streamCacheLimit {
		for {
			used := b.owner.cacheBytes.Load()
			if bytes > b.owner.cacheLimit-used {
				break
			}
			if b.owner.cacheBytes.CompareAndSwap(used, used+bytes) {
				reserve = true
				break
			}
		}
	}
	if reserve && !wal.TransformMemory.TryReserveBuffered(bytes) {
		b.owner.cacheBytes.Add(-bytes)
		reserve = false
	}
	if !reserve {
		// The authoritative Summary reader owns both its immutable durable chunks
		// and its WAL-backed staged tail. Replay waits/fails if that owner cannot
		// supply the interval. This never advances a segment's applied position.
		b.clearCacheLocked()
		b.cacheStart = max(b.cacheStart, entry.GetTimeTick())
		return
	}
	b.cacheBytes += bytes
	b.entries = append(b.entries, entry)
}
