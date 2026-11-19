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

package wal

import (
	"context"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	// Reserved extra-log RAM for caches, queues and ordinary replay workspaces.
	// One indivisible logical entry may exceed the ordinary decode workspace;
	// it is synchronously consumed and never retained in a bounded cache/queue.
	DefaultTransformMemoryBytes   int64 = 128 << 20
	TransformReplayWorkspaceBytes int64 = 16 << 20
	TransformReplayPageEntries          = 256
)

// TransformMemoryBudget accounts retained payloads separately from durable
// history pins. Engine delete indexes are outside this additional-log budget.
type TransformMemoryBudget struct {
	limit int64
	used  atomic.Int64
}

func NewTransformMemoryBudget(limit int64) *TransformMemoryBudget {
	return &TransformMemoryBudget{limit: limit}
}

func (b *TransformMemoryBudget) TryReserve(bytes int64) bool {
	for {
		used := b.used.Load()
		if bytes < 0 || bytes > b.limit-used {
			return false
		}
		if b.used.CompareAndSwap(used, used+bytes) {
			return true
		}
	}
}

// TryReserveBuffered leaves one replay workspace free even when cache limits
// are tuned upward. Otherwise an idle, full cache could prevent the very replay
// needed to consume/evict it and make the configured bound a liveness trap.
func (b *TransformMemoryBudget) TryReserveBuffered(bytes int64) bool {
	for {
		used := b.used.Load()
		if bytes < 0 || bytes > max(int64(0), b.limit-TransformReplayWorkspaceBytes)-used {
			return false
		}
		if b.used.CompareAndSwap(used, used+bytes) {
			return true
		}
	}
}
func (b *TransformMemoryBudget) Release(bytes int64) { b.used.Add(-bytes) }
func (b *TransformMemoryBudget) Used() int64         { return b.used.Load() }

var (
	TransformMemory      = NewTransformMemoryBudget(DefaultTransformMemoryBytes)
	transformReplaySlots = make(chan struct{}, 4)
)

// TransformEntryMemory estimates the resident protobuf graph, including slice
// capacity and string storage, rather than its compressed/varint wire length.
func TransformEntryMemory(entry *streamingpb.TransformLogEntry) int64 {
	if entry == nil {
		return 0
	}
	size := int64(unsafe.Sizeof(*entry)) + 128 + int64(len(entry.ProtoReflect().GetUnknown()))
	if deleted := entry.GetDelete(); deleted != nil {
		size += int64(unsafe.Sizeof(*deleted)) + int64(cap(deleted.Blocks))*8 + int64(len(deleted.ProtoReflect().GetUnknown()))
		for _, block := range deleted.Blocks {
			if block == nil {
				continue
			}
			size += int64(unsafe.Sizeof(*block)) + int64(len(block.ProtoReflect().GetUnknown()))
			if ids := block.GetPrimaryKeys(); ids != nil {
				size += int64(unsafe.Sizeof(*ids)) + 64 + int64(len(ids.ProtoReflect().GetUnknown()))
				if ints := ids.GetIntId(); ints != nil {
					size += int64(unsafe.Sizeof(*ints)) + int64(cap(ints.Data))*8 + int64(len(ints.ProtoReflect().GetUnknown()))
				}
				if strings := ids.GetStrId(); strings != nil {
					size += int64(unsafe.Sizeof(*strings)) + int64(cap(strings.Data))*16 + int64(len(strings.ProtoReflect().GetUnknown()))
					for _, str := range strings.Data {
						size += int64(len(str))
					}
				}
			}
		}
	}
	return size
}

// ReplayTransformPage replays at most one work page from authoritative Summary
// history. The server captures the prefix and streams bounded Summary pages;
// this handler keeps no payload queue. Its strict option forbids GC fast-forward.
// The returned cursor advances only after a complete successful application.
func ReplayTransformPage(ctx context.Context, streams TransformLogStreamManager, vchannel string, after, through uint64, apply func(context.Context, *streamingpb.TransformLogEntry) error) (uint64, error) {
	if after >= through {
		return after, ctx.Err()
	}
	if streams == nil {
		return after, merr.WrapErrServiceInternalMsg("transform replay stream manager is unavailable")
	}
	select {
	case transformReplaySlots <- struct{}{}:
	case <-ctx.Done():
		return after, ctx.Err()
	}
	defer func() { <-transformReplaySlots }()
	if !TransformMemory.TryReserve(TransformReplayWorkspaceBytes) {
		return after, merr.Wrapf(merr.ErrServiceResourceInsufficient, "transform replay workspace budget is occupied")
	}
	defer TransformMemory.Release(TransformReplayWorkspaceBytes)
	attempt, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := streams.AcquireStream(attempt, funcutil.ToPhysicalChannel(vchannel))
	if err != nil {
		return after, err
	}
	defer func() { cancel(); _ = stream.Close() }()
	h := &transformPageHandler{ctx: attempt, apply: apply, through: through, cursor: after, done: make(chan struct{})}
	sub, err := stream.Subscribe(attempt, TransformLogSubscriptionOption{VChannel: vchannel, StartAfterTimeTick: after, EndTimeTick: through, RequireExactStart: true, Handler: h})
	if err != nil {
		return after, err
	}
	defer func() { cancel(); _ = sub.Close() }()
	select {
	case <-h.done:
	case <-stream.Done():
	case <-ctx.Done():
	}
	// Abort callbacks before Close joins their delivery, then inspect the result.
	cancel()
	_ = sub.Close()
	h.mu.Lock()
	cursor, applyErr := h.cursor, h.err
	complete := h.complete
	h.mu.Unlock()
	if applyErr != nil {
		return cursor, applyErr
	}
	if ctx.Err() != nil {
		return cursor, ctx.Err()
	}
	if complete {
		return cursor, nil
	}
	if err = stream.Error(); err != nil {
		return cursor, err
	}
	return cursor, merr.WrapErrServiceUnavailableMsg("transform replay ended before its requested prefix")
}

type transformPageHandler struct {
	ctx      context.Context
	apply    func(context.Context, *streamingpb.TransformLogEntry) error
	through  uint64
	mu       sync.Mutex
	cursor   uint64
	count    int
	complete bool
	err      error
	done     chan struct{}
	once     sync.Once
}

func (h *transformPageHandler) finish() { h.once.Do(func() { close(h.done) }) }
func (h *transformPageHandler) Handle(event TransformLogStreamEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.complete || h.err != nil || h.ctx.Err() != nil {
		return
	}
	if entry := event.Entry; entry != nil && entry.GetTimeTick() > h.cursor {
		// A logical Delete/Txn is indivisible at its tick. An already decoded
		// entry may exceed the ordinary page workspace; apply synchronously
		// without retaining it in a queue/cache, never reject valid old writes.
		if err := h.apply(h.ctx, entry); err != nil {
			h.err = err
			h.finish()
			return
		}
		h.cursor = entry.GetTimeTick()
		h.count++
	}
	if syncUp := event.SyncUp; syncUp != nil {
		h.cursor = max(h.cursor, min(syncUp.TimeTick, h.through))
	}
	if h.cursor >= h.through || h.count >= TransformReplayPageEntries {
		h.complete = true
		h.finish()
	}
}
func (h *transformPageHandler) Close() { h.finish() }
