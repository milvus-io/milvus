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

package walsummary

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ErrTransformTruncated identifies a cursor whose required Delete history was released.
var ErrTransformTruncated = errors.New("summary transform history truncated")

// ReadLimits bounds a batch. Zero disables a limit; one whole Entry may exceed it.
type ReadLimits struct{ MaxRows, MaxBytes uint64 }

// TransformBatch proves complete coverage of (after, CoveredThrough]. Entries
// belong to the caller and include whole transactions only. Changed is captured
// atomically with coverage: a reader waiting at the tail cannot lose a wakeup.
type TransformBatch struct {
	Entries         []*streamingpb.TransformLogEntry
	CoveredThrough  uint64
	ReadableThrough uint64
	Changed         <-chan struct{}
}

// TransformReader is the storage contract shared by L0 and future subscriptions.
type TransformReader interface {
	ReadTransform(context.Context, string, uint64, uint64, ReadLimits) (TransformBatch, error)
	TransformStats(string, uint64, uint64) TransformStats
}

func (m *Manager) advanceReadableLocked(tt uint64) {
	if tt <= m.readableThrough {
		return
	}
	m.readableThrough = tt
	m.notifyReadersLocked()
}

func (m *Manager) notifyReadersLocked() {
	// Allocate a new token only when a reader captures it. With no readers,
	// ordered WAL observation must not allocate one channel per message.
	if m.readableChanged != nil {
		close(m.readableChanged)
		m.readableChanged = nil
	}
}

// ReadTransform reads a complete, bounded prefix of (after, through]. The
// snapshot includes durable chunks, sealed records and the pending tail exactly
// once, even if a writer moves records between these states during the read.
// Local physical GC is pinned only for this call. No payload cache grows with
// backlog: decoding uses at most one chunk section in addition to the batch.
func (m *Manager) ReadTransform(ctx context.Context, vchannel string, after, through uint64, limits ReadLimits) (TransformBatch, error) {
	m.readMu.RLock()
	defer m.readMu.RUnlock()
	m.mu.Lock()
	if m.readableChanged == nil {
		m.readableChanged = make(chan struct{})
	}
	batch := TransformBatch{CoveredThrough: after, ReadableThrough: m.readableThrough, Changed: m.readableChanged}
	terminal := m.terminalErr
	truncated := m.manifest.GetTransformTruncatedThrough()[vchannel]
	chunks := append([]*streamingpb.PChannelSummaryChunkIndexEntry(nil), m.manifest.GetChunks()...)
	// Only copy slice descriptors, never the unmaterialized payload window.
	sealed := make([][]*stagedRecord, 0, len(m.pendingSealed))
	for _, chunk := range m.pendingSealed {
		sealed = append(sealed, chunk.RecordsByVChannel[vchannel])
	}
	pending := m.pending
	m.mu.Unlock()
	if terminal != nil {
		return batch, terminal
	}
	if after < truncated {
		return batch, &markedStoreError{err: merr.WrapErrServiceInternalMsg("summary history for %s before %d has been truncated (cursor %d)", vchannel, truncated, after), target: ErrTransformTruncated}
	}
	target := min(through, batch.ReadableThrough)
	if target <= after {
		return batch, ctx.Err()
	}
	var rows, bytes uint64
	appendEntry := func(entry *streamingpb.TransformLogEntry) bool {
		if entry == nil || entry.GetTimeTick() <= after || entry.GetTimeTick() > target {
			return true
		}
		var n uint64
		for _, block := range entry.GetDelete().GetBlocks() {
			n += uint64(len(block.GetPrimaryKeys().GetIntId().GetData()) + len(block.GetPrimaryKeys().GetStrId().GetData()))
		}
		size := uint64(proto.Size(entry))
		if len(batch.Entries) > 0 && ((limits.MaxRows > 0 && rows+n > limits.MaxRows) || (limits.MaxBytes > 0 && bytes+size > limits.MaxBytes)) {
			return false
		}
		batch.Entries = append(batch.Entries, proto.Clone(entry).(*streamingpb.TransformLogEntry))
		batch.CoveredThrough = entry.GetTimeTick()
		rows += n
		bytes += size
		return true
	}
	for _, chunk := range chunks {
		if err := ctx.Err(); err != nil {
			return TransformBatch{}, err
		}
		if chunk.GetEndTimetick() <= after {
			continue
		}
		if chunk.GetStartTimetick() > target {
			break
		}
		index := vchannelChunkIndex(chunk, vchannel)
		if index == nil || index.GetTransform() == nil {
			continue
		}
		records, err := m.cfg.Store.ReadTransformSection(ctx, chunk.GetGeneration(), chunk.GetTerm(), vchannel, index)
		if err != nil {
			return TransformBatch{}, err
		}
		for _, record := range records {
			if !appendEntry(&streamingpb.TransformLogEntry{TimeTick: record.GetTimeTick(), Entry: &streamingpb.TransformLogEntry_Delete{Delete: record.GetDelete()}}) {
				return batch, nil
			}
		}
	}
	for _, records := range sealed {
		for _, record := range records {
			if err := ctx.Err(); err != nil {
				return TransformBatch{}, err
			}
			if !appendEntry(record.entry) {
				return batch, nil
			}
		}
	}
	for _, record := range pending {
		if err := ctx.Err(); err != nil {
			return TransformBatch{}, err
		}
		if record.vchannel == vchannel && !appendEntry(record.entry) {
			return batch, nil
		}
	}
	batch.CoveredThrough = target
	return batch, nil
}
