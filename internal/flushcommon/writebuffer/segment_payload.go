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

package writebuffer

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// flushInput is the content of one flush attempt, fixed by Snapshot. It carries
// exactly what the sync-task builder needs from the insert side of a segment;
// the delta buffer is owned Go memory on every path and is yielded by the
// coordinator itself, next to the snapshot.
type flushInput struct {
	// snapshotID names this snapshot for settlement: CommitFlush drops its
	// floor, AbortFlush keeps it. Zero is never a valid snapshot ID.
	snapshotID int64
	// insert is the snapshotted insert payload; ownership moves to the task.
	insert []*storage.InsertData
	// bm25 is the BM25 statistics accumulated for the snapshotted rows.
	bm25 map[int64]*storage.BM25Stats
	// schema is the collection schema the rows were buffered under.
	schema *schemapb.CollectionSchema
	// timeRange covers the snapshotted insert rows only (min == MaxUint64 and
	// max == 0 when empty); the caller merges the delta buffer's range itself.
	timeRange *TimeRange
	// startPos is the WAL replay origin of the snapshotted rows; nil when the
	// snapshot carries no rows. When non-nil, Snapshot has recorded it as a
	// floor that EarliestPosition keeps reporting until CommitFlush.
	startPos *msgpb.MsgPosition
	// rows / bytes are the snapshot's accounting at the moment it was taken.
	rows  int64
	bytes int64
	// growing is the growing-source side of the attempt, union-style: non-nil
	// exactly when the snapshot came from a refPayload. The coordinator itself
	// never inspects it beyond the nil check that selects the task builder.
	growing *growingFlushInput
}

// segmentPayload is a segment's unflushed insert data. The coordinator does not
// know where the bytes live; it knows only debt, positions, and flush attempts.
//
// Implementations are guarded by the write buffer's own mutex, exactly like the
// insert buffer they generalize: mutating calls (Buffer, Snapshot, CommitFlush,
// AbortFlush) run under wb.mut write lock, read-only accounting calls may run
// under the read lock.
type segmentPayload interface {
	// Buffer absorbs one insert pack's rows (owned: copy rows in; ref: record
	// batch positions/rows only — bytes stay pinned in segcore). Returns the
	// bytes this call added to resident buffered memory, for metric accounting.
	Buffer(data *InsertData, start, end *msgpb.MsgPosition) (int64, error)
	// Snapshot fixes the content of the next flush attempt up to throughTs.
	// owned: moves rows out (ownership to the task); ref: resolves the fence
	// (lastFlushedTs, throughTs] and pins the source. MAY FAIL for ref
	// (source Unavailable/Pending) — failure is a retryable debt, first-class,
	// never a terminal condition.
	Snapshot(ctx context.Context, throughTs uint64) (*flushInput, error)
	// CommitFlush is the ONLY thing that settles a snapshot. owned: drop the
	// moved rows' floor; ref: advance lastFlushedTs, trim acked batches, and
	// CommitGrowingFlush on the source (point of no return — the caller
	// guarantees metadata is durable first).
	CommitFlush(snapshotID int64)
	// AbortFlush returns what Snapshot reserved WITHOUT settling: the floor
	// stays (the checkpoint keeps pinning), the debt stays. An owned terminal
	// failure keeps the floor pinned for WAL replay — the data is gone from
	// memory and replay is the only remaining copy. ref failure just unpins
	// the source.
	AbortFlush(snapshotID int64)
	// EarliestPosition is the replay origin of everything not yet
	// CommitFlush'd — INCLUDING in-flight snapshots. It is always
	// earlier-or-equal to any data this payload still accounts for; nil means
	// nothing is outstanding.
	EarliestPosition() *msgpb.MsgPosition

	// Accounting for policies/backpressure/eviction. All of it covers the
	// BUFFERED (not yet snapshotted) content only: in-flight snapshots are
	// accounted by the sync entries that own them.

	// UnflushedRows is the number of buffered rows not yet snapshotted.
	UnflushedRows() int64
	// UnflushedBytes is the resident buffered size (owned: real bytes; ref: 0
	// evictable — ledger bytes are reported separately if a policy needs them).
	UnflushedBytes() int64
	// MinTimestamp is the earliest buffered (not in-flight) position's
	// timestamp, MaxUint64 when nothing is buffered. Policies use it; it must
	// NOT include in-flight floors, or an empty segment with a flush in flight
	// would look eternally stale.
	MinTimestamp() typeutil.Timestamp
	// IsFull reports whether the buffered content reached its flush budget.
	IsFull() bool
}
