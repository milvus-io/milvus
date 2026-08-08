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

package syncmgr

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
)

// Task is one unit of sync work owned by one segment. Every task has the same
// two-phase lifecycle — there is no second shape and no optional method, so a
// task that fails to implement the contract fails to compile rather than
// silently running in a degraded mode.
//
// The two phases exist because they have different ordering requirements:
//
//	Prepare  writes payload to storage. Slow, and independent of every other
//	         task, so the dispatcher runs it out of order and in parallel.
//	Commit   publishes the metadata that makes the written payload visible.
//	         Runs strictly in submission order per segment, because it advances
//	         segment counters and checkpoints that the next task reads.
//
// Retry ownership: each phase may spend a small bounded inner budget on
// transient blips (ioRetryOptions, gated by ClassifySyncError), then returns
// the error unchanged. The dispatcher never retries — it is only an executor.
// Whole-task retry belongs to the pipeline that owns the task: the write
// buffer keeps a failed task queued and re-drives it, so both phases must
// tolerate being run again after a failure.
type Task interface {
	SegmentID() int64
	Checkpoint() *msgpb.MsgPosition
	StartPosition() *msgpb.MsgPosition
	ChannelName() string
	IsFlush() bool
	IsDrop() bool

	// SetChunkManager and SetDrop are the void counterparts of the WithXxx
	// builders. They exist on the interface so callers that hold a Task do not
	// have to type-switch to reach two setters every implementation has.
	SetChunkManager(storage.ChunkManager)
	SetDrop()

	Prepare(context.Context) error
	Commit(context.Context) error

	// HandleError observes a failed attempt: metrics, logging, and the
	// caller-installed failure callback. It does NOT release what the task
	// owns — a retried task reuses its payload and prepared storage handle —
	// and the owner calls Abandon for terminal release instead.
	HandleError(error)
}

var (
	_ Task = (*SyncTask)(nil)
	_ Task = (*GrowingSourceSyncTask)(nil)
)
