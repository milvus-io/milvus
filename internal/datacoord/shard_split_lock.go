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

package datacoord

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// The write-switch lock.
//
// A split's write switch is three steps that must see one collection: fence the
// sources, create the target vchannels, commit the new routing. The
// CreateVChannel messages embed the collection's schema and partition set, so a
// DDL landing between the fence and the commit would leave the targets born
// from a collection that no longer exists in that shape — a partition created in
// that window would exist on every shard except the new ones.
//
// The exclusion is the same one every collection DDL takes: the Broadcaster's
// ExclusiveCollectionName resource key. Holding it across the three steps is
// what makes them one operation as far as DDL is concerned.
//
// Two things about how it is held here differ from a DDL's use of it, and both
// are deliberate:
//
//   - it is acquired WITHOUT waiting. The split manager drives every task on one
//     goroutine, and the blocking acquire cannot be interrupted by its context,
//     so waiting here would stall unrelated splits behind someone else's DDL and
//     would keep the loop from shutting down. A contended key means "not this
//     round".
//   - it is used purely as a mutex: the fence and the target creation stay plain
//     appends under it, and only the routing commit broadcasts. A lock-holding
//     broadcast would persist a task that re-acquires the key on recovery, so a
//     coordinator that died mid-split would come back to a key held by a task it
//     was about to retry — and deadlock against itself. Nothing is persisted for
//     this lock, so process death releases it.
//
// Design: docs/design-docs/design_docs/20260610-shard_split.md §6.1, §6.6.

// splitWriteSwitchLock is the held exclusion, released by Close.
type splitWriteSwitchLock interface {
	Close()
}

// collectionLocker takes the collection-level exclusion for one write switch.
//
// A function on the manager rather than a direct call, because the split
// manager's unit tests drive the state machine without a live streamingcoord,
// and the real implementation waits on one.
type collectionLocker func(dbName, collectionName string) (splitWriteSwitchLock, error)

// collectionLockAcquireTimeout bounds the wait for the exclusion.
//
// The bound is not about lock contention -- the acquire below is a try-lock and
// never waits for a holder. It is about the broadcaster itself: it is published
// as a future that resolves when streamingcoord comes up, and asking for it
// earlier parks until then. A background tick must never park; it should fail
// this round and come back.
const collectionLockAcquireTimeout = time.Second

// broadcastCollectionLocker takes the exclusion through the Broadcaster, which
// is where every collection DDL takes the same key.
func broadcastCollectionLocker(parent context.Context) collectionLocker {
	return func(dbName, collectionName string) (splitWriteSwitchLock, error) {
		ctx, cancel := context.WithTimeout(parent, collectionLockAcquireTimeout)
		defer cancel()
		return broadcast.StartBroadcastWithResourceKeysFast(ctx,
			message.NewSharedDBNameResourceKey(dbName),
			message.NewExclusiveCollectionNameResourceKey(dbName, collectionName),
		)
	}
}

// lockCollectionForWriteSwitch takes the collection-level exclusion for a
// split's write switch, or reports why it could not.
//
// The caller must Close the returned lock once the routing commit has landed --
// not before, and not after the rewrite, which runs unlocked.
func (m *shardSplitManager) lockCollectionForWriteSwitch(collection *collectionInfo) (splitWriteSwitchLock, error) {
	if m.collectionLocker == nil {
		// Wired during server initialization. A task that ticks before that must
		// wait rather than proceed unprotected: the whole point of this lock is
		// that the write switch is not safe without it.
		return nil, merr.WrapErrServiceInternalMsg("collection locker not wired yet")
	}
	return m.collectionLocker(collection.DatabaseName, collection.Schema.GetName())
}

// isCollectionBusy reports whether the write-switch lock was simply held by
// someone else, which is an ordinary outcome to retry rather than an error to
// report.
func isCollectionBusy(err error) bool {
	return errors.Is(err, broadcaster.ErrResourceKeyBusy)
}
