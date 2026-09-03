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

package extension

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// IndexDrainer lets a deployment form drop a vector index on a loaded
// collection, which milvus itself refuses to do.
//
// milvus refuses because the refusal is the only protection it has: the query
// nodes still hold the collection's segments, and the moment the index is gone
// every search against it fails. A form that can take the collection out of
// service itself has a third option between refusing and leaving the
// collection broken - allow the drop, then release the collection - and this
// capability is the three points at which milvus has to ask it.
//
// The three are one protocol, not three behaviors, which is why they are one
// interface: the first decides whether a drop is allowed at all, and the other
// two bracket the drop, because only before it can milvus be told which field
// the index was on and only after it is the drop a fact. A form that
// implements the first and not the other two has a collection that is loaded
// and unqueryable for good.
//
// With no provider installed the capability is nil: milvus keeps its refusal
// and nothing else changes.
//
// # Short-circuit contract
//
// Each method states whether it may replace milvus's native outcome. An
// undocumented method may not: it observes, and milvus does what it would have
// done anyway. This is the convention borrowed from HBASE-18770.
//
// # Concurrency
//
// Every method is called from milvus's own request goroutines, concurrently
// and without any lock of milvus's held, so an implementation does its own
// synchronization.
//
// # Mutation
//
// Every request handed to these methods is READ-ONLY; milvus performs the
// drop or the create on the same object afterwards.
//
// # The bracket is closed on every path
//
// A BeginDropIndex that returned true is followed by exactly one of
// AfterDropIndex and AbortDropIndex. The seam guarantees this with a defer
// around the drop, so a drop that panicked reports Abort rather than leaving
// the bracket open; an implementation may rely on it.
//
// NoopIndexDrainer is the Noop base under the package evolution policy.
type IndexDrainer interface {
	// AllowVectorIndexDropWhileLoaded reports whether milvus may drop a vector
	// index whose collection is loaded, instead of refusing the request.
	//
	// MAY REPLACE: returning true suppresses milvus's refusal. Everything that
	// happens to the collection afterwards is then the implementation's
	// responsibility, reached through the rest of this interface.
	//
	// Returning false leaves the native refusal in place, which is what an
	// implementation that cannot take the collection out of service must
	// answer: the refusal is a working state, a dropped index with a loaded
	// collection behind it is not.
	//
	// indexName identifies WHICH drop is asking, and matters because this is
	// consulted mid-drop, after BeginDropIndex already ran: an implementation
	// that refuses concurrent drops while one is draining needs to tell the
	// drop that opened the drain (allowed - it is the one being asked about)
	// from a second drop arriving during it (refused). It is the raw name off
	// the request. It is EMPTY in two cases the implementation must not
	// confuse: a request that named no index, and a DropAll request, which
	// names none because it means every index on the collection. The
	// DropIndexRequest BeginDropIndex saw carries DropAll and PartitionIDs,
	// so an implementation keys the drain it opened on the request, not on
	// the name.
	AllowVectorIndexDropWhileLoaded(ctx context.Context, collectionID int64, indexName string) bool

	// BeginDropIndex runs before milvus performs a drop, and reports whether
	// AfterDropIndex must run if the drop succeeds.
	//
	// The classification has to happen here rather than after the drop because
	// afterwards the index metadata is marked deleted and no longer says which
	// field it indexed - by then milvus can no longer tell a vector index from
	// any other. milvus carries the answer across the drop and does nothing
	// else with it; whatever this returns, the drop itself proceeds unchanged.
	BeginDropIndex(ctx context.Context, req *indexpb.DropIndexRequest) bool

	// AfterDropIndex runs once the drop committed, and only when
	// BeginDropIndex asked for it. A drop that failed never reaches it, so an
	// implementation can treat the call as proof the index is gone and take
	// the collection out of service on it.
	AfterDropIndex(ctx context.Context, req *indexpb.DropIndexRequest)

	// AbortDropIndex runs when a drop BeginDropIndex asked about did NOT
	// commit - the coordinator returned an error or a non-Ok status - so an
	// implementation that opened any state in BeginDropIndex can close it
	// again. Without this call a failed drop would leave that state dangling:
	// BeginDropIndex has no way to know the drop's outcome, and AfterDropIndex
	// only reports success. Exactly one of AfterDropIndex and AbortDropIndex
	// follows a BeginDropIndex that returned true.
	AbortDropIndex(ctx context.Context, req *indexpb.DropIndexRequest)

	// AfterCreateIndex runs once a CreateIndex committed. It exists for the
	// re-create that follows a drained drop: an implementation that parks
	// queries while the dropped index is absent needs to know the moment a
	// replacement index is a fact, and only the coordinator sees that moment.
	// Observe-only: milvus's create is already done, and every create is
	// reported - scalar, vector, mid-drain or not - because only the
	// implementation knows which ones matter to it.
	AfterCreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest)

	// CollectionDraining reports whether the implementation is mid-drain for
	// this collection - the window between an allowed vector-index drop and
	// the last resource group's release. The query coordinator's index
	// checker consults it to leave a draining collection's segments alone:
	// mid-drain the collection is loaded while its vector index is deleted,
	// and a segment update issued then would reopen the segment against the
	// current index set, tearing the still-serving index out from under the
	// drain's own in-flight queries. False whenever no drain is open, which
	// with no provider installed is always.
	CollectionDraining(ctx context.Context, collectionID int64) bool
}

// NoopIndexDrainer keeps milvus's refusal and observes nothing: the inert
// answer, which a form that embeds it and cannot take a collection out of
// service must keep.
type NoopIndexDrainer struct{}

var _ IndexDrainer = NoopIndexDrainer{}

func (NoopIndexDrainer) AllowVectorIndexDropWhileLoaded(context.Context, int64, string) bool {
	return false
}

func (NoopIndexDrainer) BeginDropIndex(context.Context, *indexpb.DropIndexRequest) bool { return false }

func (NoopIndexDrainer) AfterDropIndex(context.Context, *indexpb.DropIndexRequest) {}

func (NoopIndexDrainer) AbortDropIndex(context.Context, *indexpb.DropIndexRequest) {}

func (NoopIndexDrainer) AfterCreateIndex(context.Context, *indexpb.CreateIndexRequest) {}

func (NoopIndexDrainer) CollectionDraining(context.Context, int64) bool { return false }
