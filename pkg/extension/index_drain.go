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
	AllowVectorIndexDropWhileLoaded(ctx context.Context, collectionID int64) bool

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
}
