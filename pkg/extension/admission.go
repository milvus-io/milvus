package extension

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

// CoordClient is the slice of the coordinator API an admission check may call.
// It is deliberately two methods wide: counting what exists needs to list, not
// to mutate.
type CoordClient interface {
	ListDatabases(ctx context.Context, req *milvuspb.ListDatabasesRequest) (*milvuspb.ListDatabasesResponse, error)
	ShowCollections(ctx context.Context, req *milvuspb.ShowCollectionsRequest) (*milvuspb.ShowCollectionsResponse, error)
}

// AdmissionChecker enforces limits milvus itself has no concept of, such as a
// per-instance cap on how many databases or collections an instance may hold.
//
// milvus decides WHEN to ask; the policy is entirely the implementation's. An
// error rejects the request, and milvus surfaces it to the caller unchanged.
//
// An implementation is expected to fail open on its own infrastructure errors:
// refusing a user's DDL because a counting call hiccupped is worse than briefly
// admitting one request too many. It should record that it did so, so the
// bypass is visible rather than silent.
type AdmissionChecker interface {
	// CheckCreateCollection runs before a collection is created.
	CheckCreateCollection(ctx context.Context, coord CoordClient) error
	// CheckCreateDatabase runs before a database is created.
	CheckCreateDatabase(ctx context.Context, coord CoordClient) error
}
