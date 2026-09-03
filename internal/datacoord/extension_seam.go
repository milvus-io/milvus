package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/extension"
)

// This file is datacoord's seam for the index-drain capability. It declares
// WHERE datacoord consults the installed extension; what happens there lives
// outside this tree. With none installed the function below answers false
// without asking anything, which is datacoord's own answer.

// vectorIndexDropWhileLoadedAllowed reports whether the installed extension
// takes over datacoord's refusal to drop a vector index on a loaded
// collection. Only an extension that drains queries off the collection first
// can answer true; nothing installed means the refusal stands.
func vectorIndexDropWhileLoadedAllowed(ctx context.Context, collectionID int64, indexName string) bool {
	drainer := extension.Caps().IndexDrain
	if drainer == nil {
		return false
	}
	return drainer.AllowVectorIndexDropWhileLoaded(ctx, collectionID, indexName)
}
