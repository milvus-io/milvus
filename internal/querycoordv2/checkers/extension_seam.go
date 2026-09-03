package checkers

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/extension"
)

// This file is the checkers' seam for the index-drain capability. It declares
// WHERE the index checker consults the installed extension; what happens there
// lives outside this tree. With none installed the function below answers
// false without asking anything, and the checker behaves natively.

// collectionInDropIndexDrain reports whether the installed extension is
// mid-drain for this collection: a vector-index drop it allowed has committed
// and the collection's resource groups are still releasing. The index checker
// skips such a collection - see the call site for why a segment update issued
// mid-drain would tear the still-serving index out from under the drain's own
// in-flight queries.
func collectionInDropIndexDrain(ctx context.Context, collectionID int64) bool {
	drainer := extension.Caps().IndexDrain
	if drainer == nil {
		return false
	}
	return drainer.CollectionDraining(ctx, collectionID)
}
