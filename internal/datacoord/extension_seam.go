package datacoord

import (
	"context"

	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/common"
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

// The functions below are datacoord's seam for the scale-to-zero reading of an
// empty query-node session set.
//
// Natively, no query-node sessions means no readers: version negotiation has
// nobody to negotiate with, so the engine-version answers fall to zero and the
// store-path gate to the legacy layout - the conservative reading when nodes
// are expected to exist and their absence is a degradation. A deployment form
// that starts query nodes on demand inverts that: an empty session set is the
// RESTING state, and most CreateIndex calls arrive in it. Version zero there
// is not conservative, it is wrong - knowhere reads engine version 0 as "disk
// load only for DISKANN" and misroutes other disk indexes onto the in-memory
// path, corrupting their offsets.
//
// The form's declaration is the CoordinatorEngine capability itself: an
// on-demand query-node control plane is exactly the statement that empty
// means scale-to-zero. The values are NOT the form's to supply - they come
// from this process's own engine (segcore's knowhere for vectors, the
// compiled-in constant for scalars), because the form's query-node pool runs
// the same image as its coordinator, so the versions this binary carries are
// the versions the absent nodes would have reported. A pool that could run
// older images than the coordinator must not install the capability.

// scaleToZeroQueryNodes reports whether the installed extension reads an empty
// query-node session set as scale-to-zero.
func scaleToZeroQueryNodes() bool {
	return extension.Caps().CoordinatorEngine != nil
}

// scaleToZeroVectorEngineVersion answers the vector engine version for an
// empty session set, from this process's own knowhere.
func scaleToZeroVectorEngineVersion() (int32, bool) {
	if !scaleToZeroQueryNodes() {
		return 0, false
	}
	return segcore.GetIndexEngineInfo().CurrentIndexVersion, true
}

// scaleToZeroScalarEngineVersion answers the scalar engine version for an
// empty session set, from the compiled-in constant.
func scaleToZeroScalarEngineVersion() (int32, bool) {
	if !scaleToZeroQueryNodes() {
		return 0, false
	}
	return common.CurrentScalarIndexEngineVersion, true
}
