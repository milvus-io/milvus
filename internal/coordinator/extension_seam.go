package coordinator

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// This file is the coordinator-side seam for the resource-group and index-drain
// capabilities. It declares WHERE the coordinator consults the installed
// extension; what happens there lives outside this tree. Every function below
// returns without touching anything when the capability it needs is absent, so
// a stock binary runs the same coordinator it always did.
//
// The seam sits on mixCoordImpl rather than on the gRPC service in front of it
// because mixCoordImpl is the single point both the remote and the in-process
// callers of the coordinator pass through.

// resourceGroupInterceptor returns the installed interceptor, or nil when none
// is installed and the native path applies.
func resourceGroupInterceptor() extension.ResourceGroupInterceptor {
	return extension.Caps().ResourceGroups
}

// indexDrainer returns the installed drainer, or nil when none is installed
// and the native path applies.
func indexDrainer() extension.IndexDrainer {
	return extension.Caps().IndexDrain
}

// beforeCreateResourceGroup returns the request the coordinator must create,
// which is the one it was given unless an interceptor supplied a replacement.
func beforeCreateResourceGroup(ctx context.Context, req *milvuspb.CreateResourceGroupRequest) *milvuspb.CreateResourceGroupRequest {
	interceptor := resourceGroupInterceptor()
	if interceptor == nil {
		return req
	}
	if replacement := interceptor.BeforeCreateResourceGroup(ctx, req); replacement != nil {
		return replacement
	}
	return req
}

// beforeDropResourceGroup lets an interceptor read what the resource group
// holds while it still exists. It cannot stop the drop.
func beforeDropResourceGroup(ctx context.Context, req *milvuspb.DropResourceGroupRequest) {
	interceptor := resourceGroupInterceptor()
	if interceptor == nil {
		return
	}
	interceptor.BeforeDropResourceGroup(ctx, req)
}

// afterDropResourceGroupFailed reports a drop that did not commit to the
// interceptor, whose Before hook has already torn its own state down: without
// this report the group milvus still holds and the group the interceptor
// emptied diverge in silence.
func afterDropResourceGroupFailed(ctx context.Context, req *milvuspb.DropResourceGroupRequest, status *commonpb.Status, err error) {
	if err == nil && merr.Ok(status) {
		return
	}
	interceptor := resourceGroupInterceptor()
	if interceptor == nil {
		return
	}
	interceptor.AfterDropResourceGroupFailed(ctx, req)
}

// afterUpdateResourceGroups reports the committed update to the interceptor.
// It is called with the status the coordinator produced and drops the report
// when the update did not commit: an interceptor acting on an update that was
// rejected would be reading state that never changed.
func afterUpdateResourceGroups(ctx context.Context, update extension.ResourceGroupUpdate, status *commonpb.Status, err error) {
	if err != nil || !merr.Ok(status) {
		return
	}
	interceptor := resourceGroupInterceptor()
	if interceptor == nil {
		return
	}
	interceptor.AfterUpdateResourceGroups(ctx, update)
}

// beforeDropIndex classifies the drop while its index metadata is still
// readable, and reports whether afterDropIndex must run once it commits.
func beforeDropIndex(ctx context.Context, req *indexpb.DropIndexRequest) bool {
	drainer := indexDrainer()
	if drainer == nil {
		return false
	}
	return drainer.BeginDropIndex(ctx, req)
}

// afterCreateIndex reports a committed CreateIndex to the drainer. Only a
// create that really committed is reported: a drainer waking parked queries
// for an index that was never created would wake them into the very refusal
// they were parked to avoid.
func afterCreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest, status *commonpb.Status, err error) {
	if err != nil || !merr.Ok(status) {
		return
	}
	drainer := indexDrainer()
	if drainer == nil {
		return
	}
	drainer.AfterCreateIndex(ctx, req)
}

// afterDropIndex reports a committed drop to the drainer, but only for a drop
// beforeDropIndex asked about and only when the coordinator really performed
// it: a drainer that started draining for an index still in place would take a
// healthy collection out of service.
func afterDropIndex(ctx context.Context, req *indexpb.DropIndexRequest, drainOnCommit bool, status *commonpb.Status, err error) {
	if !drainOnCommit {
		return
	}
	drainer := indexDrainer()
	if drainer == nil {
		return
	}
	// A drop that did not commit is reported too, on the abort branch: the
	// drainer may have opened state in BeginDropIndex - a drain window that
	// refuses queries, say - and with no report of the failure that state
	// would outlive the drop it was opened for.
	if err != nil || !merr.Ok(status) {
		drainer.AbortDropIndex(ctx, req)
		return
	}
	drainer.AfterDropIndex(ctx, req)
}
