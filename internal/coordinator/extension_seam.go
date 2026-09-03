package coordinator

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/extension"
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
