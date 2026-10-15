package handler

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// QueryViewSyncClient is the QueryView sync domain client under HandlerClient.
//
// Note: this branch only carries the interface contract. The implementation
// (pchannel-scoped routing via HandlerClient assignment) lives on the qv
// feature branch and is out of scope for the coordinator-side balancer split.
type QueryViewSyncClient interface {
	// SyncQueryView opens a pchannel-scoped QueryView sync stream to the StreamingNode
	// that owns the current pchannel assignment.
	SyncQueryView(ctx context.Context, pchannel string) (viewpb.ViewSyncService_SyncQueryViewClient, error)
}
