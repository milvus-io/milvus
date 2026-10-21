package inspector

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// QueryBarrierInspector is the capability needed by a fixed-snapshot query.
// EnsureBarrier returns only after the current WAL owner has published a
// complete prefix covering the requested timestamp for the vchannel. It must
// honor cancellation and cannot infer coverage from a resume cursor.
// The scoped-barrier implementation is supplied by the WAL integration.
type QueryBarrierInspector interface {
	EnsureBarrier(context.Context, types.PChannelInfo, string, uint64) error
}
