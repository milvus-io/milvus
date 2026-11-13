package qviews

import (
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// PrewarmPlan pins both the View and its StreamingNode owner between phases.
// It is request-local and must never be refreshed after planning.
type PrewarmPlan struct {
	Plan          *viewpb.QueryPlan
	StreamingNode types.PChannelInfoAssigned
}
