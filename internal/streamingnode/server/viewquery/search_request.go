package viewquery

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// TODO(#40451): remove the legacy QueryNode request envelope when the shared
// QueryView execution layer consumes the query plan and selected handles directly.
func buildSNSearchRequest(req *internalpb.SearchRequest, mvcc *viewpb.QueryPlanMVCC, vchannel string, handles []snview.GrowingSegmentHandle) *querypb.SearchRequest {
	queryReq := proto.Clone(req).(*internalpb.SearchRequest)
	// Growing segments read at the growing frontier, or at the common read
	// point when the plan reads a split source in a lineage group.
	queryReq.MvccTimestamp = qviews.GrowingReadTimestamp(mvcc)

	return &querypb.SearchRequest{
		Req:         queryReq,
		SegmentIDs:  segmentIDsFromHandles(handles),
		DmlChannels: dmlChannelsFromVChannel(vchannel),
		Scope:       querypb.DataScope_Streaming,
	}
}
