package viewquery

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// TODO(#40451): remove the legacy QueryNode request envelope when the shared
// QueryView execution layer consumes the query plan and selected handles directly.
func buildSNQueryRequest(req *internalpb.RetrieveRequest, mvcc *viewpb.QueryPlanMVCC, vchannel string, handles []snview.GrowingSegmentHandle) *querypb.QueryRequest {
	queryReq := proto.Clone(req).(*internalpb.RetrieveRequest)
	queryReq.MvccTimestamp = mvcc.GetGrowingTimetick()

	return &querypb.QueryRequest{
		Req:         queryReq,
		SegmentIDs:  segmentIDsFromHandles(handles),
		DmlChannels: dmlChannelsFromVChannel(vchannel),
		Scope:       querypb.DataScope_Streaming,
	}
}
