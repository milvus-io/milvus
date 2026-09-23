package viewquery

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/snview"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func buildSNSearchRequest(req *internalpb.SearchRequest, mvcc *viewpb.QueryPlanMVCC, vchannel string, handles []snview.GrowingSegmentHandle) *querypb.SearchRequest {
	queryReq := proto.Clone(req).(*internalpb.SearchRequest)
	queryReq.MvccTimestamp = mvcc.GetGrowingTimetick()

	return &querypb.SearchRequest{
		Req:         queryReq,
		SegmentIDs:  segmentIDsFromHandles(handles),
		DmlChannels: dmlChannelsFromVChannel(vchannel),
		Scope:       querypb.DataScope_Streaming,
	}
}
