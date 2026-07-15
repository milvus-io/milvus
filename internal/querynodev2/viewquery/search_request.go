package viewquery

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func buildQNSearchRequest(req *internalpb.SearchRequest, mvcc *viewpb.QueryPlanMVCC, handles []qnview.SealedSegmentHandle) *querypb.SearchRequest {
	queryReq := proto.Clone(req).(*internalpb.SearchRequest)
	queryReq.MvccTimestamp = mvcc.GetTransformingTimetick()

	return &querypb.SearchRequest{
		Req:         queryReq,
		SegmentIDs:  segmentIDsFromHandles(handles),
		DmlChannels: dmlChannelsFromHandles(handles),
		Scope:       querypb.DataScope_Historical,
	}
}
