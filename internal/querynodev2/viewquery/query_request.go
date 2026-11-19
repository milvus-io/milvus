package viewquery

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func buildQNQueryRequest(req *internalpb.RetrieveRequest, mvcc *viewpb.QueryPlanMVCC, handles []qnview.SealedSegmentHandle) *querypb.QueryRequest {
	queryReq := proto.Clone(req).(*internalpb.RetrieveRequest)
	// Sealed segments read at the transform frontier, or at the common read
	// point when the view has lineage.
	queryReq.MvccTimestamp = qviews.SealedReadTimestamp(mvcc)

	return &querypb.QueryRequest{
		Req:         queryReq,
		SegmentIDs:  segmentIDsFromHandles(handles),
		DmlChannels: dmlChannelsFromHandles(handles),
		Scope:       querypb.DataScope_Historical,
	}
}
