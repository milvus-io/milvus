package viewquery

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func validateSearchRequest(req *viewpb.SearchOnViewRequest) error {
	if req.GetLegacyReq() == nil {
		return invalidArgument("missing legacy search request")
	}
	return validateCommonRequestShape(req.GetShardId(), req.GetVersion(), req.GetMvcc())
}

func validateQueryRequest(req *viewpb.QueryOnViewRequest) error {
	if req.GetLegacyReq() == nil {
		return invalidArgument("missing legacy query request")
	}
	return validateCommonRequestShape(req.GetShardId(), req.GetVersion(), req.GetMvcc())
}

func validateCommonRequestShape(shardID *viewpb.ShardID, version *viewpb.QueryViewVersion, mvcc *viewpb.QueryPlanMVCC) error {
	if shardID == nil {
		return invalidArgument("missing shard id")
	}
	if shardID.GetVchannel() == "" {
		return invalidArgument("empty vchannel")
	}
	if version == nil || version.GetDataVersion() == nil {
		return invalidArgument("missing query view version")
	}
	if mvcc == nil {
		return invalidArgument("missing query plan mvcc")
	}
	return nil
}

func invalidArgument(reason string) error {
	return status.Error(codes.InvalidArgument, reason)
}
