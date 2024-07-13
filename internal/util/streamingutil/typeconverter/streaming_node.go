package typeconverter

import (
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

func NewStreamingNodeInfoFromProto(proto *streamingpb.StreamingNodeInfo) types.StreamingNodeInfo {
	return types.StreamingNodeInfo{
		ServerID: proto.ServerId,
		Address:  proto.Address,
	}
}

func NewProtoFromStreamingNodeInfo(info types.StreamingNodeInfo) *streamingpb.StreamingNodeInfo {
	return &streamingpb.StreamingNodeInfo{
		ServerId: info.ServerID,
		Address:  info.Address,
	}
}
