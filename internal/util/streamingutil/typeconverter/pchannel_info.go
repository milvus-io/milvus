package typeconverter

import (
	"errors"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

// NewPChannelInfoFromProto converts protobuf PChannelInfo to PChannelInfo
func NewPChannelInfoFromProto(pchannel *streamingpb.PChannelInfo) (types.PChannelInfo, error) {
	if pchannel.GetName() == "" {
		return types.PChannelInfo{}, errors.New("pchannel name is empty")
	}
	if pchannel.GetTerm() <= 0 {
		return types.PChannelInfo{}, errors.New("pchannel term is empty or negetive")
	}
	if pchannel.GetServerId() <= 0 {
		return types.PChannelInfo{}, errors.New("pchannel server id is empty or negetive")
	}
	return types.PChannelInfo{
		Name:     pchannel.GetName(),
		Term:     pchannel.GetTerm(),
		ServerID: pchannel.GetServerId(),
	}, nil
}

// NewProtoFromPChannelInfo converts PChannelInfo to protobuf PChannelInfo
func NewProtoFromPChannelInfo(pchannel types.PChannelInfo) (*streamingpb.PChannelInfo, error) {
	if pchannel.Name == "" {
		return nil, errors.New("pchannel name is empty")
	}
	if pchannel.Term <= 0 {
		return nil, errors.New("pchannel term is empty or negetive")
	}
	if pchannel.ServerID <= 0 {
		return nil, errors.New("pchannel server id is empty or negetive")
	}
	return &streamingpb.PChannelInfo{
		Name:     pchannel.Name,
		Term:     pchannel.Term,
		ServerId: pchannel.ServerID,
	}, nil
}
