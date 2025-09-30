package message

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// newBroadcastHeaderFromProto creates a BroadcastHeader from proto.
func newBroadcastHeaderFromProto(proto *messagespb.BroadcastHeader) *BroadcastHeader {
	rks := make(typeutil.Set[ResourceKey], len(proto.GetResourceKeys()))
	for _, key := range proto.GetResourceKeys() {
		rks.Insert(NewResourceKeyFromProto(key))
	}
	return &BroadcastHeader{
		BroadcastID:  proto.GetBroadcastId(),
		VChannels:    proto.GetVchannels(),
		ResourceKeys: rks,
	}
}

type BroadcastHeader struct {
	BroadcastID  uint64
	VChannels    []string
	ResourceKeys typeutil.Set[ResourceKey]
}

// BroadcastResult is the result of broadcast operation.
type BroadcastResult[H proto.Message, B proto.Message] struct {
	Message SpecializedBroadcastMessage[H, B]
	Results map[string]*AppendResult
}

// GetControlChannelResult returns the append result of the control channel.
// Return nil if the control channel is not found.
func (br *BroadcastResult[H, B]) GetControlChannelResult() *AppendResult {
	for vchannel, result := range br.Results {
		if funcutil.IsControlChannel(vchannel) {
			return result
		}
	}
	return nil
}

// GetVChannelsWithoutControlChannel returns the vchannels without control channel.
func (br *BroadcastResult[H, B]) GetVChannelsWithoutControlChannel() []string {
	vchannels := make([]string, 0, len(br.Results))
	for vchannel := range br.Results {
		if !funcutil.IsControlChannel(vchannel) {
			vchannels = append(vchannels, vchannel)
		}
	}
	return vchannels
}

// AppendResult is the result of append operation.
type AppendResult struct {
	MessageID              MessageID
	LastConfirmedMessageID MessageID
	TimeTick               uint64
}
