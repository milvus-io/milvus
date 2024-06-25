package typeconverter

import (
	"errors"

	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
)

// NewDeliverPolicyFromProto converts protobuf DeliverPolicy to DeliverPolicy
func NewDeliverPolicyFromProto(name string, policy *streamingpb.DeliverPolicy) (options.DeliverPolicy, error) {
	switch policy := policy.GetPolicy().(type) {
	case *streamingpb.DeliverPolicy_All:
		return options.DeliverPolicyAll(), nil
	case *streamingpb.DeliverPolicy_Latest:
		return options.DeliverPolicyLatest(), nil
	case *streamingpb.DeliverPolicy_StartFrom:
		msgID, err := message.UnmarshalMessageID(name, policy.StartFrom.GetId())
		if err != nil {
			return nil, err
		}
		return options.DeliverPolicyStartFrom(msgID), nil
	case *streamingpb.DeliverPolicy_StartAfter:
		msgID, err := message.UnmarshalMessageID(name, policy.StartAfter.GetId())
		if err != nil {
			return nil, err
		}
		return options.DeliverPolicyStartAfter(msgID), nil
	default:
		return nil, errors.New("unknown deliver policy")
	}
}

// NewProtoFromDeliverPolicy converts DeliverPolicy to protobuf DeliverPolicy
func NewProtoFromDeliverPolicy(policy options.DeliverPolicy) (*streamingpb.DeliverPolicy, error) {
	switch policy.Policy() {
	case options.DeliverPolicyTypeAll:
		return &streamingpb.DeliverPolicy{
			Policy: &streamingpb.DeliverPolicy_All{},
		}, nil
	case options.DeliverPolicyTypeLatest:
		return &streamingpb.DeliverPolicy{
			Policy: &streamingpb.DeliverPolicy_Latest{},
		}, nil
	case options.DeliverPolicyTypeStartFrom:
		return &streamingpb.DeliverPolicy{
			Policy: &streamingpb.DeliverPolicy_StartFrom{
				StartFrom: &streamingpb.MessageID{
					Id: policy.MessageID().Marshal(),
				},
			},
		}, nil
	case options.DeliverPolicyTypeStartAfter:
		return &streamingpb.DeliverPolicy{
			Policy: &streamingpb.DeliverPolicy_StartAfter{
				StartAfter: &streamingpb.MessageID{
					Id: policy.MessageID().Marshal(),
				},
			},
		}, nil
	default:
		return nil, errors.New("unknown deliver policy")
	}
}
