package typeconverter

import (
	"github.com/cockroachdb/errors"

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

// NewProtosFromDeliverFilters converts DeliverFilter to protobuf DeliverFilter
func NewProtosFromDeliverFilters(filter []options.DeliverFilter) ([]*streamingpb.DeliverFilter, error) {
	protos := make([]*streamingpb.DeliverFilter, 0, len(filter))
	for _, f := range filter {
		proto, err := NewProtoFromDeliverFilter(f)
		if err != nil {
			return nil, err
		}
		protos = append(protos, proto)
	}
	return protos, nil
}

// NewProtoFromDeliverFilter converts DeliverFilter to protobuf DeliverFilter
func NewProtoFromDeliverFilter(filter options.DeliverFilter) (*streamingpb.DeliverFilter, error) {
	switch filter.Type() {
	case options.DeliverFilterTypeTimeTickGT:
		return &streamingpb.DeliverFilter{
			Filter: &streamingpb.DeliverFilter_TimeTickGt{
				TimeTickGt: &streamingpb.DeliverFilterTimeTickGT{
					TimeTick: filter.(interface{ TimeTick() uint64 }).TimeTick(),
				},
			},
		}, nil
	case options.DeliverFilterTypeTimeTickGTE:
		return &streamingpb.DeliverFilter{
			Filter: &streamingpb.DeliverFilter_TimeTickGte{
				TimeTickGte: &streamingpb.DeliverFilterTimeTickGTE{
					TimeTick: filter.(interface{ TimeTick() uint64 }).TimeTick(),
				},
			},
		}, nil
	case options.DeliverFilterTypeVChannel:
		return &streamingpb.DeliverFilter{
			Filter: &streamingpb.DeliverFilter_Vchannel{
				Vchannel: &streamingpb.DeliverFilterVChannel{
					Vchannel: filter.(interface{ VChannel() string }).VChannel(),
				},
			},
		}, nil
	default:
		return nil, errors.New("unknown deliver filter")
	}
}

// NewDeliverFiltersFromProtos converts protobuf DeliverFilter to DeliverFilter
func NewDeliverFiltersFromProtos(protos []*streamingpb.DeliverFilter) ([]options.DeliverFilter, error) {
	filters := make([]options.DeliverFilter, 0, len(protos))
	for _, p := range protos {
		f, err := NewDeliverFilterFromProto(p)
		if err != nil {
			return nil, err
		}
		filters = append(filters, f)
	}
	return filters, nil
}

// NewDeliverFilterFromProto converts protobuf DeliverFilter to DeliverFilter
func NewDeliverFilterFromProto(proto *streamingpb.DeliverFilter) (options.DeliverFilter, error) {
	switch proto.Filter.(type) {
	case *streamingpb.DeliverFilter_TimeTickGt:
		return options.DeliverFilterTimeTickGT(proto.GetTimeTickGt().GetTimeTick()), nil
	case *streamingpb.DeliverFilter_TimeTickGte:
		return options.DeliverFilterTimeTickGTE(proto.GetTimeTickGte().GetTimeTick()), nil
	case *streamingpb.DeliverFilter_Vchannel:
		return options.DeliverFilterVChannel(proto.GetVchannel().GetVchannel()), nil
	default:
		return nil, errors.New("unknown deliver filter")
	}
}
