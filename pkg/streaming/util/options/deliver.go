package options

import (
	"github.com/milvus-io/milvus/pkg/streaming/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/streaming/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

const (
	DeliverPolicyTypeAll        deliverPolicyType = 1
	DeliverPolicyTypeLatest     deliverPolicyType = 2
	DeliverPolicyTypeStartFrom  deliverPolicyType = 3
	DeliverPolicyTypeStartAfter deliverPolicyType = 4

	DeliverFilterTypeTimeTickGT  deliverFilterType = 1
	DeliverFilterTypeTimeTickGTE deliverFilterType = 2
	DeliverFilterTypeVChannel    deliverFilterType = 3
)

type (
	deliverPolicyType int
	deliverFilterType int
)

type DeliverPolicy = *streamingpb.DeliverPolicy

// DeliverPolicyAll delivers all messages.
func DeliverPolicyAll() DeliverPolicy {
	return &streamingpb.DeliverPolicy{
		Policy: &streamingpb.DeliverPolicy_All{},
	}
}

// DeliverLatest delivers the latest message.
func DeliverPolicyLatest() DeliverPolicy {
	return &streamingpb.DeliverPolicy{
		Policy: &streamingpb.DeliverPolicy_Latest{},
	}
}

// DeliverEarliest delivers the earliest message.
func DeliverPolicyStartFrom(messageID message.MessageID) DeliverPolicy {
	return &streamingpb.DeliverPolicy{
		Policy: &streamingpb.DeliverPolicy_StartFrom{
			StartFrom: &messagespb.MessageID{
				Id: messageID.Marshal(),
			},
		},
	}
}

// DeliverPolicyStartAfter delivers the message after the specified message.
func DeliverPolicyStartAfter(messageID message.MessageID) DeliverPolicy {
	return &streamingpb.DeliverPolicy{
		Policy: &streamingpb.DeliverPolicy_StartAfter{
			StartAfter: &messagespb.MessageID{
				Id: messageID.Marshal(),
			},
		},
	}
}

type DeliverFilter = *streamingpb.DeliverFilter

//
// DeliverFilters
//

// DeliverFilterTimeTickGT delivers messages by time tick greater than the specified time tick.
func DeliverFilterTimeTickGT(timeTick uint64) DeliverFilter {
	return &streamingpb.DeliverFilter{
		Filter: &streamingpb.DeliverFilter_TimeTickGt{
			TimeTickGt: &streamingpb.DeliverFilterTimeTickGT{
				TimeTick: timeTick,
			},
		},
	}
}

// DeliverFilterTimeTickGTE delivers messages by time tick greater than or equal to the specified time tick.
func DeliverFilterTimeTickGTE(timeTick uint64) DeliverFilter {
	return &streamingpb.DeliverFilter{
		Filter: &streamingpb.DeliverFilter_TimeTickGte{
			TimeTickGte: &streamingpb.DeliverFilterTimeTickGTE{
				TimeTick: timeTick,
			},
		},
	}
}

// DeliverFilterVChannel delivers messages filtered by vchannel.
func DeliverFilterVChannel(vchannel string) DeliverFilter {
	return &streamingpb.DeliverFilter{
		Filter: &streamingpb.DeliverFilter_Vchannel{
			Vchannel: &streamingpb.DeliverFilterVChannel{
				Vchannel: vchannel,
			},
		},
	}
}

// IsDeliverFilterTimeTick checks if the filter is time tick filter.
func IsDeliverFilterTimeTick(filter DeliverFilter) bool {
	switch filter.GetFilter().(type) {
	case *streamingpb.DeliverFilter_TimeTickGt, *streamingpb.DeliverFilter_TimeTickGte:
		return true
	default:
		return false
	}
}

// GetFilterFunc returns the filter function.
func GetFilterFunc(filters []DeliverFilter) (func(message.ImmutableMessage) bool, error) {
	filterFuncs := make([]func(message.ImmutableMessage) bool, 0, len(filters))
	for _, filter := range filters {
		filter := filter
		switch filter.GetFilter().(type) {
		case *streamingpb.DeliverFilter_TimeTickGt:
			filterFuncs = append(filterFuncs, func(im message.ImmutableMessage) bool {
				return im.TimeTick() > filter.GetTimeTickGt().TimeTick
			})
		case *streamingpb.DeliverFilter_TimeTickGte:
			filterFuncs = append(filterFuncs, func(im message.ImmutableMessage) bool {
				return im.TimeTick() >= filter.GetTimeTickGte().TimeTick
			})
		case *streamingpb.DeliverFilter_Vchannel:
			filterFuncs = append(filterFuncs, func(im message.ImmutableMessage) bool {
				return im.VChannel() == filter.GetVchannel().Vchannel
			})
		default:
			panic("unimplemented")
		}
	}
	return func(msg message.ImmutableMessage) bool {
		for _, f := range filterFuncs {
			if !f(msg) {
				return false
			}
		}
		return true
	}, nil
}
