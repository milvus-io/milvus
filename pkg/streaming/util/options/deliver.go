package options

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

const (
	DeliverPolicyTypeAll        deliverPolicyType = 1
	DeliverPolicyTypeLatest     deliverPolicyType = 2
	DeliverPolicyTypeStartFrom  deliverPolicyType = 3
	DeliverPolicyTypeStartAfter deliverPolicyType = 4

	DeliverFilterTypeTimeTickGT  deliverFilterType = 1
	DeliverFilterTypeTimeTickGTE deliverFilterType = 2
	DeliverFilterTypeMessageType deliverFilterType = 3
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

// DeliverFilterMessageType delivers messages filtered by message type.
func DeliverFilterMessageType(messageType ...message.MessageType) DeliverFilter {
	messageTypes := make([]messagespb.MessageType, 0, len(messageType))
	for _, mt := range messageType {
		if mt.IsSystem() {
			panic(fmt.Sprintf("system message type cannot be filter, %s", mt.String()))
		}
		messageTypes = append(messageTypes, messagespb.MessageType(mt))
	}
	return &streamingpb.DeliverFilter{
		Filter: &streamingpb.DeliverFilter_MessageType{
			MessageType: &streamingpb.DeliverFilterMessageType{
				MessageTypes: messageTypes,
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
func GetFilterFunc(filters []DeliverFilter) func(message.ImmutableMessage) bool {
	filterFuncs := make([]func(message.ImmutableMessage) bool, 0, len(filters))
	for _, filter := range filters {
		filter := filter
		switch filter.GetFilter().(type) {
		case *streamingpb.DeliverFilter_TimeTickGt:
			filterFuncs = append(filterFuncs, func(im message.ImmutableMessage) bool {
				// txn message's timetick is determined by the commit message.
				// so we only need to filter the commit message.
				if im.TxnContext() == nil || im.MessageType() == message.MessageTypeCommitTxn {
					return im.TimeTick() > filter.GetTimeTickGt().TimeTick
				}
				return true
			})
		case *streamingpb.DeliverFilter_TimeTickGte:
			filterFuncs = append(filterFuncs, func(im message.ImmutableMessage) bool {
				// txn message's timetick is determined by the commit message.
				// so we only need to filter the commit message.
				if im.TxnContext() == nil || im.MessageType() == message.MessageTypeCommitTxn {
					return im.TimeTick() >= filter.GetTimeTickGte().TimeTick
				}
				return true
			})
		case *streamingpb.DeliverFilter_MessageType:
			filterFuncs = append(filterFuncs, func(im message.ImmutableMessage) bool {
				// system message cannot be filterred.
				if im.MessageType().IsSystem() {
					return true
				}
				for _, mt := range filter.GetMessageType().MessageTypes {
					if im.MessageType() == message.MessageType(mt) {
						return true
					}
				}
				return false
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
	}
}
