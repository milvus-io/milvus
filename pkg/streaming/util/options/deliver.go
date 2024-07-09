package options

import (
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

// DeliverPolicy is the policy of delivering messages.
type DeliverPolicy interface {
	Policy() deliverPolicyType

	MessageID() message.MessageID
}

// DeliverPolicyAll delivers all messages.
func DeliverPolicyAll() DeliverPolicy {
	return &deliverPolicyWithoutMessageID{
		policy: DeliverPolicyTypeAll,
	}
}

// DeliverLatest delivers the latest message.
func DeliverPolicyLatest() DeliverPolicy {
	return &deliverPolicyWithoutMessageID{
		policy: DeliverPolicyTypeLatest,
	}
}

// DeliverEarliest delivers the earliest message.
func DeliverPolicyStartFrom(messageID message.MessageID) DeliverPolicy {
	return &deliverPolicyWithMessageID{
		policy:    DeliverPolicyTypeStartFrom,
		messageID: messageID,
	}
}

// DeliverPolicyStartAfter delivers the message after the specified message.
func DeliverPolicyStartAfter(messageID message.MessageID) DeliverPolicy {
	return &deliverPolicyWithMessageID{
		policy:    DeliverPolicyTypeStartAfter,
		messageID: messageID,
	}
}

// DeliverFilter is the filter of delivering messages.
type DeliverFilter interface {
	Type() deliverFilterType

	Filter(message.ImmutableMessage) bool
}

//
// DeliverFilters
//

// DeliverFilterTimeTickGT delivers messages by time tick greater than the specified time tick.
func DeliverFilterTimeTickGT(timeTick uint64) DeliverFilter {
	return &deliverFilterTimeTickGT{
		timeTick: timeTick,
	}
}

// DeliverFilterTimeTickGTE delivers messages by time tick greater than or equal to the specified time tick.
func DeliverFilterTimeTickGTE(timeTick uint64) DeliverFilter {
	return &deliverFilterTimeTickGTE{
		timeTick: timeTick,
	}
}

// DeliverFilterVChannel delivers messages filtered by vchannel.
func DeliverFilterVChannel(vchannel string) DeliverFilter {
	return &deliverFilterVChannel{
		vchannel: vchannel,
	}
}
