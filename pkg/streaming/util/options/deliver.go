package options

import (
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

const (
	deliverOrderTimetick DeliverOrder = 1

	DeliverPolicyTypeAll        DeliverPolicyType = 1
	DeliverPolicyTypeLatest     DeliverPolicyType = 2
	DeliverPolicyTypeStartFrom  DeliverPolicyType = 3
	DeliverPolicyTypeStartAfter DeliverPolicyType = 4
)

// DeliverOrder is the order of delivering messages.
type (
	DeliverOrder      int
	DeliverPolicyType int
)

// DeliverPolicy is the policy of delivering messages.
type DeliverPolicy interface {
	Policy() DeliverPolicyType

	MessageID() message.MessageID
}

type deliverPolicyWithoutMessageID struct {
	policy DeliverPolicyType
}

func (d *deliverPolicyWithoutMessageID) Policy() DeliverPolicyType {
	return d.policy
}

func (d *deliverPolicyWithoutMessageID) MessageID() message.MessageID {
	panic("not implemented")
}

type deliverPolicyWithMessageID struct {
	policy    DeliverPolicyType
	messageID message.MessageID
}

func (d *deliverPolicyWithMessageID) Policy() DeliverPolicyType {
	return d.policy
}

func (d *deliverPolicyWithMessageID) MessageID() message.MessageID {
	return d.messageID
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

// DeliverOrderTimeTick delivers messages by time tick.
func DeliverOrderTimeTick() DeliverOrder {
	return deliverOrderTimetick
}
