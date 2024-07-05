package options

import "github.com/milvus-io/milvus/pkg/streaming/util/message"

// deliverPolicyWithoutMessageID is the policy of delivering messages without messageID.
type deliverPolicyWithoutMessageID struct {
	policy deliverPolicyType
}

func (d *deliverPolicyWithoutMessageID) Policy() deliverPolicyType {
	return d.policy
}

func (d *deliverPolicyWithoutMessageID) MessageID() message.MessageID {
	panic("not implemented")
}

// deliverPolicyWithMessageID is the policy of delivering messages with messageID.
type deliverPolicyWithMessageID struct {
	policy    deliverPolicyType
	messageID message.MessageID
}

func (d *deliverPolicyWithMessageID) Policy() deliverPolicyType {
	return d.policy
}

func (d *deliverPolicyWithMessageID) MessageID() message.MessageID {
	return d.messageID
}

// deliverFilterTimeTickGT delivers messages by time tick greater than the specified time tick.
type deliverFilterTimeTickGT struct {
	timeTick uint64
}

func (f *deliverFilterTimeTickGT) Type() deliverFilterType {
	return DeliverFilterTypeTimeTickGT
}

func (f *deliverFilterTimeTickGT) TimeTick() uint64 {
	return f.timeTick
}

func (f *deliverFilterTimeTickGT) Filter(msg message.ImmutableMessage) bool {
	return msg.TimeTick() > f.timeTick
}

// deliverFilterTimeTickGTE delivers messages by time tick greater than or equal to the specified time tick.
type deliverFilterTimeTickGTE struct {
	timeTick uint64
}

func (f *deliverFilterTimeTickGTE) Type() deliverFilterType {
	return DeliverFilterTypeTimeTickGTE
}

func (f *deliverFilterTimeTickGTE) TimeTick() uint64 {
	return f.timeTick
}

func (f *deliverFilterTimeTickGTE) Filter(msg message.ImmutableMessage) bool {
	return msg.TimeTick() >= f.timeTick
}

// deliverFilterVChannel delivers messages by vchannel.
type deliverFilterVChannel struct {
	vchannel string
}

func (f *deliverFilterVChannel) Type() deliverFilterType {
	return DeliverFilterTypeVChannel
}

func (f *deliverFilterVChannel) VChannel() string {
	return f.vchannel
}

func (f *deliverFilterVChannel) Filter(msg message.ImmutableMessage) bool {
	return msg.VChannel() == f.vchannel
}
