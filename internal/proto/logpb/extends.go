package logpb

import (
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	ServiceMethodPrefix = "/milvus.proto.log"
	InitialTerm         = int64(-1)
)

func NewDeliverAll() *DeliverPolicy {
	return &DeliverPolicy{
		Policy: &DeliverPolicy_All{
			All: &emptypb.Empty{},
		},
	}
}

func NewDeliverLatest() *DeliverPolicy {
	return &DeliverPolicy{
		Policy: &DeliverPolicy_Latest{
			Latest: &emptypb.Empty{},
		},
	}
}

func NewDeliverStartFrom(messageID *MessageID) *DeliverPolicy {
	return &DeliverPolicy{
		Policy: &DeliverPolicy_StartFrom{
			StartFrom: messageID,
		},
	}
}

func NewDeliverStartAfter(messageID *MessageID) *DeliverPolicy {
	return &DeliverPolicy{
		Policy: &DeliverPolicy_StartAfter{
			StartAfter: messageID,
		},
	}
}
