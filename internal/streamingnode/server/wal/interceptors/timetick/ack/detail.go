package ack

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/streaming/util/message"
)

// newAckDetail creates a new default acker detail.
func newAckDetail(ts uint64, lastConfirmedMessageID message.MessageID) *AckDetail {
	if ts <= 0 {
		panic(fmt.Sprintf("ts should never less than 0 %d", ts))
	}
	return &AckDetail{
		Timestamp:              ts,
		LastConfirmedMessageID: lastConfirmedMessageID,
		IsSync:                 false,
		Err:                    nil,
	}
}

// AckDetail records the information of acker.
type AckDetail struct {
	Timestamp              uint64
	LastConfirmedMessageID message.MessageID
	IsSync                 bool
	Err                    error
}

// AckOption is the option for acker.
type AckOption func(*AckDetail)

// OptSync marks the acker is sync message.
func OptSync() AckOption {
	return func(detail *AckDetail) {
		detail.IsSync = true
	}
}

// OptError marks the timestamp ack with error info.
func OptError(err error) AckOption {
	return func(detail *AckDetail) {
		detail.Err = err
	}
}
