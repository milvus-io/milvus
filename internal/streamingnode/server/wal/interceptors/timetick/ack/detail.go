package ack

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/txn"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// newAckDetail creates a new default acker detail.
func newAckDetail(ts uint64, lastConfirmedMessageID message.MessageID) *AckDetail {
	if ts <= 0 {
		panic(fmt.Sprintf("ts should never less than 0 %d", ts))
	}
	return &AckDetail{
		BeginTimestamp:         ts,
		LastConfirmedMessageID: lastConfirmedMessageID,
		IsSync:                 false,
		Err:                    nil,
	}
}

// AckDetail records the information of acker.
type AckDetail struct {
	BeginTimestamp uint64 // the timestamp when acker is allocated.
	EndTimestamp   uint64 // the timestamp when acker is acknowledged.
	// for avoiding allocation of timestamp failure, the timestamp will use the ack manager last allocated timestamp.
	LastConfirmedMessageID message.MessageID
	Message                message.ImmutableMessage
	TxnSession             *txn.TxnSession
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

// OptImmutableMessage marks the acker is done.
func OptImmutableMessage(msg message.ImmutableMessage) AckOption {
	return func(detail *AckDetail) {
		detail.Message = msg
	}
}

// OptTxnSession marks the session for acker.
func OptTxnSession(session *txn.TxnSession) AckOption {
	return func(detail *AckDetail) {
		detail.TxnSession = session
	}
}
