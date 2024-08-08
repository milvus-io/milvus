package message

import (
	"time"

	"github.com/milvus-io/milvus/pkg/streaming/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type (
	TxnState = messagespb.TxnState
	TxnID    int64
)

const (
	TxnStateBegin      TxnState = messagespb.TxnState_TxnBegin
	TxnStateInFlight   TxnState = messagespb.TxnState_TxnInFlight
	TxnStateOnCommit   TxnState = messagespb.TxnState_TxnOnCommit
	TxnStateCommited   TxnState = messagespb.TxnState_TxnCommitted
	TxnStateOnRollback TxnState = messagespb.TxnState_TxnOnRollback
	TxnStateRollbacked TxnState = messagespb.TxnState_TxnRollbacked

	NonTxnID = TxnID(-1)
)

// NewTxnContextFromProto generates TxnContext from proto message.
func NewTxnContextFromProto(proto *messagespb.TxnContext) *TxnContext {
	if proto == nil {
		return nil
	}
	return &TxnContext{
		TxnID:    TxnID(proto.TxnId),
		BeginTSO: proto.BeginTso,
		TTL:      time.Duration(proto.TtlMilliseconds) * time.Millisecond,
	}
}

// TxnContext is the transaction context for message.
type TxnContext struct {
	TxnID    TxnID
	BeginTSO uint64
	TTL      time.Duration
}

// ExpiredTimeTick returns the expired time tick of the transaction.
func (t *TxnContext) ExpiredTimeTick() uint64 {
	return tsoutil.AddPhysicalDurationOnTs(t.BeginTSO, t.TTL)
}

// IntoProto converts TxnContext to proto message.
func (t *TxnContext) IntoProto() *messagespb.TxnContext {
	if t == nil {
		return nil
	}
	return &messagespb.TxnContext{
		TxnId:           int64(t.TxnID),
		BeginTso:        t.BeginTSO,
		TtlMilliseconds: t.TTL.Milliseconds(),
	}
}
