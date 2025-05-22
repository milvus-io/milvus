package message

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
)

type (
	TxnState = messagespb.TxnState
	TxnID    int64
)

const (
	TxnStateInFlight   TxnState = messagespb.TxnState_TxnInFlight
	TxnStateOnCommit   TxnState = messagespb.TxnState_TxnOnCommit
	TxnStateCommitted  TxnState = messagespb.TxnState_TxnCommitted
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
		TxnID:     TxnID(proto.TxnId),
		Keepalive: time.Duration(proto.KeepaliveMilliseconds) * time.Millisecond,
	}
}

// TxnContext is the transaction context for message.
type TxnContext struct {
	TxnID     TxnID
	Keepalive time.Duration
}

// IntoProto converts TxnContext to proto message.
func (t *TxnContext) IntoProto() *messagespb.TxnContext {
	if t == nil {
		return nil
	}
	return &messagespb.TxnContext{
		TxnId:                 int64(t.TxnID),
		KeepaliveMilliseconds: t.Keepalive.Milliseconds(),
	}
}
