package walview

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// CopyMessage detaches query work from recovery acknowledgement ownership.
// A transaction remains one atomic event with its commit timestamp.
func CopyMessage(msg message.ImmutableMessage) message.ImmutableMessage {
	if txn := message.AsImmutableTxnMessage(msg); txn != nil {
		builder := message.NewImmutableTxnMessageBuilder(message.MustAsImmutableBeginTxnMessageV2(CopyMessage(txn.Begin())))
		_ = txn.RangeOver(func(inner message.ImmutableMessage) error { builder.Add(CopyMessage(inner)); return nil })
		cloned, err := builder.Build(message.MustAsImmutableCommitTxnMessageV2(CopyMessage(txn.Commit())))
		if err != nil {
			panic(err)
		}
		return cloned
	}
	raw := proto.Clone(msg.IntoMessageProto()).(*messagespb.Message)
	return message.NewImmutableMesasge(msg.MessageID(), raw.Payload, raw.Properties)
}
