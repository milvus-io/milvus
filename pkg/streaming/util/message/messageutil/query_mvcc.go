package messageutil

import "github.com/milvus-io/milvus/pkg/v3/streaming/util/message"

// AdvancesQueryTransformMVCC classifies a committed VChannel message for both
// query planning and TransformLog notifications. The caller excludes uncommitted
// transaction bodies; an assembled transaction is represented by its Commit.
// This is distinct from having a Delete payload: insert-only commits and
// publication/schema barriers also advance the query's transform frontier.
func AdvancesQueryTransformMVCC(msg message.BasicMessage) bool {
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection,
		message.MessageTypeDelete,
		message.MessageTypeCommitTxn,
		message.MessageTypeCommitImport,
		message.MessageTypeFlush,
		message.MessageTypeManualFlush,
		message.MessageTypeDropPartition,
		message.MessageTypeDropCollection,
		message.MessageTypeTruncateCollection,
		message.MessageTypeFlushAll,
		message.MessageTypeAlterWAL:
		return true
	case message.MessageTypeAlterCollection:
		if immutable, ok := msg.(message.ImmutableMessage); ok {
			return IsSchemaChange(message.MustAsImmutableAlterCollectionMessageV2(immutable).Header())
		}
		return IsSchemaChange(message.MustAsMutableAlterCollectionMessageV2(msg).Header())
	default:
		return false
	}
}

// IsPChannelTransformBarrier identifies query-frontier changes applying to every
// VChannel, rather than ordinary TimeTick confirmations.
func IsPChannelTransformBarrier(typ message.MessageType) bool {
	return typ == message.MessageTypeFlushAll || typ == message.MessageTypeAlterWAL
}
