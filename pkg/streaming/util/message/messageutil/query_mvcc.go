package messageutil

import "github.com/milvus-io/milvus/pkg/v3/streaming/util/message"

// AdvancesQueryTransformMVCC classifies a committed VChannel message for both
// query planning and TransformLog notifications. The caller excludes uncommitted
// transaction bodies and pure Insert transactions before representing a
// transaction by its Commit. Publication/schema barriers can advance the
// transform frontier without a Delete payload.
func AdvancesQueryTransformMVCC(msg message.BasicMessage) bool {
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection,
		message.MessageTypeDelete,
		message.MessageTypeCommitTxn,
		message.MessageTypeCommitImport,
		message.MessageTypeFlush,
		message.MessageTypeManualFlush,
		message.MessageTypeCreateSnapshot,
		message.MessageTypeRestoreSnapshot,
		message.MessageTypeRefreshExternalCollection,
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
