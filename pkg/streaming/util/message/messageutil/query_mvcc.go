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
		message.MessageTypeCreateSnapshot,
		message.MessageTypeRestoreSnapshot,
		message.MessageTypeRefreshExternalCollection,
		message.MessageTypeDropPartition,
		message.MessageTypeDropCollection,
		message.MessageTypeTruncateCollection,
		message.MessageTypeFlushAll,
		message.MessageTypeAlterWAL,
		// A SplitShard replica fences its source (no later write lands there) or
		// is a target's genesis; either way the vchannel's transform frontier
		// moves to it.
		message.MessageTypeSplitShard:
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

// IsVChannelGenesisMessage reports whether a message is the genesis of the
// vchannel it is addressed to: the first record of a brand-new vchannel, which
// registers the vchannel as live wherever liveness is tracked per vchannel
// (the recovery manager's modules, the durable MVCC entries, the query MVCC
// frontiers).
//
// CreateCollection is one. The TARGET replica of a SplitShard broadcast is the
// other: a shard split's targets are brand-new vchannels and that replica is
// their genesis. The SOURCE replica of the same broadcast is not -- it fences a
// vchannel that already exists -- and neither is the control channel replica.
func IsVChannelGenesisMessage(msg message.BasicMessage) bool {
	switch msg.MessageType() {
	case message.MessageTypeCreateCollection:
		return true
	case message.MessageTypeSplitShard:
		switch msg := msg.(type) {
		case message.ImmutableMessage:
			header := message.MustAsImmutableSplitShardMessageV2(msg).Header()
			return message.SplitShardRoleOf(header, msg.VChannel()) == message.SplitShardRoleTarget
		case message.MutableMessage:
			header := message.MustAsMutableSplitShardMessageV2(msg).Header()
			return message.SplitShardRoleOf(header, msg.VChannel()) == message.SplitShardRoleTarget
		default:
			// A broadcast that has not been split into per-vchannel replicas is
			// addressed to no vchannel and is nobody's genesis yet.
			return false
		}
	default:
		return false
	}
}

// IsPChannelTransformBarrier identifies query-frontier changes applying to every
// VChannel, rather than ordinary TimeTick confirmations.
func IsPChannelTransformBarrier(typ message.MessageType) bool {
	return typ == message.MessageTypeFlushAll || typ == message.MessageTypeAlterWAL
}
