package registry

import "github.com/milvus-io/milvus/pkg/v3/streaming/util/message"

// BroadcastPair identifies the existing Begin/End message family. The job ID is
// allocated before Import is broadcast and is carried unchanged by both Ends.
// A zero ID denotes a legacy message with no recoverable pairing identity.
func BroadcastPair(msg message.BroadcastMutableMessage) (jobID int64, begin bool) {
	switch msg.MessageTypeWithVersion() {
	case message.MessageTypeImportV1:
		return message.MustAsBroadcastImportMessageV1(msg).MustBody().GetJobID(), true
	case message.MessageTypeCommitImportV2:
		return message.MustAsBroadcastCommitImportMessageV2(msg).Header().GetJobId(), false
	case message.MessageTypeRollbackImportV2:
		return message.MustAsBroadcastRollbackImportMessageV2(msg).Header().GetJobId(), false
	default:
		return 0, false
	}
}
