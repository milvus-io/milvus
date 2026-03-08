package replicates

import (
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
)

// newSecondaryState creates a new secondary state.
func newSecondaryState(sourceClusterID string, sourcePChannel string) *secondaryState {
	return &secondaryState{
		checkpoint: &utility.ReplicateCheckpoint{
			ClusterID: sourceClusterID,
			PChannel:  sourcePChannel,
			MessageID: nil,
			TimeTick:  0,
		},
		replicateTxnHelper: newReplicateTxnHelper(),
	}
}

// recoverSecondaryState recovers the secondary state from the recover param.
func recoverSecondaryState(param *ReplicateManagerRecoverParam) (*secondaryState, error) {
	txnHelper := newReplicateTxnHelper()
	sourceClusterID := param.InitialRecoverSnapshot.Checkpoint.ReplicateCheckpoint.ClusterID
	// recover the txn helper.
	uncommittedTxnBuilders := param.InitialRecoverSnapshot.TxnBuffer.GetUncommittedMessageBuilder()
	for _, builder := range uncommittedTxnBuilders {
		begin, body := builder.Messages()
		replicateHeader := begin.ReplicateHeader()
		// filter out the txn builders that are replicated from other cluster or not replicated.
		if replicateHeader == nil || replicateHeader.ClusterID != sourceClusterID {
			continue
		}
		// there will be only one uncommitted txn builder.
		if err := txnHelper.StartBegin(begin.TxnContext(), begin.ReplicateHeader()); err != nil {
			return nil, err
		}
		txnHelper.BeginDone(begin.TxnContext())
		for _, msg := range body {
			if err := txnHelper.AddNewMessage(msg.TxnContext(), msg.ReplicateHeader()); err != nil {
				return nil, err
			}
			txnHelper.AddNewMessageDone(msg.ReplicateHeader())
		}
	}
	return &secondaryState{
		checkpoint:         param.InitialRecoverSnapshot.Checkpoint.ReplicateCheckpoint,
		replicateTxnHelper: txnHelper,
	}, nil
}

// secondaryState describes the state of the secondary role.
type secondaryState struct {
	checkpoint          *utility.ReplicateCheckpoint
	*replicateTxnHelper // if not nil, the txn replicating operation is in progress.
}

// SourceClusterID returns the source cluster id of the secondary state.
func (s *secondaryState) SourceClusterID() string {
	return s.checkpoint.ClusterID
}

// GetCheckpoint returns the checkpoint of the secondary state.
func (s *secondaryState) GetCheckpoint() *utility.ReplicateCheckpoint {
	return s.checkpoint
}

// PushForwardCheckpoint pushes forward the checkpoint.
func (s *secondaryState) PushForwardCheckpoint(timetick uint64, lastConfirmedMessageID message.MessageID) error {
	if timetick <= s.checkpoint.TimeTick {
		return nil
	}
	s.checkpoint.TimeTick = timetick
	s.checkpoint.MessageID = lastConfirmedMessageID
	return nil
}
