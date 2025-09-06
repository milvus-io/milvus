package replicates

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
)

var ErrNotReplicateMessage = errors.New("not replicate message")

// ReplicateManagerRecoverParam is the parameter for recovering the replicate manager.
type ReplicateManagerRecoverParam struct {
	ChannelInfo            types.PChannelInfo
	CurrentClusterID       string
	InitialRecoverSnapshot *recovery.RecoverySnapshot // the initial recover snapshot of the replicate manager.
}

// RecoverReplicateManager recovers the replicate manager from the initial recover snapshot.
// It will recover the replicate manager from the initial recover snapshot.
// If the wal is on replicating mode, it will recover the replicate state.
func RecoverReplicateManager(param *ReplicateManagerRecoverParam) (ReplicateManager, error) {
	txnBuffer := param.InitialRecoverSnapshot.TxnBuffer
	replicateConfigHelper, err := replicateutil.NewConfigHelper(param.CurrentClusterID, param.InitialRecoverSnapshot.Checkpoint.ReplicateConfig)
	if err != nil {
		return nil, newReplicateViolationErrorForConfig(param.InitialRecoverSnapshot.Checkpoint.ReplicateConfig, err)
	}
	rm := &replicatesManagerImpl{
		mu:                    sync.Mutex{},
		currentClusterID:      param.CurrentClusterID,
		pchannel:              param.ChannelInfo,
		replicateConfigHelper: replicateConfigHelper,
	}
	if !rm.isPrimaryRole() {
		rm.replicatingState = &replicatingState{
			checkpoint: param.InitialRecoverSnapshot.Checkpoint.ReplicateCheckpoint,
			txnHelper:  newReplicateTxnHelper(),
		}
		// recover the txn helper.
		uncommittedTxnBuilders := txnBuffer.GetUncommittedMessageBuilder()
		for _, builder := range uncommittedTxnBuilders {
			begin, body := builder.Messages()
			replicateHeader := begin.ReplicateHeader()
			// filter out the txn builders that are replicated from other cluster or not replicated.
			if replicateHeader == nil || replicateHeader.ClusterID != param.CurrentClusterID {
				continue
			}
			// there will be only one uncommitted txn builder.
			if err := rm.txnHelper.StartBegin(begin.TxnContext(), begin.ReplicateHeader()); err != nil {
				return nil, err
			}
			rm.txnHelper.BeginDone(begin.TxnContext())
			for _, msg := range body {
				if err := rm.txnHelper.AddNewMessage(msg.TxnContext(), msg.ReplicateHeader()); err != nil {
					return nil, err
				}
				rm.txnHelper.AddNewMessageDone(msg.ReplicateHeader())
			}
		}
	}
	return rm, nil
}

// replicatingState is the state of the replicating process.
type replicatingState struct {
	checkpoint *utility.ReplicateCheckpoint // only nil if replicateGraph is nil or current cluster's role is primary.
	// if not nil, it means that current cluster is replicated from other cluster.
	txnHelper *replicateTxnHelper // if not nil, the txn replicating operation is in progress.
}

// replicatesManagerImpl is the implementation of the replicates manager.
type replicatesManagerImpl struct {
	mu                    sync.Mutex
	pchannel              types.PChannelInfo
	currentClusterID      string
	replicateConfigHelper *replicateutil.ConfigHelper
	*replicatingState     // if the current cluster is not the primary role, it will have replicatingState.
}

// SwitchReplicateMode switches the replicates manager between replicating mode and non-replicating mode.
func (impl *replicatesManagerImpl) SwitchReplicateMode(_ context.Context, msg message.MutablePutReplicateConfigMessageV2) error {
	impl.mu.Lock()
	defer impl.mu.Unlock()

	newCfg := msg.Header().ReplicateConfiguration
	newGraph, err := replicateutil.NewConfigHelper(impl.currentClusterID, newCfg)
	if err != nil {
		return newReplicateViolationErrorForConfig(newCfg, err)
	}
	newClusterConfig := newGraph.GetCurrentCluster()
	switch newClusterConfig.Role() {
	case replicateutil.RolePrimary:
		// drop the replicating state.
		impl.replicatingState = nil
	case replicateutil.RoleSecondary:
		if impl.isPrimaryRole() || impl.checkpoint.ClusterID != newClusterConfig.SourceCluster().GetClusterId() {
			// Only update the replicating state when the current cluster switch from primary to secondary,
			// or the source cluster is changed.
			impl.replicatingState = &replicatingState{
				checkpoint: &utility.ReplicateCheckpoint{
					ClusterID: newClusterConfig.SourceCluster().GetClusterId(),
					PChannel:  newClusterConfig.MustGetSourceChannel(impl.pchannel.Name),
					TimeTick:  0,
					MessageID: nil,
				},
				txnHelper: newReplicateTxnHelper(),
			}
		}
	}
	impl.replicateConfigHelper = newGraph
	return nil
}

func (impl *replicatesManagerImpl) BeginReplicateMessage(ctx context.Context, msg message.MutableMessage) (g ReplicateAcker, err error) {
	impl.mu.Lock()
	defer func() {
		if err != nil {
			impl.mu.Unlock()
		}
	}()

	rh := msg.ReplicateHeader()

	// Check if the message can be handled by current wal.
	if impl.getRole() == replicateutil.RolePrimary && rh != nil {
		return nil, status.NewReplicateViolation("replicate message cannot be received in primary role")
	}
	if impl.getRole() != replicateutil.RolePrimary && rh == nil {
		return nil, status.NewReplicateViolation("non-replicate message cannot be received in secondary role")
	}
	if rh == nil {
		return nil, ErrNotReplicateMessage
	}
	return impl.beginReplicateMessage(ctx, msg)
}

func (impl *replicatesManagerImpl) GetReplicateCheckpoint() (*utility.ReplicateCheckpoint, error) {
	impl.mu.Lock()
	defer impl.mu.Unlock()

	if impl.checkpoint == nil {
		return nil, status.NewReplicateViolation("wal is not on replicating mode")
	}
	return impl.checkpoint, nil
}

func (impl *replicatesManagerImpl) beginReplicateMessage(ctx context.Context, msg message.MutableMessage) (ReplicateAcker, error) {
	rh := msg.ReplicateHeader()
	if rh.ClusterID != impl.checkpoint.ClusterID {
		return nil, status.NewReplicateViolation("cluster id mismatch, current: %s, expected: %s", rh.ClusterID, impl.checkpoint.ClusterID)
	}

	// if the incoming message's time tick is less than the checkpoint's time tick,
	// it means that the message has been written to the wal, so it can be ignored.
	// txn message will share same time tick, so we only filter with <=.
	isTxnBody := msg.TxnContext() != nil && msg.MessageType() != message.MessageTypeBeginTxn
	if (isTxnBody && rh.TimeTick <= impl.checkpoint.TimeTick) || (!isTxnBody && rh.TimeTick < impl.checkpoint.TimeTick) {
		return nil, status.NewIgnoreOperation("message is too old, message_id: %s, time_tick: %d, txn: %t, current time tick: %d",
			rh.MessageID, rh.TimeTick, isTxnBody, impl.checkpoint.TimeTick)
	}

	if msg.TxnContext() != nil {
		return impl.startReplicateTxnMessage(ctx, msg, rh)
	}
	return impl.startReplicateNonTxnMessage(ctx, msg, rh)
}

func (impl *replicatesManagerImpl) startReplicateTxnMessage(_ context.Context, msg message.MutableMessage, rh *message.ReplicateHeader) (ReplicateAcker, error) {
	txn := msg.TxnContext()
	switch msg.MessageType() {
	case message.MessageTypeBeginTxn:
		if err := impl.txnHelper.StartBegin(txn, rh); err != nil {
			return nil, err
		}
		return replicateAckerImpl(func(err error) {
			if err == nil {
				impl.txnHelper.BeginDone(txn)
			}
			impl.mu.Unlock()
		}), nil
	case message.MessageTypeCommitTxn:
		if err := impl.txnHelper.StartCommit(txn); err != nil {
			return nil, err
		}
		// only update the checkpoint when the txn is committed.
		return replicateAckerImpl(func(err error) {
			if err == nil {
				impl.txnHelper.CommitDone(txn)
				impl.pushForwardCheckpoint(rh.TimeTick, rh.LastConfirmedMessageID)
			}
			impl.mu.Unlock()
		}), nil
	case message.MessageTypeRollbackTxn:
		panic("unreachable: rollback txn message should never be replicated when wal is on replicating mode")
	default:
		if err := impl.txnHelper.AddNewMessage(txn, rh); err != nil {
			return nil, err
		}
		return replicateAckerImpl(func(err error) {
			if err == nil {
				impl.txnHelper.AddNewMessageDone(rh)
			}
			impl.mu.Unlock()
		}), nil
	}
}

// startReplicateNonTxnMessage starts the replicate non-txn message operation.
func (impl *replicatesManagerImpl) startReplicateNonTxnMessage(_ context.Context, _ message.MutableMessage, rh *message.ReplicateHeader) (ReplicateAcker, error) {
	if impl.txnHelper.CurrentTxn() != nil {
		return nil, status.NewReplicateViolation(
			"txn is in progress, so the incoming message must be txn message, current txn: %d",
			impl.txnHelper.CurrentTxn().TxnID,
		)
	}
	return replicateAckerImpl(func(err error) {
		if err == nil {
			impl.pushForwardCheckpoint(rh.TimeTick, rh.LastConfirmedMessageID)
		}
		impl.mu.Unlock()
	}), nil
}

// pushForwardCheckpoint pushes forward the checkpoint.
func (impl *replicatesManagerImpl) pushForwardCheckpoint(timetick uint64, lastConfirmedMessageID message.MessageID) error {
	impl.checkpoint.TimeTick = timetick
	impl.checkpoint.MessageID = lastConfirmedMessageID
	return nil
}

func (impl *replicatesManagerImpl) Role() replicateutil.Role {
	impl.mu.Lock()
	defer impl.mu.Unlock()

	return impl.getRole()
}

func (impl *replicatesManagerImpl) getRole() replicateutil.Role {
	if impl.replicateConfigHelper == nil {
		return replicateutil.RolePrimary
	}
	return impl.replicateConfigHelper.MustGetCluster(impl.currentClusterID).Role()
}

// isPrimaryRole checks if the current cluster is the primary role.
func (impl *replicatesManagerImpl) isPrimaryRole() bool {
	return impl.getRole() == replicateutil.RolePrimary
}

// newReplicateViolationErrorForConfig creates a new replicate violation error for the given configuration and error.
func newReplicateViolationErrorForConfig(cfg *commonpb.ReplicateConfiguration, err error) error {
	bytes, _ := protojson.Marshal(cfg)
	return status.NewReplicateViolation("when greating replciate graph, %s, %s", string(bytes), err.Error())
}
