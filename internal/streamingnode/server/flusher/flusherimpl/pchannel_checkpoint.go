package flusherimpl

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

// recoverPChannelCheckpointManager recovers the pchannel checkpoint manager from the catalog
func recoverPChannelCheckpointManager(
	ctx context.Context,
	walName string,
	pchannel string,
	checkpoints map[string]message.MessageID,
) (*pchannelCheckpointManager, error) {
	vchannelManager := newVChannelCheckpointManager(checkpoints)
	checkpoint, err := resource.Resource().StreamingNodeCatalog().GetConsumeCheckpoint(ctx, pchannel)
	if err != nil {
		return nil, err
	}
	var startMessageID message.MessageID
	var previous message.MessageID
	if checkpoint != nil {
		startMessageID = message.MustUnmarshalMessageID(walName, checkpoint.MessageID.Id)
		previous = startMessageID
	} else {
		startMessageID = vchannelManager.MinimumCheckpoint()
	}
	u := &pchannelCheckpointManager{
		notifier:        syncutil.NewAsyncTaskNotifier[struct{}](),
		cond:            syncutil.NewContextCond(&sync.Mutex{}),
		pchannel:        pchannel,
		vchannelManager: vchannelManager,
		startMessageID:  startMessageID,
		logger:          resource.Resource().Logger().With(zap.String("pchannel", pchannel), log.FieldComponent("checkpoint-updater")),
	}
	go u.background(previous)
	return u, nil
}

// pchannelCheckpointManager is the struct to update the checkpoint of a pchannel
type pchannelCheckpointManager struct {
	notifier        *syncutil.AsyncTaskNotifier[struct{}]
	cond            *syncutil.ContextCond
	pchannel        string
	vchannelManager *vchannelCheckpointManager
	startMessageID  message.MessageID
	logger          *log.MLogger
}

// StartMessageID returns the start message checkpoint of current recovery
func (m *pchannelCheckpointManager) StartMessageID() message.MessageID {
	return m.startMessageID
}

// Update updates the checkpoint of a vchannel
func (m *pchannelCheckpointManager) Update(vchannel string, checkpoint message.MessageID) {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()

	oldMinimum := m.vchannelManager.MinimumCheckpoint()
	err := m.vchannelManager.Update(vchannel, checkpoint)
	if err != nil {
		m.logger.Warn("failed to update vchannel checkpoint", zap.String("vchannel", vchannel), zap.Error(err))
		return
	}
	if newMinimum := m.vchannelManager.MinimumCheckpoint(); oldMinimum == nil || oldMinimum.LT(newMinimum) {
		// if the minimum checkpoint is updated, notify the background goroutine to update the pchannel checkpoint
		m.cond.UnsafeBroadcast()
	}
}

// AddVChannel adds a vchannel to the pchannel
func (m *pchannelCheckpointManager) AddVChannel(vchannel string, checkpoint message.MessageID) {
	m.cond.LockAndBroadcast()
	defer m.cond.L.Unlock()

	if err := m.vchannelManager.Add(vchannel, checkpoint); err != nil {
		m.logger.Warn("failed to add vchannel checkpoint", zap.String("vchannel", vchannel), zap.Error(err))
	}
	m.logger.Info("add vchannel checkpoint", zap.String("vchannel", vchannel), zap.Stringer("checkpoint", checkpoint))
}

// DropVChannel drops a vchannel from the pchannel
func (m *pchannelCheckpointManager) DropVChannel(vchannel string) {
	m.cond.LockAndBroadcast()
	defer m.cond.L.Unlock()

	if err := m.vchannelManager.Drop(vchannel); err != nil {
		m.logger.Warn("failed to drop vchannel checkpoint", zap.String("vchannel", vchannel), zap.Error(err))
		return
	}
	m.logger.Info("drop vchannel checkpoint", zap.String("vchannel", vchannel))
}

func (m *pchannelCheckpointManager) background(previous message.MessageID) {
	defer func() {
		m.notifier.Finish(struct{}{})
		m.logger.Info("pchannel checkpoint updater is closed")
	}()
	previousStr := "nil"
	if previous != nil {
		previousStr = previous.String()
	}
	m.logger.Info("pchannel checkpoint updater started", zap.String("previous", previousStr))

	backoff := backoff.NewExponentialBackOff()
	backoff.InitialInterval = 100 * time.Millisecond
	backoff.MaxInterval = 10 * time.Second
	for {
		current, err := m.blockUntilCheckpointUpdate(previous)
		if err != nil {
			return
		}

		if previous == nil || previous.LT(current) {
			err := resource.Resource().StreamingNodeCatalog().SaveConsumeCheckpoint(m.notifier.Context(), m.pchannel, &streamingpb.WALCheckpoint{
				MessageID: &messagespb.MessageID{Id: current.Marshal()},
			})
			if err != nil {
				nextInterval := backoff.NextBackOff()
				m.logger.Warn("failed to update pchannel checkpoint", zap.Stringer("checkpoint", current), zap.Duration("nextRetryInterval", nextInterval), zap.Error(err))
				select {
				case <-time.After(nextInterval):
					continue
				case <-m.notifier.Context().Done():
					return
				}
			}
			backoff.Reset()
			previous = current
			m.logger.Debug("update pchannel checkpoint", zap.Stringer("current", current))
		}
	}
}

// blockUntilCheckpointUpdate blocks until the checkpoint of the pchannel is updated
func (m *pchannelCheckpointManager) blockUntilCheckpointUpdate(previous message.MessageID) (message.MessageID, error) {
	m.cond.L.Lock()
	// block until following conditions are met:
	// there is at least one vchannel, and minimum checkpoint of all vchannels is greater than previous.
	// if the previous is nil, block until there is at least one vchannel.
	for m.vchannelManager.Len() == 0 || (previous != nil && m.vchannelManager.MinimumCheckpoint().LTE(previous)) {
		if err := m.cond.Wait(m.notifier.Context()); err != nil {
			return nil, err
		}
	}
	minimum := m.vchannelManager.MinimumCheckpoint()
	m.cond.L.Unlock()
	return minimum, nil
}

// Close closes the pchannel checkpoint updater
func (m *pchannelCheckpointManager) Close() {
	m.notifier.Cancel()
	m.notifier.BlockUntilFinish()
}
