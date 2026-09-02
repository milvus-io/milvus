// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vchannel

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// checkpointReporter is the minimal coordinator surface the checkpoint
// updater needs: report one channel checkpoint per vchannel. broker.Broker
// (flushcommon) satisfies it.
type checkpointReporter interface {
	UpdateChannelCheckpoint(ctx context.Context, channelCPs []*msgpb.MsgPosition) error
}

// PChannelCheckpointUpdater periodically reports the pchannel-level recovery
// checkpoint to DataCoord through DataCoord.UpdateChannelCheckpoint, so that
// GetFlushState can observe flush progress.
//
// Deprecated: this component exists only to preserve the channel-checkpoint
// reporting of the removed flusher components (the old WALFlusherImpl /
// ChannelCheckpointUpdater pipeline). The recovery storage write path itself
// never calls UpdateChannelCheckpoint; once the new checkpoint-propagation
// path lands, remove this component together with its wiring
// (PChannelRecoveryManager.checkpointUpdater, the manager config fields, and
// the Start/Close hooks).
type PChannelCheckpointUpdater struct {
	pchannel      string
	vchannels     func() []string
	getCheckpoint func() *utility.WALCheckpoint
	// getVChannelFlushTimeTick returns the vchannel-level flush position of
	// one vchannel (see VChannelRecoveryModule.FlushCheckpointTimeTick): the
	// largest timetick whose insert and delete data of that vchannel has been
	// durably flushed. UpdateChannelCheckpoint is a vchannel-level operation,
	// so each reported position must carry its own vchannel's flush position
	// as the timestamp — the pchannel-level recovery checkpoint TimeTick is
	// not a per-vchannel flush bound.
	getVChannelFlushTimeTick func(vchannel string) uint64
	reporter                 checkpointReporter

	tickInterval time.Duration

	closeCh   chan struct{}
	closeOnce sync.Once
}

// newPChannelCheckpointUpdater creates the updater. getCheckpoint must
// return the pchannel-level recovery checkpoint (RecoveryStorage.GetCheckpoint)
// or nil when none is published yet; vchannels must return the currently
// active vchannels of the pchannel; getVChannelFlushTimeTick must return the
// vchannel-level flush position of a vchannel.
func newPChannelCheckpointUpdater(
	pchannel string,
	vchannels func() []string,
	getCheckpoint func() *utility.WALCheckpoint,
	getVChannelFlushTimeTick func(vchannel string) uint64,
	reporter checkpointReporter,
) *PChannelCheckpointUpdater {
	return &PChannelCheckpointUpdater{
		pchannel:                 pchannel,
		vchannels:                vchannels,
		getCheckpoint:            getCheckpoint,
		getVChannelFlushTimeTick: getVChannelFlushTimeTick,
		reporter:                 reporter,
		// Same cadence as the removed flusher's ChannelCheckpointUpdater
		// (dataNode.channel.channelCheckpointUpdateTickInSeconds).
		tickInterval: paramtable.Get().DataNodeCfg.ChannelCheckpointUpdateTickInSeconds.GetAsDuration(time.Second),
		closeCh:      make(chan struct{}),
	}
}

// Start runs the periodic report loop until Close.
func (u *PChannelCheckpointUpdater) Start() {
	mlog.Info(context.TODO(), "pchannel checkpoint updater start", mlog.String("pchannel", u.pchannel))
	ticker := time.NewTicker(u.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-u.closeCh:
			mlog.Info(context.TODO(), "pchannel checkpoint updater exit", mlog.String("pchannel", u.pchannel))
			return
		case <-ticker.C:
			u.execute()
		}
	}
}

// execute reports the current recovery checkpoint once for every active
// vchannel of the pchannel, using the same UpdateChannelCheckpoint RPC the
// old flusher used.
func (u *PChannelCheckpointUpdater) execute() {
	checkpoint := u.getCheckpoint()
	if checkpoint == nil || checkpoint.MessageID == nil {
		// No checkpoint published yet (still recovering), nothing to report.
		return
	}
	vchannels := u.vchannels()
	if len(vchannels) == 0 {
		return
	}
	msgID, ok := adaptor.TryGetMQWrapperIDFromMessage(checkpoint.MessageID)
	if !ok {
		// The checkpoint message ID has no MQ wrapper counterpart (e.g. a
		// test-only implementation); there is nothing serializable to report.
		mlog.Warn(context.TODO(), "checkpoint message id is not a supported MQ wrapper id, skip reporting",
			mlog.String("pchannel", u.pchannel))
		return
	}
	msgIDBytes := msgID.Serialize()
	channelCPs := make([]*msgpb.MsgPosition, 0, len(vchannels))
	for _, vchannel := range vchannels {
		channelCPs = append(channelCPs, &msgpb.MsgPosition{
			ChannelName: vchannel,
			MsgID:       msgIDBytes,
			Timestamp:   u.getVChannelFlushTimeTick(vchannel),
			WALName:     commonpb.WALName(checkpoint.MessageID.WALName()),
		})
	}
	timeout := paramtable.Get().DataNodeCfg.UpdateChannelCheckpointRPCTimeout.GetAsDuration(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := u.reporter.UpdateChannelCheckpoint(ctx, channelCPs); err != nil {
		// Keep the previous checkpoint on failure; the next tick retries.
		mlog.Warn(ctx, "update channel checkpoint failed",
			mlog.String("pchannel", u.pchannel),
			mlog.Uint64("checkpointTimeTick", checkpoint.TimeTick),
			mlog.Err(err))
		return
	}
	mlog.Info(ctx, "update channel checkpoint done",
		mlog.String("pchannel", u.pchannel),
		mlog.Uint64("checkpointTimeTick", checkpoint.TimeTick),
		mlog.Int("vchannels", len(vchannels)))
}

// Close stops the report loop. It is idempotent.
func (u *PChannelCheckpointUpdater) Close() {
	u.closeOnce.Do(func() {
		close(u.closeCh)
	})
}
