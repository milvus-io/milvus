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

// storedCheckpointReader reads back the time tick of the channel checkpoint
// DataCoord has stored for a vchannel. A successful UpdateChannelCheckpoint
// does not prove that: DataCoord silently drops a channel whose WAL it does
// not place on the reporting node, and still answers success.
type storedCheckpointReader interface {
	StoredChannelCheckpoint(ctx context.Context, vchannel string) (uint64, error)
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
	reporter      checkpointReporter
	storedReader  storedCheckpointReader

	tickInterval time.Duration

	// storedMu guards stored and awaited. stored is the time tick of the
	// checkpoint DataCoord was read back to hold, per vchannel; awaited is the
	// tick a caller of Reported waits for, per vchannel not yet there. Only an
	// awaited vchannel is read back.
	storedMu sync.Mutex
	stored   map[string]uint64
	awaited  map[string]uint64

	closeCh   chan struct{}
	closeOnce sync.Once
}

// newPChannelCheckpointUpdater creates the updater. getCheckpoint must
// return the pchannel-level recovery checkpoint (RecoveryStorage.GetCheckpoint)
// or nil when none is published yet; vchannels returns the channels to report.
// Insert and Delete handles guarantee that the published point is query-safe.
func newPChannelCheckpointUpdater(
	pchannel string,
	vchannels func() []string,
	getCheckpoint func() *utility.WALCheckpoint,
	reporter checkpointReporter,
	storedReader storedCheckpointReader,
) *PChannelCheckpointUpdater {
	return &PChannelCheckpointUpdater{
		pchannel:      pchannel,
		vchannels:     vchannels,
		getCheckpoint: getCheckpoint,
		reporter:      reporter,
		storedReader:  storedReader,
		// Same cadence as the removed flusher's ChannelCheckpointUpdater
		// (dataNode.channel.channelCheckpointUpdateTickInSeconds).
		tickInterval: paramtable.Get().DataNodeCfg.ChannelCheckpointUpdateTickInSeconds.GetAsDuration(time.Second),
		stored:       make(map[string]uint64),
		awaited:      make(map[string]uint64),
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
			Timestamp:   checkpoint.TimeTick,
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
	u.readBackAwaited(timeout)
}

// readBackAwaited reads back the stored checkpoint of every awaited vchannel.
// It runs after a successful report, so the read sees that report if
// DataCoord kept it.
func (u *PChannelCheckpointUpdater) readBackAwaited(timeout time.Duration) {
	if u.storedReader == nil {
		return
	}
	u.storedMu.Lock()
	awaited := make(map[string]uint64, len(u.awaited))
	for vchannel, through := range u.awaited {
		awaited[vchannel] = through
	}
	u.storedMu.Unlock()
	for vchannel, through := range awaited {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		timeTick, err := u.storedReader.StoredChannelCheckpoint(ctx, vchannel)
		cancel()
		if err != nil {
			mlog.Warn(ctx, "read back stored channel checkpoint failed",
				mlog.String("pchannel", u.pchannel),
				mlog.String("vchannel", vchannel),
				mlog.Err(err))
			continue
		}
		u.markStored(vchannel, timeTick)
		if timeTick < through {
			mlog.Info(ctx, "stored channel checkpoint is still below the awaited time tick",
				mlog.String("pchannel", u.pchannel),
				mlog.String("vchannel", vchannel),
				mlog.Uint64("storedTimeTick", timeTick),
				mlog.Uint64("awaitedTimeTick", through))
		}
	}
}

func (u *PChannelCheckpointUpdater) markStored(vchannel string, timeTick uint64) {
	u.storedMu.Lock()
	defer u.storedMu.Unlock()
	if u.stored[vchannel] < timeTick {
		u.stored[vchannel] = timeTick
	}
	if u.awaited[vchannel] <= u.stored[vchannel] {
		delete(u.awaited, vchannel)
	}
}

// Reported reports whether DataCoord was read back to hold a checkpoint of
// vchannel at or past through. A false answer marks the vchannel awaited, so
// the next successful report reads it back. What is held is memory only:
// after a restart the vchannel is still present, so it is reported and read
// back again before it can pass.
func (u *PChannelCheckpointUpdater) Reported(vchannel string, through uint64) bool {
	u.storedMu.Lock()
	defer u.storedMu.Unlock()
	if u.stored[vchannel] >= through {
		return true
	}
	if u.awaited[vchannel] < through {
		u.awaited[vchannel] = through
	}
	return false
}

// confirmsStored reports whether the updater can read back what DataCoord
// stored, which Reported needs to ever answer true.
func (u *PChannelCheckpointUpdater) confirmsStored() bool {
	return u.storedReader != nil
}

// Forget drops what was recorded for a vchannel that is no longer reported.
func (u *PChannelCheckpointUpdater) Forget(vchannel string) {
	u.storedMu.Lock()
	defer u.storedMu.Unlock()
	delete(u.stored, vchannel)
	delete(u.awaited, vchannel)
}

// Close stops the report loop. It is idempotent.
func (u *PChannelCheckpointUpdater) Close() {
	u.closeOnce.Do(func() {
		close(u.closeCh)
	})
}
