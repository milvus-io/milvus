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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// PKIndexState is the primary key index state of a vchannel as the observe
// sees it now.
type PKIndexState struct {
	EverWritten streamingpb.VChannelWriteHistory
	// PKIndex is a copy of the enabled primary key index, nil when not enabled.
	PKIndex *streamingpb.PKIndexOfVChannel
	// Schema is the latest schema of the collection. It is shared and read only.
	Schema *schemapb.CollectionSchema
}

// ObserveWrite records that rows were written to the vchannel at timetick.
// It returns true when the write history changed from NEVER to WRITTEN.
// It does not move the checkpoint: the replay of a write is idempotent.
func (info *VChannelView) ObserveWrite(timetick uint64) bool {
	info.mu.Lock()
	defer info.mu.Unlock()
	if !info.shouldObserveLocked(timetick) || !info.isNormalLocked() {
		return false
	}
	if info.meta.CollectionInfo.GetEverWritten() != streamingpb.VChannelWriteHistory_VCHANNEL_WRITE_HISTORY_NEVER {
		return false
	}
	info.meta.CollectionInfo.EverWritten = streamingpb.VChannelWriteHistory_VCHANNEL_WRITE_HISTORY_WRITTEN
	info.dirty = true
	return true
}

// ObserveCreateIndexMessageV2 decides at the CreateIndex of a primary key
// index whether the index is enabled on the vchannel: it is enabled only when
// no row was ever written. It moves the checkpoint to the message, so a replay
// of the message does not decide again.
func (info *VChannelView) ObserveCreateIndexMessageV2(msg message.ImmutableCreateIndexMessageV2) bool {
	header := msg.Header()
	if header.GetIndexType() != common.PrimaryKeyIndexType {
		return false
	}
	info.mu.Lock()
	defer info.mu.Unlock()
	if !info.shouldObserveLocked(msg.TimeTick()) || !info.isNormalLocked() {
		return false
	}
	if info.meta.CollectionInfo.GetEverWritten() == streamingpb.VChannelWriteHistory_VCHANNEL_WRITE_HISTORY_NEVER {
		info.meta.CollectionInfo.PkIndex = &streamingpb.PKIndexOfVChannel{
			IndexId:        header.GetIndexId(),
			FieldId:        header.GetFieldId(),
			EnableTimeTick: msg.TimeTick(),
		}
	}
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return true
}

// ObserveDropIndexMessageV2 disables the primary key index of the vchannel
// when the DropIndex names it, or names no index.
func (info *VChannelView) ObserveDropIndexMessageV2(msg message.ImmutableDropIndexMessageV2) bool {
	header := msg.Header()
	if header.GetIndexType() != common.PrimaryKeyIndexType {
		return false
	}
	info.mu.Lock()
	defer info.mu.Unlock()
	if !info.shouldObserveLocked(msg.TimeTick()) || !info.isNormalLocked() {
		return false
	}
	pkIndex := info.meta.CollectionInfo.GetPkIndex()
	if pkIndex == nil {
		return false
	}
	if len(header.GetIndexIds()) > 0 && !containsIndexID(header.GetIndexIds(), pkIndex.GetIndexId()) {
		return false
	}
	info.meta.CollectionInfo.PkIndex = nil
	info.meta.CheckpointTimeTick = msg.TimeTick()
	info.dirty = true
	return true
}

// PKIndexState returns the primary key index state of the vchannel, false
// when the vchannel has no collection info.
func (info *VChannelView) PKIndexState() (PKIndexState, bool) {
	info.mu.Lock()
	defer info.mu.Unlock()
	collection := info.meta.GetCollectionInfo()
	if collection == nil {
		return PKIndexState{}, false
	}
	state := PKIndexState{EverWritten: collection.GetEverWritten()}
	if pkIndex := collection.GetPkIndex(); pkIndex != nil {
		state.PKIndex = proto.Clone(pkIndex).(*streamingpb.PKIndexOfVChannel)
	}
	_, state.Schema = info.GetSchemaLocked(0)
	return state, true
}

func (info *VChannelView) isNormalLocked() bool {
	return !info.closingLocked(0) &&
		info.meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_NORMAL &&
		info.meta.GetCollectionInfo() != nil
}

func containsIndexID(indexIDs []int64, indexID int64) bool {
	for _, id := range indexIDs {
		if id == indexID {
			return true
		}
	}
	return false
}

func (m *VChannelRecoveryModule) observeWrite(timetick uint64) {
	if m.vchannelView != nil {
		m.vchannelView.ObserveWrite(timetick)
	}
}

func (m *VChannelRecoveryModule) handleCreateIndexMessage(msg message.ImmutableCreateIndexMessageV2) {
	if m.vchannelView != nil {
		m.vchannelView.ObserveCreateIndexMessageV2(msg)
	}
}

func (m *VChannelRecoveryModule) handleDropIndexMessage(msg message.ImmutableDropIndexMessageV2) {
	if m.vchannelView != nil {
		m.vchannelView.ObserveDropIndexMessageV2(msg)
	}
}

// PKIndexState returns the primary key index state of the vchannel, false
// when the module has no vchannel view.
func (m *VChannelRecoveryModule) PKIndexState() (PKIndexState, bool) {
	if m == nil {
		return PKIndexState{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.vchannelView == nil {
		return PKIndexState{}, false
	}
	return m.vchannelView.PKIndexState()
}

// PKIndexState returns the primary key index state of vchannel, false when
// the pchannel has no recovery module of the vchannel.
func (m *PChannelRecoveryManager) PKIndexState(vchannel string) (PKIndexState, bool) {
	if m == nil {
		return PKIndexState{}, false
	}
	return m.Module(vchannel).PKIndexState()
}
