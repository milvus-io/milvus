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

package walsummary

import (
	"context"
	"math"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Restore reads the newest complete manifest and its continuous tail. The
// caller must claim its assignment/checkpoint before calling this method once,
// before observation. Publication is submitted to NodeScheduler, never done
// inline. An absent manifest leaves WAL replay to the caller's safe checkpoint.
// Writers must receive a fresh assignment term on every reopen, including when
// the previous attempt left no manifest. Same-term recovery supports reads only.
func (m *Manager) Restore(ctx context.Context) error {
	terms, err := m.cfg.Store.ListManifestTerms(ctx, math.MaxInt64)
	if err != nil {
		return err
	}
	manifest := &streamingpb.PChannelSummaryManifest{}
	var sourceTerm int64
	found := len(terms) > 0
	if found {
		sourceTerm = terms[0]
		if sourceTerm > m.cfg.Term {
			return merr.WrapErrServiceUnavailableMsg("summary manifest term %d supersedes writer term %d", sourceTerm, m.cfg.Term)
		}
		var exists bool
		manifest, exists, err = m.cfg.Store.ReadManifestOfTerm(ctx, sourceTerm)
		if err != nil {
			return err
		}
		if !exists {
			return merr.WrapErrServiceUnavailableMsg("summary manifest disappeared during recovery")
		}
		if err := validateManifest(manifest); err != nil {
			return err
		}
	}
	original := proto.Clone(manifest).(*streamingpb.PChannelSummaryManifest)
	if found && (manifest.LastChunk == nil || manifest.LastChunk.GetGeneration() != math.MaxUint64) {
		from := uint64(0)
		if manifest.LastChunk != nil {
			from = manifest.LastChunk.GetGeneration() + 1
		}
		tail, err := m.cfg.Store.ProbeChunkForwardOfTerm(ctx, sourceTerm, from)
		if err != nil {
			return err
		}
		for _, entry := range tail {
			if entry.GetStartTimetick() <= manifest.GetCoveredPosition().GetTimeTick() {
				return storeCorruptedf("summary tail overlaps the covered WAL prefix at generation %d", entry.GetGeneration())
			}
			recordChunk(manifest, entry)
		}
	}
	if err := validateManifest(manifest); err != nil {
		return err
	}
	m.mu.Lock()
	m.manifest = manifest
	m.reopenedTerm = found && sourceTerm == m.cfg.Term
	m.manifestPublished = found && sourceTerm == m.cfg.Term
	m.manifestVersion = 1
	m.publishedVersion = 0
	if m.manifestPublished && proto.Equal(original, manifest) {
		m.publishedVersion = 1
	}
	if last := manifest.GetLastChunk(); last != nil {
		m.generationExhausted = last.GetGeneration() == math.MaxUint64
		if !m.generationExhausted {
			m.nextGeneration = last.GetGeneration() + 1
		}
	}
	m.latestCoveredTimeTick = manifest.GetCoveredPosition().GetTimeTick()
	m.restoredTimeTick = m.latestCoveredTimeTick
	m.advanceLastAckedLocked(summaryCheckpoint(manifest.GetCoveredPosition()))
	for _, chunk := range manifest.GetChunks() {
		for _, index := range chunk.GetVchannels() {
			m.durableFrontiers[index.GetVchannel()] = max(m.durableFrontiers[index.GetVchannel()], index.GetEndTimetick())
		}
	}
	m.mu.Unlock()
	m.scheduleManifest()
	return nil
}

// validateManifest checks structure without reading retained chunk bodies.
func validateManifest(manifest *streamingpb.PChannelSummaryManifest) error {
	if err := validateSummaryPosition(manifest.GetCoveredPosition()); err != nil {
		return err
	}
	if manifest.GetLastChunk() == nil && manifest.GetCoveredPosition() != nil {
		return storeCorruptedf("summary coverage has no generation boundary")
	}
	if manifest.GetLastChunk() != nil && manifest.GetCoveredPosition() == nil {
		return storeCorruptedf("summary generation boundary has no covered position")
	}
	if len(manifest.GetChunks()) == 0 {
		return nil
	}
	if manifest.LastChunk == nil {
		return storeCorruptedf("summary manifest is missing its coverage boundary")
	}
	var previous *streamingpb.PChannelSummaryChunkIndexEntry
	for _, chunk := range manifest.GetChunks() {
		if chunk == nil || chunk.GetStartTimetick() > chunk.GetEndTimetick() || chunk.GetGeneration() > manifest.LastChunk.GetGeneration() ||
			chunk.GetEndTimetick() > manifest.GetCoveredPosition().GetTimeTick() ||
			(previous != nil && (previous.GetGeneration() == math.MaxUint64 || chunk.GetGeneration() != previous.GetGeneration()+1 || chunk.GetStartTimetick() <= previous.GetEndTimetick())) {
			return storeCorruptedf("invalid or discontinuous summary manifest")
		}
		if position := chunk.GetCoveredPosition(); position != nil &&
			(position.GetTimeTick() < chunk.GetEndTimetick() || position.GetTimeTick() > manifest.GetCoveredPosition().GetTimeTick()) {
			return storeCorruptedf("summary chunk coverage is outside its manifest boundary")
		}
		previous = chunk
	}
	if previous.GetGeneration() != manifest.LastChunk.GetGeneration() || previous.GetTerm() != manifest.LastChunk.GetTerm() {
		return storeCorruptedf("summary manifest tail differs from its coverage boundary")
	}
	return nil
}

// Manifest returns an independent snapshot; callers cannot mutate runtime state.
func (m *Manager) Manifest() *streamingpb.PChannelSummaryManifest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return proto.Clone(m.manifest).(*streamingpb.PChannelSummaryManifest)
}

// RestoreTransformGCTimeTicks seeds retention from durable VChannel metadata.
// Missing metadata does not prove cleanup: unknown channels remain pinned.
func (m *Manager) RestoreTransformGCTimeTicks(vchannels map[string]*streamingpb.VChannelMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for vchannel, meta := range vchannels {
		frontier := meta.GetTransformMaterializedTimeTick()
		switch meta.GetState() {
		case streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED:
			frontier = DroppedVChannelTimeTick
		}
		if frontier > m.gcFrontiers[vchannel] {
			m.gcFrontiers[vchannel] = frontier
		}
	}
}

// Validate untrusted persisted message IDs before using the checkpoint helpers.
func validateSummaryPosition(position *streamingpb.PChannelSummaryPosition) error {
	if position.GetMessageId() == nil {
		return nil
	}
	if _, err := message.UnmarshalMessageID(position.GetMessageId()); err != nil {
		return markStoreCorrupted(merr.Wrap(err, "invalid summary coverage message ID"))
	}
	return nil
}
