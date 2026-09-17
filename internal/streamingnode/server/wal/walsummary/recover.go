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
	if found && (manifest.Coverage == nil || manifest.Coverage.GetGeneration() != math.MaxUint64) {
		from := uint64(0)
		if manifest.Coverage != nil {
			from = manifest.Coverage.GetGeneration() + 1
		}
		tail, err := m.cfg.Store.ProbeChunkForwardOfTerm(ctx, sourceTerm, from)
		if err != nil {
			return err
		}
		for _, entry := range tail {
			if manifest.Coverage != nil && entry.GetStartAfterTimeTick() != manifest.Coverage.GetEndTimeTick() {
				return storeCorruptedf("summary tail is not contiguous with covered WAL prefix at generation %d", entry.GetGeneration())
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
	if last := manifest.GetCoverage(); last != nil {
		m.generationExhausted = last.GetGeneration() == math.MaxUint64
		if !m.generationExhausted {
			m.nextGeneration = last.GetGeneration() + 1
		}
	}
	m.latestCoveredTimeTick = manifest.GetCoverage().GetEndTimeTick()
	m.restoredTimeTick = m.latestCoveredTimeTick
	m.advanceReadableLocked(m.restoredTimeTick)
	m.advanceLastAckedLocked(m.restoredTimeTick)
	m.sealedThrough = m.restoredTimeTick
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
	coverage := manifest.GetCoverage()
	if coverage != nil && (coverage.GetStartAfterTimeTick() >= coverage.GetEndTimeTick() || coverage.GetTerm() <= 0) {
		return storeCorruptedf("invalid summary coverage")
	}
	for _, frontier := range manifest.GetTransformFastForwardTimeTick() {
		if frontier > coverage.GetEndTimeTick() {
			return storeCorruptedf("summary fast-forward exceeds coverage")
		}
	}
	if len(manifest.GetChunks()) == 0 {
		return nil
	}
	if coverage == nil {
		return storeCorruptedf("summary manifest is missing coverage")
	}
	var previous *streamingpb.PChannelSummaryChunkIndexEntry
	for _, chunk := range manifest.GetChunks() {
		if err := validateChunkIndex(chunk); err != nil {
			return err
		}
		if chunk.GetStartAfterTimeTick() < coverage.GetStartAfterTimeTick() || chunk.GetEndTimetick() > coverage.GetEndTimeTick() ||
			(previous != nil && (previous.GetGeneration() == math.MaxUint64 || chunk.GetGeneration() != previous.GetGeneration()+1 || chunk.GetStartAfterTimeTick() != previous.GetEndTimetick())) {
			return storeCorruptedf("invalid or discontinuous summary manifest")
		}
		previous = chunk
	}
	if previous.GetGeneration() != coverage.GetGeneration() || previous.GetTerm() != coverage.GetTerm() || previous.GetEndTimetick() != coverage.GetEndTimeTick() {
		return storeCorruptedf("summary manifest tail differs from coverage")
	}
	return nil
}

func validateChunkIndex(chunk *streamingpb.PChannelSummaryChunkIndexEntry) error {
	if chunk == nil || chunk.GetStartAfterTimeTick() >= chunk.GetEndTimetick() {
		return storeCorruptedf("invalid summary chunk coverage")
	}
	for _, index := range chunk.GetVchannels() {
		if index.GetStartTimetick() <= chunk.GetStartAfterTimeTick() || index.GetEndTimetick() > chunk.GetEndTimetick() || index.GetStartTimetick() > index.GetEndTimetick() {
			return storeCorruptedf("summary section exceeds chunk coverage")
		}
		if err := validateTransformIndex(index); err != nil {
			return err
		}
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
		// Dropped/tombstoned metadata can precede L0 completion. Only its
		// persisted M releases records; final catalog cleanup reports infinity.
		if frontier > m.gcFrontiers[vchannel] {
			m.gcFrontiers[vchannel] = frontier
			m.materializedFrontiers[vchannel] = max(m.materializedFrontiers[vchannel], frontier)
		}
	}
}
