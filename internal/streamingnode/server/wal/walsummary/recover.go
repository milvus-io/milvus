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

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// probeLimit bounds a single forward probe. A term that wrote more than this
// many chunks past its manifest is pathological; failing loudly beats scanning
// object storage without end.
const probeLimit = 1 << 16

// Restore rebuilds the in-memory state from the durable store, before WAL
// replay. It must be called once before any message is observed. The vchannel
// metas (already read from the catalog by the recovery caller) restore the
// GC positions; the manifest restores the chunk index and the durable
// frontiers. All recovery logic lives here, kept out of the recovery storage
// wiring.
//
// The sequence is fixed, and every step exists to close a specific way data
// could otherwise be lost:
//
//  1. read this term's manifest (missing is fine: a term that never wrote one);
//  2. probe chunks forward from the manifest's newest generation — this
//     recovers everything written after the last manifest publish (the crash
//     window between chunk write and manifest write);
//  3. on a term handoff, inherit the previous term's index — chunks the
//     previous owner published (handles released, the WAL checkpoint may have
//     passed them) but never materialized must stay visible, or those delete
//     records are lost forever;
//  4. publish this term's manifest, sealing the inherited and probed sets
//     into it — without this the tail is invisible to the NEXT recovery and
//     is lost silently;
//  5. only now may this owner write chunks (generations start past the
//     inherited set).
//
// Restore is read-only with respect to the catalog: the summary store owns no
// fencing marker of its own. What arbitration exists comes from the object keys
// being term-scoped, so a fenced owner can never collide with the successor's
// chunks -- it writes beside it and the loser's objects are orphans the
// manifest never names.
//
// There is NO fencing on the consume checkpoint itself: its persistence is not
// a compare-and-swap today (see the TODO on recoveryStorageImpl.backgroundTask),
// so a stale owner that is still running can in principle advance it. That is a
// pre-existing property of the recovery storage rather than something the
// summary introduces, and closing it is follow-up work -- do not assume a fence
// is already in place here.
func (m *Manager) Restore(ctx context.Context) error {
	manifest, needsPublish, err := m.recoverManifestOfTerm(ctx, m.cfg.Term)
	if err != nil {
		return err
	}
	if !needsPublish && m.cfg.Term > 0 {
		// Term handoff: this term has no chunks of its own yet. Adopt the most
		// recent non-empty earlier term's index wholesale so un-materialized
		// records stay reachable, then seal the union into this term's manifest.
		//
		// The search must look back past a single term: an intermediate term can
		// be assigned (TryAssignToServerID burns a term on every assignment
		// attempt) and then die before ever sealing a manifest, leaving an
		// empty manifest at term-1 while the real records live at an older term.
		// Reading only term-1 would strand those records: their delete would
		// silently resurrect and the orphaned chunk objects would be unreachable
		// to GC.
		//
		// Listing manifests is sufficient BECAUSE a term writes its manifest
		// before its first chunk (see the design doc): a term that holds chunks
		// always has a manifest object, so a term absent from this list holds
		// nothing to inherit. The current term's own probe above still covers
		// the crash-after-chunk-before-manifest window for THIS term.
		//
		// One list of the manifest prefix answers which terms to look at. A
		// downward probe over every term would be unbounded in the number of
		// burned terms -- each step paying a manifest read plus a full
		// chunk-prefix list -- and on a pchannel with a high term counter and no
		// chunk it would walk all the way to zero while the channel is
		// unwritable.
		terms, err := m.cfg.Store.ListManifestTerms(ctx, m.cfg.Term-1)
		if err != nil {
			return err
		}
		for _, t := range terms {
			previous, previousNeedsPublish, err := m.recoverManifestOfTerm(ctx, t)
			if err != nil {
				return err
			}
			if previousNeedsPublish {
				manifest = previous
				needsPublish = true
				break
			}
		}
	}
	// Publish this term's manifest whenever it now records anything (its own
	// chunks, a probed tail, or an inherited previous-term index): the seal
	// keeps the whole set visible to the NEXT recovery and makes the inherited
	// set durable before any new chunk is written.
	if needsPublish {
		if err := m.cfg.Store.WriteManifest(ctx, manifest); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.manifest = manifest
	m.manifestVersion++
	if latest, ok := manifestNewest(manifest); ok {
		m.nextGeneration = latest.GetGeneration() + 1
		m.latestCoveredTimeTick = latest.GetEndTimetick()
	} else {
		m.nextGeneration = 0
	}
	// The durable frontier per vchannel is the newest chunk end covering it:
	// WAL replay re-observes records the manifest already covers, and
	// ObserveMessage skips them.
	for _, chunk := range manifest.GetChunks() {
		for _, index := range chunk.GetVchannels() {
			if end := index.GetEndTimetick(); end > m.durableFrontiers[index.GetVchannel()] {
				m.durableFrontiers[index.GetVchannel()] = end
			}
		}
	}
	m.mu.Unlock()
	if logger := m.cfg.Logger; logger != nil {
		logger.Info(ctx, "walsummary restored",
			mlog.String("pchannel", m.cfg.PChannel),
			mlog.Int64("term", m.cfg.Term),
			mlog.Int("chunks", len(manifest.GetChunks())),
			mlog.Uint64("nextGeneration", m.nextGeneration))
	}
	return nil
}

// recoverManifestOfTerm reads one term's manifest and probes the chunk tail
// written past its last manifest publish, returning the sealed union and
// whether the term records anything at all.
func (m *Manager) recoverManifestOfTerm(ctx context.Context, term int64) (*streamingpb.PChannelSummaryManifest, bool, error) {
	previous, found, err := m.cfg.Store.ReadManifestOfTerm(ctx, term)
	if err != nil {
		return nil, false, err
	}
	manifest := inheritManifest(previous, nil)
	var fromGeneration uint64
	if latest, ok := manifestNewest(manifest); ok {
		fromGeneration = latest.GetGeneration() + 1
	}
	discovered, err := m.cfg.Store.ProbeChunkForwardOfTerm(ctx, term, fromGeneration)
	if err != nil {
		return nil, false, err
	}
	if len(discovered) > probeLimit {
		return nil, false, storeCorruptedf("summary store of %s has %d unrecorded chunks beyond generation %d",
			m.cfg.PChannel, len(discovered), fromGeneration)
	}
	if len(discovered) > 0 {
		manifest = inheritManifest(manifest, discovered)
		found = true
	}
	if !found {
		return &streamingpb.PChannelSummaryManifest{}, false, nil
	}
	return manifest, true, nil
}

// manifestNewest returns the newest chunk the manifest records.
func manifestNewest(manifest *streamingpb.PChannelSummaryManifest) (*streamingpb.PChannelSummaryChunkIndexEntry, bool) {
	chunks := manifest.GetChunks()
	if len(chunks) == 0 {
		return nil, false
	}
	return chunks[len(chunks)-1], true
}

// Manifest returns a snapshot of the current manifest.
func (m *Manager) Manifest() *streamingpb.PChannelSummaryManifest {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.manifest
}
