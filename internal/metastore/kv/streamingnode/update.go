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

package streamingnode

import (
	"context"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/txn"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// SaveRecoverySnapshot saves a WAL recovery snapshot in one compound
// operation: segment assignments, vchannels, salvage checkpoint, and
// strictly last the consume checkpoint - the commit point of the snapshot.
// Nil or empty parts of the snapshot are skipped.
//
// The ops are staged into a txn.Builder (reusing the per-key encoders -
// buildSegmentAssignmentKey, getRemovalAndSaveForVChannel,
// buildSalvageCheckpointPath, buildConsumeCheckpointKey) and applied via
// txn.Commit: atomically in a single guarded txn when the whole op set fits
// the store's txn op limit, otherwise via the ordered chunked fallback. Either way the consume checkpoint is staged
// with CommitSave, so it is the last write to become visible; a crash before
// it lands leaves the whole snapshot invisible and the next retry
// re-persists everything (every part is an idempotent put on a deterministic
// key).
func (c *catalog) SaveRecoverySnapshot(ctx context.Context, pChannelName string, snapshot *metastore.WALRecoverySnapshot) error {
	if snapshot == nil {
		return nil
	}
	b := txn.New()
	// Aggregate every removal and every save across segments, vchannels and
	// the salvage checkpoint into two disjoint sets first, then stage all
	// removals before all saves. The keys never overlap (a segment/schema/
	// vchannel key is either removed or saved, never both; segment, vchannel,
	// schema and salvage keyspaces are disjoint), so this global two-phase
	// ordering is equivalent to the per-entry ordering while letting the
	// chunked fallback coalesce into a single remove run and a single save run
	// (O(total/limit) round trips) instead of alternating per entry.
	// Pre-size to the segment count (a lower bound - vchannels add more) to
	// avoid the first rounds of slice/map growth on a segment-heavy snapshot.
	removes := make([]string, 0, len(snapshot.SegmentAssignments))
	saves := make(map[string]string, len(snapshot.SegmentAssignments))
	for _, info := range snapshot.SegmentAssignments {
		key := buildSegmentAssignmentKey(pChannelName, info.GetSegmentId())
		if info.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
			// Flushed segment should be removed from meta.
			removes = append(removes, key)
			continue
		}
		data, err := proto.Marshal(info)
		if err != nil {
			return errors.Wrapf(err, "marshal segment %d at pchannel %s failed", info.GetSegmentId(), pChannelName)
		}
		saves[key] = string(data)
	}
	for _, info := range snapshot.VChannels {
		vremoves, kvs, err := c.getRemovalAndSaveForVChannel(pChannelName, info)
		if err != nil {
			return err
		}
		removes = append(removes, vremoves...)
		for k, v := range kvs {
			saves[k] = v
		}
	}
	for _, r := range removes {
		b.Remove(r)
	}
	for k, v := range saves {
		b.Save(k, v)
	}
	// The salvage checkpoint must be persisted before the consume checkpoint
	// to guarantee ordering across a crash in between. It is a plain save
	// staged after the bulk saves (still coalesced into the same save run), so
	// on the fallback path txn.Commit always flushes it before the CommitSave
	// below.
	if snapshot.SalvageCheckpoint != nil {
		key := buildSalvageCheckpointPath(pChannelName, snapshot.SalvageCheckpoint.GetClusterId())
		data, err := proto.Marshal(snapshot.SalvageCheckpoint)
		if err != nil {
			return errors.Wrapf(err, "marshal salvage checkpoint at pchannel %s failed", pChannelName)
		}
		b.Save(key, string(data))
	}
	// The consume checkpoint is the commit point of the snapshot: staging it
	// with CommitSave makes it the last write to become visible, after every
	// other part of the snapshot has landed.
	if snapshot.ConsumeCheckpoint != nil {
		key := buildConsumeCheckpointKey(pChannelName)
		data, err := proto.Marshal(snapshot.ConsumeCheckpoint)
		if err != nil {
			return errors.Wrapf(err, "marshal consume checkpoint at pchannel %s failed", pChannelName)
		}
		b.CommitSave(key, string(data))
	}
	return txn.Commit(ctx, c.metaKV, b)
}
