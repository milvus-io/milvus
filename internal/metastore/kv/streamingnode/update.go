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
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SaveRecoverySnapshot saves a WAL recovery snapshot in one compound
// operation: module upserts/removals, salvage checkpoint, and strictly last
// the consume checkpoint - the commit point of the snapshot.
// Nil or empty parts of the snapshot are skipped.
//
// Every component transaction compares the checkpoint value captured before
// writing, including every batch of an oversized snapshot. The final checkpoint
// remains the last write. Initial ownership must be established with a
// checkpoint-only snapshot before publishing components.
func (c *catalog) SaveRecoverySnapshot(ctx context.Context, pChannelName string, snapshot *metastore.WALRecoverySnapshot) error {
	if snapshot == nil {
		return nil
	}
	b := txn.New()
	// Aggregate every module mutation before adding the checkpoint commit
	// marker. Closed and tombstoned recovery metadata remains persisted until
	// the growing-module cleanup task explicitly includes its removal here.
	removes := make([]string, 0, len(snapshot.RemovedSegmentIDs))
	vchannelSaves := make(map[string]string, len(snapshot.VChannels)+len(snapshot.VChannelBaseMetas))
	segmentSaves := make(map[string]string, len(snapshot.SegmentAssignments))
	for _, info := range snapshot.SegmentAssignments {
		key := buildSegmentAssignmentKey(pChannelName, info.GetSegmentId())
		data, err := proto.Marshal(info)
		if err != nil {
			return merr.WrapErrSerializationFailed(err, "marshal segment %d at pchannel %s", info.GetSegmentId(), pChannelName)
		}
		segmentSaves[key] = string(data)
	}
	for _, segmentID := range snapshot.RemovedSegmentIDs {
		removes = append(removes, buildSegmentAssignmentKey(pChannelName, segmentID))
	}
	for _, info := range snapshot.VChannels {
		vremoves, kvs, err := c.getRemovalAndSaveForVChannel(pChannelName, info)
		if err != nil {
			return err
		}
		removes = append(removes, vremoves...)
		for k, v := range kvs {
			vchannelSaves[k] = v
		}
	}
	for _, info := range snapshot.VChannelBaseMetas {
		data, err := marshalVChannelBaseMeta(pChannelName, info)
		if err != nil {
			return err
		}
		vchannelSaves[buildVChannelKey(pChannelName, info.GetVchannel())] = data
	}
	// A vchannel cleanup also removes its transform-log meta. Keep the
	// vchannel removal last so the chunked fallback never exposes an orphaned
	// transform log for a vchannel that has already disappeared.
	for _, info := range snapshot.RemovedVChannels {
		removes = append(removes, buildVChannelKey(pChannelName, info.GetVchannel()))
		for _, schema := range info.GetCollectionInfo().GetSchemas() {
			removes = append(removes, buildVChannelSchemaKey(
				pChannelName,
				info.GetVchannel(),
				schema.GetCheckpointTimeTick(),
			))
		}
	}
	for vchannel, timeticks := range snapshot.RemovedVChannelSchemas {
		for _, timetick := range timeticks {
			key := buildVChannelSchemaKey(pChannelName, vchannel, timetick)
			removes = append(removes, key)
			// A schema change can be frozen after cleanup selected these durable
			// tombstones. Do not rewrite them from that newer full snapshot.
			delete(vchannelSaves, key)
		}
	}
	for _, r := range removes {
		b.Remove(r)
	}
	// Persist vchannel ownership before its dependent segment metadata on the
	// chunked fallback path. Atomic commits are unaffected.
	for k, v := range vchannelSaves {
		b.Save(k, v)
	}
	for k, v := range segmentSaves {
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
			return merr.WrapErrSerializationFailed(err, "marshal salvage checkpoint at pchannel %s", pChannelName)
		}
		b.Save(key, string(data))
	}
	// The consume checkpoint is the commit point of the snapshot: staging it
	// with CommitSave makes it the last write to become visible, after every
	// other part of the snapshot has landed. Its advancement is additionally
	// guarded by a value comparison on every transaction: the checkpoint
	// may only advance when the recorded term is not newer than the
	// publisher's own term, so an older-term publisher that survived a
	// takeover can never advance it past the successor's inherited manifest
	// coverage (which would let WAL truncation outrun that coverage and lose
	// un-materialized transform records).
	//
	// The guard is a plain value CAS, not a term comparison inside the txn:
	// etcd cannot compare fields of a serialized proto. The term pre-check
	// below is a fast-fail (a strictly older publisher is refused without
	// touching the store); the CAS is the authoritative fence under
	// concurrency (a publisher that read a stale value loses the commit).
	checkpointKey := buildConsumeCheckpointKey(pChannelName)
	checkpointValue := ""
	checkpointFirstCreation := false
	var current string
	var snapshotKV kv.TxnKV = c.metaKV
	if snapshot.ConsumeCheckpoint != nil {
		data, err := proto.Marshal(snapshot.ConsumeCheckpoint)
		if err != nil {
			return merr.WrapErrSerializationFailed(err, "marshal consume checkpoint at pchannel %s", pChannelName)
		}
		checkpointValue = string(data)
		current, err = c.metaKV.Load(ctx, checkpointKey)
		if err != nil && !errors.Is(err, merr.ErrIoKeyNotFound) {
			return err
		}
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			if len(removes)+len(vchannelSaves)+len(segmentSaves) != 0 || snapshot.SalvageCheckpoint != nil {
				return merr.WrapErrServiceInternalMsg("initialize consume checkpoint before publishing components of pchannel %s", pChannelName)
			}
			checkpointFirstCreation = true
		} else {
			// Fast-fail on a strictly older publisher before the commit txn.
			currentCP := &streamingpb.WALCheckpoint{}
			if uerr := proto.Unmarshal([]byte(current), currentCP); uerr == nil &&
				currentCP.GetTerm() > snapshot.ConsumeCheckpoint.GetTerm() {
				return merr.WrapErrServiceInternalMsg(
					"consume checkpoint of pchannel %s is fenced: recorded term %d is newer than publisher term %d",
					pChannelName, currentCP.GetTerm(), snapshot.ConsumeCheckpoint.GetTerm(),
				)
			}
			b.CommitSave(checkpointKey, checkpointValue)
		}
		snapshotKV = &recoverySnapshotKV{
			TxnKV: c.metaKV,
			guard: predicates.ValueEqual(checkpointKey, current),
		}
	}
	// A guarded commit is not retried by the kv wrapper, because its predicate
	// cannot be re-sent: a leader change or a timeout can apply the transaction
	// and still report an error, and the guard would then compare against a
	// value this very attempt replaced. The error therefore arrives here, and it
	// does not say whether the write landed -- only a read does. Re-read before
	// deciding: finding our own value there means the commit applied and the
	// error described the reply, not the write.
	commitErr := txn.Commit(ctx, snapshotKV, b)
	// An unchanged checkpoint cannot prove that component writes landed.
	// Keep the dirty snapshot and retry instead of acknowledging an uncertain write.
	if commitErr != nil && (snapshot.ConsumeCheckpoint == nil || checkpointFirstCreation || current == checkpointValue) {
		return commitErr
	}
	if commitErr != nil {
		after, err := c.metaKV.Load(ctx, checkpointKey)
		if err != nil {
			return commitErr
		}
		if after != checkpointValue {
			return commitErr
		}
	}
	// The guard can also fail silently on a store that reports a rejected
	// predicate as a successful commit, so the checkpoint write is verified
	// either way: a stale publisher that lost the CAS must be told the write did
	// not land, or it would keep advancing components against a checkpoint it no
	// longer owns.
	if snapshot.ConsumeCheckpoint != nil {
		if checkpointFirstCreation {
			ok, err := c.metaKV.CompareVersionAndSwap(ctx, checkpointKey, 0, checkpointValue)
			if err != nil {
				return err
			}
			if !ok {
				return merr.WrapErrIoKeyNotFound("consume checkpoint of pchannel %s was created concurrently", pChannelName)
			}
			return nil
		}
		after, err := c.metaKV.Load(ctx, checkpointKey)
		if err != nil {
			return err
		}
		if after != checkpointValue {
			return merr.WrapErrServiceInternalMsg(
				"consume checkpoint of pchannel %s advanced concurrently: CAS on term %d lost",
				pChannelName, snapshot.ConsumeCheckpoint.GetTerm(),
			)
		}
	}
	return nil
}

// recoverySnapshotKV applies the same ownership guard to every transaction,
// including component batches before the final checkpoint commit. Other catalog
// users retain txn.Commit's ordinary chunked behavior.
type recoverySnapshotKV struct {
	kv.TxnKV
	guard predicates.Predicate
}

func (k *recoverySnapshotKV) MultiSave(ctx context.Context, saves map[string]string) error {
	return k.MultiSaveAndRemove(ctx, saves, nil)
}

func (k *recoverySnapshotKV) MultiSaveAndRemove(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	return k.TxnKV.MultiSaveAndRemove(ctx, saves, removals, append(preds, k.guard)...)
}

func (k *recoverySnapshotKV) MultiSaveAndRemoveWithPrefix(ctx context.Context, saves map[string]string, removals []string, preds ...predicates.Predicate) error {
	return k.TxnKV.MultiSaveAndRemoveWithPrefix(ctx, saves, removals, append(preds, k.guard)...)
}
